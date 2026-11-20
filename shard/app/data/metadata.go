package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Таблица files создаётся миграциями (копия DDL для тестов — в testSchema в files_test.go).
// CHECK-ограничения в схеме дублируют ParseID и validExt на уровне PostgreSQL.

// ErrNotFound возвращается, если метаданных файла нет в базе.
var ErrNotFound = errors.New("file not found")

// File — метаданные файла. Сами байты лежат в Store по (ID, Ext).
type File struct {
	ID          ID
	Ext         string
	Name        string // исходное имя от клиента; в путях на диске не участвует
	ContentType string
	Size        int64
	CreatedAt   time.Time
}

type metadata struct {
	db *sql.DB
}

// insert сохраняет метаданные и записывает в f серверный created_at.
func (m metadata) insert(ctx context.Context, f *File) error {
	if !f.ID.Valid() {
		return ErrInvalidID
	}
	const q = `
		INSERT INTO files (id, ext, name, content_type, size)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING created_at`

	err := m.db.QueryRowContext(ctx, q, f.ID.String(), f.Ext, f.Name, f.ContentType, f.Size).Scan(&f.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert metadata %s: %w", f.ID, err)
	}
	return nil
}

func (m metadata) get(ctx context.Context, id ID) (File, error) {
	if !id.Valid() {
		return File{}, ErrInvalidID
	}
	const q = `SELECT ` + fileColumns + ` FROM files WHERE id = $1`

	f, err := scanFile(m.db.QueryRowContext(ctx, q, id.String()))
	if err != nil {
		return File{}, fmt.Errorf("get metadata %s: %w", id, err)
	}
	return f, nil
}

// delete удаляет строку и возвращает её: из Ext собирается путь к блобу.
func (m metadata) delete(ctx context.Context, id ID) (File, error) {
	if !id.Valid() {
		return File{}, ErrInvalidID
	}
	const q = `DELETE FROM files WHERE id = $1 RETURNING ` + fileColumns

	f, err := scanFile(m.db.QueryRowContext(ctx, q, id.String()))
	if err != nil {
		return File{}, fmt.Errorf("delete metadata %s: %w", id, err)
	}
	return f, nil
}

// fileColumns задаёт список и порядок колонок для SELECT и RETURNING.
// Порядок полей должен строго совпадать с аргументами row.Scan в scanFile.
const fileColumns = `id, ext, name, content_type, size, created_at`

func scanFile(row *sql.Row) (File, error) {
	var (
		rawID string
		f     File
	)
	err := row.Scan(&rawID, &f.Ext, &f.Name, &f.ContentType, &f.Size, &f.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, err
	}

	id, err := ParseID(rawID)
	if err != nil {
		return File{}, fmt.Errorf("parse row id %q: %w", rawID, err)
	}
	f.ID = id
	return f, nil
}
