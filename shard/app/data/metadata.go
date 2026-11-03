package data

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type fileMetadata struct {
	Id      int    `db:"id"`
	Path    string `db:"path"`
	FileUid string `db:"file_uid"`
	BlobUid string `db:"blob_uid"`
}

type MetadataRepo struct {
	db         *sqlx.DB
	addStmt    *sqlx.NamedStmt
	readStmt   *sqlx.Stmt
	updateStmt *sqlx.NamedStmt
	deleteStmt *sqlx.Stmt
}

func newMetadataRepo(db *sqlx.DB) (*MetadataRepo, error) {
	addStmt, err := db.PrepareNamed(`
		INSERT INTO file_metadata (path, file_uid, blob_uid) 
		VALUES (:path, :file_uid, :blob_uid)
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare add: %w", err)
	}

	updateStmt, err := db.PrepareNamed(`
		UPDATE file_metadata 
		SET path = :path, blob_uid = :blob_uid 
		WHERE file_uid = :file_uid
	`)
	if err != nil {
		addStmt.Close()
		return nil, fmt.Errorf("prepare update: %w", err)
	}

	readQuery := db.Rebind(`SELECT id, path, file_uid, blob_uid FROM file_metadata WHERE file_uid = ?`)
	readStmt, err := db.Preparex(readQuery)
	if err != nil {
		addStmt.Close()
		updateStmt.Close()
		return nil, fmt.Errorf("prepare read: %w", err)
	}

	deleteQuery := db.Rebind(`DELETE FROM file_metadata WHERE file_uid = ?`)
	deleteStmt, err := db.Preparex(deleteQuery)
	if err != nil {
		addStmt.Close()
		updateStmt.Close()
		readStmt.Close()
		return nil, fmt.Errorf("prepare delete: %w", err)
	}

	return &MetadataRepo{
		db:         db,
		addStmt:    addStmt,
		readStmt:   readStmt,
		updateStmt: updateStmt,
		deleteStmt: deleteStmt,
	}, nil
}

func (r *MetadataRepo) Close() error {
	_ = r.addStmt.Close()
	_ = r.readStmt.Close()
	_ = r.updateStmt.Close()
	return r.deleteStmt.Close()
}

func (r *MetadataRepo) addMetadata(path string, fileUid string, blobUid string) error {
	meta := fileMetadata{
		Path:    path,
		FileUid: fileUid,
		BlobUid: blobUid,
	}
	_, err := r.addStmt.Exec(meta)
	return err
}

func (r *MetadataRepo) readMetadata(fileUid string) (fileMetadata, error) {
	var meta fileMetadata
	err := r.readStmt.Get(&meta, fileUid)
	return meta, err
}

func (r *MetadataRepo) updateMetadata(path string, fileUid string, blobUid string) error {
	meta := fileMetadata{
		Path:    path,
		FileUid: fileUid,
		BlobUid: blobUid,
	}
	_, err := r.updateStmt.Exec(meta)
	return err
}
func (r *MetadataRepo) deleteMetadata(fileUid string) error {
	_, err := r.deleteStmt.Exec(fileUid)
	return err
}
