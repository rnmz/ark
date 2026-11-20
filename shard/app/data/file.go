package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

type Files struct {
	store *Store
	meta  metadata
}

func NewFiles(store *Store, db *sql.DB) *Files {
	return &Files{
		store: store,
		meta:  metadata{db: db},
	}
}

type UploadParams struct {
	Name        string
	ContentType string
	Ext         string
}

func (f *Files) Upload(ctx context.Context, p UploadParams, r io.Reader) (File, error) {
	cr := &countingReader{r: r}
	id, err := f.store.UploadFile(p.Ext, cr)
	if err != nil {
		return File{}, err
	}

	file := File{
		ID:          id,
		Ext:         p.Ext,
		Name:        p.Name,
		ContentType: p.ContentType,
		Size:        cr.n,
	}
	if err = f.meta.insert(ctx, &file); err != nil {
		if delErr := f.store.DeleteFile(id, p.Ext); delErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback blob %s: %w", id, delErr))
		}
		return File{}, err
	}
	return file, nil
}

func (f *Files) Stat(ctx context.Context, id ID) (File, error) {
	return f.meta.get(ctx, id)
}

func (f *Files) Open(ctx context.Context, id ID) (File, *os.File, error) {
	file, err := f.meta.get(ctx, id)
	if err != nil {
		return File{}, nil, err
	}
	blob, err := f.store.Open(id, file.Ext)
	if err != nil {
		return File{}, nil, err
	}
	return file, blob, nil
}

func (f *Files) Delete(ctx context.Context, id ID) error {
	file, err := f.meta.delete(ctx, id)
	if err != nil {
		return err
	}
	if err = f.store.DeleteFile(id, file.Ext); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete blob %s (metadata deleted): %w", id, err)
	}
	return nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
