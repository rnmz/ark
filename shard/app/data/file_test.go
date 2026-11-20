package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
)

const testSchema = `
CREATE TABLE files (
    id           text        PRIMARY KEY CHECK (id ~ '^[0-9A-F]{4}[0-9A-HJKMNP-TV-Z]{8}$'),
    ext          text        NOT NULL CHECK (ext ~ '^(\.[a-z0-9]{1,8})?$'),
    name         text        NOT NULL,
    content_type text        NOT NULL,
    size         bigint      NOT NULL CHECK (size >= 0),
    created_at   timestamptz NOT NULL DEFAULT now()
);`

func openTestFiles(t *testing.T) (*Files, string) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}

	var b [8]byte
	if _, err = rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	schemaName := fmt.Sprintf("test_storage_%x", b)

	cfg.RuntimeParams["search_path"] = schemaName
	db := stdlib.OpenDB(*cfg)

	ctx := context.Background()
	if _, err = db.ExecContext(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		_ = db.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP SCHEMA "+schemaName+" CASCADE")
		_ = db.Close()
	})

	if _, err = db.ExecContext(ctx, testSchema); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	return NewFiles(store, db), dir
}

func TestFiles_ValidationWithoutDB(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
.
	files := NewFiles(store, nil)
	ctx := context.Background()

	_, err = files.Upload(ctx, UploadParams{
		Name:        "archive.tar.gz",
		ContentType: "application/gzip",
		Ext:         "bad_ext_without_dot",
	}, strings.NewReader("payload"))
	if !errors.Is(err, ErrBadExt) {
		t.Fatalf("Upload with bad ext err = %v, want ErrBadExt", err)
	}

	var zeroID ID
	if _, err = files.Stat(ctx, zeroID); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Stat with zero ID err = %v, want ErrInvalidID", err)
	}
	if _, _, err = files.Open(ctx, zeroID); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Open with zero ID err = %v, want ErrInvalidID", err)
	}
	if err = files.Delete(ctx, zeroID); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Delete with zero ID err = %v, want ErrInvalidID", err)
	}
}

func TestFiles_Lifecycle(t *testing.T) {
	files, _ := openTestFiles(t)
	ctx := context.Background()
	payload := []byte("hello idiomatic go")

	uploaded, err := files.Upload(ctx, UploadParams{
		Name:        "greeting.txt",
		ContentType: "text/plain",
		Ext:         ".txt",
	}, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if uploaded.Size != int64(len(payload)) {
		t.Fatalf("Size = %d, want %d", uploaded.Size, len(payload))
	}
	if uploaded.CreatedAt.IsZero() {
		t.Fatal("CreatedAt is zero")
	}

	stat, err := files.Stat(ctx, uploaded.ID)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.ID != uploaded.ID || stat.Name != "greeting.txt" {
		t.Fatalf("Stat returned %+v, want %+v", stat, uploaded)
	}

	meta, blob, err := files.Open(ctx, uploaded.ID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, readErr := io.ReadAll(blob)
	if closeErr := blob.Close(); closeErr != nil {
		t.Fatalf("Close blob: %v", closeErr)
	}
	if readErr != nil {
		t.Fatalf("ReadAll: %v", readErr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("blob content = %q, want %q", got, payload)
	}
	if meta.ID != uploaded.ID {
		t.Fatalf("Open meta.ID = %s, want %s", meta.ID, uploaded.ID)
	}

	if err = files.Delete(ctx, uploaded.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err = files.Stat(ctx, uploaded.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat after Delete err = %v, want ErrNotFound", err)
	}
	if err = files.Delete(ctx, uploaded.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete err = %v, want ErrNotFound", err)
	}
}

func TestFiles_BlobMissing(t *testing.T) {
	files, _ := openTestFiles(t)
	ctx := context.Background()

	up, err := files.Upload(ctx, UploadParams{
		Name:        "a.txt",
		ContentType: "text/plain",
		Ext:         ".txt",
	}, strings.NewReader("x"))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if err = files.store.DeleteFile(up.ID, up.Ext); err != nil {
		t.Fatalf("remove blob: %v", err)
	}

	if _, _, err = files.Open(ctx, up.ID); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open err = %v, want fs.ErrNotExist", err)
	}
	if err = files.Delete(ctx, up.ID); err != nil {
		t.Fatalf("Delete with missing blob: %v", err)
	}
	if _, err = files.Stat(ctx, up.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat after Delete err = %v, want ErrNotFound", err)
	}
}

func TestFiles_UploadRollbackOnInsertError(t *testing.T) {
	files, dir := openTestFiles(t)
	ctx := context.Background()

	_, err := files.Upload(ctx, UploadParams{
		Name:        "bad\x00name.txt",
		ContentType: "text/plain",
		Ext:         ".txt",
	}, strings.NewReader("orphan data"))
	if err == nil {
		t.Fatal("Upload succeeded, want database error")
	}

	var regularFiles int
	err = filepath.WalkDir(dir, func(_ string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			regularFiles++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	if regularFiles != 0 {
		t.Fatalf("found %d leftover files on disk after rollback, want 0", regularFiles)
	}
}

func TestFiles_ConcurrentUpload(t *testing.T) {
	files, _ := openTestFiles(t)
	ctx := context.Background()

	const workers = 16
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for i := range workers {
		wg.Go(func() {
			content := fmt.Sprintf("payload-%d", i)
			wantName := fmt.Sprintf("file-%d.txt", i)

			up, err := files.Upload(ctx, UploadParams{
				Name:        wantName,
				ContentType: "text/plain",
				Ext:         ".txt",
			}, strings.NewReader(content))
			if err != nil {
				errs <- fmt.Errorf("worker %d upload: %w", i, err)
				return
			}

			stat, err := files.Stat(ctx, up.ID)
			if err != nil {
				errs <- fmt.Errorf("worker %d stat %s: %w", i, up.ID, err)
				return
			}
			if stat.Name != wantName {
				errs <- fmt.Errorf("worker %d stat.Name = %q, want %q", i, stat.Name, wantName)
				return
			}

			_, blob, err := files.Open(ctx, up.ID)
			if err != nil {
				errs <- fmt.Errorf("worker %d open %s: %w", i, up.ID, err)
				return
			}
			got, readErr := io.ReadAll(blob)
			closeErr := blob.Close()
			if readErr != nil {
				errs <- fmt.Errorf("worker %d read %s: %w", i, up.ID, readErr)
				return
			}
			if closeErr != nil {
				errs <- fmt.Errorf("worker %d close %s: %w", i, up.ID, closeErr)
				return
			}
			if string(got) != content {
				errs <- fmt.Errorf("worker %d content = %q, want %q", i, got, content)
				return
			}

			if err = files.Delete(ctx, up.ID); err != nil {
				errs <- fmt.Errorf("worker %d delete %s: %w", i, up.ID, err)
				return
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}
