package storage_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"ark/shard/app/data"
)

// ---------- helpers ----------

var errBoom = errors.New("boom")

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errBoom }

func openTestStore(t *testing.T) (*storage.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(dir)
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	require.NoError(t, err)
	return files
}

func noTmpFiles(t *testing.T, dir string) {
	t.Helper()
	for _, f := range listFiles(t, dir) {
		assert.False(t, strings.HasPrefix(filepath.Base(f), ".tmp-"), "leftover temp file %s", f)
	}
}

func mustUpload(t *testing.T, s *storage.Store, ext, content string) storage.ID {
	t.Helper()
	id, err := s.UploadFile(ext, strings.NewReader(content))
	require.NoError(t, err, "UploadFile")
	return id
}

func readFile(t *testing.T, s *storage.Store, id storage.ID, ext string) []byte {
	t.Helper()
	f, err := s.Open(id, ext)
	require.NoError(t, err, "Open(%s%s)", id, ext)
	defer f.Close()
	b, err := io.ReadAll(f)
	require.NoError(t, err, "ReadAll")
	return b
}

func diskPath(dir string, id storage.ID, ext string) string {
	s := id.String()
	return filepath.Join(dir, s[0:2], s[2:4], s[4:]+ext)
}

// ---------- UploadFile ----------

var (
	layoutTxt   = regexp.MustCompile(`^[0-9A-F]{2}/[0-9A-F]{2}/[0-9A-HJKMNP-TV-Z]{8}\.txt$`)
	layoutNoExt = regexp.MustCompile(`^[0-9A-F]{2}/[0-9A-F]{2}/[0-9A-HJKMNP-TV-Z]{8}$`)
)

func TestUploadFile_RoundTripAndLayout(t *testing.T) {
	store, dir := openTestStore(t)

	const payload = "hello go 1.27"
	id := mustUpload(t, store, ".txt", payload)

	require.Equal(t, payload, string(readFile(t, store, id, ".txt")))

	files := listFiles(t, dir)
	require.Len(t, files, 1)
	require.Regexp(t, layoutTxt, files[0])
	assert.Equal(t, id.String()+".txt", strings.ReplaceAll(files[0], "/", ""), "path does not encode ID")
	noTmpFiles(t, dir)
}

func TestUploadFile_EmptyExt(t *testing.T) {
	store, dir := openTestStore(t)

	id := mustUpload(t, store, "", "no extension")
	require.Equal(t, "no extension", string(readFile(t, store, id, "")))

	files := listFiles(t, dir)
	require.Len(t, files, 1)
	require.Regexp(t, layoutNoExt, files[0])
}

func TestUploadFile_EmptyContent(t *testing.T) {
	store, _ := openTestStore(t)

	id := mustUpload(t, store, ".txt", "")
	require.Empty(t, readFile(t, store, id, ".txt"))
}

func TestUploadFile_ExtBoundary(t *testing.T) {
	store, _ := openTestStore(t)

	for _, ext := range []string{".a", ".abcdefgh", ".jpg", ".tar0"} { // 1..8 символов
		t.Run("ok"+ext, func(t *testing.T) {
			id := mustUpload(t, store, ext, "x")
			require.Equal(t, "x", string(readFile(t, store, id, ext)))
		})
	}
}

func TestUploadFile_BadExt(t *testing.T) {
	bad := []string{
		"jpg", ".", ".JPG", ".jp g", ".a/b", ".a\\b", "../evil", "/etc/passwd",
		".a.b", ".abcdefghi", ".é", "..", ".\x00",
	}
	for _, ext := range bad {
		t.Run(fmt.Sprintf("%q", ext), func(t *testing.T) {
			store, dir := openTestStore(t)

			_, err := store.UploadFile(ext, strings.NewReader("x"))
			require.ErrorIs(t, err, storage.ErrBadExt)
			require.Empty(t, listFiles(t, dir), "files created despite bad ext")
		})
	}
}

func TestUploadFile_ReaderError(t *testing.T) {
	store, dir := openTestStore(t)

	_, err := store.UploadFile(".txt", failReader{})
	require.ErrorIs(t, err, errBoom)
	require.Empty(t, listFiles(t, dir), "files left after failed upload")
}

func TestUploadFile_PartialReadLeavesNothing(t *testing.T) {
	store, dir := openTestStore(t)

	r := io.MultiReader(strings.NewReader("partial data"), failReader{})
	_, err := store.UploadFile(".txt", r)
	require.ErrorIs(t, err, errBoom)
	require.Empty(t, listFiles(t, dir), "files left after partial upload")
}

func TestUploadFile_LargePayload(t *testing.T) {
	store, _ := openTestStore(t)

	payload := bytes.Repeat([]byte("0123456789abcdef"), 3<<16) // 3 MiB
	id, err := store.UploadFile(".bin", bytes.NewReader(payload))
	require.NoError(t, err, "UploadFile")

	got := readFile(t, store, id, ".bin")
	require.True(t, bytes.Equal(got, payload), "payload mismatch: got %d bytes, want %d", len(got), len(payload))
}

func TestUploadFile_UniqueIDsSequential(t *testing.T) {
	store, dir := openTestStore(t)

	n := 200
	if testing.Short() {
		n = 20
	}
	seen := make(map[storage.ID]struct{}, n)
	for i := range n {
		id := mustUpload(t, store, ".txt", fmt.Sprintf("payload-%d", i))
		require.NotContains(t, seen, id, "duplicate ID %s", id)
		seen[id] = struct{}{}
	}
	require.Len(t, listFiles(t, dir), n)
	noTmpFiles(t, dir)
}

func TestUploadFile_Concurrent(t *testing.T) {
	store, dir := openTestStore(t)

	const n = 32
	ids := make([]storage.ID, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			ids[i], errs[i] = store.UploadFile(".txt", strings.NewReader(fmt.Sprintf("payload-%d", i)))
		})
	}
	wg.Wait()

	seen := make(map[storage.ID]struct{}, n)
	for i := range n {
		require.NoError(t, errs[i], "upload %d", i)
		require.NotContains(t, seen, ids[i], "duplicate ID %s", ids[i])
		seen[ids[i]] = struct{}{}

		assert.Equal(t, fmt.Sprintf("payload-%d", i), string(readFile(t, store, ids[i], ".txt")), "upload %d", i)
	}
	noTmpFiles(t, dir)
}

func TestUploadFile_AfterClose(t *testing.T) {
	store, _ := openTestStore(t)

	require.NoError(t, store.Close(), "Close")
	_, err := store.UploadFile(".txt", strings.NewReader("x"))
	require.Error(t, err, "UploadFile on closed store must return an error")
}

// ---------- Open ----------

func TestOpen_NotFound(t *testing.T) {
	store, _ := openTestStore(t)

	_, err := store.Open(storage.NewID(), ".txt")
	pe, ok := errors.AsType[*fs.PathError](err)
	require.True(t, ok, "expected *fs.PathError, got %v", err)
	assert.ErrorIs(t, pe.Err, fs.ErrNotExist)
}

func TestOpen_WrongExt(t *testing.T) {
	store, _ := openTestStore(t)

	id := mustUpload(t, store, ".txt", "x")
	_, err := store.Open(id, ".png")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestOpen_InvalidArgs(t *testing.T) {
	store, _ := openTestStore(t)

	_, err := store.Open(storage.ID{}, ".txt")
	assert.ErrorIs(t, err, storage.ErrInvalidID, "zero ID")

	_, err = store.Open(storage.NewID(), "../x")
	assert.ErrorIs(t, err, storage.ErrBadExt, "bad ext")
}

func TestOpen_SymlinkEscapeBlocked(t *testing.T) {
	store, dir := openTestStore(t)

	outside := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o644))

	id := mustUpload(t, store, ".txt", "x")
	p := diskPath(dir, id, ".txt")
	require.NoError(t, os.Remove(p))
	if err := os.Symlink(outside, p); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	f, err := store.Open(id, ".txt")
	if err == nil {
		_ = f.Close()
	}
	require.Error(t, err, "Open must not follow a symlink out of the store root")
}

// ---------- DeleteFile ----------

func TestDeleteFile_RemovesFile(t *testing.T) {
	store, dir := openTestStore(t)

	id := mustUpload(t, store, ".txt", "x")
	require.NoError(t, store.DeleteFile(id, ".txt"), "DeleteFile")

	_, err := os.Stat(diskPath(dir, id, ".txt"))
	assert.ErrorIs(t, err, fs.ErrNotExist, "file still on disk")

	_, err = store.Open(id, ".txt")
	assert.ErrorIs(t, err, fs.ErrNotExist, "Open after delete")
}

func TestDeleteFile_NotFound(t *testing.T) {
	store, _ := openTestStore(t)

	require.ErrorIs(t, store.DeleteFile(storage.NewID(), ".txt"), fs.ErrNotExist)

	id := mustUpload(t, store, ".txt", "x")
	require.NoError(t, store.DeleteFile(id, ".txt"))
	require.ErrorIs(t, store.DeleteFile(id, ".txt"), fs.ErrNotExist, "second delete")
}

func TestDeleteFile_InvalidArgs(t *testing.T) {
	store, _ := openTestStore(t)

	assert.ErrorIs(t, store.DeleteFile(storage.ID{}, ".txt"), storage.ErrInvalidID, "zero ID")
	assert.ErrorIs(t, store.DeleteFile(storage.NewID(), "txt"), storage.ErrBadExt, "bad ext")
}

func TestDeleteFile_WrongExtKeepsFile(t *testing.T) {
	store, _ := openTestStore(t)

	id := mustUpload(t, store, ".txt", "keep me")
	require.ErrorIs(t, store.DeleteFile(id, ".png"), fs.ErrNotExist)
	require.Equal(t, "keep me", string(readFile(t, store, id, ".txt")), "file damaged")
}

func TestDeleteFile_OtherFilesUntouched(t *testing.T) {
	store, _ := openTestStore(t)

	a := mustUpload(t, store, ".txt", "a")
	b := mustUpload(t, store, ".txt", "b")
	require.NoError(t, store.DeleteFile(a, ".txt"))
	require.Equal(t, "b", string(readFile(t, store, b, ".txt")), "other file damaged")
}

func TestDeleteFile_KeepsShardDir(t *testing.T) {
	store, dir := openTestStore(t)

	id := mustUpload(t, store, ".txt", "x")
	require.NoError(t, store.DeleteFile(id, ".txt"))

	fi, err := os.Stat(filepath.Dir(diskPath(dir, id, ".txt")))
	require.NoError(t, err, "shard dir must remain")
	assert.True(t, fi.IsDir())
}

func TestDeleteFile_SymlinkNotFollowed(t *testing.T) {
	store, dir := openTestStore(t)

	outside := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o644))

	id := mustUpload(t, store, ".txt", "x")
	p := diskPath(dir, id, ".txt")
	require.NoError(t, os.Remove(p))
	if err := os.Symlink(outside, p); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	require.NoError(t, store.DeleteFile(id, ".txt"), "DeleteFile")

	_, err := os.Lstat(p)
	assert.ErrorIs(t, err, fs.ErrNotExist, "symlink must be removed")

	got, err := os.ReadFile(outside)
	require.NoError(t, err, "file outside the root was touched")
	assert.Equal(t, "secret", string(got))
}

// ---------- CleanTmp ----------

func touchOld(t *testing.T, path string) {
	t.Helper()
	past := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(path, past, past))
}

func TestCleanTmp_RemovesOldKeepsFresh(t *testing.T) {
	store, dir := openTestStore(t)

	oldPath := filepath.Join(dir, ".tmp-OLD")
	freshPath := filepath.Join(dir, ".tmp-FRESH")
	for _, p := range []string{oldPath, freshPath} {
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	}
	touchOld(t, oldPath)

	require.NoError(t, store.CleanTmp(time.Hour), "CleanTmp")

	_, err := os.Stat(oldPath)
	assert.ErrorIs(t, err, fs.ErrNotExist, "old temp must be removed")
	_, err = os.Stat(freshPath)
	assert.NoError(t, err, "fresh temp must stay")
}

func TestCleanTmp_RemovesAllOld(t *testing.T) {
	store, dir := openTestStore(t)

	for i := range 3 {
		p := filepath.Join(dir, fmt.Sprintf(".tmp-OLD%d", i))
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
		touchOld(t, p)
	}
	require.NoError(t, store.CleanTmp(time.Hour), "CleanTmp")
	require.Empty(t, listFiles(t, dir), "orphans left")
}

func TestCleanTmp_IgnoresNonTmp(t *testing.T) {
	store, dir := openTestStore(t)

	id := mustUpload(t, store, ".txt", "published")
	touchOld(t, diskPath(dir, id, ".txt"))

	keep := filepath.Join(dir, "keep.txt")
	require.NoError(t, os.WriteFile(keep, []byte("x"), 0o644))
	touchOld(t, keep)

	tmpDir := filepath.Join(dir, ".tmp-DIR")
	require.NoError(t, os.Mkdir(tmpDir, 0o755))
	touchOld(t, tmpDir)

	require.NoError(t, store.CleanTmp(time.Hour), "CleanTmp")

	assert.Equal(t, "published", string(readFile(t, store, id, ".txt")), "published file damaged")

	_, err := os.Stat(keep)
	assert.NoError(t, err, "non-tmp file removed")

	fi, err := os.Stat(tmpDir)
	if assert.NoError(t, err, "tmp-prefixed directory removed") {
		assert.True(t, fi.IsDir())
	}
}

func TestCleanTmp_EmptyStore(t *testing.T) {
	store, _ := openTestStore(t)

	require.NoError(t, store.CleanTmp(time.Hour), "CleanTmp on empty store")
}

// ---------- Open (store) ----------

func TestOpenStore_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b", "store")
	store, err := storage.Open(dir)
	require.NoError(t, err, "Open(%q)", dir)
	defer store.Close()

	fi, err := os.Stat(dir)
	require.NoError(t, err, "store dir not created")
	assert.True(t, fi.IsDir())
}

func TestOpenStore_PathIsFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))

	store, err := storage.Open(p)
	if err == nil {
		_ = store.Close()
	}
	require.Error(t, err, "Open on a regular file must fail")
}
