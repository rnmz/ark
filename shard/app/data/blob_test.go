package data

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const generatedUIDPattern = "^[0-9a-f]{16}$"

// ---------- helpers ----------

// newStore returns a store backed by a fresh temporary directory.
func newStore(t *testing.T) BlobStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "blobs")
	require.NoError(t, os.MkdirAll(dir, 0755))
	return BlobStore{BlobPath: dir}
}

// payload returns deterministic test data of the given size.
func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// recordSize calculates the exact on-disk byte size of a single record.
func recordSize(fileUID string, data []byte) int64 {
	return int64(2 + len(fileUID) + 8 + len(data))
}

// buildRawRecord manually encodes a record for corruption and boundary tests.
func buildRawRecord(uid string, dataLen uint64, rawPayload []byte) []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, uint16(len(uid)))
	_, _ = buf.WriteString(uid)
	_ = binary.Write(buf, binary.LittleEndian, dataLen)
	_, _ = buf.Write(rawPayload)
	return buf.Bytes()
}

// encodeRecord encodes a well-formed record (header length matches payload).
func encodeRecord(uid string, data []byte) []byte {
	return buildRawRecord(uid, uint64(len(data)), data)
}

// isErr reports whether err contains an error of type T (works for wrapped errors too).
func isErr[T error](err error) bool {
	var target T
	return errors.As(err, &target)
}

// asErr requires err to contain an error of type T and returns it.
func asErr[T error](t *testing.T, err error) T {
	t.Helper()
	var target T
	require.ErrorAs(t, err, &target, "unexpected error type %T: %v", err, err)
	return target
}

// requireSameBytes compares byte slices by content (a nil and an empty slice are equal).
func requireSameBytes(t *testing.T, want, got []byte, msg string) {
	t.Helper()
	require.True(t, bytes.Equal(want, got), msg)
}

// invalidUIDs are non-empty UIDs that must be rejected across all operations.
var invalidUIDs = []string{
	"../etc/passwd",
	"..",
	".",
	".hidden",
	"a/b",
	`a\b`,
	"a.b",
	"blob.blob",
	"/abs/path",
	"with space",
	" leading",
	"trailing ",
	"tab\tchar",
	"new\nline",
	"carriage\rreturn",
	"null\x00byte",
	"ünïcödé",
	"日本語",
	"a;b",
	"a'b",
	"a\"b",
	"a$b",
	strings.Repeat("a", 65),
	strings.Repeat("a", 1024),
}

func assertInvalidUID(t *testing.T, err error, uid string) {
	t.Helper()
	e := asErr[*ErrInvalidUID](t, err)
	assert.Equal(t, uid, e.UID)
}

func assertBlobNotFound(t *testing.T, err error, blobUID string) {
	t.Helper()
	e := asErr[*ErrBlobNotFound](t, err)
	assert.Equal(t, blobUID, e.BlobUID)
}

func assertFileNotFound(t *testing.T, err error, blobUID, fileUID string) {
	t.Helper()
	e := asErr[*ErrFileNotFound](t, err)
	assert.Equal(t, blobUID, e.BlobUID)
	assert.Equal(t, fileUID, e.FileUID)
}

func assertEmptyBlobPath(t *testing.T, err error) {
	t.Helper()
	asErr[*ErrEmptyBlobPath](t, err)
}

func assertFileNotFoundAfterDelete(t *testing.T, store BlobStore, blobUID, fileUID string) {
	t.Helper()
	_, err := store.getFileFromBlob(blobUID, fileUID)
	assertFileNotFound(t, err, blobUID, fileUID)
}

// assertEmptyBlobExists checks that the blob file still exists and has no records.
func assertEmptyBlobExists(t *testing.T, store BlobStore, blobUID string) {
	t.Helper()
	info, err := os.Stat(store.blobPath(blobUID))
	require.NoError(t, err, "the blob itself must not be deleted")
	assert.Equal(t, int64(0), info.Size())
	assert.NoFileExists(t, store.blobPath(blobUID)+".tmp")
}

func readRaw(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// assertOnlyBlobFiles checks that the store directory contains nothing but *.blob files
// (no leftover .tmp files, no strays).
func assertOnlyBlobFiles(t *testing.T, store BlobStore) {
	t.Helper()
	entries, err := os.ReadDir(store.BlobPath)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, e.IsDir(), "unexpected directory %s", e.Name())
		assert.True(t, strings.HasSuffix(e.Name(), ".blob"), "unexpected file %s", e.Name())
	}
}

// waitOrFail fails the test (instead of hanging forever) if wg does not finish in time.
func waitOrFail(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timeout: possible deadlock")
	}
}

// preallocValidBlob creates a sparse blob of exactly size bytes that is still a valid record
// stream: a sequence of "pad" records (each not larger than maxFileSize) whose payloads
// are holes in the file.
func preallocValidBlob(t *testing.T, store BlobStore, blobUID string, size int) {
	t.Helper()

	const hdr = 2 + len("pad") + 8
	require.Greater(t, size, hdr)

	f, err := os.Create(store.blobPath(blobUID))
	require.NoError(t, err)

	remaining := size
	for remaining > 0 {
		payloadLen := min(remaining-hdr, maxFileSize)
		leftover := remaining - hdr - payloadLen
		if leftover > 0 && leftover < hdr {
			// The rest would be too small to hold another record header: shift bytes over.
			payloadLen -= hdr - leftover
			leftover = hdr
		}
		require.GreaterOrEqual(t, payloadLen, 0)

		_, err = f.Write(buildRawRecord("pad", uint64(payloadLen), nil))
		require.NoError(t, err)
		_, err = f.Seek(int64(payloadLen), io.SeekCurrent)
		require.NoError(t, err)

		remaining = leftover
	}

	require.NoError(t, f.Truncate(int64(size)))
	require.NoError(t, f.Close())
}

// writeSparseRecord writes one structurally valid record whose payload is a hole in the file.
func writeSparseRecord(t *testing.T, path, uid string, payloadLen int) {
	t.Helper()

	header := buildRawRecord(uid, uint64(payloadLen), nil)
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write(header)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(int64(len(header)+payloadLen)))
	require.NoError(t, f.Close())
}

// rawRecord is a reference (test-side) view of one record.
type rawRecord struct {
	uid  string
	data []byte
}

// parseRaw is an independent reference parser. It returns all fully valid records
// before the first malformation and clean=true only if the whole input is well formed.
func parseRaw(raw []byte) (recs []rawRecord, clean bool) {
	pos := 0
	for pos < len(raw) {
		if len(raw)-pos < 2 {
			return recs, false
		}
		uidLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if len(raw)-pos < uidLen {
			return recs, false
		}
		uid := string(raw[pos : pos+uidLen])
		pos += uidLen
		if len(raw)-pos < 8 {
			return recs, false
		}
		dataLen := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8
		if dataLen > uint64(len(raw)-pos) {
			return recs, false
		}
		data := raw[pos : pos+int(dataLen)]
		pos += int(dataLen)
		recs = append(recs, rawRecord{uid: uid, data: data})
	}
	return recs, true
}

// ---------- error types ----------

func classify(err error) string {
	switch {
	case isErr[*ErrInvalidUID](err):
		return "InvalidUID"
	case isErr[*ErrNotEnoughSpace](err):
		return "NotEnoughSpace"
	case isErr[*ErrTruncatedRecord](err):
		return "TruncatedRecord"
	case isErr[*ErrFileTooLarge](err):
		return "FileTooLarge"
	case isErr[*ErrFileNotFound](err):
		return "FileNotFound"
	case isErr[*ErrBlobNotFound](err):
		return "BlobNotFound"
	case isErr[*ErrEmptyBlobPath](err):
		return "EmptyBlobPath"
	case isErr[*ErrNotDurable](err):
		return "NotDurable"
	}
	return "other"
}

func TestErrorTypes_Messages(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&ErrInvalidUID{UID: "bad"}, "invalid uid: bad"},
		{&ErrNotEnoughSpace{Needed: 10, Used: 5}, "not enough space: 10 needed, 5 used"},
		{
			&ErrTruncatedRecord{Offset: 17, FileSize: 20, Needed: 6, Available: 3},
			"truncated blob record at offset 17 (file size 20): header requires 6 bytes, only 3 available",
		},
		{&ErrFileTooLarge{Size: 2, Limit: 1}, "file size 2 exceeds maximum allowed size 1"},
		{&ErrFileNotFound{BlobUID: "b", FileUID: "f"}, `file "f" not found in blob "b"`},
		{&ErrBlobNotFound{BlobUID: "b"}, `blob "b" not found`},
		{&ErrEmptyBlobPath{}, "blob path is empty"},
		{
			&ErrNotDurable{Op: "delete", Err: errors.New("disk")},
			"delete was applied but could not be made durable: disk",
		},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.err.Error())
	}
}

func TestErrorTypes_DistinctAndWrappable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"InvalidUID", &ErrInvalidUID{UID: "x"}},
		{"NotEnoughSpace", &ErrNotEnoughSpace{Needed: 1, Used: 2}},
		{"TruncatedRecord", &ErrTruncatedRecord{}},
		{"FileTooLarge", &ErrFileTooLarge{Size: 2, Limit: 1}},
		{"FileNotFound", &ErrFileNotFound{BlobUID: "b", FileUID: "f"}},
		{"BlobNotFound", &ErrBlobNotFound{BlobUID: "b"}},
		{"EmptyBlobPath", &ErrEmptyBlobPath{}},
		{"NotDurable", &ErrNotDurable{Op: "delete", Err: errors.New("disk")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.name, classify(tc.err), "direct")
			assert.Equal(t, tc.name, classify(fmt.Errorf("ctx: %w", tc.err)), "wrapped once")
			assert.Equal(t, tc.name, classify(fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", tc.err))), "wrapped twice")

			// Only the truncated record error is an io.ErrUnexpectedEOF.
			assert.Equal(t, tc.name == "TruncatedRecord", errors.Is(tc.err, io.ErrUnexpectedEOF))
		})
	}
	assert.Equal(t, "other", classify(errors.New("something else")))
	assert.Equal(t, "other", classify(nil))
}

func TestErrNotDurable_UnwrapsCause(t *testing.T) {
	err := &ErrNotDurable{Op: "delete", Err: os.ErrPermission}
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.NotErrorIs(t, err, os.ErrNotExist)
}

// Guard: errors.Is on these pointer types compares identity, so callers MUST use errors.As.
func TestErrorTypes_ErrorsIsDoesNotMatchByValue(t *testing.T) {
	a := &ErrBlobNotFound{BlobUID: "x"}
	b := &ErrBlobNotFound{BlobUID: "x"}
	assert.False(t, errors.Is(a, b))
	assert.True(t, errors.Is(a, a))
}

// ---------- validUID / generateShortUID ----------

func TestValidUID(t *testing.T) {
	valid := []string{
		"a",
		"0",
		"_",
		"-",
		"abc123",
		"A_b-C_9",
		strings.Repeat("a", 64),
		strings.Repeat("9_-Z", 16), // 64 chars
	}
	for _, uid := range valid {
		assert.True(t, validUID(uid), "should be valid: %q", uid)
	}

	for _, uid := range append([]string{""}, invalidUIDs...) {
		assert.False(t, validUID(uid), "should be invalid: %q", uid)
	}
}

func TestGenerateShortUID(t *testing.T) {
	const count = 5000
	seen := make(map[string]struct{}, count)
	for i := 0; i < count; i++ {
		uid, err := generateShortUID()
		require.NoError(t, err)
		assert.Regexp(t, generatedUIDPattern, uid)
		assert.True(t, validUID(uid))
		seen[uid] = struct{}{}
	}
	assert.Len(t, seen, count, "generated UIDs must not repeat")
}

// ---------- locking & directory sync ----------

func TestGetBlobLock_DeterministicPerPath(t *testing.T) {
	store := BlobStore{BlobPath: "/some/path"}
	assert.Same(t, store.getBlobLock("abc"), store.getBlobLock("abc"))

	other := BlobStore{BlobPath: "/other/path"}
	assert.Same(t, other.getBlobLock("abc"), other.getBlobLock("abc"))
}

// Different spellings of the same directory must map to the same lock.
func TestGetBlobLock_AliasesOfOneDirectoryShareLock(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "blobs")
	require.NoError(t, os.MkdirAll(dir, 0755))
	sep := string(os.PathSeparator)

	canonical := BlobStore{BlobPath: dir}
	want := canonical.getBlobLock("x")

	t.Run("dotdot", func(t *testing.T) {
		s := BlobStore{BlobPath: dir + sep + ".." + sep + "blobs"}
		assert.Same(t, want, s.getBlobLock("x"))
	})

	t.Run("trailing_separator", func(t *testing.T) {
		s := BlobStore{BlobPath: dir + sep}
		assert.Same(t, want, s.getBlobLock("x"))
	})

	t.Run("relative_path", func(t *testing.T) {
		wd, err := os.Getwd()
		require.NoError(t, err)
		rel, err := filepath.Rel(wd, dir)
		if err != nil {
			t.Skip("no relative path from the working directory to the temp directory")
		}
		s := BlobStore{BlobPath: rel}
		assert.Same(t, want, s.getBlobLock("x"))
	})

	t.Run("symlink", func(t *testing.T) {
		link := filepath.Join(root, "link")
		if err := os.Symlink(dir, link); err != nil {
			t.Skipf("symlinks are not available: %v", err)
		}
		s := BlobStore{BlobPath: link}
		assert.Same(t, want, s.getBlobLock("x"))
	})
}

// Two stores that point to the same directory through different strings must not corrupt the blob.
func TestAliasedStoresShareLock_ConcurrentAppends(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "blobs")
	require.NoError(t, os.MkdirAll(dir, 0755))
	sep := string(os.PathSeparator)

	stores := []BlobStore{
		{BlobPath: dir},
		{BlobPath: dir + sep + ".." + sep + "blobs"},
		{BlobPath: dir + sep},
	}

	const n = 60
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := stores[i%len(stores)].createBlob("blob", fmt.Sprintf("f-%d", i), payload(100+i))
			errs <- err
		}(i)
	}
	waitOrFail(t, &wg, 30*time.Second)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	recs, clean := parseRaw(readRaw(t, filepath.Join(dir, "blob.blob")))
	assert.True(t, clean, "aliased stores produced a malformed blob")
	assert.Len(t, recs, n)
}

func TestSyncDir(t *testing.T) {
	require.NoError(t, syncDir(t.TempDir()))
	assert.Error(t, syncDir(filepath.Join(t.TempDir(), "missing")))
}

// ---------- empty BlobPath ----------

func TestEmptyBlobPath(t *testing.T) {
	store := BlobStore{}

	_, err := store.getBlobSize("abc")
	assertEmptyBlobPath(t, err)

	_, _, err = store.createBlob("", "", []byte("x"))
	assertEmptyBlobPath(t, err)

	_, _, err = store.createBlob("abc", "def", []byte("x"))
	assertEmptyBlobPath(t, err)

	_, err = store.getFileFromBlob("abc", "f")
	assertEmptyBlobPath(t, err)

	err = store.deleteFileFromBlob("abc", "f")
	assertEmptyBlobPath(t, err)
}

// The empty path check must win over UID validation in every operation.
func TestEmptyBlobPath_TakesPrecedenceOverInvalidUID(t *testing.T) {
	store := BlobStore{}

	_, err := store.getBlobSize("../bad")
	assertEmptyBlobPath(t, err)

	_, _, err = store.createBlob("../bad", "../bad", []byte("x"))
	assertEmptyBlobPath(t, err)

	_, err = store.getFileFromBlob("../bad", "../bad")
	assertEmptyBlobPath(t, err)

	err = store.deleteFileFromBlob("../bad", "../bad")
	assertEmptyBlobPath(t, err)
}

// ---------- createBlob ----------

func TestCreateBlob_UIDGenerationCombinations(t *testing.T) {
	store := newStore(t)

	// 1. Both blobUID and fileUID generated.
	b1, f1, err := store.createBlob("", "", []byte("both-generated"))
	require.NoError(t, err)
	assert.Regexp(t, generatedUIDPattern, b1)
	assert.Regexp(t, generatedUIDPattern, f1)

	// 2. Explicit blobUID, generated fileUID.
	b2, f2, err := store.createBlob("custom-blob", "", []byte("file-generated"))
	require.NoError(t, err)
	assert.Equal(t, "custom-blob", b2)
	assert.Regexp(t, generatedUIDPattern, f2)

	// 3. Generated blobUID, explicit fileUID.
	b3, f3, err := store.createBlob("", "custom-file", []byte("blob-generated"))
	require.NoError(t, err)
	assert.Regexp(t, generatedUIDPattern, b3)
	assert.Equal(t, "custom-file", f3)

	got1, err := store.getFileFromBlob(b1, f1)
	require.NoError(t, err)
	assert.Equal(t, []byte("both-generated"), got1)

	got2, err := store.getFileFromBlob(b2, f2)
	require.NoError(t, err)
	assert.Equal(t, []byte("file-generated"), got2)

	got3, err := store.getFileFromBlob(b3, f3)
	require.NoError(t, err)
	assert.Equal(t, []byte("blob-generated"), got3)

	assertOnlyBlobFiles(t, store)
}

func TestCreateBlob_BoundaryLengthUIDs_EndToEnd(t *testing.T) {
	store := newStore(t)

	minBlobUID := "b"
	minFileUID := "f"
	maxBlobUID := strings.Repeat("B", 64)
	maxFileUID := strings.Repeat("F", 64)

	_, _, err := store.createBlob(minBlobUID, minFileUID, []byte("min"))
	require.NoError(t, err)

	_, _, err = store.createBlob(maxBlobUID, maxFileUID, []byte("max"))
	require.NoError(t, err)

	gotMin, err := store.getFileFromBlob(minBlobUID, minFileUID)
	require.NoError(t, err)
	assert.Equal(t, []byte("min"), gotMin)

	gotMax, err := store.getFileFromBlob(maxBlobUID, maxFileUID)
	require.NoError(t, err)
	assert.Equal(t, []byte("max"), gotMax)

	require.NoError(t, store.deleteFileFromBlob(minBlobUID, minFileUID))
	require.NoError(t, store.deleteFileFromBlob(maxBlobUID, maxFileUID))
}

func TestCreateBlob_DifferentBlobsForGeneratedUIDs(t *testing.T) {
	store := newStore(t)

	blob1, _, err := store.createBlob("", "", []byte("one"))
	require.NoError(t, err)
	blob2, _, err := store.createBlob("", "", []byte("two"))
	require.NoError(t, err)

	assert.NotEqual(t, blob1, blob2)
}

func TestCreateBlob_CreatesMissingDirectory(t *testing.T) {
	store := BlobStore{BlobPath: filepath.Join(t.TempDir(), "a", "b", "c")}

	blobUID, fileUID, err := store.createBlob("", "", []byte("hello"))
	require.NoError(t, err)

	got, err := store.getFileFromBlob(blobUID, fileUID)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
}

func TestCreateBlob_AppendsToGivenBlob(t *testing.T) {
	store := newStore(t)

	blobUID, fileUID1, err := store.createBlob("my-blob", "f1", []byte("first"))
	require.NoError(t, err)
	assert.Equal(t, "my-blob", blobUID)
	assert.Equal(t, "f1", fileUID1)

	blobUID, fileUID2, err := store.createBlob("my-blob", "f2", []byte("second"))
	require.NoError(t, err)
	assert.Equal(t, "my-blob", blobUID)
	assert.Equal(t, "f2", fileUID2)

	got1, err := store.getFileFromBlob("my-blob", "f1")
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), got1)

	got2, err := store.getFileFromBlob("my-blob", "f2")
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), got2)
}

func TestCreateBlob_NilAndEmptyData(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob", "nil-data", nil)
	require.NoError(t, err)

	_, _, err = store.createBlob("blob", "empty-slice", []byte{})
	require.NoError(t, err)

	_, _, err = store.createBlob("blob", "non-empty", []byte("record"))
	require.NoError(t, err)

	gotNil, err := store.getFileFromBlob("blob", "nil-data")
	require.NoError(t, err)
	assert.Empty(t, gotNil)

	gotEmpty, err := store.getFileFromBlob("blob", "empty-slice")
	require.NoError(t, err)
	assert.Empty(t, gotEmpty)

	gotNonEmpty, err := store.getFileFromBlob("blob", "non-empty")
	require.NoError(t, err)
	assert.Equal(t, []byte("record"), gotNonEmpty)
}

func TestCreateBlob_BinaryAndUTF8Integrity(t *testing.T) {
	store := newStore(t)

	// All 256 possible byte values, simulating binary headers (JPEG/PDF) and null bytes.
	allBytes := make([]byte, 256*4)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}

	utf8Text := []byte("Report №42: name — Alex, size — 5 MB. ☀️🎨 日本語 ünïcödé")

	_, _, err := store.createBlob("media", "bin-raw", allBytes)
	require.NoError(t, err)
	_, _, err = store.createBlob("media", "text-utf8", utf8Text)
	require.NoError(t, err)

	gotBin, err := store.getFileFromBlob("media", "bin-raw")
	require.NoError(t, err)
	assert.Equal(t, allBytes, gotBin)

	gotText, err := store.getFileFromBlob("media", "text-utf8")
	require.NoError(t, err)
	assert.Equal(t, utf8Text, gotText)
}

func TestCreateBlob_SliceMutationDoesNotAffectStoredData(t *testing.T) {
	store := newStore(t)

	input := []byte("original-data")
	_, _, err := store.createBlob("blob", "f1", input)
	require.NoError(t, err)

	// Mutate caller's input slice after createBlob.
	input[0] = 'X'

	got1, err := store.getFileFromBlob("blob", "f1")
	require.NoError(t, err)
	assert.Equal(t, []byte("original-data"), got1)

	// Mutate returned slice and re-read from store.
	got1[0] = 'Z'

	got2, err := store.getFileFromBlob("blob", "f1")
	require.NoError(t, err)
	assert.Equal(t, []byte("original-data"), got2)
}

func TestCreateBlob_LargeData(t *testing.T) {
	store := newStore(t)
	data := payload(5 * 1024 * 1024) // 5 MB

	blobUID, fileUID, err := store.createBlob("", "", data)
	require.NoError(t, err)

	got, err := store.getFileFromBlob(blobUID, fileUID)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got))
}

func TestCreateBlob_InvalidBlobUID(t *testing.T) {
	for i, uid := range invalidUIDs {
		t.Run(fmt.Sprintf("%d_%q", i, uid), func(t *testing.T) {
			store := newStore(t)

			_, _, err := store.createBlob(uid, "valid-file", []byte("x"))
			assertInvalidUID(t, err, uid)

			entries, err := os.ReadDir(store.BlobPath)
			require.NoError(t, err)
			assert.Empty(t, entries, "nothing must be written when validation fails")
		})
	}
}

func TestCreateBlob_InvalidFileUID(t *testing.T) {
	for i, uid := range invalidUIDs {
		t.Run(fmt.Sprintf("%d_%q", i, uid), func(t *testing.T) {
			store := newStore(t)

			// Explicit blob UID.
			_, _, err := store.createBlob("blob", uid, []byte("x"))
			assertInvalidUID(t, err, uid)
			assert.NoFileExists(t, store.blobPath("blob"))

			// Generated blob UID.
			_, _, err = store.createBlob("", uid, []byte("x"))
			assertInvalidUID(t, err, uid)

			entries, err := os.ReadDir(store.BlobPath)
			require.NoError(t, err)
			assert.Empty(t, entries, "nothing must be written when validation fails")
		})
	}
}

func TestCreateBlob_PathTraversalDoesNotEscape(t *testing.T) {
	root := t.TempDir()
	store := BlobStore{BlobPath: filepath.Join(root, "blobs")}

	for _, traversal := range []string{"../evil", "..", "/tmp/evil", `..\evil`} {
		_, _, err := store.createBlob(traversal, "f", []byte("x"))
		assertInvalidUID(t, err, traversal)

		_, _, err = store.createBlob("b", traversal, []byte("x"))
		assertInvalidUID(t, err, traversal)

		_, err = store.getFileFromBlob(traversal, "f")
		assertInvalidUID(t, err, traversal)

		err = store.deleteFileFromBlob(traversal, "f")
		assertInvalidUID(t, err, traversal)

		_, err = store.getBlobSize(traversal)
		assertInvalidUID(t, err, traversal)
	}

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	for _, e := range entries {
		assert.Equal(t, "blobs", e.Name(), "unexpected file created outside BlobPath: %s", e.Name())
	}
}

// Documents current behaviour: the same file UID can be appended twice to one blob.
// Get returns the first record, delete removes all records with that UID
// (the blob itself stays, empty).
func TestCreateBlob_DuplicateFileUID(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob", "dup", []byte("one"))
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "dup", []byte("two"))
	require.NoError(t, err)

	got, err := store.getFileFromBlob("blob", "dup")
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), got)

	require.NoError(t, store.deleteFileFromBlob("blob", "dup"))
	assertEmptyBlobExists(t, store, "blob")
	assertFileNotFoundAfterDelete(t, store, "blob", "dup")
}

// A damaged blob (for example a torn write left by a crash) must never be appended to:
// the new record would sit behind the damage and be unreachable.
func TestCreateBlob_RefusesToAppendToDamagedBlob(t *testing.T) {
	good := encodeRecord("good", []byte("good-data"))
	withTail := func(tail ...byte) []byte {
		return append(append([]byte{}, good...), tail...)
	}

	cases := []struct {
		name    string
		raw     []byte
		hasGood bool
	}{
		{"torn_uid_len", withTail(0x05), true},
		{"torn_uid", withTail(0x05, 0x00, 'a', 'b'), true},
		{"torn_data_len", withTail(0x01, 0x00, 'x', 1, 2, 3), true},
		{"torn_payload", append(append([]byte{}, good...), buildRawRecord("torn", 10, []byte("123"))...), true},
		{"huge_data_len", append(append([]byte{}, good...), buildRawRecord("huge", math.MaxUint64, nil)...), true},
		{"garbage_only", []byte{0xDE, 0xAD, 0xBE}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			blobPath := store.blobPath("blob")
			require.NoError(t, os.WriteFile(blobPath, tc.raw, 0644))

			_, _, err := store.createBlob("blob", "new", []byte("new-data"))
			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
			assert.False(t, isErr[*ErrFileNotFound](err))

			err = store.appendRecord("blob", "new", []byte("new-data"), false)
			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

			assert.Equal(t, tc.raw, readRaw(t, blobPath), "a refused append must not change the blob")
			assertOnlyBlobFiles(t, store)

			if tc.hasGood {
				got, err := store.getFileFromBlob("blob", "good")
				require.NoError(t, err)
				assert.Equal(t, []byte("good-data"), got, "records before the damage stay readable")
			}
		})
	}
}

// A record larger than maxFileSize makes readers fail while scanning past it,
// so appending behind it would create unreachable records. The append must be refused.
func TestCreateBlob_RefusesToAppendBehindOversizedRecord(t *testing.T) {
	store := newStore(t)
	path := store.blobPath("blob")
	writeSparseRecord(t, path, "big", maxFileSize+1)

	before, err := os.Stat(path)
	require.NoError(t, err)

	_, _, err = store.createBlob("blob", "new", []byte("x"))
	tooLarge := asErr[*ErrFileTooLarge](t, err)
	assert.Equal(t, int64(maxFileSize+1), tooLarge.Size)
	assert.Equal(t, int64(maxFileSize), tooLarge.Limit)
	assert.Contains(t, err.Error(), "refusing to append")

	after, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, before.Size(), after.Size())
	assertOnlyBlobFiles(t, store)
}

// ---------- file size limits ----------

func TestCreateBlob_ExactlyMaxFileSizeIsAccepted(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~64 MiB")
	}
	store := newStore(t)
	data := payload(maxFileSize)

	_, _, err := store.createBlob("blob", "max", data)
	require.NoError(t, err)

	got, err := store.getFileFromBlob("blob", "max")
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "max-size file must round-trip bit-for-bit")
}

func TestCreateBlob_OverMaxFileSizeIsRejectedAndWritesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~32 MiB")
	}
	store := newStore(t)
	data := make([]byte, maxFileSize+1)

	// Generated blob UID: no file may appear in the directory.
	_, _, err := store.createBlob("", "", data)
	tooLarge := asErr[*ErrFileTooLarge](t, err)
	assert.Equal(t, int64(maxFileSize+1), tooLarge.Size)
	assert.Equal(t, int64(maxFileSize), tooLarge.Limit)
	entries, err := os.ReadDir(store.BlobPath)
	require.NoError(t, err)
	assert.Empty(t, entries)

	// Existing blob: must stay byte-for-byte unchanged.
	_, _, err = store.createBlob("blob", "small", []byte("small"))
	require.NoError(t, err)
	before := readRaw(t, store.blobPath("blob"))

	_, _, err = store.createBlob("blob", "big", data)
	asErr[*ErrFileTooLarge](t, err)
	assert.Equal(t, before, readRaw(t, store.blobPath("blob")))
	assertOnlyBlobFiles(t, store)
}

// A record whose header claims more than maxFileSize (and whose payload really is that large)
// must be refused on read.
func TestGetFileFromBlob_RecordLargerThanMaxFileSize(t *testing.T) {
	store := newStore(t)
	writeSparseRecord(t, store.blobPath("big"), "big", maxFileSize+1)

	for _, target := range []string{"big", "other"} {
		_, err := store.getFileFromBlob("big", target)
		tooLarge := asErr[*ErrFileTooLarge](t, err)
		assert.Equal(t, int64(maxFileSize+1), tooLarge.Size)
		assert.Equal(t, int64(maxFileSize), tooLarge.Limit)
	}
}

// ---------- blob capacity ----------

func TestCreateBlob_CapacityBoundary(t *testing.T) {
	data := payload(1000)
	rec := recordSize("f", data)

	t.Run("exactly_fits", func(t *testing.T) {
		store := newStore(t)
		preallocValidBlob(t, store, "blob", int(maxBlobSize-rec))

		_, _, err := store.createBlob("blob", "f", data)
		require.NoError(t, err)

		size, err := store.getBlobSize("blob")
		require.NoError(t, err)
		assert.Equal(t, int64(maxBlobSize), size)
		assert.NoError(t, validateBlobFile(store.blobPath("blob")), "blob must stay a valid record stream")
	})

	t.Run("one_byte_too_many", func(t *testing.T) {
		store := newStore(t)
		used := maxBlobSize - rec + 1
		preallocValidBlob(t, store, "blob", int(used))

		_, _, err := store.createBlob("blob", "f", data)
		space := asErr[*ErrNotEnoughSpace](t, err)
		assert.Equal(t, rec, space.Needed)
		assert.Equal(t, used, space.Used)
		assert.False(t, isErr[*ErrFileTooLarge](err), "blob overflow must not masquerade as file-too-large")

		size, err := store.getBlobSize("blob")
		require.NoError(t, err)
		assert.Equal(t, used, size, "failed append must not change the blob")
		assertOnlyBlobFiles(t, store)
	})

	t.Run("completely_full_rejects_even_empty_file", func(t *testing.T) {
		store := newStore(t)
		preallocValidBlob(t, store, "blob", maxBlobSize)

		_, _, err := store.createBlob("blob", "f", nil)
		space := asErr[*ErrNotEnoughSpace](t, err)
		assert.Equal(t, recordSize("f", nil), space.Needed)
		assert.Equal(t, int64(maxBlobSize), space.Used)

		size, err := store.getBlobSize("blob")
		require.NoError(t, err)
		assert.Equal(t, int64(maxBlobSize), size)
	})
}

// ---------- on-disk format & appendRecord ----------

// Pins the binary format: uint16 LE uid length, uid, uint64 LE data length, data.
// Any accidental format change would make existing production data unreadable.
func TestOnDiskFormat_ByteExact(t *testing.T) {
	store := newStore(t)

	d1 := []byte("first")
	d2 := payload(300)
	d3 := []byte{}

	_, _, err := store.createBlob("blob", "a", d1)
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "bb", d2)
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "ccc", d3)
	require.NoError(t, err)

	want := append(append(encodeRecord("a", d1), encodeRecord("bb", d2)...), encodeRecord("ccc", d3)...)
	assert.Equal(t, want, readRaw(t, store.blobPath("blob")))

	// Hand-written expectation for the very first record, independent of the helper.
	handWritten := []byte{
		0x01, 0x00, // uid length = 1
		'a',
		0x05, 0, 0, 0, 0, 0, 0, 0, // data length = 5
		'f', 'i', 'r', 's', 't',
	}
	assert.Equal(t, handWritten, readRaw(t, store.blobPath("blob"))[:len(handWritten)])
}

func TestAppendRecord_Exclusive(t *testing.T) {
	store := newStore(t)
	data := []byte("data")

	// Missing blob: exclusive create succeeds.
	require.NoError(t, store.appendRecord("blob", "f", data, true))
	assert.Equal(t, encodeRecord("f", data), readRaw(t, store.blobPath("blob")))

	// Existing blob: exclusive must fail with os.ErrExist and leave the blob untouched.
	err := store.appendRecord("blob", "g", []byte("other"), true)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrExist)
	assert.Equal(t, encodeRecord("f", data), readRaw(t, store.blobPath("blob")))

	// Non-exclusive append still works.
	require.NoError(t, store.appendRecord("blob", "g", []byte("other"), false))
	want := append(encodeRecord("f", data), encodeRecord("g", []byte("other"))...)
	assert.Equal(t, want, readRaw(t, store.blobPath("blob")))
}

// ---------- failure handling: write errors, rollback, directory sync ----------

func TestWriteRecord_WritesExactFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rec")
	f, err := os.Create(path)
	require.NoError(t, err)

	require.NoError(t, writeRecord(f, []byte("uid"), []byte("data")))
	require.NoError(t, f.Close())

	assert.Equal(t, encodeRecord("uid", []byte("data")), readRaw(t, path))
}

// Linux only: /dev/full makes every write fail with ENOSPC. The error must reach the caller intact.
func TestWriteRecord_ReportsWriteFailure(t *testing.T) {
	f, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skip("/dev/full is not available on this system")
	}
	defer f.Close()

	err = writeRecord(f, []byte("uid"), []byte("data"))
	require.Error(t, err)
	assert.ErrorIs(t, err, syscall.ENOSPC)
}

// The rollback must work from the path alone: the original handle is already closed.
func TestRollbackAppend_ExistingBlobIsTruncatedWithoutOriginalHandle(t *testing.T) {
	store := newStore(t)
	_, _, err := store.createBlob("blob", "a", []byte("aaa"))
	require.NoError(t, err)
	path := store.blobPath("blob")
	orig := readRaw(t, path)

	// Simulate a half-finished append whose file handle has already been closed.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte("partial-garbage"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, store.rollbackAppend(path, true, int64(len(orig))))
	assert.Equal(t, orig, readRaw(t, path))

	// The blob is healthy again and accepts new records.
	_, _, err = store.createBlob("blob", "b", []byte("bbb"))
	require.NoError(t, err)
	want := append(append([]byte{}, orig...), encodeRecord("b", []byte("bbb"))...)
	assert.Equal(t, want, readRaw(t, path))
}

func TestRollbackAppend_NewBlobIsRemoved(t *testing.T) {
	store := newStore(t)
	path := store.blobPath("blob")
	require.NoError(t, os.WriteFile(path, []byte("half-written"), 0644))

	require.NoError(t, store.rollbackAppend(path, false, 0))
	assert.NoFileExists(t, path)

	// Rolling back twice is harmless.
	require.NoError(t, store.rollbackAppend(path, false, 0))
}

func TestRollbackAppend_ReportsFailure(t *testing.T) {
	store := newStore(t)
	err := store.rollbackAppend(store.blobPath("missing"), true, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// If the directory sync fails after a new blob was created, the append must be rolled back.
func TestAppend_NewBlob_DirSyncFailure_RollsBack(t *testing.T) {
	injected := errors.New("injected directory sync failure")

	t.Run("rollback_succeeds", func(t *testing.T) {
		store := newStore(t)
		calls := 0
		store.hooks = &storeHooks{syncDir: func(string) error {
			calls++
			if calls == 1 {
				return injected
			}
			return nil
		}}

		_, _, err := store.createBlob("blob", "f", []byte("x"))
		require.Error(t, err)
		assert.ErrorIs(t, err, injected)
		assert.NotContains(t, err.Error(), "rollback")
		assert.NoFileExists(t, store.blobPath("blob"))
		assert.Equal(t, 2, calls, "one sync for the create and one for the rollback")
		assertOnlyBlobFiles(t, store)

		// The store stays usable afterwards.
		_, _, err = store.createBlob("blob", "f", []byte("x"))
		require.NoError(t, err)
		assert.Equal(t, encodeRecord("f", []byte("x")), readRaw(t, store.blobPath("blob")))
	})

	t.Run("rollback_also_fails", func(t *testing.T) {
		store := newStore(t)
		store.hooks = &storeHooks{syncDir: func(string) error { return injected }}

		_, _, err := store.createBlob("blob", "f", []byte("x"))
		require.Error(t, err)
		assert.ErrorIs(t, err, injected)
		assert.Contains(t, err.Error(), "rollback", "a failed rollback must be reported, not swallowed")
		assert.NoFileExists(t, store.blobPath("blob"))
	})
}

// Appending to an existing blob does not create a directory entry, so no directory sync is needed.
func TestAppend_ExistingBlob_DoesNotSyncDir(t *testing.T) {
	store := newStore(t)
	calls := 0
	store.hooks = &storeHooks{syncDir: func(string) error {
		calls++
		return nil
	}}

	_, _, err := store.createBlob("blob", "a", []byte("a"))
	require.NoError(t, err)
	assert.Equal(t, 1, calls)

	_, _, err = store.createBlob("blob", "b", []byte("b"))
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "appending to an existing blob must not sync the directory")
}

// A failed directory sync after a delete means: applied, but not durable.
func TestDelete_DirSyncFailure_ReportsNotDurable(t *testing.T) {
	injected := errors.New("injected directory sync failure")

	setup := func(t *testing.T) (BlobStore, *bool) {
		store := newStore(t)
		failNow := false
		store.hooks = &storeHooks{syncDir: func(string) error {
			if failNow {
				return injected
			}
			return nil
		}}
		return store, &failNow
	}

	t.Run("other_files_remain", func(t *testing.T) {
		store, failNow := setup(t)
		_, _, err := store.createBlob("blob", "a", []byte("aaa"))
		require.NoError(t, err)
		_, _, err = store.createBlob("blob", "b", []byte("bbb"))
		require.NoError(t, err)

		*failNow = true
		err = store.deleteFileFromBlob("blob", "a")
		nd := asErr[*ErrNotDurable](t, err)
		assert.Equal(t, "delete", nd.Op)
		assert.ErrorIs(t, err, injected)

		// The delete really happened.
		assert.Equal(t, encodeRecord("b", []byte("bbb")), readRaw(t, store.blobPath("blob")))
		assertOnlyBlobFiles(t, store)

		// ...which is why a blind retry reports "not found".
		err = store.deleteFileFromBlob("blob", "a")
		assertFileNotFound(t, err, "blob", "a")
	})

	t.Run("last_file_blob_stays_empty", func(t *testing.T) {
		store, failNow := setup(t)
		_, _, err := store.createBlob("blob", "a", []byte("aaa"))
		require.NoError(t, err)

		*failNow = true
		err = store.deleteFileFromBlob("blob", "a")
		nd := asErr[*ErrNotDurable](t, err)
		assert.Equal(t, "delete", nd.Op)
		assert.ErrorIs(t, err, injected)

		assertEmptyBlobExists(t, store, "blob")

		err = store.deleteFileFromBlob("blob", "a")
		assertFileNotFound(t, err, "blob", "a")
	})

	t.Run("not_found_does_not_sync", func(t *testing.T) {
		store, failNow := setup(t)
		_, _, err := store.createBlob("blob", "a", []byte("aaa"))
		require.NoError(t, err)

		*failNow = true
		err = store.deleteFileFromBlob("blob", "missing")
		assertFileNotFound(t, err, "blob", "missing")
		assert.False(t, isErr[*ErrNotDurable](err))
	})
}

// ---------- getBlobSize ----------

func TestGetBlobSize(t *testing.T) {
	store := newStore(t)

	data1 := payload(10)
	data2 := payload(25)

	blobUID, _, err := store.createBlob("blob", "file1", data1)
	require.NoError(t, err)

	rec1 := recordSize("file1", data1)

	size, err := store.getBlobSize(blobUID)
	require.NoError(t, err)
	assert.Equal(t, rec1, size)

	_, _, err = store.createBlob("blob", "file2", data2)
	require.NoError(t, err)
	rec2 := recordSize("file2", data2)

	size, err = store.getBlobSize(blobUID)
	require.NoError(t, err)
	assert.Equal(t, rec1+rec2, size)
}

func TestGetBlobSize_NotFound(t *testing.T) {
	store := newStore(t)

	_, err := store.getBlobSize("missing")
	assertBlobNotFound(t, err, "missing")
}

func TestGetBlobSize_InvalidUID(t *testing.T) {
	store := newStore(t)

	for _, uid := range append([]string{""}, invalidUIDs...) {
		_, err := store.getBlobSize(uid)
		assertInvalidUID(t, err, uid)
	}
}

// ---------- getFileFromBlob ----------

func TestGetFileFromBlob_PicksCorrectFileInOrder(t *testing.T) {
	store := newStore(t)

	type item struct {
		uid  string
		data []byte
	}
	items := []item{
		{"empty-start", nil},
		{"item-1", payload(1)},
		{"empty-mid", []byte{}},
		{"item-2", payload(4096)},
		{"item-3", []byte("hello world")},
		{"empty-end", nil},
	}

	for _, it := range items {
		_, _, err := store.createBlob("blob", it.uid, it.data)
		require.NoError(t, err)
	}

	for _, it := range items {
		got, err := store.getFileFromBlob("blob", it.uid)
		require.NoError(t, err, "file %q", it.uid)
		if len(it.data) == 0 {
			assert.Empty(t, got, "file %q", it.uid)
		} else {
			assert.Equal(t, it.data, got, "file %q", it.uid)
		}
	}
}

func TestGetFileFromBlob_BlobNotFound(t *testing.T) {
	store := newStore(t)

	_, err := store.getFileFromBlob("missing", "f")
	assertBlobNotFound(t, err, "missing")
}

func TestGetFileFromBlob_FileNotFound(t *testing.T) {
	store := newStore(t)
	_, _, err := store.createBlob("blob", "exists", []byte("x"))
	require.NoError(t, err)

	_, err = store.getFileFromBlob("blob", "missing")
	assertFileNotFound(t, err, "blob", "missing")
}

func TestGetFileFromBlob_EmptyBlobFile(t *testing.T) {
	store := newStore(t)
	require.NoError(t, os.WriteFile(store.blobPath("empty"), nil, 0644))

	_, err := store.getFileFromBlob("empty", "f")
	assertFileNotFound(t, err, "empty", "f")
}

func TestGetFileFromBlob_InvalidUID(t *testing.T) {
	store := newStore(t)

	for _, uid := range append([]string{""}, invalidUIDs...) {
		_, err := store.getFileFromBlob(uid, "f")
		assertInvalidUID(t, err, uid)

		_, err = store.getFileFromBlob("blob", uid)
		assertInvalidUID(t, err, uid)
	}
}

// UIDs are compared as whole strings: prefixes and extensions must never match.
func TestGetFileFromBlob_UIDPrefixesDoNotMatch(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob", "abcd", []byte("long"))
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "abc", []byte("short"))
	require.NoError(t, err)

	got, err := store.getFileFromBlob("blob", "abc")
	require.NoError(t, err)
	assert.Equal(t, []byte("short"), got)

	got, err = store.getFileFromBlob("blob", "abcd")
	require.NoError(t, err)
	assert.Equal(t, []byte("long"), got)

	for _, missing := range []string{"ab", "a", "abcde", "ABC"} {
		_, err = store.getFileFromBlob("blob", missing)
		assertFileNotFound(t, err, "blob", missing)
	}
}

// A payload that looks like a valid record must be skipped as opaque data, never parsed.
func TestGetAndDelete_PayloadLookingLikeRecordIsOpaque(t *testing.T) {
	store := newStore(t)

	inner := encodeRecord("inner", []byte("xyz"))
	_, _, err := store.createBlob("blob", "outer", inner)
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "after", []byte("tail"))
	require.NoError(t, err)

	before := readRaw(t, store.blobPath("blob"))

	_, err = store.getFileFromBlob("blob", "inner")
	assertFileNotFound(t, err, "blob", "inner")

	err = store.deleteFileFromBlob("blob", "inner")
	assertFileNotFound(t, err, "blob", "inner")
	assert.Equal(t, before, readRaw(t, store.blobPath("blob")))

	got, err := store.getFileFromBlob("blob", "outer")
	require.NoError(t, err)
	assert.Equal(t, inner, got)

	got, err = store.getFileFromBlob("blob", "after")
	require.NoError(t, err)
	assert.Equal(t, []byte("tail"), got)
}

func TestReads_NeverModifyBlob(t *testing.T) {
	store := newStore(t)
	for i := 0; i < 5; i++ {
		_, _, err := store.createBlob("blob", fmt.Sprintf("f%d", i), payload(100*i))
		require.NoError(t, err)
	}
	before := readRaw(t, store.blobPath("blob"))

	for i := 0; i < 5; i++ {
		_, err := store.getFileFromBlob("blob", fmt.Sprintf("f%d", i))
		require.NoError(t, err)
	}
	_, err := store.getFileFromBlob("blob", "missing")
	assertFileNotFound(t, err, "blob", "missing")
	_, err = store.getBlobSize("blob")
	require.NoError(t, err)

	assert.Equal(t, before, readRaw(t, store.blobPath("blob")))
	assertOnlyBlobFiles(t, store)
}

// ---------- Corruption & malicious header resilience ----------

// TestByteByByteTruncationMatrix cuts a 2-record blob at every byte offset and verifies that
// neither read nor delete ever mistakes a truncated record for a clean EOF or "not found",
// and that the file on disk is never modified by a failed operation.
func TestByteByByteTruncationMatrix(t *testing.T) {
	rec1 := buildRawRecord("item-1", 6, []byte("record"))
	rec2 := buildRawRecord("item-2", 8, []byte("document"))
	fullBlob := append(append([]byte{}, rec1...), rec2...)

	hdr1 := 2 + len("item-1") + 8
	hdr2 := 2 + len("item-2") + 8

	for cut := 1; cut < len(fullBlob); cut++ {
		// Exact boundary between rec1 and rec2 is a valid 1-record blob, skip it.
		if cut == len(rec1) {
			continue
		}

		t.Run(fmt.Sprintf("cut_at_byte_%d_of_%d", cut, len(fullBlob)), func(t *testing.T) {
			store := newStore(t)
			truncated := fullBlob[:cut]
			blobPath := store.blobPath("truncated")
			require.NoError(t, os.WriteFile(blobPath, truncated, 0644))

			// The full header is present but the payload is short: ErrTruncatedRecord with exact
			// diagnostics. Otherwise the cut is inside a header: io.ErrUnexpectedEOF.
			inRec1Payload := cut >= hdr1 && cut < len(rec1)
			inRec2Payload := cut >= len(rec1)+hdr2

			check := func(t *testing.T, err error) {
				t.Helper()
				require.Error(t, err)
				assert.False(t, isErr[*ErrFileNotFound](err), "truncation at byte %d masked as ErrFileNotFound", cut)
				assert.False(t, isErr[*ErrBlobNotFound](err))
				assert.False(t, errors.Is(err, io.EOF), "a clean-EOF signal must never leak out of a damaged blob")
				assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

				switch {
				case inRec1Payload:
					tr := asErr[*ErrTruncatedRecord](t, err)
					assert.Equal(t, int64(hdr1), tr.Offset)
					assert.Equal(t, int64(cut), tr.FileSize)
					assert.Equal(t, uint64(6), tr.Needed)
					assert.Equal(t, int64(cut-hdr1), tr.Available)
				case inRec2Payload:
					tr := asErr[*ErrTruncatedRecord](t, err)
					assert.Equal(t, int64(len(rec1)+hdr2), tr.Offset)
					assert.Equal(t, int64(cut), tr.FileSize)
					assert.Equal(t, uint64(8), tr.Needed)
					assert.Equal(t, int64(cut-len(rec1)-hdr2), tr.Available)
				default:
					assert.False(t, isErr[*ErrTruncatedRecord](err), "cut inside a header is not a payload truncation")
				}
			}

			// Reads.
			_, err := store.getFileFromBlob("truncated", "non-existent")
			check(t, err)

			_, err = store.getFileFromBlob("truncated", "item-2")
			check(t, err)

			got1, err := store.getFileFromBlob("truncated", "item-1")
			if cut > len(rec1) {
				// rec1 is fully intact and precedes the damage: it must stay readable.
				require.NoError(t, err)
				assert.Equal(t, []byte("record"), got1)
			} else {
				check(t, err)
			}

			// Deletes must fail safely: no modification, no .tmp leftovers.
			for _, target := range []string{"item-1", "item-2", "non-existent"} {
				err = store.deleteFileFromBlob("truncated", target)
				check(t, err)

				assert.Equal(t, truncated, readRaw(t, blobPath), "blob modified after failed delete of %s at cut=%d", target, cut)
				assert.NoFileExists(t, blobPath+".tmp")
			}

			// Appends must be refused as well.
			_, _, err = store.createBlob("truncated", "new", []byte("new"))
			check(t, err)
			assert.Equal(t, truncated, readRaw(t, blobPath), "blob modified after refused append at cut=%d", cut)
		})
	}
}

// TestCorruptedDataLen_OverflowAndHugeValues ensures that corrupted dataLen values
// (math.MaxUint64 -> -1 as int64, 1 TB which would panic make([]byte), ...)
// are caught by checkPayloadFits before allocation or Seek.
func TestCorruptedDataLen_OverflowAndHugeValues(t *testing.T) {
	maliciousLengths := []struct {
		name    string
		dataLen uint64
	}{
		{"MaxUint64_NegativeInt64Cast", math.MaxUint64},
		{"SignBitSet_MinInt64Cast", uint64(math.MaxInt64) + 1},
		{"MaxInt64_OOMRisk", uint64(math.MaxInt64)},
		{"OneTerabyte_OOMRisk", 1 << 40},
		{"MaxFileSizePlusOne_Lie", uint64(maxFileSize) + 1},
		{"OffByOne_OneByteShort", 5}, // payload is 4 bytes, header claims 5
	}

	for _, tc := range maliciousLengths {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			corrupted := buildRawRecord("target", tc.dataLen, []byte("1234"))
			blobPath := store.blobPath("bad")
			require.NoError(t, os.WriteFile(blobPath, corrupted, 0644))

			check := func(t *testing.T, err error) {
				t.Helper()
				assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
				tr := asErr[*ErrTruncatedRecord](t, err)
				assert.Equal(t, int64(2+len("target")+8), tr.Offset)
				assert.Equal(t, int64(len(corrupted)), tr.FileSize)
				assert.Equal(t, tc.dataLen, tr.Needed)
				assert.Equal(t, int64(4), tr.Available)
			}

			// Must fail when target matches (guards make([]byte, dataLen)).
			_, err := store.getFileFromBlob("bad", "target")
			check(t, err)

			// Must fail when target does not match (guards f.Seek(int64(dataLen), SeekCurrent)).
			_, err = store.getFileFromBlob("bad", "other")
			check(t, err)

			// Must fail on delete (matching and non-matching target) and preserve the file.
			err = store.deleteFileFromBlob("bad", "target")
			check(t, err)

			err = store.deleteFileFromBlob("bad", "other")
			check(t, err)

			// Must be refused on append and preserve the file.
			_, _, err = store.createBlob("bad", "new", []byte("x"))
			check(t, err)

			assert.Equal(t, corrupted, readRaw(t, blobPath))
			assert.NoFileExists(t, blobPath+".tmp")
		})
	}
}

// Garbage in the uid-length field: a huge uid length must not allocate or loop, just fail.
func TestCorruptedUIDLen(t *testing.T) {
	store := newStore(t)
	raw := []byte{0xff, 0xff, 'a', 'b', 'c'} // claims a 65535-byte uid, has 3 bytes
	blobPath := store.blobPath("bad")
	require.NoError(t, os.WriteFile(blobPath, raw, 0644))

	_, err := store.getFileFromBlob("bad", "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.False(t, isErr[*ErrFileNotFound](err))

	err = store.deleteFileFromBlob("bad", "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.False(t, isErr[*ErrFileNotFound](err))

	assert.Equal(t, raw, readRaw(t, blobPath))
	assert.NoFileExists(t, blobPath+".tmp")
}

// Trailing garbage after valid records must not be reported as "not found".
func TestTrailingGarbageIsNotSilentlyIgnored(t *testing.T) {
	for _, garbage := range [][]byte{{0x00}, {0x01, 0x00}, {0x00, 0x00, 0x00}, bytes.Repeat([]byte{0xAB}, 7)} {
		t.Run(fmt.Sprintf("%x", garbage), func(t *testing.T) {
			store := newStore(t)
			raw := append(encodeRecord("ok", []byte("fine")), garbage...)
			blobPath := store.blobPath("tail")
			require.NoError(t, os.WriteFile(blobPath, raw, 0644))

			got, err := store.getFileFromBlob("tail", "ok")
			require.NoError(t, err)
			assert.Equal(t, []byte("fine"), got)

			_, err = store.getFileFromBlob("tail", "missing")
			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
			assert.False(t, isErr[*ErrFileNotFound](err), "damaged tail must not look like a clean 'not found'")

			err = store.deleteFileFromBlob("tail", "ok")
			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

			_, _, err = store.createBlob("tail", "new", []byte("new"))
			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

			assert.Equal(t, raw, readRaw(t, blobPath))
			assert.NoFileExists(t, blobPath+".tmp")
		})
	}
}

// ---------- deleteFileFromBlob ----------

func TestDeleteFileFromBlob_SequentialFirstMiddleLastUntilEmpty(t *testing.T) {
	store := newStore(t)

	files := []struct {
		uid  string
		data []byte
	}{
		{"first", payload(111)},
		{"second", payload(222)},
		{"middle", payload(333)},
		{"fourth", payload(444)},
		{"last", payload(555)},
	}

	var expectedSize int64
	for _, f := range files {
		_, _, err := store.createBlob("blob", f.uid, f.data)
		require.NoError(t, err)
		expectedSize += recordSize(f.uid, f.data)
	}

	size, err := store.getBlobSize("blob")
	require.NoError(t, err)
	assert.Equal(t, expectedSize, size)

	// Delete in order: first -> middle -> last -> second -> fourth (last remaining).
	deleteOrder := []int{0, 2, 4, 1, 3}
	deleted := make(map[string]bool)

	for step, idx := range deleteOrder {
		target := files[idx]
		require.NoError(t, store.deleteFileFromBlob("blob", target.uid), "step %d: delete %s", step, target.uid)
		deleted[target.uid] = true
		expectedSize -= recordSize(target.uid, target.data)

		assert.NoFileExists(t, store.blobPath("blob")+".tmp")

		// The blob itself is never deleted, even when the last file is gone.
		size, err := store.getBlobSize("blob")
		require.NoError(t, err, "step %d: the blob must still exist", step)
		assert.Equal(t, expectedSize, size)

		for _, f := range files {
			got, err := store.getFileFromBlob("blob", f.uid)
			if deleted[f.uid] {
				assertFileNotFound(t, err, "blob", f.uid)
			} else {
				require.NoError(t, err)
				assert.Equal(t, f.data, got)
			}
		}
	}

	// All files are gone: the blob is an empty file.
	assertEmptyBlobExists(t, store, "blob")

	// Deleting from an empty blob says "file not found", not "blob not found".
	err = store.deleteFileFromBlob("blob", "first")
	assertFileNotFound(t, err, "blob", "first")
	assertEmptyBlobExists(t, store, "blob")
}

// Deleting the last file keeps the blob: the blob can be reused afterwards.
func TestDeleteLastFile_KeepsEmptyBlobAndItStaysUsable(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob", "only", []byte("only-data"))
	require.NoError(t, err)

	require.NoError(t, store.deleteFileFromBlob("blob", "only"))

	assertEmptyBlobExists(t, store, "blob")
	size, err := store.getBlobSize("blob")
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	_, err = store.getFileFromBlob("blob", "only")
	assertFileNotFound(t, err, "blob", "only")
	assert.False(t, isErr[*ErrBlobNotFound](err))

	// The empty blob accepts new files.
	_, _, err = store.createBlob("blob", "next", []byte("next-data"))
	require.NoError(t, err)
	assert.Equal(t, encodeRecord("next", []byte("next-data")), readRaw(t, store.blobPath("blob")))

	got, err := store.getFileFromBlob("blob", "next")
	require.NoError(t, err)
	assert.Equal(t, []byte("next-data"), got)
	assertOnlyBlobFiles(t, store)
}

// A generated blob UID is never reused for a new blob while the old (empty) blob exists.
func TestDeleteLastFile_GeneratedBlobStaysEmptyBlob(t *testing.T) {
	store := newStore(t)

	blobUID, fileUID, err := store.createBlob("", "", []byte("data"))
	require.NoError(t, err)

	require.NoError(t, store.deleteFileFromBlob(blobUID, fileUID))
	assertEmptyBlobExists(t, store, blobUID)

	err = store.appendRecord(blobUID, "x", []byte("x"), true)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrExist, "an empty blob still occupies its UID")
}

// After a delete the remaining records must be byte-identical and keep their order.
// Sizes deliberately cross common buffer boundaries (0, 1, 64 KiB +/- 1, 1 MiB).
func TestDeleteFileFromBlob_RemainingRecordsByteExactAndOrdered(t *testing.T) {
	store := newStore(t)

	type item struct {
		uid  string
		data []byte
	}
	items := []item{
		{"r0", nil},
		{"r1", payload(1)},
		{"r2", payload(64*1024 - 1)},
		{"r3", payload(64 * 1024)},
		{"r4", payload(64*1024 + 1)},
		{"r5", payload(1 << 20)},
		{"r6", []byte{}},
	}
	for _, it := range items {
		_, _, err := store.createBlob("blob", it.uid, it.data)
		require.NoError(t, err)
	}

	for _, victim := range []string{"r3", "r0", "r6", "r2"} {
		require.NoError(t, store.deleteFileFromBlob("blob", victim))

		var want []byte
		kept := items[:0:0]
		for _, it := range items {
			if it.uid == victim {
				continue
			}
			want = append(want, encodeRecord(it.uid, it.data)...)
			kept = append(kept, it)
		}
		items = kept

		require.True(t, bytes.Equal(want, readRaw(t, store.blobPath("blob"))), "after deleting %s", victim)
		assert.NoFileExists(t, store.blobPath("blob")+".tmp")
	}
}

// A stale .tmp left over from a crashed delete must be overwritten and must not leak into the blob.
func TestDeleteFileFromBlob_StaleTmpFileIsOverwritten(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob", "a", []byte("aaa"))
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "b", []byte("bbb"))
	require.NoError(t, err)

	staleTmp := store.blobPath("blob") + ".tmp"
	require.NoError(t, os.WriteFile(staleTmp, bytes.Repeat([]byte{0xEE}, 10_000), 0644))

	require.NoError(t, store.deleteFileFromBlob("blob", "a"))

	assert.NoFileExists(t, staleTmp)
	assert.Equal(t, encodeRecord("b", []byte("bbb")), readRaw(t, store.blobPath("blob")))
	assertFileNotFoundAfterDelete(t, store, "blob", "a")
	assertOnlyBlobFiles(t, store)
}

// A stale .tmp must not leak into the blob even when the last file is deleted.
func TestDeleteLastFile_StaleTmpFileIsNotUsed(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob", "a", []byte("aaa"))
	require.NoError(t, err)

	staleTmp := store.blobPath("blob") + ".tmp"
	require.NoError(t, os.WriteFile(staleTmp, bytes.Repeat([]byte{0xEE}, 10_000), 0644))

	require.NoError(t, store.deleteFileFromBlob("blob", "a"))

	assertEmptyBlobExists(t, store, "blob")
	assertOnlyBlobFiles(t, store)
}

// Deleting a file from one blob leaves other blobs alone; the emptied blob itself stays.
func TestDeleteFileFromBlob_DoesNotAffectOtherBlobs(t *testing.T) {
	store := newStore(t)

	_, _, err := store.createBlob("blob-1", "doc", []byte("blob-1-data"))
	require.NoError(t, err)
	_, _, err = store.createBlob("blob-2", "doc", []byte("blob-2-data"))
	require.NoError(t, err)

	require.NoError(t, store.deleteFileFromBlob("blob-1", "doc"))
	assertEmptyBlobExists(t, store, "blob-1")

	got2, err := store.getFileFromBlob("blob-2", "doc")
	require.NoError(t, err)
	assert.Equal(t, []byte("blob-2-data"), got2)
}

func TestDeleteFileFromBlob_FileNotFoundLeavesBlobIntact(t *testing.T) {
	store := newStore(t)
	_, _, err := store.createBlob("blob", "a", []byte("data"))
	require.NoError(t, err)

	before := readRaw(t, store.blobPath("blob"))

	err = store.deleteFileFromBlob("blob", "missing")
	assertFileNotFound(t, err, "blob", "missing")

	assert.Equal(t, before, readRaw(t, store.blobPath("blob")))
	assert.NoFileExists(t, store.blobPath("blob")+".tmp")
}

func TestDeleteFileFromBlob_BlobNotFound(t *testing.T) {
	store := newStore(t)

	err := store.deleteFileFromBlob("missing", "f")
	assertBlobNotFound(t, err, "missing")
	assert.NoFileExists(t, store.blobPath("missing")+".tmp")
	assert.NoFileExists(t, store.blobPath("missing"), "deleting from a missing blob must not create it")
}

func TestDeleteFileFromBlob_InvalidUID(t *testing.T) {
	store := newStore(t)

	for _, uid := range append([]string{""}, invalidUIDs...) {
		err := store.deleteFileFromBlob(uid, "f")
		assertInvalidUID(t, err, uid)

		err = store.deleteFileFromBlob("blob", uid)
		assertInvalidUID(t, err, uid)
	}
}

// ---------- differential fuzzing against a reference parser ----------

// FuzzBlobParsing feeds arbitrary bytes as a blob file and compares get/append/delete with an
// independent reference parser. Run longer with: go test -fuzz=FuzzBlobParsing -fuzztime=2m
// Under plain `go test` it runs the seed corpus below.
func FuzzBlobParsing(f *testing.F) {
	rec1 := buildRawRecord("item-1", 6, []byte("record"))
	rec2 := buildRawRecord("item-2", 8, []byte("document"))
	full := append(append([]byte{}, rec1...), rec2...)

	f.Add(full, "item-1")
	f.Add(full, "item-2")
	f.Add(full, "missing")
	f.Add(full[:len(full)-1], "item-1")
	f.Add(full[:len(rec1)+3], "missing")
	f.Add(rec1[:5], "x")
	f.Add([]byte{}, "x")
	f.Add([]byte{0xff, 0xff, 0xff}, "x")
	f.Add(buildRawRecord("t", math.MaxUint64, []byte("1234")), "t")
	f.Add(buildRawRecord("t", 1<<40, []byte("1234")), "other")
	f.Add(append(encodeRecord("dup", []byte("1")), encodeRecord("dup", []byte("2"))...), "dup")
	f.Add(append(encodeRecord("ok", []byte("fine")), 0x00), "missing")
	f.Add(encodeRecord("outer", encodeRecord("inner", []byte("xyz"))), "inner")
	f.Add(encodeRecord("", []byte("empty-uid-record")), "x")
	f.Add(encodeRecord("only", []byte("data")), "only")

	f.Fuzz(func(t *testing.T, raw []byte, target string) {
		if !validUID(target) {
			t.Skip()
		}
		store := newStore(t)
		blobPath := store.blobPath("fz")
		require.NoError(t, os.WriteFile(blobPath, raw, 0644))

		recs, clean := parseRaw(raw)

		// ----- get -----
		var want []byte
		found := false
		for _, r := range recs {
			if r.uid == target {
				want, found = r.data, true
				break
			}
		}

		got, err := store.getFileFromBlob("fz", target)
		switch {
		case found:
			require.NoError(t, err)
			requireSameBytes(t, want, got, "get returned wrong data")
		case !clean:
			require.Error(t, err)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.False(t, isErr[*ErrFileNotFound](err), "damaged blob reported as clean not-found")
		default:
			assertFileNotFound(t, err, "fz", target)
		}
		requireSameBytes(t, raw, readRaw(t, blobPath), "get must never modify the blob")

		// ----- append (on a separate copy so that delete below sees the original) -----
		appendPath := store.blobPath("fz2")
		require.NoError(t, os.WriteFile(appendPath, raw, 0644))
		newData := []byte("appended")
		_, _, aerr := store.createBlob("fz2", "app", newData)
		if clean {
			require.NoError(t, aerr)
			wantRaw := append(append([]byte{}, raw...), encodeRecord("app", newData)...)
			requireSameBytes(t, wantRaw, readRaw(t, appendPath), "append produced wrong bytes")
		} else {
			require.Error(t, aerr)
			require.ErrorIs(t, aerr, io.ErrUnexpectedEOF)
			requireSameBytes(t, raw, readRaw(t, appendPath), "append to a damaged blob must not modify it")
		}

		// ----- delete -----
		err = store.deleteFileFromBlob("fz", target)
		require.NoFileExists(t, blobPath+".tmp")

		if !clean {
			require.Error(t, err)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			require.False(t, isErr[*ErrFileNotFound](err))
			requireSameBytes(t, raw, readRaw(t, blobPath), "failed delete must not modify the blob")
			return
		}

		var kept []byte
		removed := 0
		for _, r := range recs {
			if r.uid == target {
				removed++
				continue
			}
			kept = append(kept, encodeRecord(r.uid, r.data)...)
		}

		if removed == 0 {
			assertFileNotFound(t, err, "fz", target)
			requireSameBytes(t, raw, readRaw(t, blobPath), "not-found delete must not modify the blob")
			return
		}

		// The blob itself always stays, even if no records are left.
		require.NoError(t, err)
		require.FileExists(t, blobPath)
		requireSameBytes(t, kept, readRaw(t, blobPath), "delete produced wrong bytes")
	})
}

// ---------- randomized model-based test ----------

type modelRec struct {
	uid  string
	data []byte
}

func encodeModel(recs []modelRec) []byte {
	var out []byte
	for _, r := range recs {
		out = append(out, encodeRecord(r.uid, r.data)...)
	}
	return out
}

// TestRandomizedModelBased runs long random sequences of create/get/delete/size against an
// in-memory model and compares both API results and raw bytes on disk.
// A blob that lost its last file stays in the model as an empty blob.
func TestRandomizedModelBased(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 42, 1337} {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			runModelTest(t, seed, 1200)
		})
	}
}

func runModelTest(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	store := newStore(t)
	model := map[string][]modelRec{}

	blobs := []string{"b0", "b1", "b2", "b3"}
	files := []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"}
	sizes := []int{0, 1, 2, 255, 256, 4096, 70000}

	verifyDisk := func(step int) {
		t.Helper()
		for _, b := range blobs {
			recs, ok := model[b]
			if !ok {
				assert.NoFileExists(t, store.blobPath(b), "seed=%d step=%d blob=%s must not exist", seed, step, b)
				continue
			}
			require.True(t, bytes.Equal(encodeModel(recs), readRaw(t, store.blobPath(b))),
				"seed=%d step=%d blob=%s on-disk bytes diverge from model", seed, step, b)
		}
		assertOnlyBlobFiles(t, store)
	}

	for step := 0; step < steps; step++ {
		b := blobs[rng.Intn(len(blobs))]
		f := files[rng.Intn(len(files))]
		msg := fmt.Sprintf("seed=%d step=%d blob=%s file=%s", seed, step, b, f)

		switch op := rng.Intn(10); {
		case op <= 3: // create
			data := make([]byte, sizes[rng.Intn(len(sizes))])
			rng.Read(data)
			gotBlob, gotFile, err := store.createBlob(b, f, data)
			require.NoError(t, err, msg)
			require.Equal(t, b, gotBlob, msg)
			require.Equal(t, f, gotFile, msg)
			model[b] = append(model[b], modelRec{uid: f, data: data})

		case op <= 6: // get
			recs, ok := model[b]
			got, err := store.getFileFromBlob(b, f)
			if !ok {
				assertBlobNotFound(t, err, b)
				break
			}
			var want []byte
			found := false
			for _, r := range recs {
				if r.uid == f {
					want, found = r.data, true
					break
				}
			}
			if found {
				require.NoError(t, err, msg)
				require.True(t, bytes.Equal(want, got), msg)
			} else {
				assertFileNotFound(t, err, b, f)
			}

		case op <= 8: // delete
			recs, ok := model[b]
			err := store.deleteFileFromBlob(b, f)
			if !ok {
				assertBlobNotFound(t, err, b)
				break
			}
			var kept []modelRec
			for _, r := range recs {
				if r.uid != f {
					kept = append(kept, r)
				}
			}
			if len(kept) == len(recs) {
				assertFileNotFound(t, err, b, f)
			} else {
				require.NoError(t, err, msg)
				model[b] = kept // the blob stays in the model, even if it is now empty
				require.FileExists(t, store.blobPath(b), msg)
			}

		default: // size
			recs, ok := model[b]
			size, err := store.getBlobSize(b)
			if !ok {
				assertBlobNotFound(t, err, b)
				break
			}
			require.NoError(t, err, msg)
			require.Equal(t, int64(len(encodeModel(recs))), size, msg)
		}

		if step%40 == 0 {
			verifyDisk(step)
		}
	}
	verifyDisk(steps)
}

// ---------- concurrency (run with -race) ----------

func TestConcurrentAppendsToSameBlob(t *testing.T) {
	store := newStore(t)
	_, _, err := store.createBlob("blob", "first", []byte("x"))
	require.NoError(t, err)

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := store.createBlob("blob", fmt.Sprintf("file-%d", i), fmt.Appendf(nil, "data-%d", i))
			errs <- err
		}(i)
	}
	waitOrFail(t, &wg, 30*time.Second)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	for i := 0; i < n; i++ {
		got, err := store.getFileFromBlob("blob", fmt.Sprintf("file-%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Appendf(nil, "data-%d", i), got)
	}

	// The blob must be a clean record stream: exactly n+1 well-formed records, no garbage.
	recs, clean := parseRaw(readRaw(t, store.blobPath("blob")))
	assert.True(t, clean, "interleaved writes produced a malformed blob")
	assert.Len(t, recs, n+1)
	assertOnlyBlobFiles(t, store)
}

// TestConcurrentCreateReadDeleteSizeOnSameBlob exercises all 4 operations simultaneously
// on a single blob while keeping "anchor" records that must never be lost or corrupted.
func TestConcurrentCreateReadDeleteSizeOnSameBlob(t *testing.T) {
	store := newStore(t)

	const anchors = 5
	anchorData := make(map[string][]byte, anchors)
	for i := 0; i < anchors; i++ {
		uid := fmt.Sprintf("anchor-%d", i)
		data := payload(500 + i*100)
		anchorData[uid] = data
		_, _, err := store.createBlob("shared-blob", uid, data)
		require.NoError(t, err)
	}

	const workers = 30
	var wg sync.WaitGroup
	errs := make(chan error, workers*4)

	for i := 0; i < workers; i++ {
		wg.Add(3)

		// Writer + deleter of an ephemeral file.
		go func(i int) {
			defer wg.Done()
			uid := fmt.Sprintf("temp-%d", i)
			data := payload(200 + i)
			if _, _, err := store.createBlob("shared-blob", uid, data); err != nil {
				errs <- fmt.Errorf("create %s: %w", uid, err)
				return
			}
			if err := store.deleteFileFromBlob("shared-blob", uid); err != nil {
				errs <- fmt.Errorf("delete %s: %w", uid, err)
			}
		}(i)

		// Reader of anchor files.
		go func(i int) {
			defer wg.Done()
			uid := fmt.Sprintf("anchor-%d", i%anchors)
			got, err := store.getFileFromBlob("shared-blob", uid)
			if err != nil {
				errs <- fmt.Errorf("read %s: %w", uid, err)
				return
			}
			if !bytes.Equal(got, anchorData[uid]) {
				errs <- fmt.Errorf("corrupted data for %s", uid)
			}
		}(i)

		// Size checker.
		go func() {
			defer wg.Done()
			size, err := store.getBlobSize("shared-blob")
			if err != nil {
				errs <- fmt.Errorf("getBlobSize: %w", err)
				return
			}
			if size <= 0 {
				errs <- fmt.Errorf("unexpected non-positive blob size: %d", size)
			}
		}()
	}

	waitOrFail(t, &wg, 60*time.Second)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	for uid, want := range anchorData {
		got, err := store.getFileFromBlob("shared-blob", uid)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	// Every ephemeral file was deleted again: only the anchors may remain.
	recs, clean := parseRaw(readRaw(t, store.blobPath("shared-blob")))
	assert.True(t, clean)
	assert.Len(t, recs, anchors)
	assertOnlyBlobFiles(t, store)
}

// Many goroutines delete the same file: exactly one wins, the rest get ErrFileNotFound,
// and the neighbouring record survives untouched.
func TestConcurrentDeleteSameFile_ExactlyOneWins(t *testing.T) {
	store := newStore(t)
	_, _, err := store.createBlob("blob", "anchor", []byte("anchor-data"))
	require.NoError(t, err)
	_, _, err = store.createBlob("blob", "target", []byte("target-data"))
	require.NoError(t, err)

	const n = 40
	var wg sync.WaitGroup
	results := make(chan error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- store.deleteFileFromBlob("blob", "target")
		}()
	}
	close(start)
	waitOrFail(t, &wg, 30*time.Second)
	close(results)

	wins := 0
	for err := range results {
		if err == nil {
			wins++
			continue
		}
		assertFileNotFound(t, err, "blob", "target")
	}
	assert.Equal(t, 1, wins, "exactly one delete must succeed")

	got, err := store.getFileFromBlob("blob", "anchor")
	require.NoError(t, err)
	assert.Equal(t, []byte("anchor-data"), got)
	assert.Equal(t, encodeRecord("anchor", []byte("anchor-data")), readRaw(t, store.blobPath("blob")))
	assertOnlyBlobFiles(t, store)
}

// The last file is deleted by many goroutines at once: exactly one wins, the others see
// "file not found" (never "blob not found"), and the empty blob is still there.
func TestConcurrentDeleteLastFile_BlobStaysAndNobodySeesBlobNotFound(t *testing.T) {
	store := newStore(t)
	_, _, err := store.createBlob("blob", "only", []byte("only-data"))
	require.NoError(t, err)

	const n = 40
	var wg sync.WaitGroup
	results := make(chan error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- store.deleteFileFromBlob("blob", "only")
		}()
	}
	close(start)
	waitOrFail(t, &wg, 30*time.Second)
	close(results)

	wins := 0
	for err := range results {
		if err == nil {
			wins++
			continue
		}
		assertFileNotFound(t, err, "blob", "only")
		assert.False(t, isErr[*ErrBlobNotFound](err))
	}
	assert.Equal(t, 1, wins, "exactly one delete must succeed")

	assertEmptyBlobExists(t, store, "blob")
	assertOnlyBlobFiles(t, store)
}

// Concurrent creation with generated UIDs must never produce the same blob twice
// and must never lose a record.
func TestConcurrentGeneratedBlobUIDsAreUnique(t *testing.T) {
	store := newStore(t)

	const n = 200
	type result struct {
		blob, file string
		data       []byte
		err        error
	}
	var wg sync.WaitGroup
	results := make(chan result, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := fmt.Appendf(nil, "payload-%d", i)
			b, f, err := store.createBlob("", "", data)
			results <- result{b, f, data, err}
		}(i)
	}
	waitOrFail(t, &wg, 60*time.Second)
	close(results)

	seen := make(map[string]struct{}, n)
	for r := range results {
		require.NoError(t, r.err)
		_, dup := seen[r.blob]
		require.False(t, dup, "blob UID %s generated twice", r.blob)
		seen[r.blob] = struct{}{}

		got, err := store.getFileFromBlob(r.blob, r.file)
		require.NoError(t, err)
		assert.Equal(t, r.data, got)
	}
	assert.Len(t, seen, n)
	assertOnlyBlobFiles(t, store)
}

// Readers doing full scans while a writer appends must never see a torn record:
// a scan for a missing file must always end in a clean ErrFileNotFound.
func TestConcurrentReadersNeverObserveTornAppends(t *testing.T) {
	store := newStore(t)
	anchor := payload(777)
	_, _, err := store.createBlob("blob", "anchor", anchor)
	require.NoError(t, err)

	var stop atomic.Bool
	errs := make(chan error, 64)
	report := func(e error) {
		select {
		case errs <- e:
		default:
		}
	}

	var readers sync.WaitGroup
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for !stop.Load() {
				got, err := store.getFileFromBlob("blob", "anchor")
				if err != nil {
					report(fmt.Errorf("anchor read: %w", err))
					return
				}
				if !bytes.Equal(got, anchor) {
					report(errors.New("anchor corrupted"))
					return
				}
				_, err = store.getFileFromBlob("blob", "never-exists")
				if !isErr[*ErrFileNotFound](err) {
					report(fmt.Errorf("full scan saw an inconsistent blob: %v", err))
					return
				}
			}
		}()
	}

	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; i < 150; i++ {
			if _, _, err := store.createBlob("blob", fmt.Sprintf("w-%d", i), payload(500+i*13)); err != nil {
				report(fmt.Errorf("append %d: %w", i, err))
				return
			}
		}
	}()

	waitOrFail(t, &writer, 60*time.Second)
	stop.Store(true)
	waitOrFail(t, &readers, 60*time.Second)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	recs, clean := parseRaw(readRaw(t, store.blobPath("blob")))
	assert.True(t, clean)
	assert.Len(t, recs, 151)
}

// TestConcurrentMultiBlobLifecycle stresses lock striping (256 RWMutexes, guaranteed hash collisions)
// across 300 independent blobs being created, read, and deleted in parallel.
func TestConcurrentMultiBlobLifecycle(t *testing.T) {
	store := newStore(t)

	const blobsCount = 300
	var wg sync.WaitGroup
	errs := make(chan error, blobsCount)

	for i := 0; i < blobsCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p1 := fmt.Appendf(nil, "doc-%d", i)
			p2 := fmt.Appendf(nil, "img-%d", i)

			blobUID, f1, err := store.createBlob("", "", p1)
			if err != nil {
				errs <- err
				return
			}
			_, f2, err := store.createBlob(blobUID, "", p2)
			if err != nil {
				errs <- err
				return
			}

			got1, err := store.getFileFromBlob(blobUID, f1)
			if err != nil || !bytes.Equal(got1, p1) {
				errs <- fmt.Errorf("mismatch f1 in %s: %v", blobUID, err)
				return
			}

			if err := store.deleteFileFromBlob(blobUID, f1); err != nil {
				errs <- err
				return
			}

			got2, err := store.getFileFromBlob(blobUID, f2)
			if err != nil || !bytes.Equal(got2, p2) {
				errs <- fmt.Errorf("mismatch f2 in %s: %v", blobUID, err)
				return
			}

			if _, err := store.getFileFromBlob(blobUID, f1); !isErr[*ErrFileNotFound](err) {
				errs <- fmt.Errorf("deleted file f1 still visible in %s: %v", blobUID, err)
			}
		}(i)
	}

	waitOrFail(t, &wg, 60*time.Second)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	assertOnlyBlobFiles(t, store)
}
