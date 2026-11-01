package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const (
	maxUIDAttempts = 5
	maxFileSize    = 32 << 20  // 32 MiB
	maxBlobSize    = 128 << 20 // 128 MiB
)

var (
	locks [256]sync.RWMutex
)

// storeHooks lets tests simulate failures that cannot be provoked on a real file system.
type storeHooks struct {
	syncDir func(dir string) error
}

// BlobStore stores many files in one blob file as a stream of records:
//
//	uint16 LE uid length | uid | uint64 LE data length | data
//
// Deleting a file removes only its records from the blob. The blob itself is never deleted,
// even when its last file is removed: it stays as an empty blob.
//
// The locks are in-process only: BlobPath must not be shared between several processes.
// Different strings that point to the same directory (relative path, "..", trailing slash,
// symlink) share the same locks.
type BlobStore struct {
	BlobPath string

	hooks *storeHooks
}

func (s BlobStore) blobPath(blobUID string) string {
	return filepath.Join(s.BlobPath, blobUID+".blob")
}

// lockKey returns a canonical form of the blob path: absolute, cleaned and with symlinks
// resolved when the directory exists. All aliases of one physical file map to the same key.
func (s BlobStore) lockKey(blobUID string) string {
	dir := filepath.Clean(s.BlobPath)
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	return filepath.Join(dir, blobUID+".blob")
}

// Locking is based on the canonical path, ensuring that different storage locations do not interfere with each other
// more than necessary (collisions are acceptable; they simply result in sharing the same mutex).
func (s BlobStore) getBlobLock(blobUID string) *sync.RWMutex {
	h := fnv.New32a()
	h.Write([]byte(s.lockKey(blobUID)))
	return &locks[h.Sum32()%256]
}

// syncDir flushes a directory to disk. Creating, renaming or removing a file is only
// durable after the directory that contains it has been synced.
// Windows cannot flush a directory handle, so there the directory is only opened and closed.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return d.Close()
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

func (s BlobStore) syncDirectory() error {
	if s.hooks != nil && s.hooks.syncDir != nil {
		return s.hooks.syncDir(s.BlobPath)
	}
	return syncDir(s.BlobPath)
}

// checkPayloadFits returns ErrTruncatedRecord if dataLen bytes do not remain
// between the current position of f and the end of the file.
// Seek past the end of a file does not fail, so truncated records must be detected explicitly.
// The file must be read without buffering so that the current offset is accurate.
func checkPayloadFits(f *os.File, fileSize int64, dataLen uint64) error {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	available := max(fileSize-offset, 0)
	if offset > fileSize || dataLen > uint64(available) {
		return &ErrTruncatedRecord{
			Offset:    offset,
			FileSize:  fileSize,
			Needed:    dataLen,
			Available: available,
		}
	}
	return nil
}

// truncatedHeader turns an EOF that happened in the middle of a record header into
// io.ErrUnexpectedEOF with the offset where the data ended. Other errors pass through unchanged.
func truncatedHeader(f *os.File, err error) error {
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	offset, seekErr := f.Seek(0, io.SeekCurrent)
	if seekErr != nil {
		return fmt.Errorf("truncated blob record header: %w", io.ErrUnexpectedEOF)
	}
	return fmt.Errorf("truncated blob record header at offset %d: %w", offset, io.ErrUnexpectedEOF)
}

// readRecordHeader reads one record header (uid and data length) and checks that the payload
// fits into the file. On success f is positioned at the start of the payload.
//
// It returns exactly io.EOF only when the file ends cleanly between two records.
// Any EOF in the middle of a record is reported as io.ErrUnexpectedEOF
// (or ErrTruncatedRecord, which unwraps to it).
func readRecordHeader(f *os.File, fileSize int64) ([]byte, uint64, error) {
	var uidLen uint16
	if err := binary.Read(f, binary.LittleEndian, &uidLen); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.EOF
		}
		return nil, 0, truncatedHeader(f, err)
	}

	uid := make([]byte, uidLen)
	if _, err := io.ReadFull(f, uid); err != nil {
		return nil, 0, truncatedHeader(f, err)
	}

	var dataLen uint64
	if err := binary.Read(f, binary.LittleEndian, &dataLen); err != nil {
		return nil, 0, truncatedHeader(f, err)
	}

	if err := checkPayloadFits(f, fileSize, dataLen); err != nil {
		return nil, 0, err
	}
	return uid, dataLen, nil
}

// validateBlobFile checks that the whole file is a well-formed record stream
// (it only reads headers and skips payloads). A record larger than maxFileSize is also
// treated as damage: readers refuse to scan past it, so everything behind it would be unreachable.
// An empty file is a valid blob with no records.
func validateBlobFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	for {
		_, dataLen, err := readRecordHeader(f, size)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if dataLen > maxFileSize {
			return &ErrFileTooLarge{Size: int64(dataLen), Limit: maxFileSize}
		}
		if _, err := f.Seek(int64(dataLen), io.SeekCurrent); err != nil {
			return err
		}
	}
}

// getBlobSize returns the size of the blob file with the given UID.
func (s BlobStore) getBlobSize(blobUID string) (int64, error) {
	if s.BlobPath == "" {
		return 0, &ErrEmptyBlobPath{}
	}

	if !validUID(blobUID) {
		return 0, &ErrInvalidUID{UID: blobUID}
	}

	lock := s.getBlobLock(blobUID)
	lock.RLock()
	defer lock.RUnlock()

	info, err := os.Stat(s.blobPath(blobUID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, &ErrBlobNotFound{BlobUID: blobUID}
		}
		return 0, err
	}
	return info.Size(), nil
}

// writeRecord writes one record at the end of f and syncs it.
func writeRecord(f *os.File, uid []byte, data []byte) error {
	if err := binary.Write(f, binary.LittleEndian, uint16(len(uid))); err != nil {
		return err
	}
	if _, err := f.Write(uid); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint64(len(data))); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// rollbackAppend undoes a failed append. It deliberately works from the path and does not
// reuse the original file handle: after a failed Close() that handle is not usable.
// An existing blob is truncated back to origSize; a blob created by the failed append is removed.
func (s BlobStore) rollbackAppend(path string, existed bool, origSize int64) error {
	if existed {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		if err := f.Truncate(origSize); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	}

	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.syncDirectory()
}

// appendRecord appends a single file record to the blob.
// If exclusive is true, it fails with os.ErrExist when the blob file already exists.
// An existing blob is validated first: appending after a damaged record (for example a torn
// write left by a crash) would make the new record unreachable, so the append is refused instead.
// If writing, closing or syncing fails, the blob is rolled back to its previous state.
// If the rollback itself fails, the returned error contains both failures and the state of
// the blob must be considered unknown.
func (s BlobStore) appendRecord(blobUID string, fileUID string, fileData []byte, exclusive bool) error {
	lock := s.getBlobLock(blobUID)
	lock.Lock()
	defer lock.Unlock()

	path := s.blobPath(blobUID)

	var origSize int64
	existed := false
	if info, err := os.Stat(path); err == nil {
		existed = true
		origSize = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if exclusive && existed {
		return os.ErrExist
	}

	uidBytes := []byte(fileUID)
	recordSize := int64(2 + len(uidBytes) + 8 + len(fileData))
	if recordSize > maxBlobSize-origSize {
		return &ErrNotEnoughSpace{Needed: recordSize, Used: origSize}
	}

	if existed {
		if err := validateBlobFile(path); err != nil {
			return fmt.Errorf("refusing to append to damaged blob %q: %w", blobUID, err)
		}
	}

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if exclusive {
		flags |= os.O_EXCL
	}

	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return err
	}

	err = writeRecord(f, uidBytes, fileData)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil && !existed {
		// A new directory entry is only durable after the directory is synced.
		err = s.syncDirectory()
	}
	if err == nil {
		return nil
	}

	if rollbackErr := s.rollbackAppend(path, existed, origSize); rollbackErr != nil {
		return errors.Join(err, fmt.Errorf("rollback of blob %q failed, its state is unknown: %w", blobUID, rollbackErr))
	}
	return err
}

// createBlob appends a file to a blob and returns: the blob UID, the file UID, and an error.
// If blobUID is empty, a new blob with a unique generated UID is created.
// If blobUID is given, the file is appended to that blob (it is created if missing).
func (s BlobStore) createBlob(blobUID string, fileUID string, fileData []byte) (string, string, error) {
	if s.BlobPath == "" {
		return "", "", &ErrEmptyBlobPath{}
	}

	if fileUID == "" {
		var err error
		fileUID, err = generateShortUID()
		if err != nil {
			return "", "", err
		}
	} else if !validUID(fileUID) {
		return "", "", &ErrInvalidUID{UID: fileUID}
	}

	generated := blobUID == ""
	if !generated && !validUID(blobUID) {
		return "", "", &ErrInvalidUID{UID: blobUID}
	}

	if uint64(len(fileData)) > maxFileSize {
		return "", "", &ErrFileTooLarge{Size: int64(len(fileData)), Limit: maxFileSize}
	}

	if err := os.MkdirAll(s.BlobPath, 0755); err != nil {
		return "", "", fmt.Errorf("failed to create blob dir: %w", err)
	}

	var err error
	for attempt := 0; attempt < maxUIDAttempts; attempt++ {
		if generated {
			blobUID, err = generateShortUID()
			if err != nil {
				return "", "", err
			}
		}

		err = s.appendRecord(blobUID, fileUID, fileData, generated)
		if generated && errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", "", err
		}
		return blobUID, fileUID, nil
	}

	return "", "", fmt.Errorf("failed to generate unique blob uid: %w", err)
}

// getFileFromBlob returns the contents of a file stored in a blob.
// ErrBlobNotFound means the blob does not exist; ErrFileNotFound means the blob exists
// (possibly empty) but does not contain the file.
func (s BlobStore) getFileFromBlob(blobUID string, targetFileUID string) ([]byte, error) {
	if s.BlobPath == "" {
		return nil, &ErrEmptyBlobPath{}
	}

	if !validUID(blobUID) {
		return nil, &ErrInvalidUID{UID: blobUID}
	}
	if !validUID(targetFileUID) {
		return nil, &ErrInvalidUID{UID: targetFileUID}
	}

	lock := s.getBlobLock(blobUID)
	lock.RLock()
	defer lock.RUnlock()

	f, err := os.Open(s.blobPath(blobUID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &ErrBlobNotFound{BlobUID: blobUID}
		}
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	for {
		uid, dataLen, err := readRecordHeader(f, fileSize)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		if dataLen > maxFileSize {
			return nil, &ErrFileTooLarge{Size: int64(dataLen), Limit: maxFileSize}
		}

		if string(uid) == targetFileUID {
			data := make([]byte, dataLen)
			if _, err := io.ReadFull(f, data); err != nil {
				return nil, err
			}
			return data, nil
		}

		if _, err := f.Seek(int64(dataLen), io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	return nil, &ErrFileNotFound{BlobUID: blobUID, FileUID: targetFileUID}
}

// deleteFileFromBlob deletes a file from a blob.
// All records with the given file UID are removed. The blob is rewritten into a temporary
// file which atomically replaces the original. The blob itself is never deleted: if the last
// file is removed, the blob stays as an empty file.
//
// Error semantics:
//   - any error other than ErrNotDurable: the blob was not changed;
//   - ErrNotDurable: the delete WAS applied, only the directory sync failed. Do not retry blindly.
func (s BlobStore) deleteFileFromBlob(blobUID string, targetFileUID string) error {
	if s.BlobPath == "" {
		return &ErrEmptyBlobPath{}
	}

	if !validUID(blobUID) {
		return &ErrInvalidUID{UID: blobUID}
	}
	if !validUID(targetFileUID) {
		return &ErrInvalidUID{UID: targetFileUID}
	}

	lock := s.getBlobLock(blobUID)
	lock.Lock()
	defer lock.Unlock()

	srcPath := s.blobPath(blobUID)
	tmpPath := srcPath + ".tmp"

	src, err := os.Open(srcPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &ErrBlobNotFound{BlobUID: blobUID}
		}
		return err
	}
	defer src.Close()

	info, err := src.Stat()
	if err != nil {
		return err
	}
	srcSize := info.Size()

	dst, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	success := false
	defer func() {
		_ = dst.Close()
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	found := false

	for {
		uid, dataLen, err := readRecordHeader(src, srcSize)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		if string(uid) == targetFileUID {
			found = true
			if _, err := src.Seek(int64(dataLen), io.SeekCurrent); err != nil {
				return err
			}
			continue
		}

		if err := binary.Write(dst, binary.LittleEndian, uint16(len(uid))); err != nil {
			return err
		}
		if _, err := dst.Write(uid); err != nil {
			return err
		}
		if err := binary.Write(dst, binary.LittleEndian, dataLen); err != nil {
			return err
		}
		if _, err := io.CopyN(dst, src, int64(dataLen)); err != nil {
			return err
		}
	}

	if err := src.Close(); err != nil {
		return err
	}

	if !found {
		return &ErrFileNotFound{BlobUID: blobUID, FileUID: targetFileUID}
	}

	if err := dst.Sync(); err != nil {
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}

	// Even if no records are left, the (now empty) temporary file replaces the blob.
	if err := os.Rename(tmpPath, srcPath); err != nil {
		return err
	}
	success = true

	if err := s.syncDirectory(); err != nil {
		return &ErrNotDurable{Op: "delete", Err: err}
	}
	return nil
}
