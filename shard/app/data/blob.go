package data

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	BlobDir       = "./blob" // Get from env, not hardcoded!!! Dummy!!!
	MaxFileSizeMB = 10       // Get from env, not hardcoded!!! Dummy!!!
)

var (
	ErrFileTooLarge = errors.New("file size exceeds maximum allowed size for blob")
	ErrFileNotFound = errors.New("file not found in blob")
	locks           [256]sync.RWMutex
)

func generateShortUID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func blobPath(blobUID string) string {
	return filepath.Join(BlobDir, blobUID+".blob")
}

func getBlobLock(blobUID string) *sync.RWMutex {
	h := fnv.New32a()
	h.Write([]byte(blobUID))
	return &locks[h.Sum32()%256]
}

func createBlob(blobUID string, fileUID string, fileData []byte) (string, string, error) {
	maxBytes := int64(MaxFileSizeMB) * 1024 * 1024
	if int64(len(fileData)) >= maxBytes {
		return "", "", ErrFileTooLarge
	}

	if err := os.MkdirAll(BlobDir, 0755); err != nil {
		return "", "", fmt.Errorf("failed to create blob dir: %w", err)
	}

	if blobUID == "" {
		var err error
		blobUID, err = generateShortUID()
		if err != nil {
			return "", "", err
		}
	}

	lock := getBlobLock(blobUID)
	lock.Lock()
	defer lock.Unlock()

	if fileUID == "" {
		var err error
		fileUID, err = generateShortUID()
		if err != nil {
			return "", "", err
		}
	}

	f, err := os.OpenFile(blobPath(blobUID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	uidBytes := []byte(fileUID)
	if err := binary.Write(f, binary.LittleEndian, uint16(len(uidBytes))); err != nil {
		return "", "", err
	}
	if _, err := f.Write(uidBytes); err != nil {
		return "", "", err
	}
	if err := binary.Write(f, binary.LittleEndian, uint64(len(fileData))); err != nil {
		return "", "", err
	}
	if _, err := f.Write(fileData); err != nil {
		return "", "", err
	}

	return blobUID, fileUID, nil
}

func getFileFromBlob(blobUID string, targetFileUID string) ([]byte, error) {
	lock := getBlobLock(blobUID)
	lock.RLock()
	defer lock.RUnlock()

	f, err := os.Open(blobPath(blobUID))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	for {
		var uidLen uint16
		err := binary.Read(f, binary.LittleEndian, &uidLen)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		uidBytes := make([]byte, uidLen)
		if _, err := io.ReadFull(f, uidBytes); err != nil {
			return nil, err
		}

		var dataLen uint64
		if err := binary.Read(f, binary.LittleEndian, &dataLen); err != nil {
			return nil, err
		}

		if string(uidBytes) == targetFileUID {
			data := make([]byte, dataLen)
			if _, err := io.ReadFull(f, data); err != nil {
				return nil, err
			}
			return data, nil
		}

		// Если файл не тот — перепрыгиваем его содержимое без чтения в память
		if _, err := f.Seek(int64(dataLen), io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	return nil, ErrFileNotFound
}

func deleteFileFromBlob(blobUID string, targetFileUID string) error {
	lock := getBlobLock(blobUID)
	lock.Lock()
	defer lock.Unlock()

	srcPath := blobPath(blobUID)
	tmpPath := srcPath + ".tmp"

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	success := false
	defer func() {
		dst.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()

	found := false
	remainingFiles := 0

	for {
		var uidLen uint16
		err := binary.Read(src, binary.LittleEndian, &uidLen)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		uidBytes := make([]byte, uidLen)
		if _, err := io.ReadFull(src, uidBytes); err != nil {
			return err
		}

		var dataLen uint64
		if err := binary.Read(src, binary.LittleEndian, &dataLen); err != nil {
			return err
		}

		if string(uidBytes) == targetFileUID {
			found = true
			if _, err := src.Seek(int64(dataLen), io.SeekCurrent); err != nil {
				return err
			}
			continue
		}

		remainingFiles++
		if err := binary.Write(dst, binary.LittleEndian, uidLen); err != nil {
			return err
		}
		if _, err := dst.Write(uidBytes); err != nil {
			return err
		}
		if err := binary.Write(dst, binary.LittleEndian, dataLen); err != nil {
			return err
		}
		if _, err := io.CopyN(dst, src, int64(dataLen)); err != nil {
			return err
		}
	}

	src.Close()
	dst.Close()

	if !found {
		return ErrFileNotFound
	}

	if remainingFiles == 0 {
		return os.Remove(srcPath)
	}

	if err := os.Rename(tmpPath, srcPath); err != nil {
		return err
	}

	success = true
	return nil
}
