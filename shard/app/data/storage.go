package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	openFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	attempts  = 5
)

var (
	ErrBadExt    = errors.New("invalid file extension")
	ErrCollision = errors.New("too many uid collisions")
)

func validExt(ext string) bool {
	if ext == "" {
		return true
	}
	if len(ext) < 2 || len(ext) > 9 || ext[0] != '.' {
		return false
	}
	for i := 1; i < len(ext); i++ {
		c := ext[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func (id ID) relPath(ext string) (string, error) {
	if !id.Valid() {
		return "", ErrInvalidID
	}
	if !validExt(ext) {
		return "", fmt.Errorf("%w: %q", ErrBadExt, ext)
	}
	s := id.String()
	return filepath.Join(s[0:2], s[2:4], s[4:]+ext), nil
}

type Store struct {
	root *os.Root
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating store dir: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("opening store root: %w", err)
	}
	return &Store{root: root}, nil
}

func (s *Store) Close() error { return s.root.Close() }

func (s *Store) Open(id ID, ext string) (*os.File, error) {
	rel, err := id.relPath(ext)
	if err != nil {
		return nil, err
	}
	f, err := s.root.Open(rel)
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", rel, err)
	}
	return f, nil
}

func (s *Store) UploadFile(ext string, r io.Reader) (ID, error) {
	if !validExt(ext) {
		return ID{}, fmt.Errorf("%w: %q", ErrBadExt, ext)
	}

	tmp, tmpName, err := s.createTemp()
	if err != nil {
		return ID{}, err
	}
	defer func() {
		if tmp != nil {
			_ = tmp.Close()
		}
		_ = s.root.Remove(tmpName)
	}()

	if _, err := io.Copy(tmp, r); err != nil {
		return ID{}, fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return ID{}, fmt.Errorf("syncing temp file: %w", err)
	}
	err = tmp.Close()
	tmp = nil
	if err != nil {
		return ID{}, fmt.Errorf("closing temp file: %w", err)
	}

	for range attempts {
		id := NewID()
		rel, err := id.relPath(ext)
		if err != nil {
			return ID{}, err
		}

		err = s.root.Link(tmpName, rel)
		if errors.Is(err, fs.ErrNotExist) {
			if mkdirErr := s.root.MkdirAll(filepath.Dir(rel), 0o755); mkdirErr != nil {
				return ID{}, fmt.Errorf("creating shard dir: %w", mkdirErr)
			}
			err = s.root.Link(tmpName, rel)
		}
		switch {
		case err == nil:
			return id, nil
		case errors.Is(err, fs.ErrExist):
			continue
		default:
			return ID{}, fmt.Errorf("publishing %s: %w", rel, err)
		}
	}
	return ID{}, ErrCollision
}

func (s *Store) DeleteFile(id ID, ext string) error {
	rel, err := id.relPath(ext)
	if err != nil {
		return err
	}
	if err := s.root.Remove(rel); err != nil {
		return fmt.Errorf("deleting file %s: %w", rel, err)
	}
	return nil
}

func (s *Store) createTemp() (*os.File, string, error) {
	for range attempts {
		name := ".tmp-" + NewID().String()
		f, err := s.root.OpenFile(name, openFlags, 0o644)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("creating temp file: %w", err)
		}
		return f, name, nil
	}
	return nil, "", ErrCollision
}

func (s *Store) CleanTmp(olderThan time.Duration) error {
	ents, err := fs.ReadDir(s.root.FS(), ".")
	if err != nil {
		return fmt.Errorf("reading store root: %w", err)
	}
	var errs []error
	for _, e := range ents {
		if !e.Type().IsRegular() || !strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		info, err := e.Info()
		if err != nil || time.Since(info.ModTime()) <= olderThan {
			continue
		}
		if err := s.root.Remove(e.Name()); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("removing orphan temp %s: %w", e.Name(), err))
		}
	}
	return errors.Join(errs...)
}
