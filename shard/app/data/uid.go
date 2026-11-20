package storage

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
)

const (
	alphabet  = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	hexDigits = "0123456789ABCDEF"
)

var ErrInvalidID = errors.New("invalid id")

var validBase32 = func() [256]bool {
	var t [256]bool
	for i := range len(alphabet) {
		t[alphabet[i]] = true
	}
	return t
}()

type ID struct {
	dir  uint16
	file [8]byte
}

func NewID() ID {
	var b [10]byte
	_,_ = rand.Read(b[:])

	var id ID
	id.dir = binary.BigEndian.Uint16(b[:2])
	for i := range id.file {
		id.file[i] = alphabet[b[2+i]&31]
	}
	return id
}

func ParseID(s string) (ID, error) {
	if len(s) != 12 {
		return ID{}, ErrInvalidID
	}
	var dir uint16
	for i := range 4 {
		c := s[i]
		var v uint16
		switch {
		case c >= '0' && c <= '9':
			v = uint16(c - '0')
		case c >= 'A' && c <= 'F':
			v = uint16(c - 'A' + 10)
		default:
			return ID{}, ErrInvalidID
		}
		dir = (dir << 4) | v
	}

	var id ID
	id.dir = dir
	for i := range id.file {
		c := s[4+i]
		if !validBase32[c] {
			return ID{}, ErrInvalidID
		}
		id.file[i] = c
	}
	return id, nil
}

func (id ID) Valid() bool {
	for _, c := range id.file {
		if !validBase32[c] {
			return false
		}
	}
	return true
}

func (id ID) String() string {
	if !id.Valid() {
		return ""
	}
	var b [12]byte
	b[0] = hexDigits[id.dir>>12]
	b[1] = hexDigits[(id.dir>>8)&0x0F]
	b[2] = hexDigits[(id.dir>>4)&0x0F]
	b[3] = hexDigits[id.dir&0x0F]
	copy(b[4:], id.file[:])
	return string(b[:])
}
