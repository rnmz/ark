package data

import (
	"fmt"
	"io"
)

type ErrInvalidUID struct {
	UID string
}

func (e *ErrInvalidUID) Error() string {
	return fmt.Sprintf("invalid uid: %s", e.UID)
}

type ErrNotEnoughSpace struct {
	Needed int64
	Used   int64
}

func (e *ErrNotEnoughSpace) Error() string {
	return fmt.Sprintf(
		"not enough space: %d needed, %d used",
		e.Needed,
		e.Used,
	)
}

type ErrTruncatedRecord struct {
	Offset    int64
	FileSize  int64
	Needed    uint64
	Available int64
}

func (e *ErrTruncatedRecord) Error() string {
	return fmt.Sprintf(
		"truncated blob record at offset %d (file size %d): header requires %d bytes, only %d available",
		e.Offset,
		e.FileSize,
		e.Needed,
		e.Available,
	)
}

func (e *ErrTruncatedRecord) Unwrap() error {
	return io.ErrUnexpectedEOF
}

type ErrFileTooLarge struct {
	Size  int64
	Limit int64
}

func (e *ErrFileTooLarge) Error() string {
	return fmt.Sprintf(
		"file size %d exceeds maximum allowed size %d",
		e.Size,
		e.Limit,
	)
}

type ErrFileNotFound struct {
	BlobUID string
	FileUID string
}

func (e *ErrFileNotFound) Error() string {
	return fmt.Sprintf(
		"file %q not found in blob %q",
		e.FileUID,
		e.BlobUID,
	)
}

type ErrBlobNotFound struct {
	BlobUID string
}

func (e *ErrBlobNotFound) Error() string {
	return fmt.Sprintf("blob %q not found", e.BlobUID)
}

type ErrEmptyBlobPath struct{}

func (e *ErrEmptyBlobPath) Error() string {
	return "blob path is empty"
}

// ErrNotDurable means: the operation WAS applied to the file system, but the directory sync
// that makes it survive a power loss failed. Do not blindly retry such an operation:
// a repeated delete will report ErrFileNotFound / ErrBlobNotFound because the first one succeeded.
type ErrNotDurable struct {
	Op  string
	Err error
}

func (e *ErrNotDurable) Error() string {
	return fmt.Sprintf("%s was applied but could not be made durable: %v", e.Op, e.Err)
}

func (e *ErrNotDurable) Unwrap() error {
	return e.Err
}
