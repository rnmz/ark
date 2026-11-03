package data

import (
	"io"

	"github.com/jmoiron/sqlx"
)

type FileDataStruct struct {
	TargetPath string
	CachePath  string
	DB         *sqlx.DB
}

func InitData(db *sqlx.DB, targetPath, cachePath string) *FileDataStruct {

}

// UploadFile Upload new file
func (f *FileDataStruct) UploadFile(fileStream io.Reader) error {

}

func (f *FileDataStruct) GetFile(uid string) {

}

func (f *FileDataStruct) DeleteFile() {

}
