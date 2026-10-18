package data

type FileMetadata struct {
	Id         int    `json:"id"`
	FileInBlob bool   `db:"blob"`
	Filename   string `db:"filename"`
	Path       string `db:"path"`
}

func getFileMetadata(id int) (FileMetadata, error) {

}

func addFileMetadata(fileMetadata FileMetadata) (FileMetadata, error) {

}

func editFileMetadata(id int) (FileMetadata, error) {

}

func deleteFileMetadata(id int) error {

}
