package data

import "github.com/jmoiron/sqlx"

type fileMetadata struct {
	Id          int    `db:"id"`
	FileInBlob  bool   `db:"blob"`
	FilenameUID string `db:"filename"`
	Path        string `db:"path"`
}

func createFileMetadataTable(db *sqlx.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS file_metadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_in_blob BOOLEAN,
			filename_uid VARCHAR(64),
			path VARCHAR(255)
			)
			`)
	return err
}
