package api

import (
	"github.com/gofiber/fiber/v3"
	"github.com/jmoiron/sqlx"
)

type GetFileMetadataHandler struct {
	db *sqlx.DB
}

func (handler *GetFileMetadataHandler) NewGetFileMetadataHandler(ctx fiber.Ctx) *GetFileMetadataHandler {

}

type GetFileWithMetadataHandler struct {
	db *sqlx.DB
}

func (handler *GetFileWithMetadataHandler) NewGetFileWithMetadataHandler(ctx fiber.Ctx) *GetFileWithMetadataHandler {

}

type DeleteFileHandler struct {
	db *sqlx.DB
}

func (handler *DeleteFileHandler) NewDeleteFileHandler(ctx fiber.Ctx) *DeleteFileHandler {

}

type GetServerHealthHandler struct {
	db *sqlx.DB
}

func (handler *GetServerHealthHandler) NewGetServerHealthHandler(ctx fiber.Ctx) *GetServerHealthHandler {

}
