package api

import "github.com/gofiber/fiber/v3"

func router(app *fiber.App) {
	app.Get("/file", func(c *fiber.Ctx) error {
		return nil
	})

	app.Post("/file", func(c *fiber.Ctx) error {
		return nil
	})

	app.Delete("/file", func(c *fiber.Ctx) error {
		return nil
	})

	app.Use(func(ctx fiber.Ctx) error {
		return ctx.SendStatus(fiber.StatusNotFound)
	})
}
