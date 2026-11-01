package server

import (
	"ark/shard/app/config"
	"ark/shard/app/security"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
)

func CreateFiberApp(config config.AppConfig) *fiber.App {
	app := fiber.New(fiber.Config{
		JSONEncoder: sonic.Marshal,
		JSONDecoder: sonic.Unmarshal,
	})

	app.Use(cors.New(
		cors.Config{
			AllowOrigins: []string{"*"},
			AllowMethods: []string{"GET", "POST", "DELETE", "OPTIONS"},
			AllowHeaders: []string{"Origin", "Content-Length", "Content-Type", "X-Signature", "Accept-Encoding", "X-Requested-With", "Authorization", "Cache-Control"},
		}),
	)

	app.Use(xSignatureMiddleware(config.SecretKey))
	app.Use(optionsMiddleware())

	return app
}

// --- Middleware ---

func xSignatureMiddleware(secret string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		signature := ctx.Get("X-Signature")
		if signature == "" {
			slog.Info("[Fiber] Missing signature")
			return fiber.NewError(fiber.StatusUnauthorized, "Missing signature")
		}

		ok, err := security.Verify(
			[]byte(secret),
			ctx.Body(),
			[]byte(signature),
		)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "Signature verification failed")
		}

		if !ok {
			slog.Info("[Fiber] Invalid signature")
			return fiber.NewError(fiber.StatusUnauthorized, "Invalid signature")
		}

		return ctx.Next()
	}
}

func optionsMiddleware() fiber.Handler {
	return func(ctx fiber.Ctx) error {
		if ctx.Method() == "OPTIONS" {
			return ctx.SendStatus(fiber.StatusNoContent)
		}
		return ctx.Next()
	}
}
