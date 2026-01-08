package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"

	"comment-processing-service/internal/config"
	"comment-processing-service/internal/controller"
	"comment-processing-service/internal/db"
	"comment-processing-service/internal/repository"
)

func main() {
	config.LoadEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := config.GetEnv("DATABASE_URL", "")
	if dsn == "" {
		logrus.StandardLogger().Fatal("DATABASE_URL is required")
	}
	addr := config.GetEnv("API_ADDR", ":8080")

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		logrus.StandardLogger().WithError(err).Fatal("db connect")
	}
	defer pool.Close()

	repo := repository.NewRepository(pool)
	logger := logrus.StandardLogger()
	commentController := controller.NewCommentController(repo, logger)

	app := fiber.New()
	app.Get("/comments", commentController.ListComments)
	app.Get("/comments/:commentId", commentController.GetComment)

	go func() {
		<-ctx.Done()
		_ = app.Shutdown()
	}()

	logger.WithField("addr", addr).Info("api listening")
	if err := app.Listen(addr); err != nil {
		logger.WithError(err).Fatal("http server error")
	}
}
