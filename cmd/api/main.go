package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"

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
		log.Fatal("DATABASE_URL is required")
	}
	addr := config.GetEnv("API_ADDR", ":8080")

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	repo := repository.NewRepository(pool)
	commentController := controller.NewCommentController(repo, log.Default())

	app := fiber.New()
	app.Get("/comments", commentController.ListComments)
	app.Get("/comments/:commentId", commentController.GetComment)

	go func() {
		<-ctx.Done()
		_ = app.Shutdown()
	}()

	log.Printf("api listening on %s", addr)
	if err := app.Listen(addr); err != nil {
		log.Fatalf("http server error: %v", err)
	}
}
