package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"comment-processing-service/internal/config"
	"comment-processing-service/internal/db"
	"comment-processing-service/internal/repository"
	"comment-processing-service/internal/worker"
	"comment-processing-service/proto/sentimentpb"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	config.LoadEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := config.GetEnv("DATABASE_URL", "")
	if dsn == "" {
		logrus.StandardLogger().Fatal("DATABASE_URL is required")
	}

	redisAddr := config.GetEnv("REDIS_ADDR", "127.0.0.1:6379")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := config.GetEnvInt("REDIS_DB", 0)

	grpcAddr := config.GetEnv("SENTIMENT_GRPC_ADDR", "127.0.0.1:50051")
	workerID := config.GetEnv("WORKER_ID", "worker-1")

	maxAttempts := config.GetEnvInt("MAX_ATTEMPTS", 10)
	baseBackoff := config.GetEnvDuration("BACKOFF_BASE", 200*time.Millisecond)
	maxBackoff := config.GetEnvDuration("BACKOFF_MAX", 30*time.Second)
	jitter := config.GetEnvDuration("BACKOFF_JITTER", 200*time.Millisecond)
	lockTTL := config.GetEnvDuration("LOCK_TTL", 30*time.Second)
	shared := config.Load()
	textCacheTTL := config.GetEnvDuration("TEXT_CACHE_TTL", 7*24*time.Hour)
	rateLimit := config.GetEnvInt("RPC_RATE_LIMIT", 100)
	rpcTimeout := config.GetEnvDuration("RPC_TIMEOUT", 2*time.Second)

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		logrus.StandardLogger().WithError(err).Fatal("db connect")
	}
	defer pool.Close()

	// NewClient returns a go-redis client with the provided options.
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	defer func() { _ = redisClient.Close() }()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.StandardLogger().WithError(err).Fatal("grpc dial")
	}
	defer func() { _ = conn.Close() }()

	client := sentimentpb.NewSentimentServiceClient(conn)

	cfg := worker.Config{
		MaxAttempts:     maxAttempts,
		BaseBackoff:     baseBackoff,
		MaxBackoff:      maxBackoff,
		Jitter:          jitter,
		LockTTL:         lockTTL,
		RetryZSet:       shared.RetryZSet,
		TextCacheTTL:    textCacheTTL,
		RateLimitPerSec: rateLimit,
		RPCTimeout:      rpcTimeout,
	}

	repo := repository.NewRepository(pool)
	proc := worker.NewProcessor(repo, redisClient, client, cfg, workerID, logrus.StandardLogger())
	logrus.StandardLogger().WithFields(logrus.Fields{
		"worker_id": workerID,
		"grpc":      grpcAddr,
	}).Info("worker started")

	if err := proc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logrus.StandardLogger().WithError(err).Fatal("worker stopped")
	}
}
