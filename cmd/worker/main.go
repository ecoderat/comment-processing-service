package main

import (
	"context"
	"log"
	"os"
	"time"

	"comment-processing-service/internal/config"
	"comment-processing-service/internal/db"
	"comment-processing-service/internal/repository"
	"comment-processing-service/internal/worker"
	"comment-processing-service/proto/sentimentpb"

	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	config.LoadEnv()

	ctx := context.Background()

	dsn := config.GetEnv("DATABASE_URL", "")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
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
	retryZSet := config.GetEnv("RETRY_ZSET", "retry:zset")
	textCacheTTL := config.GetEnvDuration("TEXT_CACHE_TTL", 7*24*time.Hour)
	rateLimit := config.GetEnvInt("RPC_RATE_LIMIT", 100)
	rpcTimeout := config.GetEnvDuration("RPC_TIMEOUT", 2*time.Second)

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	// NewClient returns a go-redis client with the provided options.
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	defer func() { _ = redisClient.Close() }()

	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := sentimentpb.NewSentimentServiceClient(conn)

	cfg := worker.Config{
		MaxAttempts:     maxAttempts,
		BaseBackoff:     baseBackoff,
		MaxBackoff:      maxBackoff,
		Jitter:          jitter,
		LockTTL:         lockTTL,
		RetryZSet:       retryZSet,
		TextCacheTTL:    textCacheTTL,
		RateLimitPerSec: rateLimit,
		RPCTimeout:      rpcTimeout,
	}

	repo := repository.NewRepository(pool)
	proc := worker.NewProcessor(repo, redisClient, client, cfg, workerID)
	log.Printf("worker started id=%s grpc=%s", workerID, grpcAddr)

	if err := proc.Run(ctx); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}
