package main

import (
	"context"
	"log"
	"strings"
	"time"

	"comment-processing-service/internal/config"
	"comment-processing-service/internal/db"
	kafkautil "comment-processing-service/internal/kafka"
	"comment-processing-service/internal/outbox"
	"comment-processing-service/internal/repository"

	"github.com/segmentio/kafka-go"
)

func main() {
	config.LoadEnv()

	ctx := context.Background()

	dsn := config.GetEnv("DATABASE_URL", "")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	brokers := config.GetEnv("KAFKA_BROKERS", "127.0.0.1:9092")
	defaultTopic := config.GetEnv("KAFKA_PROCESSED_TOPIC", "processed-comments")
	batchSize := config.GetEnvInt("BATCH_SIZE", 100)
	loopInterval := config.GetEnvDuration("LOOP_INTERVAL", 200*time.Millisecond)

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	repo := repository.NewOutboxRepository(pool)

	brokerList := strings.Split(brokers, ",")
	if err := kafkautil.EnsureTopics(brokerList, []string{defaultTopic}); err != nil {
		log.Fatalf("ensure topic: %v", err)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokerList,
		Balancer: &kafka.Hash{},
	})
	defer func() { _ = writer.Close() }()

	log.Printf("outbox publisher started brokers=%s topic=%s", brokers, defaultTopic)

	cfg := outbox.Config{
		DefaultTopic: defaultTopic,
		BatchSize:    batchSize,
		LoopInterval: loopInterval,
	}
	service := outbox.NewService(repo, writer, cfg)
	if err := service.Run(ctx); err != nil {
		log.Fatalf("outbox stopped: %v", err)
	}
}
