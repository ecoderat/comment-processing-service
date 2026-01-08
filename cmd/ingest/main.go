package main

import (
	"context"
	"os"
	"strings"
	"time"

	"comment-processing-service/internal/cache"
	"comment-processing-service/internal/config"
	"comment-processing-service/internal/db"
	"comment-processing-service/internal/ingest"
	kafkautil "comment-processing-service/internal/kafka"
	"comment-processing-service/internal/repository"

	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	config.LoadEnv()

	ctx := context.Background()

	brokers := config.GetEnv("KAFKA_BROKERS", "127.0.0.1:9092")
	topic := config.GetEnv("KAFKA_TOPIC", "raw-comments")
	groupID := config.GetEnv("KAFKA_GROUP_ID", "comment-ingest")

	redisAddr := config.GetEnv("REDIS_ADDR", "127.0.0.1:6379")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := config.GetEnvInt("REDIS_DB", 0)
	idempotencyTTL := config.GetEnvDuration("IDEMPOTENCY_TTL", 24*time.Hour)

	dsn := config.GetEnv("DATABASE_URL", "")
	if dsn == "" {
		logrus.StandardLogger().Fatal("DATABASE_URL is required")
	}

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		logrus.StandardLogger().WithError(err).Fatal("db connect")
	}
	defer pool.Close()

	cacheClient := cache.NewWithPassword(redisAddr, redisPassword, redisDB)
	defer func() {
		_ = cacheClient.Close()
	}()

	brokerList := strings.Split(brokers, ",")
	if err := kafkautil.EnsureTopics(brokerList, []string{topic}); err != nil {
		logrus.StandardLogger().WithError(err).Fatal("ensure topic")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer func() {
		_ = reader.Close()
	}()

	logrus.StandardLogger().WithFields(logrus.Fields{
		"topic":   topic,
		"brokers": brokers,
		"group":   groupID,
	}).Info("ingest started")

	repo := repository.NewRepository(pool)
	service := ingest.NewService(reader, repo, cacheClient, ingest.Config{IdempotencyTTL: idempotencyTTL}, logrus.StandardLogger())
	if err := service.Run(ctx); err != nil {
		logrus.StandardLogger().WithError(err).Fatal("ingest stopped")
	}
}
