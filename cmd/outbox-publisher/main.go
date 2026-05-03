package main

import (
	"context"
	"errors"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"comment-processing-service/internal/config"
	"comment-processing-service/internal/db"
	kafkautil "comment-processing-service/internal/kafka"
	"comment-processing-service/internal/outbox"
	"comment-processing-service/internal/repository"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	config.LoadEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := config.GetEnv("DATABASE_URL", "")
	if dsn == "" {
		logrus.StandardLogger().Fatal("DATABASE_URL is required")
	}
	brokers := config.GetEnv("KAFKA_BROKERS", "127.0.0.1:9092")
	defaultTopic := config.GetEnv("KAFKA_PROCESSED_TOPIC", "processed-comments")
	batchSize := config.GetEnvInt("BATCH_SIZE", 100)
	loopInterval := config.GetEnvDuration("LOOP_INTERVAL", 200*time.Millisecond)

	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		logrus.StandardLogger().WithError(err).Fatal("db connect")
	}
	defer pool.Close()

	repo := repository.NewOutboxRepository(pool)

	brokerList := strings.Split(brokers, ",")
	if err := kafkautil.EnsureTopics(brokerList, []string{defaultTopic}); err != nil {
		logrus.StandardLogger().WithError(err).Fatal("ensure topic")
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokerList,
		Balancer: &kafka.Hash{},
	})
	defer func() { _ = writer.Close() }()

	logrus.StandardLogger().WithFields(logrus.Fields{
		"brokers": brokers,
		"topic":   defaultTopic,
	}).Info("outbox publisher started")

	cfg := outbox.Config{
		DefaultTopic: defaultTopic,
		BatchSize:    batchSize,
		LoopInterval: loopInterval,
	}
	service := outbox.NewService(repo, writer, cfg, logrus.StandardLogger())
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logrus.StandardLogger().WithError(err).Fatal("outbox stopped")
	}
}
