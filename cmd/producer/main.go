package main

import (
	"context"
	"flag"
	"strings"
	"time"

	"comment-processing-service/internal/config"
	kafkautil "comment-processing-service/internal/kafka"
	"comment-processing-service/internal/producer"

	"github.com/sirupsen/logrus"
)

func main() {
	config.LoadEnv()

	interval := flag.Duration("interval", 1*time.Second, "base interval between messages")
	size := flag.Int("size", 24, "approximate number of words per message")
	reusePercent := flag.Int("reuse-percent", 20, "chance (0-100) to reuse previous text")
	flag.Parse()

	brokers := config.GetEnv("KAFKA_BROKERS", "127.0.0.1:9092")
	topic := config.GetEnv("KAFKA_TOPIC", "raw-comments")
	burstPercent := config.GetEnvInt("PRODUCER_BURST_PERCENT", 10)
	burstInterval := config.GetEnvDuration("PRODUCER_BURST_INTERVAL", 100*time.Millisecond)
	burstCount := config.GetEnvInt("PRODUCER_BURST_COUNT", 20)
	seed := time.Now().UnixNano()

	ctx := context.Background()
	brokerList := strings.Split(brokers, ",")
	if err := kafkautil.EnsureTopics(brokerList, []string{topic}); err != nil {
		logrus.StandardLogger().WithError(err).Fatal("ensure topic")
	}

	cfg := producer.Config{
		Brokers:       brokerList,
		Topic:         topic,
		Interval:      *interval,
		BurstPercent:  burstPercent,
		BurstInterval: burstInterval,
		BurstCount:    burstCount,
		Size:          *size,
		ReusePercent:  *reusePercent,
		Seed:          seed,
	}

	service := producer.NewService(cfg, logrus.StandardLogger())
	if err := service.Run(ctx); err != nil {
		logrus.StandardLogger().WithError(err).Fatal("producer stopped")
	}
}
