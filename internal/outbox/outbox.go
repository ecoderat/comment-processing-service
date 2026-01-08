package outbox

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"comment-processing-service/internal/repository"
)

type Config struct {
	DefaultTopic string
	BatchSize    int
	LoopInterval time.Duration
}

type Service interface {
	Run(ctx context.Context) error
}

type service struct {
	repo   repository.OutboxRepository
	writer *kafka.Writer
	cfg    Config
}

// NewService builds an outbox service with dependencies.
func NewService(repo repository.OutboxRepository, writer *kafka.Writer, cfg Config) Service {
	return &service{repo: repo, writer: writer, cfg: cfg}
}

// Run publishes outbox rows to Kafka.
func (s *service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := s.repo.FetchOutboxBatch(ctx, s.cfg.BatchSize)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(s.cfg.LoopInterval)
			continue
		}
		if len(rows) == 0 {
			time.Sleep(s.cfg.LoopInterval)
			continue
		}

		for _, r := range rows {
			topic := r.Topic
			if topic == "" {
				topic = s.cfg.DefaultTopic
			}

			msg := kafka.Message{
				Key:   []byte(r.CommentID),
				Value: r.Payload,
				Topic: topic,
			}

			if err := s.writer.WriteMessages(ctx, msg); err != nil {
				log.Printf("publish failed id=%d comment_id=%s: %v", r.ID, r.CommentID, err)
				if err := s.repo.MarkOutboxPublishError(ctx, r.ID, err); err != nil {
					log.Printf("update error id=%d: %v", r.ID, err)
				}
				continue
			}

			if err := s.repo.MarkOutboxPublished(ctx, r.ID); err != nil {
				log.Printf("mark published error id=%d: %v", r.ID, err)
			}
		}
	}
}
