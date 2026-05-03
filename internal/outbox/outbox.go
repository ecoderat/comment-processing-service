package outbox

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"comment-processing-service/internal/repository"
)

type Config struct {
	DefaultTopic string
	BatchSize    int
	LoopInterval time.Duration
}

type Service struct {
	repo   repository.OutboxRepository
	writer *kafka.Writer
	cfg    Config
	logger *logrus.Logger
}

// NewService builds an outbox service with dependencies.
func NewService(repo repository.OutboxRepository, writer *kafka.Writer, cfg Config, logger *logrus.Logger) *Service {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	return &Service{repo: repo, writer: writer, cfg: cfg, logger: logger}
}

// Run publishes outbox rows to Kafka.
func (s *Service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := s.repo.FetchOutboxBatch(ctx, s.cfg.BatchSize)
		if err != nil {
			s.logger.WithError(err).Error("outbox fetch error")
			time.Sleep(s.cfg.LoopInterval)
			continue
		}
		if len(rows) == 0 {
			time.Sleep(s.cfg.LoopInterval)
			continue
		}
		s.logger.WithField("count", len(rows)).Info("outbox batch fetched")

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
				s.logger.WithError(err).WithFields(logrus.Fields{
					"id":         r.ID,
					"comment_id": r.CommentID,
				}).Error("outbox publish failed")
				if err := s.repo.MarkOutboxPublishError(ctx, r.ID, err); err != nil {
					s.logger.WithError(err).WithField("id", r.ID).Error("outbox update error")
				}
				continue
			}

			if err := s.repo.MarkOutboxPublished(ctx, r.ID); err != nil {
				s.logger.WithError(err).WithField("id", r.ID).Error("outbox mark published error")
			} else {
				s.logger.WithFields(logrus.Fields{
					"id":         r.ID,
					"comment_id": r.CommentID,
					"topic":      msg.Topic,
				}).Info("outbox published")
			}
		}
	}
}
