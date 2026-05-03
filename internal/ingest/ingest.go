package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"comment-processing-service/internal/cache"
	"comment-processing-service/internal/model"
	"comment-processing-service/internal/repository"
	"comment-processing-service/internal/text"
)

type Config struct {
	IdempotencyTTL time.Duration
	RetryZSet      string
}

// CommentWriter is the slice of the repository this service depends on.
type CommentWriter interface {
	UpsertComment(ctx context.Context, params repository.UpsertCommentParams) (bool, error)
}

type Service struct {
	reader *kafka.Reader
	repo   CommentWriter
	cache  *cache.Client
	cfg    Config
	logger *logrus.Logger
}

// NewService builds an ingest service with dependencies.
func NewService(reader *kafka.Reader, repo CommentWriter, cacheClient *cache.Client, cfg Config, logger *logrus.Logger) *Service {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	return &Service{
		reader: reader,
		repo:   repo,
		cache:  cacheClient,
		cfg:    cfg,
		logger: logger,
	}
}

// Run consumes Kafka messages, stores comments, and enqueues retries.
func (s *Service) Run(ctx context.Context) error {
	s.logger.WithField("idempotency_ttl", s.cfg.IdempotencyTTL.String()).Info("ingest service started")
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			s.logger.WithError(err).Error("ingest fetch error")
			continue
		}

		if err := s.handleMessage(ctx, msg); err != nil {
			s.logger.WithError(err).Error("ingest handle error")
			continue
		}

		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			s.logger.WithError(err).Error("ingest commit error")
		}
	}
}

func (s *Service) handleMessage(ctx context.Context, msg kafka.Message) error {
	start := time.Now()
	var payload model.RawComment
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		s.logger.WithError(err).Warn("invalid json")
		return nil
	}

	if payload.EventTime.IsZero() {
		s.logger.WithFields(logrus.Fields{
			"event_id":   payload.EventID,
			"comment_id": payload.CommentID,
		}).Warn("invalid event_time")
		return nil
	}

	if payload.EventID == "" || payload.CommentID == "" {
		s.logger.WithFields(logrus.Fields{
			"event_id":   payload.EventID,
			"comment_id": payload.CommentID,
		}).Warn("missing identifiers")
		return nil
	}
	s.logger.WithFields(logrus.Fields{
		"event_id":   payload.EventID,
		"comment_id": payload.CommentID,
	}).Debug("ingest message received")

	eventID := payload.EventID
	set, err := s.cache.CheckAndMarkEvent(ctx, eventID, s.cfg.IdempotencyTTL)
	if err != nil {
		return err
	}
	if !set {
		s.logger.WithFields(logrus.Fields{
			"event_id":   payload.EventID,
			"comment_id": payload.CommentID,
		}).Info("duplicate event")
		return nil
	}

	textHash := text.HashSHA256Hex(payload.Text)

	upserted, err := s.repo.UpsertComment(ctx, repository.UpsertCommentParams{
		CommentID: payload.CommentID,
		EventID:   payload.EventID,
		EventTime: payload.EventTime,
		Text:      payload.Text,
		TextHash:  textHash,
	})
	if err != nil {
		_ = s.cache.UnmarkEvent(ctx, eventID)
		return err
	}

	if !upserted {
		s.logger.WithFields(logrus.Fields{
			"event_id":   payload.EventID,
			"comment_id": payload.CommentID,
		}).Info("ignored older event")
		return nil
	}

	if err := s.cache.EnqueueRetry(ctx, s.cfg.RetryZSet, payload.CommentID); err != nil {
		return fmt.Errorf("enqueue retry %s: %w", payload.CommentID, err)
	}

	s.logger.WithFields(logrus.Fields{
		"event_id":    payload.EventID,
		"comment_id":  payload.CommentID,
		"text_hash":   textHash,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("ingested comment")
	return nil
}
