package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"comment-processing-service/internal/cache"
	"comment-processing-service/internal/model"
	"comment-processing-service/internal/repository"
	"comment-processing-service/internal/text"
)

type Config struct {
	IdempotencyTTL time.Duration
}

type Service struct {
	reader *kafka.Reader
	repo   repository.Repository
	cache  cache.Cache
	cfg    Config
}

// NewService builds an ingest service with dependencies.
func NewService(reader *kafka.Reader, repo repository.Repository, cacheClient cache.Cache, cfg Config) *Service {
	return &Service{
		reader: reader,
		repo:   repo,
		cache:  cacheClient,
		cfg:    cfg,
	}
}

// Run consumes Kafka messages, stores comments, and enqueues retries.
func (s *Service) Run(ctx context.Context) error {
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		if err := s.handleMessage(ctx, msg); err != nil {
			log.Printf("handle error: %v", err)
			continue
		}

		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func (s *Service) handleMessage(ctx context.Context, msg kafka.Message) error {
	var payload model.RawComment
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		log.Printf("invalid json: %v", err)
		return nil
	}

	eventTime, err := time.Parse(time.RFC3339Nano, payload.EventTime)
	if err != nil {
		log.Printf("invalid event_time: %v event_id=%s comment_id=%s", err, payload.EventID, payload.CommentID)
		return nil
	}

	if payload.EventID == "" || payload.CommentID == "" {
		log.Printf("missing identifiers event_id=%s comment_id=%s", payload.EventID, payload.CommentID)
		return nil
	}

	eventID := payload.EventID
	set, err := s.cache.CheckAndMarkEvent(ctx, eventID, s.cfg.IdempotencyTTL)
	if err != nil {
		return err
	}
	if !set {
		log.Printf("duplicate event_id=%s comment_id=%s", payload.EventID, payload.CommentID)
		return nil
	}

	textHash := text.HashSHA256Hex(payload.Text)

	upserted, err := s.repo.UpsertComment(ctx, repository.UpsertCommentParams{
		CommentID: payload.CommentID,
		EventID:   payload.EventID,
		EventTime: eventTime,
		Text:      payload.Text,
		TextHash:  textHash,
	})
	if err != nil {
		_ = s.cache.UnmarkEvent(ctx, eventID)
		return err
	}

	if !upserted {
		log.Printf("ignored older event_id=%s comment_id=%s", payload.EventID, payload.CommentID)
		return nil
	}

	for {
		if err := s.cache.EnqueueRetry(ctx, payload.CommentID); err != nil {
			log.Printf("enqueue error comment_id=%s: %v", payload.CommentID, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		break
	}

	log.Printf("ingested event_id=%s comment_id=%s text_hash=%s", payload.EventID, payload.CommentID, textHash)
	return nil
}
