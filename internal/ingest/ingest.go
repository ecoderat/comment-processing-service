package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	IdempotencyTTL time.Duration
}

type rawComment struct {
	EventID   string `json:"event_id"`
	EventTime string `json:"event_time"`
	CommentID string `json:"comment_id"`
	Text      string `json:"text"`
}

// Run consumes Kafka messages, stores comments, and enqueues retries.
func Run(ctx context.Context, reader *kafka.Reader, pool *pgxpool.Pool, redisClient *redis.Client, cfg Config) error {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		if err := handleMessage(ctx, pool, redisClient, msg, cfg.IdempotencyTTL); err != nil {
			log.Printf("handle error: %v", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func handleMessage(ctx context.Context, pool *pgxpool.Pool, redisClient *redis.Client, msg kafka.Message, ttl time.Duration) error {
	var payload rawComment
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

	idempotencyKey := "processed:event:" + payload.EventID
	set, err := redisClient.SetNX(ctx, idempotencyKey, "1", ttl).Result()
	if err != nil {
		return err
	}
	if !set {
		log.Printf("duplicate event_id=%s comment_id=%s", payload.EventID, payload.CommentID)
		return nil
	}

	textHash := hashText(payload.Text)

	upserted, err := upsertComment(ctx, pool, payload, eventTime, textHash)
	if err != nil {
		_ = redisClient.Del(ctx, idempotencyKey).Err()
		return err
	}

	if !upserted {
		log.Printf("ignored older event_id=%s comment_id=%s", payload.EventID, payload.CommentID)
		return nil
	}

	for {
		if err := enqueueRetry(ctx, redisClient, payload.CommentID); err != nil {
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

func upsertComment(ctx context.Context, pool *pgxpool.Pool, payload rawComment, eventTime time.Time, textHash string) (bool, error) {
	cmd, err := pool.Exec(ctx, `
INSERT INTO comments (comment_id, event_id, event_time, text, text_hash, status, attempt_count, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'pending', 0, NOW(), NOW())
ON CONFLICT (comment_id) DO UPDATE SET
  event_id = EXCLUDED.event_id,
  event_time = EXCLUDED.event_time,
  text = EXCLUDED.text,
  text_hash = EXCLUDED.text_hash,
  status = 'pending',
  attempt_count = 0,
  last_error = NULL,
  processed_at = NULL,
  updated_at = NOW()
WHERE EXCLUDED.event_time > comments.event_time
`, payload.CommentID, payload.EventID, eventTime, payload.Text, textHash)
	if err != nil {
		return false, err
	}
	return cmd.RowsAffected() > 0, nil
}

func enqueueRetry(ctx context.Context, client *redis.Client, commentID string) error {
	score := float64(time.Now().UnixMilli())
	return client.ZAdd(ctx, "retry:zset", redis.Z{
		Score:  score,
		Member: commentID,
	}).Err()
}

func hashText(text string) string {
	h := sha256.Sum256([]byte(text))
	return hex.EncodeToString(h[:])
}
