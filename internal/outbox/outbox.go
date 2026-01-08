package outbox

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	DefaultTopic string
	BatchSize    int
	LoopInterval time.Duration
}

type row struct {
	ID        int64
	CommentID string
	Payload   []byte
	Topic     string
}

// Run publishes outbox rows to Kafka.
func Run(ctx context.Context, pool *pgxpool.Pool, writer *kafka.Writer, cfg Config) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := fetchBatch(ctx, pool, cfg.BatchSize)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(cfg.LoopInterval)
			continue
		}
		if len(rows) == 0 {
			time.Sleep(cfg.LoopInterval)
			continue
		}

		for _, r := range rows {
			topic := r.Topic
			if topic == "" {
				topic = cfg.DefaultTopic
			}

			msg := kafka.Message{
				Key:   []byte(r.CommentID),
				Value: r.Payload,
				Topic: topic,
			}

			if err := writer.WriteMessages(ctx, msg); err != nil {
				log.Printf("publish failed id=%d comment_id=%s: %v", r.ID, r.CommentID, err)
				if err := markPublishError(ctx, pool, r.ID, err); err != nil {
					log.Printf("update error id=%d: %v", r.ID, err)
				}
				continue
			}

			if err := markPublished(ctx, pool, r.ID); err != nil {
				log.Printf("mark published error id=%d: %v", r.ID, err)
			}
		}
	}
}

func fetchBatch(ctx context.Context, pool *pgxpool.Pool, limit int) ([]row, error) {
	const query = `
SELECT id, comment_id, payload_json, topic
FROM outbox_processed
WHERE published_at IS NULL
ORDER BY created_at
LIMIT $1
`
	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []row
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.ID, &item.CommentID, &item.Payload, &item.Topic); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func markPublished(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	_, err := pool.Exec(ctx, `
UPDATE outbox_processed
SET published_at = NOW()
WHERE id = $1
`, id)
	return err
}

func markPublishError(ctx context.Context, pool *pgxpool.Pool, id int64, err error) error {
	_, execErr := pool.Exec(ctx, `
UPDATE outbox_processed
SET publish_attempts = publish_attempts + 1,
    last_error = $2
WHERE id = $1
`, id, err.Error())
	if execErr != nil && execErr != pgx.ErrNoRows {
		return execErr
	}
	return nil
}
