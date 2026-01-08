package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type OutboxRow struct {
	ID        int64
	CommentID string
	Payload   []byte
	Topic     string
}

// OutboxRepository defines outbox-specific read/write operations.
type OutboxRepository interface {
	FetchOutboxBatch(ctx context.Context, limit int) ([]OutboxRow, error)
	MarkOutboxPublished(ctx context.Context, id int64) error
	MarkOutboxPublishError(ctx context.Context, id int64, err error) error
}

// FetchOutboxBatch returns unpublished outbox rows up to limit.
func (r *repository) FetchOutboxBatch(ctx context.Context, limit int) ([]OutboxRow, error) {
	const query = `
SELECT id, comment_id, payload_json, topic
FROM outbox_processed
WHERE published_at IS NULL
ORDER BY created_at
LIMIT $1
`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []OutboxRow
	for rows.Next() {
		var item OutboxRow
		if err := rows.Scan(&item.ID, &item.CommentID, &item.Payload, &item.Topic); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// MarkOutboxPublished marks an outbox row as published.
func (r *repository) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx, `
UPDATE outbox_processed
SET published_at = NOW()
WHERE id = $1
`, id)
	return err
}

// MarkOutboxPublishError updates publish attempts and last error.
func (r *repository) MarkOutboxPublishError(ctx context.Context, id int64, err error) error {
	_, execErr := r.pool.Exec(ctx, `
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
