package repository

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Comment struct {
	CommentID   string
	Text        string
	Sentiment   *string
	Status      string
	EventTime   time.Time
	ProcessedAt *time.Time
}

type CommentProcessing struct {
	CommentID    string
	Text         string
	TextHash     string
	Status       string
	AttemptCount int
}

type CommentPayload struct {
	EventID   string
	EventTime time.Time
	Text      string
}

type ListParams struct {
	Sentiment string
	Status    string
	Since     *time.Time
	Until     *time.Time
	Limit     int
	Offset    int
}

type UpsertCommentParams struct {
	CommentID string
	EventID   string
	EventTime time.Time
	Text      string
	TextHash  string
}

// Repository defines the methods for interacting with the data store.
type Repository interface {
	ListComments(ctx context.Context, params ListParams) ([]Comment, error)
	GetComment(ctx context.Context, commentID string) (Comment, error)
	UpsertComment(ctx context.Context, params UpsertCommentParams) (bool, error)
	LoadCommentForProcessing(ctx context.Context, commentID string) (*CommentProcessing, error)
	LoadCommentPayload(ctx context.Context, commentID string) (CommentPayload, error)
	MarkCommentProcessed(ctx context.Context, commentID, label string, processedAt time.Time, payload []byte) error
	MarkCommentFailure(ctx context.Context, commentID string, attemptCount int, lastErr string, status string) error
}

type repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new Repository instance.
func NewRepository(pool *pgxpool.Pool) Repository {
	return &repository{pool: pool}
}

// NewOutboxRepository creates a repository for outbox operations.
func NewOutboxRepository(pool *pgxpool.Pool) OutboxRepository {
	return &repository{pool: pool}
}

// ListComments fetches comments using optional filters.
func (r *repository) ListComments(ctx context.Context, params ListParams) ([]Comment, error) {
	where := []string{"1=1"}
	args := []interface{}{}

	if params.Sentiment != "" {
		where = append(where, "sentiment = $"+strconv.Itoa(len(args)+1))
		args = append(args, params.Sentiment)
	}
	if params.Status != "" {
		where = append(where, "status = $"+strconv.Itoa(len(args)+1))
		args = append(args, params.Status)
	}
	if params.Since != nil {
		where = append(where, "event_time >= $"+strconv.Itoa(len(args)+1))
		args = append(args, *params.Since)
	}
	if params.Until != nil {
		where = append(where, "event_time <= $"+strconv.Itoa(len(args)+1))
		args = append(args, *params.Until)
	}

	args = append(args, params.Limit, params.Offset)
	query := `
SELECT comment_id, text, sentiment, status, event_time, processed_at
FROM comments
WHERE ` + strings.Join(where, " AND ") + `
ORDER BY event_time DESC
LIMIT $` + strconv.Itoa(len(args)-1) + ` OFFSET $` + strconv.Itoa(len(args)) + `
`

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Comment
	for rows.Next() {
		var comment Comment
		if err := rows.Scan(
			&comment.CommentID,
			&comment.Text,
			&comment.Sentiment,
			&comment.Status,
			&comment.EventTime,
			&comment.ProcessedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, comment)
	}

	return result, rows.Err()
}

// GetComment fetches a single comment by ID.
func (r *repository) GetComment(ctx context.Context, commentID string) (Comment, error) {
	const query = `
SELECT comment_id, text, sentiment, status, event_time, processed_at
FROM comments
WHERE comment_id = $1
`

	var comment Comment
	if err := r.pool.QueryRow(ctx, query, commentID).Scan(
		&comment.CommentID,
		&comment.Text,
		&comment.Sentiment,
		&comment.Status,
		&comment.EventTime,
		&comment.ProcessedAt,
	); err != nil {
		return Comment{}, err
	}

	return comment, nil
}

// LoadCommentForProcessing fetches the fields needed by the worker.
func (r *repository) LoadCommentForProcessing(ctx context.Context, commentID string) (*CommentProcessing, error) {
	const query = `
SELECT comment_id, text, text_hash, status, attempt_count
FROM comments
WHERE comment_id = $1
`
	var row CommentProcessing
	if err := r.pool.QueryRow(ctx, query, commentID).Scan(
		&row.CommentID,
		&row.Text,
		&row.TextHash,
		&row.Status,
		&row.AttemptCount,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

// LoadCommentPayload fetches the payload fields for outbox publishing.
func (r *repository) LoadCommentPayload(ctx context.Context, commentID string) (CommentPayload, error) {
	const query = `
SELECT event_id, event_time, text
FROM comments
WHERE comment_id = $1
`
	var payload CommentPayload
	if err := r.pool.QueryRow(ctx, query, commentID).Scan(
		&payload.EventID,
		&payload.EventTime,
		&payload.Text,
	); err != nil {
		return CommentPayload{}, err
	}
	return payload, nil
}

// MarkCommentProcessed updates the comment and inserts the outbox entry in a transaction.
func (r *repository) MarkCommentProcessed(ctx context.Context, commentID, label string, processedAt time.Time, payload []byte) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	_, err = tx.Exec(ctx, `
UPDATE comments
SET sentiment = $1,
    status = 'processed',
    processed_at = $2,
    last_error = NULL,
    updated_at = NOW()
WHERE comment_id = $3
`, label, processedAt, commentID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
INSERT INTO outbox_processed (comment_id, payload_json)
VALUES ($1, $2)
`, commentID, payload)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// MarkCommentFailure updates the comment with failure details.
func (r *repository) MarkCommentFailure(ctx context.Context, commentID string, attemptCount int, lastErr string, status string) error {
	_, err := r.pool.Exec(ctx, `
UPDATE comments
SET attempt_count = $1,
    last_error = $2,
    status = $3,
    updated_at = NOW()
WHERE comment_id = $4
`, attemptCount, lastErr, status, commentID)
	return err
}

// UpsertComment inserts or updates a comment if the event_time is newer.
func (r *repository) UpsertComment(ctx context.Context, params UpsertCommentParams) (bool, error) {
	cmd, err := r.pool.Exec(ctx, `
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
`, params.CommentID, params.EventID, params.EventTime, params.Text, params.TextHash)
	if err != nil {
		return false, err
	}
	return cmd.RowsAffected() > 0, nil
}
