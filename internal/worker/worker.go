package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"comment-processing-service/proto/sentimentpb"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	redisv9 "github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	MaxAttempts     int
	BaseBackoff     time.Duration
	MaxBackoff      time.Duration
	Jitter          time.Duration
	LockTTL         time.Duration
	RetryZSet       string
	TextCacheTTL    time.Duration
	RateLimitPerSec int
	RPCTimeout      time.Duration
}

type Processor struct {
	DB       *pgxpool.Pool
	Redis    *redisv9.Client
	Limiter  *rate.Limiter
	RPC      sentimentpb.SentimentServiceClient
	RetryLua *redisv9.Script
	Config   Config
	WorkerID string
	Rand     *rand.Rand
}

type commentRow struct {
	CommentID    string
	Text         string
	TextHash     string
	Status       string
	AttemptCount int
}

type processedPayload struct {
	CommentID   string `json:"comment_id"`
	EventID     string `json:"event_id"`
	EventTime   string `json:"event_time"`
	Text        string `json:"text"`
	Sentiment   string `json:"sentiment"`
	ProcessedAt string `json:"processed_at"`
}

func NewProcessor(db *pgxpool.Pool, redis *redisv9.Client, rpc sentimentpb.SentimentServiceClient, cfg Config, workerID string) *Processor {
	limiter := rate.NewLimiter(rate.Limit(cfg.RateLimitPerSec), cfg.RateLimitPerSec)
	return &Processor{
		DB:       db,
		Redis:    redis,
		Limiter:  limiter,
		RPC:      rpc,
		RetryLua: redisv9.NewScript(retryLua),
		Config:   cfg,
		WorkerID: workerID,
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (p *Processor) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		commentID, err := p.popDue(ctx)
		if err != nil {
			if errors.Is(err, redisv9.Nil) {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			return err
		}
		if commentID == "" {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err := p.processComment(ctx, commentID); err != nil {
			// Processing errors are logged by caller; keep running.
			continue
		}
	}
}

func (p *Processor) popDue(ctx context.Context) (string, error) {
	res, err := p.RetryLua.Run(ctx, p.Redis, []string{p.Config.RetryZSet}, time.Now().UnixMilli()).Result()
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", redisv9.Nil
	}
	return fmt.Sprint(res), nil
}

func (p *Processor) processComment(ctx context.Context, commentID string) error {
	lockKey := "lock:comment:" + commentID
	locked, err := p.Redis.SetNX(ctx, lockKey, p.WorkerID, p.Config.LockTTL).Result()
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}
	defer func() {
		_, _ = p.Redis.Del(ctx, lockKey).Result()
	}()

	row, err := p.loadComment(ctx, commentID)
	if err != nil {
		return err
	}
	if row == nil {
		return nil
	}
	if row.Status == "processed" {
		return nil
	}

	label, err := p.getSentiment(ctx, row)
	if err != nil {
		return p.handleFailure(ctx, commentID, row.AttemptCount, err)
	}

	return p.handleSuccess(ctx, commentID, label)
}

func (p *Processor) loadComment(ctx context.Context, commentID string) (*commentRow, error) {
	var row commentRow
	const query = `
SELECT comment_id, text, text_hash, status, attempt_count
FROM comments
WHERE comment_id = $1
`
	err := p.DB.QueryRow(ctx, query, commentID).Scan(&row.CommentID, &row.Text, &row.TextHash, &row.Status, &row.AttemptCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if row.TextHash == "" {
		row.TextHash = hashText(row.Text)
	}
	return &row, nil
}

func (p *Processor) getSentiment(ctx context.Context, row *commentRow) (string, error) {
	cacheKey := "sentiment:text:" + row.TextHash
	label, err := p.Redis.Get(ctx, cacheKey).Result()
	if err == nil && label != "" {
		return label, nil
	}
	if err != nil && !errors.Is(err, redisv9.Nil) {
		return "", err
	}

	if err := p.Limiter.Wait(ctx); err != nil {
		return "", err
	}

	rpcCtx, cancel := context.WithTimeout(ctx, p.Config.RPCTimeout)
	defer cancel()

	resp, err := p.RPC.Analyze(rpcCtx, &sentimentpb.AnalyzeRequest{Text: row.Text})
	if err != nil {
		if isRetryableRPC(err) {
			return "", err
		}
		return "", err
	}

	if err := p.Redis.Set(ctx, cacheKey, resp.Label, p.Config.TextCacheTTL).Err(); err != nil {
		return "", err
	}
	return resp.Label, nil
}

func (p *Processor) handleSuccess(ctx context.Context, commentID, label string) error {
	processedAt := time.Now().UTC()

	payload, err := p.buildProcessedPayload(ctx, commentID, label, processedAt)
	if err != nil {
		return err
	}

	tx, err := p.DB.Begin(ctx)
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

func (p *Processor) buildProcessedPayload(ctx context.Context, commentID, label string, processedAt time.Time) ([]byte, error) {
	var eventID, text string
	var eventTime time.Time
	const query = `
SELECT event_id, event_time, text
FROM comments
WHERE comment_id = $1
`
	if err := p.DB.QueryRow(ctx, query, commentID).Scan(&eventID, &eventTime, &text); err != nil {
		return nil, err
	}

	payload := processedPayload{
		CommentID:   commentID,
		EventID:     eventID,
		EventTime:   eventTime.UTC().Format(time.RFC3339Nano),
		Text:        text,
		Sentiment:   label,
		ProcessedAt: processedAt.UTC().Format(time.RFC3339Nano),
	}

	return json.Marshal(payload)
}

func (p *Processor) handleFailure(ctx context.Context, commentID string, attemptCount int, err error) error {
	attemptCount++
	lastErr := err.Error()

	status := "pending"
	if attemptCount >= p.Config.MaxAttempts {
		status = "failed"
	}

	_, updateErr := p.DB.Exec(ctx, `
UPDATE comments
SET attempt_count = $1,
    last_error = $2,
    status = $3,
    updated_at = NOW()
WHERE comment_id = $4
`, attemptCount, lastErr, status, commentID)
	if updateErr != nil {
		return updateErr
	}

	if status == "failed" {
		return nil
	}

	nextAttempt := time.Now().Add(p.retryDelay(attemptCount))
	return p.Redis.ZAdd(ctx, p.Config.RetryZSet, redisv9.Z{
		Score:  float64(nextAttempt.UnixMilli()),
		Member: commentID,
	}).Err()
}

func (p *Processor) retryDelay(attempt int) time.Duration {
	backoff := float64(p.Config.BaseBackoff) * math.Pow(2, float64(attempt-1))
	if backoff > float64(p.Config.MaxBackoff) {
		backoff = float64(p.Config.MaxBackoff)
	}
	jitter := time.Duration(p.Rand.Int63n(int64(p.Config.Jitter) + 1))
	return time.Duration(backoff) + jitter
}

func isRetryableRPC(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func hashText(text string) string {
	h := sha256.Sum256([]byte(text))
	return hex.EncodeToString(h[:])
}

const retryLua = `
local zset = KEYS[1]
local now = tonumber(ARGV[1])

local items = redis.call('ZRANGEBYSCORE', zset, '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then
  return nil
end
redis.call('ZREM', zset, items[1])
return items[1]
`
