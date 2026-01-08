package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"comment-processing-service/internal/repository"
	"comment-processing-service/internal/text"
	"comment-processing-service/proto/sentimentpb"

	redisv9 "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
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

const (
	statusProcessed = "processed"
	statusPending   = "pending"
	statusFailed    = "failed"

	lockKeyPrefix       = "lock:comment:"
	sentimentCachePrefx = "sentiment:text:"
	pollSleep           = 200 * time.Millisecond
)

type Processor struct {
	Repo     repository.Repository
	Redis    *redisv9.Client
	Limiter  *rate.Limiter
	RPC      sentimentpb.SentimentServiceClient
	RetryLua *redisv9.Script
	Config   Config
	WorkerID string
	Rand     *rand.Rand
	Logger   *logrus.Logger
}

type processedPayload struct {
	CommentID   string `json:"comment_id"`
	EventID     string `json:"event_id"`
	EventTime   string `json:"event_time"`
	Text        string `json:"text"`
	Sentiment   string `json:"sentiment"`
	ProcessedAt string `json:"processed_at"`
}

func NewProcessor(repo repository.Repository, redis *redisv9.Client, rpc sentimentpb.SentimentServiceClient, cfg Config, workerID string, logger *logrus.Logger) *Processor {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	limiter := rate.NewLimiter(rate.Limit(cfg.RateLimitPerSec), cfg.RateLimitPerSec)
	return &Processor{
		Repo:     repo,
		Redis:    redis,
		Limiter:  limiter,
		RPC:      rpc,
		RetryLua: redisv9.NewScript(retryLua),
		Config:   cfg,
		WorkerID: workerID,
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		Logger:   logger,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		commentID, err := p.dequeueDue(ctx)
		if err != nil {
			if errors.Is(err, redisv9.Nil) {
				time.Sleep(pollSleep)
				continue
			}
			return err
		}
		if commentID == "" {
			time.Sleep(pollSleep)
			continue
		}

		if err := p.process(ctx, commentID); err != nil {
			p.Logger.WithError(err).WithField("comment_id", commentID).Error("worker process error")
			continue
		}
	}
}

func (p *Processor) dequeueDue(ctx context.Context) (string, error) {
	res, err := p.RetryLua.Run(ctx, p.Redis, []string{p.Config.RetryZSet}, time.Now().UnixMilli()).Result()
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", redisv9.Nil
	}
	return fmt.Sprint(res), nil
}

func (p *Processor) process(ctx context.Context, commentID string) error {
	lockKey := lockKeyPrefix + commentID
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

	row, err := p.loadForProcessing(ctx, commentID)
	if err != nil {
		return err
	}
	if row == nil {
		return nil
	}
	if row.Status == statusProcessed {
		return nil
	}

	label, err := p.fetchSentiment(ctx, row)
	if err != nil {
		return p.markFailure(ctx, commentID, row.AttemptCount, err)
	}

	return p.markSuccess(ctx, commentID, label)
}

func (p *Processor) loadForProcessing(ctx context.Context, commentID string) (*repository.CommentProcessing, error) {
	row, err := p.Repo.LoadCommentForProcessing(ctx, commentID)
	if err != nil {
		return nil, fmt.Errorf("load comment %s: %w", commentID, err)
	}

	if row == nil {
		return nil, nil
	}

	if row.TextHash == "" {
		row.TextHash = text.HashSHA256Hex(row.Text)
	}

	return row, nil
}

func (p *Processor) fetchSentiment(ctx context.Context, row *repository.CommentProcessing) (string, error) {
	cacheKey := sentimentCachePrefx + row.TextHash
	label, err := p.Redis.Get(ctx, cacheKey).Result()
	if err == nil && label != "" {
		return label, nil
	}
	if err != nil && !errors.Is(err, redisv9.Nil) {
		return "", fmt.Errorf("cache get: %w", err)
	}

	if err := p.Limiter.Wait(ctx); err != nil {
		return "", fmt.Errorf("rate limit: %w", err)
	}

	rpcCtx, cancel := context.WithTimeout(ctx, p.Config.RPCTimeout)
	defer cancel()

	resp, err := p.RPC.Analyze(rpcCtx, &sentimentpb.AnalyzeRequest{Text: row.Text})
	if err != nil {
		if isRetryableRPC(err) {
			return "", err
		}
		return "", fmt.Errorf("rpc analyze: %w", err)
	}

	if err := p.Redis.Set(ctx, cacheKey, resp.Label, p.Config.TextCacheTTL).Err(); err != nil {
		return "", fmt.Errorf("cache set: %w", err)
	}
	return resp.Label, nil
}

func (p *Processor) markSuccess(ctx context.Context, commentID, label string) error {
	processedAt := time.Now().UTC()

	payload, err := p.buildProcessedPayload(ctx, commentID, label, processedAt)
	if err != nil {
		return fmt.Errorf("build payload %s: %w", commentID, err)
	}
	if err := p.Repo.MarkCommentProcessed(ctx, commentID, label, processedAt, payload); err != nil {
		return fmt.Errorf("mark processed %s: %w", commentID, err)
	}
	return nil
}

func (p *Processor) buildProcessedPayload(ctx context.Context, commentID, label string, processedAt time.Time) ([]byte, error) {
	payloadRow, err := p.Repo.LoadCommentPayload(ctx, commentID)
	if err != nil {
		return nil, fmt.Errorf("load payload %s: %w", commentID, err)
	}

	payload := processedPayload{
		CommentID:   commentID,
		EventID:     payloadRow.EventID,
		EventTime:   payloadRow.EventTime.UTC().Format(time.RFC3339Nano),
		Text:        payloadRow.Text,
		Sentiment:   label,
		ProcessedAt: processedAt.UTC().Format(time.RFC3339Nano),
	}

	return json.Marshal(payload)
}

func (p *Processor) markFailure(ctx context.Context, commentID string, attemptCount int, err error) error {
	attemptCount++
	lastErr := err.Error()

	status := statusPending
	if attemptCount >= p.Config.MaxAttempts {
		status = statusFailed
	}

	if updateErr := p.Repo.MarkCommentFailure(ctx, commentID, attemptCount, lastErr, status); updateErr != nil {
		return fmt.Errorf("mark failure %s: %w", commentID, updateErr)
	}

	if status == statusFailed {
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
