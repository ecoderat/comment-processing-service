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
	sentimentCachePrefix = "sentiment:text:"
	pollSleep           = 200 * time.Millisecond
)

// WorkerRepo is the slice of the repository this worker depends on.
type WorkerRepo interface {
	LoadCommentForProcessing(ctx context.Context, commentID string) (*repository.CommentProcessing, error)
	LoadCommentPayload(ctx context.Context, commentID string) (repository.CommentPayload, error)
	MarkCommentProcessed(ctx context.Context, commentID, label string, processedAt time.Time, payload []byte) error
	RecordRetryableFailure(ctx context.Context, commentID string, attemptCount int, lastErr string, status string) error
	MarkAsTerminallyFailed(ctx context.Context, commentID, lastErr string) error
}

type Processor struct {
	repo     WorkerRepo
	redis    *redisv9.Client
	limiter  *rate.Limiter
	rpc      sentimentpb.SentimentServiceClient
	retryLua *redisv9.Script
	config   Config
	workerID string
	rand     *rand.Rand
	logger   *logrus.Logger
}

// nonRetryableError marks an error that should not be re-scheduled.
// The worker terminates the comment as failed instead of incrementing
// attempt_count and pushing it back onto the retry ZSET.
type nonRetryableError struct{ err error }

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

func isNonRetryable(err error) bool {
	var nre *nonRetryableError
	return errors.As(err, &nre)
}

// releaseLockScript releases a SETNX lock only if the caller still owns it
// (the stored value matches the worker's token). Without this guard, a
// deferred plain DEL would delete another worker's lock if our TTL expired
// during a slow RPC.
var releaseLockScript = redisv9.NewScript(`
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    end
    return 0
`)

type processedPayload struct {
	CommentID   string    `json:"comment_id"`
	EventID     string    `json:"event_id"`
	EventTime   time.Time `json:"event_time"`
	Text        string    `json:"text"`
	Sentiment   string    `json:"sentiment"`
	ProcessedAt time.Time `json:"processed_at"`
}

func NewProcessor(repo WorkerRepo, redis *redisv9.Client, rpc sentimentpb.SentimentServiceClient, cfg Config, workerID string, logger *logrus.Logger) *Processor {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	limiter := rate.NewLimiter(rate.Limit(cfg.RateLimitPerSec), cfg.RateLimitPerSec)
	return &Processor{
		repo:     repo,
		redis:    redis,
		limiter:  limiter,
		rpc:      rpc,
		retryLua: redisv9.NewScript(retryLua),
		config:   cfg,
		workerID: workerID,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:   logger,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	p.logger.WithFields(logrus.Fields{
		"worker_id":  p.workerID,
		"retry_zset": p.config.RetryZSet,
	}).Info("worker started")
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
			p.logger.WithError(err).WithField("comment_id", commentID).Error("worker process error")
			continue
		}
	}
}

func (p *Processor) dequeueDue(ctx context.Context) (string, error) {
	res, err := p.retryLua.Run(ctx, p.redis, []string{p.config.RetryZSet}, time.Now().UnixMilli()).Result()
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
	locked, err := p.redis.SetNX(ctx, lockKey, p.workerID, p.config.LockTTL).Result()
	if err != nil {
		return err
	}
	if !locked {
		nextAttempt := float64(time.Now().Add(p.config.LockTTL).UnixMilli())
		if err := p.redis.ZAdd(ctx, p.config.RetryZSet, redisv9.Z{
			Score:  nextAttempt,
			Member: commentID,
		}).Err(); err != nil {
			p.logger.WithError(err).WithField("comment_id", commentID).Error("re-enqueue after lock contention failed")
			return err
		}
		return nil
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := releaseLockScript.Run(releaseCtx, p.redis, []string{lockKey}, p.workerID).Err(); err != nil && !errors.Is(err, redisv9.Nil) {
			p.logger.WithError(err).WithField("comment_id", commentID).Warn("lock release failed")
		}
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

	if err := p.markSuccess(ctx, commentID, label); err != nil {
		return err
	}
	p.logger.WithFields(logrus.Fields{
		"comment_id": commentID,
		"label":      label,
	}).Info("comment processed")
	return nil
}

func (p *Processor) loadForProcessing(ctx context.Context, commentID string) (*repository.CommentProcessing, error) {
	row, err := p.repo.LoadCommentForProcessing(ctx, commentID)
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
	cacheKey := sentimentCachePrefix + row.TextHash
	label, err := p.redis.Get(ctx, cacheKey).Result()
	if err == nil && label != "" {
		return label, nil
	}
	if err != nil && !errors.Is(err, redisv9.Nil) {
		return "", fmt.Errorf("cache get: %w", err)
	}

	if err := p.limiter.Wait(ctx); err != nil {
		return "", fmt.Errorf("rate limit: %w", err)
	}

	rpcCtx, cancel := context.WithTimeout(ctx, p.config.RPCTimeout)
	defer cancel()

	resp, err := p.rpc.Analyze(rpcCtx, &sentimentpb.AnalyzeRequest{Text: row.Text})
	if err != nil {
		if !isRetryableRPC(err) {
			return "", &nonRetryableError{err: fmt.Errorf("rpc analyze: %w", err)}
		}
		return "", err
	}

	if err := p.redis.Set(ctx, cacheKey, resp.Label, p.config.TextCacheTTL).Err(); err != nil {
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
	if err := p.repo.MarkCommentProcessed(ctx, commentID, label, processedAt, payload); err != nil {
		return fmt.Errorf("mark processed %s: %w", commentID, err)
	}
	return nil
}

func (p *Processor) buildProcessedPayload(ctx context.Context, commentID, label string, processedAt time.Time) ([]byte, error) {
	payloadRow, err := p.repo.LoadCommentPayload(ctx, commentID)
	if err != nil {
		return nil, fmt.Errorf("load payload %s: %w", commentID, err)
	}

	payload := processedPayload{
		CommentID:   commentID,
		EventID:     payloadRow.EventID,
		EventTime:   payloadRow.EventTime.UTC(),
		Text:        payloadRow.Text,
		Sentiment:   label,
		ProcessedAt: processedAt.UTC(),
	}

	return json.Marshal(payload)
}

func (p *Processor) markFailure(ctx context.Context, commentID string, attemptCount int, err error) error {
	if isNonRetryable(err) {
		if updateErr := p.repo.MarkAsTerminallyFailed(ctx, commentID, err.Error()); updateErr != nil {
			return fmt.Errorf("mark failed %s: %w", commentID, updateErr)
		}
		p.logger.WithFields(logrus.Fields{
			"comment_id": commentID,
			"reason":     err.Error(),
		}).Warn("comment marked failed (non-retryable)")
		return nil
	}

	attemptCount++
	lastErr := err.Error()

	status := statusPending
	if attemptCount >= p.config.MaxAttempts {
		status = statusFailed
	}

	if updateErr := p.repo.RecordRetryableFailure(ctx, commentID, attemptCount, lastErr, status); updateErr != nil {
		return fmt.Errorf("mark failure %s: %w", commentID, updateErr)
	}

	if status == statusFailed {
		p.logger.WithFields(logrus.Fields{
			"comment_id":    commentID,
			"attempt_count": attemptCount,
		}).Warn("comment marked failed")
		return nil
	}

	nextAttempt := time.Now().Add(p.retryDelay(attemptCount))
	p.logger.WithFields(logrus.Fields{
		"comment_id":    commentID,
		"attempt_count": attemptCount,
		"next_attempt":  nextAttempt.UTC().Format(time.RFC3339Nano),
	}).Info("comment retry scheduled")
	return p.redis.ZAdd(ctx, p.config.RetryZSet, redisv9.Z{
		Score:  float64(nextAttempt.UnixMilli()),
		Member: commentID,
	}).Err()
}

func (p *Processor) retryDelay(attempt int) time.Duration {
	backoff := float64(p.config.BaseBackoff) * math.Pow(2, float64(attempt-1))
	if backoff > float64(p.config.MaxBackoff) {
		backoff = float64(p.config.MaxBackoff)
	}
	jitter := time.Duration(p.rand.Int63n(int64(p.config.Jitter) + 1))
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
