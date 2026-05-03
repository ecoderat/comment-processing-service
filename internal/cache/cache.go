package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idemKeyPrefix = "idem:event:"
)

type Client struct {
	rdb *redis.Client
}

func NewWithPassword(addr, password string, db int) *Client {
	return &Client{
		rdb: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

func (c *Client) CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	key := idemKeyPrefix + eventID
	set, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return set, nil
}

func (c *Client) UnmarkEvent(ctx context.Context, eventID string) error {
	key := idemKeyPrefix + eventID
	return c.rdb.Del(ctx, key).Err()
}

// EnqueueRetry adds a comment ID to the retry sorted set.
// Uses ZADD NX so an existing future-dated retry score is not clobbered by a
// duplicate ingest (e.g. after Redis flush or idempotency TTL expiry).
func (c *Client) EnqueueRetry(ctx context.Context, key, commentID string) error {
	score := float64(time.Now().UnixMilli())
	return c.rdb.ZAddNX(ctx, key, redis.Z{
		Score:  score,
		Member: commentID,
	}).Err()
}
