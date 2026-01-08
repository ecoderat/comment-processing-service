package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idemKeyPrefix = "idem:event:"
	retryZSetKey  = "retry:zset"
)

type Cache interface {
	CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error)
	UnmarkEvent(ctx context.Context, eventID string) error
	EnqueueRetry(ctx context.Context, commentID string) error
	Close() error
}

type cache struct {
	rdb *redis.Client
}

func New(addr string, db int) *cache {
	return &cache{
		rdb: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   db,
		}),
	}
}

func NewWithPassword(addr, password string, db int) *cache {
	return &cache{
		rdb: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (c *cache) Close() error {
	return c.rdb.Close()
}

func (c *cache) CheckAndMarkEvent(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	key := idemKeyPrefix + eventID
	set, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return set, nil
}

func (c *cache) UnmarkEvent(ctx context.Context, eventID string) error {
	key := idemKeyPrefix + eventID
	return c.rdb.Del(ctx, key).Err()
}

// EnqueueRetry adds a comment ID to the retry sorted set.
func (c *cache) EnqueueRetry(ctx context.Context, commentID string) error {
	score := float64(time.Now().UnixMilli())
	return c.rdb.ZAdd(ctx, retryZSetKey, redis.Z{
		Score:  score,
		Member: commentID,
	}).Err()
}
