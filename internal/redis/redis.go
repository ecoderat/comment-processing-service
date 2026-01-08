package redis

import "github.com/redis/go-redis/v9"

// NewClient returns a go-redis client with the provided options.
func NewClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}
