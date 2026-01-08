package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool aliases the pgx pool type for internal packages.
type Pool = pgxpool.Pool

// NewPool opens a pgx connection pool using the provided DSN.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
