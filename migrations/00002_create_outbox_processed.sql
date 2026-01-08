-- +migrate Up
CREATE TABLE IF NOT EXISTS outbox_processed (
  id BIGSERIAL PRIMARY KEY,
  comment_id TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  topic TEXT NOT NULL DEFAULT 'processed-comments',
  publish_attempts INT NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_published_created ON outbox_processed (published_at, created_at);

-- +migrate Down
DROP TABLE IF EXISTS outbox_processed;
