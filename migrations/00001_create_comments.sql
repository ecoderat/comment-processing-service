-- +goose Up
CREATE TABLE IF NOT EXISTS comments (
  comment_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  text TEXT NOT NULL,
  text_hash TEXT NOT NULL,
  sentiment TEXT NULL,
  status TEXT NOT NULL,
  attempt_count INT NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  processed_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_comments_status_event_time ON comments (status, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_comments_sentiment_event_time ON comments (sentiment, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_comments_text_hash ON comments (text_hash);

-- +goose Down
DROP TABLE IF EXISTS comments;
