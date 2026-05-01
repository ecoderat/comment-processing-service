-- +migrate Up
DROP INDEX IF EXISTS idx_comments_text_hash;

-- +migrate Down
CREATE INDEX idx_comments_text_hash ON comments (text_hash);
