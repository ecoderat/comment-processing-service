package model

import "time"

type RawComment struct {
	EventID   string    `json:"event_id"`
	EventTime time.Time `json:"event_time"`
	CommentID string    `json:"comment_id"`
	Text      string    `json:"text"`
}
