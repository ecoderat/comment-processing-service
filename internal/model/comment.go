package model

type RawComment struct {
	EventID   string `json:"event_id"`
	EventTime string `json:"event_time"`
	CommentID string `json:"comment_id"`
	Text      string `json:"text"`
}
