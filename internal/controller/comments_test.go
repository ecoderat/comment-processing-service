package controller

import (
	"testing"
	"time"

	"comment-processing-service/internal/repository"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		fallback int
		want     int
	}{
		{"empty returns fallback", "", 50, 50},
		{"valid positive", "42", 0, 42},
		{"valid zero", "0", 50, 0},
		{"valid negative", "-7", 50, -7},
		{"non-numeric returns fallback", "abc", 50, 50},
		{"trailing garbage returns fallback", "12abc", 50, 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseInt(tt.value, tt.fallback); got != tt.want {
				t.Errorf("parseInt(%q, %d) = %d, want %d", tt.value, tt.fallback, got, tt.want)
			}
		})
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "rfc3339",
			value: "2025-12-01T10:00:00Z",
			want:  time.Date(2025, 12, 1, 10, 0, 0, 0, time.UTC),
		},
		{
			name:  "rfc3339nano",
			value: "2025-12-01T10:00:00.123456789Z",
			want:  time.Date(2025, 12, 1, 10, 0, 0, 123456789, time.UTC),
		},
		{name: "invalid", value: "not-a-time", wantErr: true},
		{name: "empty", value: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTime(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("parseTime(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestToResponse(t *testing.T) {
	eventTime := time.Date(2025, 12, 1, 10, 0, 0, 0, time.UTC)
	processedAt := time.Date(2025, 12, 1, 10, 0, 5, 0, time.UTC)

	t.Run("nullable fields omitted when nil", func(t *testing.T) {
		got := toResponse(repository.Comment{
			CommentID: "CMT-1",
			Text:      "hello",
			Status:    "pending",
			EventTime: eventTime,
		})
		if got.CommentID != "CMT-1" || got.Status != "pending" {
			t.Errorf("CommentID/Status mismatch: %+v", got)
		}
		if got.Sentiment != nil {
			t.Errorf("Sentiment = %v, want nil", got.Sentiment)
		}
		if got.ProcessedAt != nil {
			t.Errorf("ProcessedAt = %v, want nil", got.ProcessedAt)
		}
		if got.EventTime != "2025-12-01T10:00:00Z" {
			t.Errorf("EventTime = %q", got.EventTime)
		}
	})

	t.Run("nullable fields populated when set", func(t *testing.T) {
		sentiment := "positive"
		got := toResponse(repository.Comment{
			CommentID:   "CMT-2",
			Text:        "great",
			Sentiment:   &sentiment,
			Status:      "processed",
			EventTime:   eventTime,
			ProcessedAt: &processedAt,
		})
		if got.Sentiment == nil || *got.Sentiment != "positive" {
			t.Errorf("Sentiment = %v", got.Sentiment)
		}
		if got.ProcessedAt == nil || *got.ProcessedAt != "2025-12-01T10:00:05Z" {
			t.Errorf("ProcessedAt = %v", got.ProcessedAt)
		}
	})
}

func TestToResponses_PreservesOrder(t *testing.T) {
	in := []repository.Comment{
		{CommentID: "A", Status: "pending"},
		{CommentID: "B", Status: "processed"},
		{CommentID: "C", Status: "failed"},
	}
	got := toResponses(in)
	if len(got) != len(in) {
		t.Fatalf("len = %d, want %d", len(got), len(in))
	}
	for i, want := range []string{"A", "B", "C"} {
		if got[i].CommentID != want {
			t.Errorf("got[%d].CommentID = %q, want %q", i, got[i].CommentID, want)
		}
	}
}
