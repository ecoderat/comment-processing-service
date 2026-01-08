package controller

import (
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"

	"comment-processing-service/internal/repository"
)

type CommentController interface {
	ListComments(c fiber.Ctx) error
	GetComment(c fiber.Ctx) error
}

type commentController struct {
	repo   repository.Repository
	logger *log.Logger
}

type CommentResponse struct {
	CommentID   string  `json:"commentId"`
	Text        string  `json:"text"`
	Sentiment   *string `json:"sentiment,omitempty"`
	Status      string  `json:"status"`
	EventTime   string  `json:"eventTime"`
	ProcessedAt *string `json:"processedAt,omitempty"`
}

type ListResponse struct {
	Comments []CommentResponse `json:"comments"`
	Limit    int               `json:"limit"`
	Offset   int               `json:"offset"`
}

func NewCommentController(repo repository.Repository, logger *log.Logger) CommentController {
	if logger == nil {
		logger = log.Default()
	}
	return &commentController{repo: repo, logger: logger}
}

func (c *commentController) ListComments(ctx fiber.Ctx) error {
	start := time.Now()
	limit := parseInt(ctx.Query("limit"), 50)
	if limit > 200 {
		limit = 200
	}
	offset := parseInt(ctx.Query("offset"), 0)
	if offset < 0 {
		offset = 0
	}

	var since *time.Time
	if sinceStr := ctx.Query("since"); sinceStr != "" {
		parsed, err := parseTime(sinceStr)
		if err != nil {
			return writeError(ctx, fiber.StatusBadRequest, err.Error())
		}
		since = &parsed
	}

	var until *time.Time
	if untilStr := ctx.Query("until"); untilStr != "" {
		parsed, err := parseTime(untilStr)
		if err != nil {
			return writeError(ctx, fiber.StatusBadRequest, err.Error())
		}
		until = &parsed
	}

	params := repository.ListParams{
		Sentiment: ctx.Query("sentiment"),
		Status:    ctx.Query("status"),
		Since:     since,
		Until:     until,
		Limit:     limit,
		Offset:    offset,
	}

	comments, err := c.repo.ListComments(ctx.Context(), params)
	if err != nil {
		c.logger.Printf("list comments failed: %v", err)
		return writeError(ctx, fiber.StatusInternalServerError, "query error")
	}

	resp := ListResponse{Comments: toResponses(comments), Limit: limit, Offset: offset}
	c.logger.Printf("list comments ok count=%d duration_ms=%d", len(comments), time.Since(start).Milliseconds())
	return ctx.Status(fiber.StatusOK).JSON(resp)
}

func (c *commentController) GetComment(ctx fiber.Ctx) error {
	start := time.Now()
	commentID := ctx.Params("commentId")
	if commentID == "" {
		return writeError(ctx, fiber.StatusNotFound, "commentId required")
	}

	comment, err := c.repo.GetComment(ctx.Context(), commentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return writeError(ctx, fiber.StatusNotFound, "not found")
		}
		c.logger.Printf("get comment failed: %v", err)
		return writeError(ctx, fiber.StatusInternalServerError, "query error")
	}

	c.logger.Printf("get comment ok id=%s duration_ms=%d", commentID, time.Since(start).Milliseconds())
	return ctx.Status(fiber.StatusOK).JSON(toResponse(comment))
}

func toResponses(comments []repository.Comment) []CommentResponse {
	result := make([]CommentResponse, 0, len(comments))
	for _, comment := range comments {
		result = append(result, toResponse(comment))
	}
	return result
}

func toResponse(comment repository.Comment) CommentResponse {
	resp := CommentResponse{
		CommentID: comment.CommentID,
		Text:      comment.Text,
		Sentiment: comment.Sentiment,
		Status:    comment.Status,
		EventTime: comment.EventTime.UTC().Format(time.RFC3339Nano),
	}
	if comment.ProcessedAt != nil {
		val := comment.ProcessedAt.UTC().Format(time.RFC3339Nano)
		resp.ProcessedAt = &val
	}
	return resp
}

func parseInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseTime(value string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, value)
}

func writeError(ctx fiber.Ctx, status int, message string) error {
	return ctx.Status(status).JSON(fiber.Map{"error": message})
}
