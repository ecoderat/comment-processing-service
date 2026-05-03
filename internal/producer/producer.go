package producer

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"comment-processing-service/internal/model"
)

type Config struct {
	Brokers       []string
	Topic         string
	Interval      time.Duration
	BurstPercent  int
	BurstInterval time.Duration
	BurstCount    int
	Size          int
	ReusePercent  int
	Seed          int64
}

type Service struct {
	cfg    Config
	writer *kafka.Writer
	rng    *rand.Rand
	logger *logrus.Logger
}

// NewService builds a producer service with its writer and RNG.
func NewService(cfg Config, logger *logrus.Logger) *Service {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		Balancer: &kafka.Hash{},
	})
	return &Service{
		cfg:    cfg,
		writer: writer,
		rng:    rand.New(rand.NewSource(cfg.Seed)),
		logger: logger,
	}
}

// Run emits raw comments to Kafka until the context is cancelled.
func (s *Service) Run(ctx context.Context) error {
	defer func() {
		_ = s.writer.Close()
	}()

	s.logger.WithFields(logrus.Fields{
		"topic":   s.cfg.Topic,
		"brokers": strings.Join(s.cfg.Brokers, ","),
	}).Info("producer started")

	var recentTexts []string
	burstRemaining := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if burstRemaining == 0 && s.rng.Intn(100) < clampPercent(s.cfg.BurstPercent) {
			burstRemaining = s.cfg.BurstCount
		}

		text := generateText(s.rng, s.cfg.Size, &recentTexts, clampPercent(s.cfg.ReusePercent))
		msg := model.RawComment{
			EventID:   uuid.NewString(),
			EventTime: time.Now().UTC(),
			CommentID: randomID(s.rng, 12),
			Text:      text,
		}

		payload, err := json.Marshal(msg)
		if err != nil {
			s.logger.WithError(err).Error("marshal error")
			continue
		}

		kmsg := kafka.Message{
			Key:   []byte(msg.CommentID),
			Value: payload,
		}

		if err := s.writer.WriteMessages(ctx, kmsg); err != nil {
			s.logger.WithError(err).Error("kafka write error")
		} else {
			s.logger.WithFields(logrus.Fields{
				"comment_id": msg.CommentID,
				"event_id":   msg.EventID,
				"text_size":  len(msg.Text),
			}).Info("sent comment")
		}

		if burstRemaining > 0 {
			burstRemaining--
			sleepWithJitter(s.rng, s.cfg.BurstInterval)
			continue
		}

		sleepWithJitter(s.rng, s.cfg.Interval)
	}
}

func generateText(rng *rand.Rand, size int, cache *[]string, reusePercent int) string {
	if len(*cache) > 0 && rng.Intn(100) < reusePercent {
		return (*cache)[rng.Intn(len(*cache))]
	}

	wordCount := size
	if wordCount < 3 {
		wordCount = 3
	}
	wordCount += rng.Intn(int(math.Max(3, float64(size/3))))

	words := make([]string, 0, wordCount)
	for i := 0; i < wordCount; i++ {
		words = append(words, loremWords[rng.Intn(len(loremWords))])
	}

	text := strings.Join(words, " ")
	*cache = append(*cache, text)
	if len(*cache) > 50 {
		*cache = (*cache)[len(*cache)-50:]
	}
	return text
}

func sleepWithJitter(rng *rand.Rand, d time.Duration) {
	if d <= 0 {
		return
	}
	jitter := time.Duration(rng.Int63n(int64(d / 5)))
	time.Sleep(d + jitter)
}

func clampPercent(v int) int {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func randomID(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

var loremWords = []string{
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
	"sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore",
	"magna", "aliqua", "ut", "enim", "ad", "minim", "veniam", "quis", "nostrud",
	"exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea",
	"commodo", "consequat", "duis", "aute", "irure", "dolor", "in", "reprehenderit",
	"in", "voluptate", "velit", "esse", "cillum", "dolore", "eu", "fugiat",
	"nulla", "pariatur", "excepteur", "sint", "occaecat", "cupidatat", "non",
	"proident", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit",
	"anim", "id", "est", "laborum",
}
