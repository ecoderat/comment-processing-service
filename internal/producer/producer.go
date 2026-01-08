package producer

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
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

type rawComment struct {
	EventID   string `json:"event_id"`
	EventTime string `json:"event_time"`
	CommentID string `json:"comment_id"`
	Text      string `json:"text"`
}

// Run emits raw comments to Kafka until the context is cancelled.
func Run(ctx context.Context, cfg Config) error {
	rng := rand.New(rand.NewSource(cfg.Seed))

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		Balancer: &kafka.Hash{},
	})
	defer func() {
		_ = writer.Close()
	}()

	log.Printf("producer started topic=%s brokers=%s", cfg.Topic, strings.Join(cfg.Brokers, ","))

	var recentTexts []string
	burstRemaining := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if burstRemaining == 0 && rng.Intn(100) < clampPercent(cfg.BurstPercent) {
			burstRemaining = cfg.BurstCount
		}

		text := generateText(rng, cfg.Size, &recentTexts, clampPercent(cfg.ReusePercent))
		msg := rawComment{
			EventID:   newUUID(),
			EventTime: time.Now().UTC().Format(time.RFC3339Nano),
			CommentID: randomID(rng, 12),
			Text:      text,
		}

		payload, err := json.Marshal(msg)
		if err != nil {
			log.Printf("marshal error: %v", err)
			continue
		}

		kmsg := kafka.Message{
			Key:   []byte(msg.CommentID),
			Value: payload,
		}

		if err := writer.WriteMessages(ctx, kmsg); err != nil {
			log.Printf("kafka write error: %v", err)
		} else {
			log.Printf("sent comment_id=%s event_id=%s size=%d", msg.CommentID, msg.EventID, len(msg.Text))
		}

		if burstRemaining > 0 {
			burstRemaining--
			sleepWithJitter(rng, cfg.BurstInterval)
			continue
		}

		sleepWithJitter(rng, cfg.Interval)
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

func newUUID() string {
	var b [16]byte
	_, err := crand.Read(b[:])
	if err != nil {
		return randomHex(16)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return formatUUID(b[:])
}

func formatUUID(b []byte) string {
	buf := make([]byte, 36)
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf)
}

func randomID(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

func randomHex(n int) string {
	max := new(big.Int).Lsh(big.NewInt(1), uint(n*4))
	v, err := crand.Int(crand.Reader, max)
	if err != nil {
		return "0000000000000000"
	}
	s := v.Text(16)
	if len(s) < n {
		return strings.Repeat("0", n-len(s)) + s
	}
	return s
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
