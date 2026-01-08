package sentiment

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"time"

	"comment-processing-service/proto/sentimentpb"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	sentimentpb.UnimplementedSentimentServiceServer
	limiter *rate.Limiter
	mu      sync.Mutex
	labels  map[string]string
	rng     *rand.Rand
}

func newServer() *server {
	return &server{
		limiter: rate.NewLimiter(100, 100),
		labels:  make(map[string]string),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run starts the sentiment gRPC server on the given address.
func Run(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	sentimentpb.RegisterSentimentServiceServer(grpcServer, newServer())
	logrus.StandardLogger().WithField("addr", lis.Addr().String()).Info("sentiment-grpc listening")
	return grpcServer.Serve(lis)
}

func (s *server) Analyze(ctx context.Context, req *sentimentpb.AnalyzeRequest) (*sentimentpb.AnalyzeResponse, error) {
	if !s.limiter.Allow() {
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	if s.randIntn(100) < 5 {
		return nil, status.Error(codes.Unavailable, "random drop")
	}

	sleep := time.Duration(len(req.GetText())) * 2 * time.Millisecond
	if sleep > 2*time.Second {
		sleep = 2 * time.Second
	}
	if sleep > 0 {
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.Error(codes.DeadlineExceeded, "client cancelled")
		case <-timer.C:
		}
	}

	text := req.GetText()
	s.mu.Lock()
	label, ok := s.labels[text]
	if !ok {
		label = s.randomLabelLocked()
		s.labels[text] = label
	}
	s.mu.Unlock()

	return &sentimentpb.AnalyzeResponse{Label: label}, nil
}

func (s *server) randIntn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Intn(n)
}

func (s *server) randomLabelLocked() string {
	switch s.rng.Intn(3) {
	case 0:
		return "positive"
	case 1:
		return "negative"
	default:
		return "neutral"
	}
}
