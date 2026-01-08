package kafka

import (
	"net"
	"strconv"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

// EnsureTopics creates topics if they do not exist.
func EnsureTopics(brokers []string, topics []string) error {
	if len(brokers) == 0 || len(topics) == 0 {
		return nil
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		return err
	}

	addr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = ctrlConn.Close() }()

	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			continue
		}
		if _, err := ctrlConn.ReadPartitions(topic); err == nil {
			continue
		}
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil {
			if strings.Contains(err.Error(), "Topic with this name already exists") {
				continue
			}
			return err
		}
	}

	return nil
}
