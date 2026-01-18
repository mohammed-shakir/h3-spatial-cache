package kafkaconsumer

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	Brokers             []string
	Topic               string
	GroupID             string
	SessionTimeout      time.Duration
	Heartbeat           time.Duration
	RebalanceTimeout    time.Duration
	InitialOffsetOldest bool
}

func FromEnv() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "spatial-invalidation"
	}
	group := os.Getenv("KAFKA_GROUP_ID")
	if group == "" {
		group = "cache-invalidator"
	}

	return Config{
		Brokers:             splitCSV(brokers),
		Topic:               topic,
		GroupID:             group,
		SessionTimeout:      30 * time.Second,
		Heartbeat:           3 * time.Second,
		RebalanceTimeout:    30 * time.Second,
		InitialOffsetOldest: true,
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
