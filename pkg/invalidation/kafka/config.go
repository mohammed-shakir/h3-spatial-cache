package kafka

import (
	"os"
	"strings"
	"time"
)

type Driver string

const (
	DriverNone  Driver = "none"
	DriverKafka Driver = "kafka"
)

type TLSConfig struct {
	Enable     bool   `yaml:"enable"`
	CaFile     string `yaml:"ca_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	SkipVerify bool   `yaml:"skip_verify"`
}

type SASLConfig struct {
	Enable    bool   `yaml:"enable"`
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	TokenURL  string `yaml:"token_url"`
}

type InvalidationConfig struct {
	Enabled bool   `yaml:"enabled"`
	Driver  Driver `yaml:"driver"`

	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupID string   `yaml:"group_id"`

	SessionTimeout   time.Duration `yaml:"session_timeout"`
	Heartbeat        time.Duration `yaml:"heartbeat"`
	RebalanceTimeout time.Duration `yaml:"rebalance_timeout"`
	InitialOldest    bool          `yaml:"initial_oldest"`

	TLS  TLSConfig  `yaml:"tls"`
	SASL SASLConfig `yaml:"sasl"`
}

func FromEnv() InvalidationConfig {
	enabled := strings.ToLower(os.Getenv("INVALIDATION_ENABLED")) == "true"
	driver := Driver(strings.TrimSpace(os.Getenv("INVALIDATION_DRIVER")))
	if driver == "" {
		driver = DriverNone
	}
	brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = "spatial-invalidation"
	}
	group := strings.TrimSpace(os.Getenv("KAFKA_GROUP_ID"))
	if group == "" {
		group = "cache-invalidator"
	}

	return InvalidationConfig{
		Enabled:          enabled,
		Driver:           driver,
		Brokers:          split(brokers),
		Topic:            topic,
		GroupID:          group,
		SessionTimeout:   30 * time.Second,
		Heartbeat:        3 * time.Second,
		RebalanceTimeout: 30 * time.Second,
		InitialOldest:    true,
	}
}

func split(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if x := strings.TrimSpace(p); x != "" {
			out = append(out, x)
		}
	}
	return out
}
