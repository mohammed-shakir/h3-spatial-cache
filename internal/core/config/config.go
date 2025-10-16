package config

import (
	"os"
	"strconv"
)

type Config struct {
	Addr         string
	LogLevel     string
	GeoServerURL string
	RedisAddr    string
	KafkaBrokers string
	H3Res        int
	Scenario     string
}

func FromEnv() Config {
	return Config{
		Addr:         getenv("ADDR", ":8090"),
		LogLevel:     getenv("LOG_LEVEL", "info"),
		GeoServerURL: getenv("GEOSERVER_URL", "http://localhost:8080/geoserver"),
		RedisAddr:    getenv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: getenv("KAFKA_BROKERS", "localhost:9092"),
		H3Res:        getint("H3_RES", 8),
		Scenario:     getenv("SCENARIO", "baseline"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getint(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
