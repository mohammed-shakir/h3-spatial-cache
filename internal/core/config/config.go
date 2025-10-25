package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr           string
	LogLevel       string
	GeoServerURL   string
	RedisAddr      string
	KafkaBrokers   string
	H3Res          int
	Scenario       string
	HotThreshold   float64
	HotHalfLife    time.Duration
	H3ResMin       int
	H3ResMax       int
	CacheOpTimeout time.Duration
}

func FromEnv() Config {
	res := getint("H3_RES", 8)
	minRes := getint("H3_RES_MIN", res)
	maxRes := getint("H3_RES_MAX", res)

	// clamp to 0..15 and repair bad bounds
	if minRes < 0 {
		minRes = 0
	}
	if maxRes > 15 {
		maxRes = 15
	}
	if minRes > maxRes {
		minRes, maxRes = res, res
	}

	return Config{
		Addr:           getenv("ADDR", ":8090"),
		LogLevel:       getenv("LOG_LEVEL", "info"),
		GeoServerURL:   getenv("GEOSERVER_URL", "http://localhost:8080/geoserver"),
		RedisAddr:      getenv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:   getenv("KAFKA_BROKERS", "localhost:9092"),
		H3Res:          res,
		Scenario:       getenv("SCENARIO", "baseline"),
		HotThreshold:   getfloat("HOT_THRESHOLD", 10.0),
		HotHalfLife:    getduration("HOT_HALF_LIFE", time.Minute),
		H3ResMin:       minRes,
		H3ResMax:       maxRes,
		CacheOpTimeout: getduration("CACHE_OP_TIMEOUT", 250*time.Millisecond),
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

func getfloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getduration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
