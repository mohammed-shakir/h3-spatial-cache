package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr                string
	LogLevel            string
	GeoServerURL        string
	RedisAddr           string
	KafkaBrokers        string
	H3Res               int
	Scenario            string
	HotThreshold        float64
	HotHalfLife         time.Duration
	H3ResMin            int
	H3ResMax            int
	CacheOpTimeout      time.Duration
	CacheTTLDefault     time.Duration
	CacheTTLOvr         map[string]time.Duration
	CacheFillMaxWorkers int
	CacheFillQueue      int
}

func FromEnv() Config {
	res := getint("H3_RES", 8)
	minRes := getint("H3_RES_MIN", res)
	maxRes := getint("H3_RES_MAX", res)

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
		Addr:                getenv("ADDR", ":8090"),
		LogLevel:            getenv("LOG_LEVEL", "info"),
		GeoServerURL:        getenv("GEOSERVER_URL", "http://localhost:8080/geoserver"),
		RedisAddr:           getenv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:        getenv("KAFKA_BROKERS", "localhost:9092"),
		H3Res:               res,
		Scenario:            getenv("SCENARIO", "baseline"),
		HotThreshold:        getfloat("HOT_THRESHOLD", 10.0),
		HotHalfLife:         getduration("HOT_HALF_LIFE", time.Minute),
		H3ResMin:            minRes,
		H3ResMax:            maxRes,
		CacheOpTimeout:      getduration("CACHE_OP_TIMEOUT", 250*time.Millisecond),
		CacheTTLDefault:     getduration("CACHE_TTL_DEFAULT", 60*time.Second),
		CacheTTLOvr:         parseDurationMap(getenv("CACHE_TTL_OVERRIDES", "")),
		CacheFillMaxWorkers: getint("CACHE_FILL_MAX_WORKERS", 8),
		CacheFillQueue:      getint("CACHE_FILL_QUEUE", 64),
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

// parse "layer=5m,other=30s" into map
func parseDurationMap(s string) map[string]time.Duration {
	out := map[string]time.Duration{}
	s = strings.TrimSpace(s)
	if s == "" {
		return out
	}
	parts := strings.SplitSeq(s, ",")
	for p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		if k == "" {
			continue
		}
		if d, err := time.ParseDuration(v); err == nil {
			out[k] = d
		}
	}
	return out
}
