package main

import (
	"flag"
	"os"
)

type Config struct {
	Addr         string
	LogLevel     string
	GeoServerURL string
}

// Configurations for baseline-server
func LoadConfig() Config {
	var cfg Config
	cfg.Addr = getEnv("BASELINE_ADDR", ":8090")
	cfg.LogLevel = getEnv("LOG_LEVEL", "info")
	cfg.GeoServerURL = getEnv("GEOSERVER_URL", "http://localhost:8080/geoserver")

	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP listen address")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level: debug|info|warn|error")
	flag.StringVar(&cfg.GeoServerURL, "geoserver", cfg.GeoServerURL, "GeoServer base URL")
	flag.Parse()
	return cfg
}

func getEnv(k, def string) string {
	value := os.Getenv(k)
	if value != "" {
		return value
	}
	return def
}
