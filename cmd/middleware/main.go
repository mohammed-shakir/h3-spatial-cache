package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/app/server"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/observability"
)

var Version = "dev"

func main() {
	cfg := config.FromEnv()
	logger := observability.NewLogger(cfg.LogLevel)
	logger.Info("starting middleware",
		"addr", cfg.Addr,
		"version", Version,
		"geoserver", cfg.GeoServerURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := server.Run(ctx, cfg, logger)
	stop()

	if err != nil {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("server stopped")
}
