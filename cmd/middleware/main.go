package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/httpclient"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/server"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/baseline"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/cache"
)

var Version = "dev"

func main() {
	// overriding scenario via flag
	scenarioFlag := flag.String("scenario", "", "scenario name")
	flag.Parse()

	cfg := config.FromEnv()
	if *scenarioFlag != "" {
		cfg.Scenario = strings.TrimSpace(*scenarioFlag)
	}
	logger := observability.NewLogger(cfg.LogLevel)
	observability.SetScenario(cfg.Scenario)
	observability.ExposeBuildInfo(Version)
	logger.Info("starting middleware",
		"addr", cfg.Addr,
		"version", Version,
		"geoserver", cfg.GeoServerURL,
		"scenario", cfg.Scenario)

	httpClient := httpclient.NewOutbound()
	owsURL := ogc.OWSEndpoint(cfg.GeoServerURL)

	exec, err := executor.New(logger, httpClient, owsURL)
	if err != nil {
		logger.Error("failed to initialize executor", "err", err)
		os.Exit(1)
	}

	// selected scenario
	handler, err := scenarios.New(cfg.Scenario, cfg, logger, exec)
	if err != nil {
		logger.Error("scenario setup failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err = server.Run(ctx, cfg, logger, handler)
	stop()

	if err != nil {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("server stopped")
}
