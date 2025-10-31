package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/httpclient"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/server"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/metrics"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/baseline"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/cache"
)

var Version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
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
		return 1
	}

	// selected scenario
	handler, err := scenarios.New(cfg.Scenario, cfg, logger, exec)
	if err != nil {
		logger.Error("scenario setup failed", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metricsEnabled := os.Getenv("METRICS_ENABLED") == "true"
	if metricsEnabled {
		addr := os.Getenv("METRICS_ADDR")
		if addr == "" {
			addr = ":9090"
		}
		path := os.Getenv("METRICS_PATH")
		if path == "" {
			path = "/metrics"
		}

		p := metrics.Init(metrics.Config{
			Enabled: true,
			Addr:    addr,
			Path:    path,
			Build: metrics.BuildInfo{
				Version:   os.Getenv("BUILD_VERSION"),
				Revision:  os.Getenv("BUILD_REVISION"),
				Branch:    os.Getenv("BUILD_BRANCH"),
				BuildDate: os.Getenv("BUILD_DATE"),
			},
		})
		mux := http.NewServeMux()
		mux.Handle(path, p.Handler())

		srv := &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		// start server
		go func() {
			log.Printf("metrics: listening on %s%s", addr, path)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("metrics server exited: %v", err)
			}
		}()

		// shutdown on signal
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				log.Printf("metrics: shutdown error: %v", err)
			}
		}()
	}

	if err := server.Run(ctx, cfg, logger, handler); err != nil {
		logger.Error("server exited with error", "err", err)
		return 1
	}
	logger.Info("server stopped")
	return 0
}
