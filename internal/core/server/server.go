package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/health"
	middleware "github.com/mohammed-shakir/h3-spatial-cache/internal/core/middleware"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
)

// sets up http and starts serving
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger, handler router.QueryHandler) error {
	r := chi.NewRouter()
	r.Use(middleware.Recover())
	r.Use(middleware.Logging(logger))
	r.Use(middleware.CORS())

	r.Get("/healthz", health.Liveness())
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Get("/query", router.HandleQuery(logger, cfg, handler))

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http listen", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}
