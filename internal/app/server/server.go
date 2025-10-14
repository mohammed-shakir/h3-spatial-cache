package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/health"
	imw "github.com/mohammed-shakir/h3-spatial-cache/internal/middleware"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/ogc"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/router"
)

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	// shared http client for outbound calls (GeoServer)
	httpTransport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	httpClient := &http.Client{Transport: httpTransport, Timeout: 30 * time.Second}

	owsURL := ogc.OWSEndpoint(cfg.GeoServerURL)
	exec := executor.New(logger, httpClient, owsURL)

	// router
	r := chi.NewRouter()
	r.Use(imw.Recover())
	r.Use(imw.Logging(logger))
	r.Use(imw.CORS())

	r.Get("/healthz", health.Liveness())

	// /query handler that parses the request and forwards to geoserver
	r.Get("/query", func(w http.ResponseWriter, req *http.Request) {
		qr, warn, err := router.ParseQueryRequest(req)
		if warn != "" {
			logger.Warn(warn)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exec.ForwardGetFeature(w, req, qr)
	})

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
