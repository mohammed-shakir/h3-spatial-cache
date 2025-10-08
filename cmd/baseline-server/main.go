package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var Version = "dev"

func main() {
	cfg := LoadConfig()
	logger := newLogger(cfg.LogLevel)
	logger.Info("starting baseline-server", "addr", cfg.Addr, "version", Version, "geoserver", cfg.GeoServerURL)

	// HTTP client for forwarding requests to GeoServer
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

	// HTTP server
	router := http.NewServeMux()
	router.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	router.Handle("GET /query", queryHandler(logger, cfg.GeoServerURL, httpClient))
	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Start server
	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("http listen", "addr", cfg.Addr)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// Shutdown
	shutdownSignalCh := make(chan os.Signal, 1)
	signal.Notify(shutdownSignalCh, syscall.SIGINT, syscall.SIGTERM) // SIGINT=Ctrl+C, SIGTERM=sent by docker stop or kubernetes
	select {
	case sig := <-shutdownSignalCh:
		logger.Info("signal received, shutting down", "signal", sig.String())
	case err := <-serverErrCh:
		logger.Error("server error", "err", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownCtx)
	logger.Info("server stopped")
}

func newLogger(level string) *slog.Logger {
	logLevel := new(slog.LevelVar)
	switch level {
	case "debug":
		logLevel.Set(slog.LevelDebug)
	case "info":
		logLevel.Set(slog.LevelInfo)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		logLevel.Set(slog.LevelInfo)
	}
	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	return slog.New(textHandler)
}
