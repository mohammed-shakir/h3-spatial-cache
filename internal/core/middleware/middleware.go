// Package middleware defines HTTP middlewares for the core server.
package middleware

import (
	"log/slog"
	"net/http"

	mylog "github.com/mohammed-shakir/h3-spatial-cache/internal/logger"
)

func Logging(l *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			reqID := r.Header.Get("X-Request-ID")
			if reqID == "" {
				reqID = mylog.NewID()
				w.Header().Set("X-Request-ID", reqID)
			}
			ctx := mylog.WithRequestID(r.Context(), reqID)
			ctx = mylog.WithComponent(ctx, "http")
			l.LogAttrs(ctx, slog.LevelDebug, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
			)
			next.ServeHTTP(w, r.WithContext(ctx))
		}
		return http.HandlerFunc(fn)
	}
}

// Recover basic panic recovery middleware
func Recover() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("panic recovered", "err", rec)
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}

// CORS minimal cors placeholder
func CORS() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Methods", "GET,OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}
