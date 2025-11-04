package logger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	Level     string
	Console   bool
	SampleN   int
	Scenario  string
	Component string
}

type ctxKey string

const (
	ctxReqIDKey  ctxKey = "request_id"
	ctxHitClass  ctxKey = "hit_class"
	ctxComponent ctxKey = "component"
	ctxScenario  ctxKey = "scenario"
)

func WithRequestID(ctx context.Context, reqID string) context.Context {
	if reqID == "" {
		reqID = NewID()
	}
	return context.WithValue(ctx, ctxReqIDKey, reqID)
}

func WithHitClass(ctx context.Context, hit string) context.Context {
	if hit == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxHitClass, hit)
}

func WithScenario(ctx context.Context, scenario string) context.Context {
	if scenario == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxScenario, scenario)
}

func WithComponent(ctx context.Context, component string) context.Context {
	if component == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxComponent, component)
}

func NewID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func safeUint32(n int) uint32 {
	if n <= 0 {
		return 0
	}
	if n > int(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(n)
}

func Build(cfg Config, out io.Writer) zerolog.Logger {
	if out == nil {
		out = os.Stdout
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "timestamp"
	zerolog.LevelFieldName = "level"
	zerolog.MessageFieldName = "msg"

	if cfg.Console {
		out = zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	}

	base := zerolog.New(out)

	if cfg.SampleN > 0 {
		n := safeUint32(cfg.SampleN)
		if n > 0 {
			base = base.Sample(&zerolog.BasicSampler{N: n})
		}
	}

	lvl := strings.ToLower(strings.TrimSpace(cfg.Level))
	switch lvl {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	ctx := base.With().Timestamp()
	if cfg.Scenario != "" {
		ctx = ctx.Str("scenario", cfg.Scenario)
	}
	if cfg.Component != "" {
		ctx = ctx.Str("component", cfg.Component)
	}
	return ctx.Logger()
}

// returns a child logger with context fields applied
func FromContext(ctx context.Context, parent *zerolog.Logger) *zerolog.Logger {
	var base zerolog.Logger
	if parent == nil {
		base = zerolog.New(io.Discard)
	} else {
		base = *parent
	}
	w := base.With()
	if v := ctx.Value(ctxReqIDKey); v != nil {
		if s, ok := v.(string); ok && s != "" {
			w = w.Str("request_id", s)
		}
	}
	if v := ctx.Value(ctxScenario); v != nil {
		if s, ok := v.(string); ok && s != "" {
			w = w.Str("scenario", s)
		}
	}
	if v := ctx.Value(ctxComponent); v != nil {
		if s, ok := v.(string); ok && s != "" {
			w = w.Str("component", s)
		}
	}
	if v := ctx.Value(ctxHitClass); v != nil {
		if s, ok := v.(string); ok && s != "" {
			w = w.Str("hit_class", s)
		}
	}
	l := w.Logger()
	return &l
}
