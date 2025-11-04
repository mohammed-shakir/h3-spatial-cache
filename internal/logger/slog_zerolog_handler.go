package logger

import (
	"context"
	"log/slog"

	"github.com/rs/zerolog"
)

type zlHandler struct {
	zl   *zerolog.Logger
	attr []slog.Attr
}

func NewSlog(zl *zerolog.Logger) *slog.Logger {
	return slog.New(&zlHandler{zl: zl})
}

func (h *zlHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *zlHandler) Handle(ctx context.Context, r slog.Record) error {
	base := FromContext(ctx, h.zl)

	var ev *zerolog.Event
	switch {
	case r.Level <= slog.LevelDebug:
		ev = base.Debug()
	case r.Level == slog.LevelWarn:
		ev = base.Warn()
	case r.Level >= slog.LevelError:
		ev = base.Error()
	default:
		ev = base.Info()
	}

	// attach accumulated attrs
	for _, a := range h.attr {
		ev = addAttr(ev, a)
	}
	// attach record attrs
	r.Attrs(func(a slog.Attr) bool {
		ev = addAttr(ev, a)
		return true
	})

	ev.Msg(r.Message)
	return nil
}

func (h *zlHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := *h
	cp.attr = append(cp.attr, attrs...)
	return &cp
}

func (h *zlHandler) WithGroup(_ string) slog.Handler { return h }

func addAttr(ev *zerolog.Event, a slog.Attr) *zerolog.Event {
	a.Value = a.Value.Resolve()
	switch a.Value.Kind() {
	case slog.KindString:
		return ev.Str(a.Key, a.Value.String())
	case slog.KindInt64:
		return ev.Int64(a.Key, a.Value.Int64())
	case slog.KindFloat64:
		return ev.Float64(a.Key, a.Value.Float64())
	case slog.KindBool:
		return ev.Bool(a.Key, a.Value.Bool())
	default:
		return ev.Interface(a.Key, a.Value.Any())
	}
}
