package scenarios

import (
	"fmt"
	"log/slog"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
)

type Factory func(cfg config.Config, logger *slog.Logger, exec executor.Interface) (router.QueryHandler, error)

var reg = map[string]Factory{}

func Register(name string, f Factory) {
	reg[name] = f
}

func New(name string, cfg config.Config, logger *slog.Logger, exec executor.Interface) (router.QueryHandler, error) {
	if f, ok := reg[name]; ok {
		return f(cfg, logger, exec)
	}
	if f, ok := reg["baseline"]; ok {
		logger.Warn("unknown scenario; falling back to baseline", "scenario", name)
		return f(cfg, logger, exec)
	}
	return nil, fmt.Errorf("no factory for scenario %q and no baseline registered", name)
}
