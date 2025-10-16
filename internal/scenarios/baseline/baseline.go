package baseline

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
)

type Engine struct {
	logger *slog.Logger
	exec   executor.Interface
}

func init() {
	scenarios.Register("baseline", newBaseline)
}

func newBaseline(cfg config.Config, logger *slog.Logger, exec executor.Interface) (router.QueryHandler, error) {
	return &Engine{logger: logger, exec: exec}, nil
}

func (e *Engine) HandleQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	e.exec.ForwardWFS(ctx, w, r, q)
}
