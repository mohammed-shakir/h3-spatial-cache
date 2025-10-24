package baseline

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
)

type Engine struct {
	logger *slog.Logger
	exec   executor.Interface

	res  int
	mapr *h3mapper.Mapper
}

func init() {
	scenarios.Register("baseline", newBaseline)
}

func newBaseline(cfg config.Config, logger *slog.Logger, exec executor.Interface) (router.QueryHandler, error) {
	return &Engine{
		logger: logger,
		exec:   exec,
		res:    cfg.H3Res,
		mapr:   h3mapper.New(),
	}, nil
}

func (e *Engine) HandleQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	var cells model.Cells
	var err error

	if q.Polygon != nil {
		cells, err = e.mapr.CellsForPolygon(*q.Polygon, e.res)
	} else if q.BBox != nil {
		cells, err = e.mapr.CellsForBBox(*q.BBox, e.res)
	}

	if err != nil {
		e.logger.Debug("h3 mapping failed", "err", err)
	} else if len(cells) > 0 {
		e.logger.Debug("h3 mapping success",
			"layer", q.Layer,
			"res", e.res,
			"cells", len(cells))
	}

	q.H3Res = e.res
	q.Cells = cells

	e.exec.ForwardWFS(ctx, w, r, q)
}
