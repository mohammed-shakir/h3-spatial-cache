package baseline

import (
	"context"
	"log/slog"
	"net/http"
	"sort"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/decision"
	simpledec "github.com/mohammed-shakir/h3-spatial-cache/internal/decision/simple"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness/expdecay"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
)

type Engine struct {
	logger *slog.Logger
	exec   executor.Interface
	res    int
	mapr   *h3mapper.Mapper
	hot    hotness.Interface
	dec    decision.Interface
	thr    float64
}

func init() {
	scenarios.Register("baseline", newBaseline)
}

func newBaseline(cfg config.Config, logger *slog.Logger, exec executor.Interface) (router.QueryHandler, error) {
	hot := expdecay.New(cfg.HotHalfLife)
	dec := simpledec.New(hot, cfg.HotThreshold, cfg.H3Res, cfg.H3ResMin, cfg.H3ResMax, h3mapper.New())

	// collects hotness metrics
	return &Engine{
		logger: logger,
		exec:   exec,
		res:    cfg.H3Res,
		mapr:   h3mapper.New(),

		hot: hot,
		dec: dec,
		thr: cfg.HotThreshold,
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

	// track h3 mapped regions and update hotness
	if err != nil {
		e.logger.Debug("h3 mapping failed", "err", err)
	} else if len(cells) > 0 {
		e.logger.Debug("h3 mapping success", "layer", q.Layer, "res", e.res, "cells", len(cells))
	}

	for _, c := range cells {
		e.hot.Inc(c)
	}

	should := e.dec.ShouldCache(cells)

	if should {
		observability.IncDecision("cache")
	} else {
		observability.IncDecision("nocache")
	}

	// log top hot cells
	type sc struct {
		cell  string
		score float64
	}
	top := make([]sc, 0, len(cells))
	for _, c := range cells {
		top = append(top, sc{cell: c, score: e.hot.Score(c)})
	}
	sort.Slice(top, func(i, j int) bool { return top[i].score > top[j].score })
	if len(top) > 5 {
		top = top[:5]
	}
	topPairs := make([]any, 0, 2*len(top))
	for _, t := range top {
		topPairs = append(topPairs, t.cell, t.score)
	}
	e.logger.Debug("cache decision",
		append([]any{
			"layer", q.Layer,
			"res", e.res,
			"cells", len(cells),
			"shouldCache", should,
			"threshold", e.thr,
		}, topPairs...)...,
	)

	q.H3Res = e.res
	q.Cells = cells
	e.exec.ForwardWFS(ctx, w, r, q)
}
