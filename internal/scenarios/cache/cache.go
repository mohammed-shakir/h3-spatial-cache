// Package cache implements the cache-aware query scenario.
package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	h3 "github.com/uber/h3-go/v4"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
	cacheiface "github.com/mohammed-shakir/h3-spatial-cache/internal/cache"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/cellindex"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/featurestore"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
	cachev2 "github.com/mohammed-shakir/h3-spatial-cache/internal/cache/v2"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/composer"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/httpclient"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness/expdecay"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness/metricswrap"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
	"github.com/mohammed-shakir/h3-spatial-cache/pkg/adaptive"
	adaptSimple "github.com/mohammed-shakir/h3-spatial-cache/pkg/adaptive/simple"
)

type Engine struct {
	logger          *slog.Logger
	res             int
	minRes          int
	maxRes          int
	mapr            *h3mapper.Mapper
	eng             composer.Engine
	store           cacheiface.Interface
	fs              featurestore.FeatureStore
	idx             cellindex.CellIndex
	owsURL          *url.URL
	http            *http.Client
	exec            executor.Interface
	ttlDefault      time.Duration
	ttlMap          map[string]time.Duration
	maxWorkers      int
	queueSize       int
	opTimeout       time.Duration
	adaptiveEnabled bool
	adaptiveDryRun  bool
	serveFreshOnly  bool
	gmlStreaming    bool
	decider         adaptive.Decider
	hot             *metricswrap.WithMetrics
	runID           string
}

func init() {
	scenarios.Register("cache", newCache)
}

// creates cache scenario query handler
func newCache(cfg config.Config, logger *slog.Logger, ex executor.Interface) (router.QueryHandler, error) {
	rc, err := redisstore.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		return nil, fmt.Errorf("redis client: %w", err)
	}
	v2store := cachev2.NewRedisStore(rc, cfg.CacheTTLDefault)
	ows := ogc.OWSEndpoint(cfg.GeoServerURL)
	u, err := url.Parse(ows)
	if err != nil {
		return nil, fmt.Errorf("parse ows url: %w", err)
	}

	e := &Engine{
		logger: logger,
		res:    cfg.H3Res,
		minRes: cfg.H3ResMin,
		maxRes: cfg.H3ResMax,

		mapr: h3mapper.New(),
		eng: composer.Engine{
			V2: composer.NewGeoJSONV2Adapter(geojsonagg.NewAdvanced()),
		},

		store: newCacheAdapter(rc, cfg.CacheOpTimeout),

		fs:  v2store.Features,
		idx: v2store.Cells,

		owsURL: u,
		http:   httpclient.NewOutbound(),
		exec:   ex,

		ttlDefault: cfg.CacheTTLDefault,
		ttlMap:     cfg.CacheTTLOvr,

		maxWorkers: cfg.CacheFillMaxWorkers,
		queueSize:  cfg.CacheFillQueue,
		opTimeout:  cfg.CacheOpTimeout,

		adaptiveEnabled: cfg.AdaptiveEnabled,
		adaptiveDryRun:  cfg.AdaptiveDryRun,
		serveFreshOnly:  cfg.AdaptiveServeOnlyIfFresh,
		gmlStreaming:    cfg.Features.GMLStreaming,
		runID:           fmt.Sprintf("%016x", cfg.AdaptiveSeed),
	}

	// Adaptive: construct hotness tracker and decider (but respect feature flag).
	if e.adaptiveEnabled {
		tr := expdecay.New(cfg.HotHalfLife)
		e.hot = metricswrap.New(tr, "topN")
		e.decider = adaptSimple.New(
			adaptSimple.Config{
				Threshold: cfg.HotThreshold,
				BaseRes:   cfg.H3Res,
				MinRes:    cfg.H3ResMin,
				MaxRes:    cfg.H3ResMax,
				TTLCold:   cfg.AdaptiveTTLCold,
				TTLWarm:   cfg.AdaptiveTTLWarm,
				TTLHot:    cfg.AdaptiveTTLHot,
				Seed:      cfg.AdaptiveSeed,
			},
			hotReadOnly{w: e.hot},
			e.mapr,
		)
	}

	return e, nil
}

type cacheAdapter struct {
	cli     *redisstore.Client
	timeout time.Duration
}

func newCacheAdapter(c *redisstore.Client, t time.Duration) cacheiface.Interface {
	return &cacheAdapter{cli: c, timeout: t}
}

// returns context with timeout if set
func (a *cacheAdapter) withTimeout() (context.Context, context.CancelFunc) {
	if a.timeout <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), a.timeout)
}

func (a *cacheAdapter) MGet(ks []string) (map[string][]byte, error) {
	ctx, cancel := a.withTimeout()
	defer cancel()
	m, err := a.cli.MGet(ctx, ks)
	if err != nil {
		return nil, fmt.Errorf("cache mget: %w", err)
	}
	return m, nil
}

func (a *cacheAdapter) Set(key string, val []byte, ttl time.Duration) error {
	ctx, cancel := a.withTimeout()
	defer cancel()
	if err := a.cli.Set(ctx, key, val, ttl); err != nil {
		return fmt.Errorf("cache set %q: %w", key, err)
	}
	return nil
}

func (a *cacheAdapter) Del(ks ...string) error {
	ctx, cancel := a.withTimeout()
	defer cancel()
	if err := a.cli.Del(ctx, ks...); err != nil {
		return fmt.Errorf("cache del %d keys: %w", len(ks), err)
	}
	return nil
}

type result struct {
	cell string
	key  string
	body []byte
	err  error
}

func (e *Engine) HandleQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	start := time.Now()

	neg := composer.NegotiateFormat(composer.NegotiationInput{
		AcceptHeader:  r.Header.Get("Accept"),
		OutputFormat:  r.URL.Query().Get("outputFormat"),
		DefaultFormat: composer.FormatGeoJSON,
	})
	if neg.Format == composer.FormatGML32 {
		if e.gmlStreaming && e.exec != nil {
			const gml32 = "application/gml+xml; version=3.2"
			e.exec.ForwardGetFeatureFormat(w, r, q, gml32)
			return
		}
		w.Header().Set("Vary", "Accept")
		http.Error(w, "gml not enabled; request GeoJSON or enable features.gml_streaming", http.StatusNotAcceptable)
		return
	}

	cells, err := e.cellsForRes(q, e.res)
	if err != nil {
		e.logger.Error("h3 mapping failed", "err", err)
		http.Error(w, "failed to map query footprint", http.StatusBadRequest)
		return
	}
	if len(cells) == 0 {
		req := composer.Request{
			Query:        composer.QueryParams{Limit: 0, Offset: 0},
			Pages:        nil,
			AcceptHeader: r.Header.Get("Accept"),
			OutputFormat: r.URL.Query().Get("outputFormat"),
		}
		res, err := composer.Compose(r.Context(), e.eng, req)
		if err != nil {
			http.Error(w, "compose error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", res.ContentType)
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(res.Body)
		return
	}

	if e.adaptiveEnabled && e.hot != nil {
		for _, c := range cells {
			e.hot.Inc(c)
			observability.ObserveHotnessValueSample(c, e.hot.Score(c))
		}
	}

	dec := adaptive.Decision{Type: adaptive.DecisionFill, Resolution: e.res, TTL: e.ttlFor(q.Layer)}
	reason := adaptive.ReasonDefaultFill
	applyDecision := e.adaptiveEnabled && !e.adaptiveDryRun && e.decider != nil

	if e.adaptiveEnabled && e.decider != nil {
		decideStart := time.Now()
		d, r := e.decider.Decide(adaptive.Query{
			Layer:   q.Layer,
			Cells:   cells,
			BaseRes: e.res,
			MinRes:  e.minRes,
			MaxRes:  e.maxRes,
		}, hotReadOnly{w: e.hot})
		dec, reason = d, r

		observability.ObserveAdaptiveDecision(decisionLabel(dec.Type), string(reason))
		e.logger.Info("adaptive_decision",
			"run_id", e.runID,
			"layer", q.Layer,
			"decision", decisionLabel(dec.Type),
			"reason", string(reason),
			"resolution", dec.Resolution,
			"ttl", dec.TTL.String(),
			"cells", len(cells),
			"dry_run", e.adaptiveDryRun,
			"dur", time.Since(decideStart).String(),
		)
	}

	resToUse := e.res
	if applyDecision {
		resToUse = dec.Resolution
	}
	ttl := e.ttlFor(q.Layer)
	if applyDecision && dec.TTL > 0 {
		ttl = dec.TTL
	}

	if resToUse != e.res {
		cells, err = e.cellsForRes(q, resToUse)
		if err != nil {
			http.Error(w, "failed to compute cells for adaptive resolution", http.StatusBadRequest)
			return
		}
	}

	if applyDecision && dec.Type == adaptive.DecisionBypass {
		body, _, err := e.exec.FetchGetFeature(ctx, q)
		if err != nil {
			e.logger.Error("cache bypass upstream error",
				"scenario", "cache",
				"layer", q.Layer,
				"res_to_use", resToUse,
				"cells", len(cells),
				"decision", decisionLabel(dec.Type),
				"reason", string(reason),
				"run_id", e.runID,
				"err", err,
			)
			http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
			return
		}

		req := composer.Request{
			Query: composer.QueryParams{
				Limit:  0,
				Offset: 0,
			},
			Pages: []composer.ShardPage{
				{Body: body, CacheStatus: composer.CacheMiss},
			},
			AcceptHeader: r.Header.Get("Accept"),
			OutputFormat: r.URL.Query().Get("outputFormat"),
		}

		res, err := composer.Compose(ctx, e.eng, req)
		if err != nil {
			e.logger.Error("cache compose error on bypass",
				"scenario", "cache",
				"layer", q.Layer,
				"res", resToUse,
				"cells", len(cells),
				"decision", decisionLabel(dec.Type),
				"reason", string(reason),
				"run_id", e.runID,
				"err", err,
			)
			http.Error(w, "compose error: "+err.Error(), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", res.ContentType)
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(res.Body)

		observability.ObserveSpatialRead("miss", false)

		e.logger.Info("cache bypass",
			"layer", q.Layer,
			"res_to_use", resToUse,
			"cells", len(cells),
			"decision", decisionLabel(dec.Type),
			"reason", string(reason),
			"run_id", e.runID,
			"dur", time.Since(start).String(),
		)
		return
	}

	serveOnlyIfFresh := e.serveFreshOnly || (applyDecision && dec.Type == adaptive.DecisionServeOnlyIfFresh)

	pages := make([]composer.ShardPage, 0, len(cells))
	var (
		missing        []string
		indexHitCount  int
		indexMissCount int
		allIDs         []string
	)

	if e.idx == nil || e.fs == nil {
		missing = append(missing, cells...)

		if serveOnlyIfFresh && len(missing) > 0 {
			observability.IncFreshReject("miss")
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte("fresh content required"))
			return
		}
	} else {
		cellToIDs := make(map[string][]string, len(cells))
		cellsWithIndexHit := make([]string, 0, len(cells))
		missingCells := make([]string, 0, len(cells))

		allIDsSet := make(map[string]struct{}, len(cells)*4)
		allIDs = allIDs[:0]

		idsByCell, err := e.idx.MGetIDs(ctx, q.Layer, resToUse, cells, model.Filters(q.Filters))
		if err != nil {
			e.logger.Warn("cell index mget error, treating all cells as miss",
				"layer", q.Layer,
				"res", resToUse,
				"cells", len(cells),
				"err", err,
			)
			missingCells = append(missingCells, cells...)
			indexMissCount += len(cells)
		} else {
			for _, cell := range cells {
				ids, ok := idsByCell[cell]
				if !ok || len(ids) == 0 {
					missingCells = append(missingCells, cell)
					indexMissCount++
					continue
				}

				// Known-empty marker: treat as hit with zero features
				// (no feature-store lookup and no upstream call).
				if len(ids) == 1 && ids[0] == cellindex.EmptyMarkerID {
					indexHitCount++
					continue
				}

				cellToIDs[cell] = ids
				cellsWithIndexHit = append(cellsWithIndexHit, cell)
				indexHitCount++

				for _, id := range ids {
					if _, seen := allIDsSet[id]; seen {
						continue
					}
					allIDsSet[id] = struct{}{}
					allIDs = append(allIDs, id)
				}
			}
		}

		featsByID := make(map[string][]byte, len(allIDs))
		var featsFound, featsMissing int

		if len(allIDs) > 0 {
			m, err := e.fs.MGetFeatures(ctx, q.Layer, allIDs)
			if err != nil {
				e.logger.Warn("feature store mget error, treating as miss for affected cells",
					"layer", q.Layer,
					"res", resToUse,
					"ids", len(allIDs),
					"err", err,
				)
				missingCells = append(missingCells, cellsWithIndexHit...)
				indexMissCount += len(cellsWithIndexHit)
				cellsWithIndexHit = cellsWithIndexHit[:0]
			} else {
				featsByID = m
				for _, id := range allIDs {
					if _, ok := featsByID[id]; ok {
						featsFound++
					} else {
						featsMissing++
						e.logger.Debug("feature missing from feature store",
							"layer", q.Layer,
							"id", id,
						)
					}
				}
			}
		}

		for _, cell := range cellsWithIndexHit {
			ids := cellToIDs[cell]
			if len(ids) == 0 {
				continue
			}

			feats := make([]json.RawMessage, 0, len(ids))
			for _, id := range ids {
				if f, ok := featsByID[id]; ok {
					feats = append(feats, json.RawMessage(f))
				}
			}

			if len(feats) == 0 {
				missingCells = append(missingCells, cell)
				continue
			}

			pages = append(pages, composer.ShardPage{
				Features:    feats,
				CacheStatus: composer.CacheHit,
			})
		}

		staleAny := false
		lastInv := observability.GetLayerInvalidatedAtUnix(q.Layer)
		if lastInv > 0 && len(pages) > 0 {
			staleAny = true
		}

		if serveOnlyIfFresh && (staleAny || len(missingCells) > 0) {
			reasonStr := "miss"
			if staleAny {
				reasonStr = "stale"
			}
			observability.IncFreshReject(reasonStr)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte("fresh content required"))
			return
		}

		if len(missingCells) == 0 {
			req := composer.Request{
				Query:        composer.QueryParams{Limit: 0, Offset: 0},
				Pages:        pages,
				AcceptHeader: r.Header.Get("Accept"),
				OutputFormat: r.URL.Query().Get("outputFormat"),
			}

			res, err := composer.Compose(r.Context(), e.eng, req)
			if err != nil {
				e.logger.Error("cache compose error on full-hit (feature-centric)",
					"scenario", "cache",
					"layer", q.Layer,
					"res_to_use", resToUse,
					"cells", len(cells),
					"index_hits", indexHitCount,
					"index_misses", indexMissCount,
					"unique_ids", len(allIDs),
					"features_found", featsFound,
					"features_missing", featsMissing,
					"ttl_used", ttl.String(),
					"decision", decisionLabel(dec.Type),
					"reason", string(reason),
					"run_id", e.runID,
					"err", err,
				)
				http.Error(w, "compose error: "+err.Error(), http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", res.ContentType)
			w.WriteHeader(res.StatusCode)
			_, _ = w.Write(res.Body)

			observability.ObserveSpatialRead("hit", staleAny)
			observability.AddCacheHits(len(pages))

			e.logger.Info("cache full-hit (feature-centric)",
				"layer", q.Layer,
				"res_to_use", resToUse,
				"cells", len(cells),
				"index_hits", indexHitCount,
				"index_misses", indexMissCount,
				"unique_ids", len(allIDs),
				"features_found", featsFound,
				"features_missing", featsMissing,
				"ttl_used", ttl.String(),
				"dur", time.Since(start).String(),
				"decision", decisionLabel(dec.Type),
				"reason", string(reason),
				"run_id", e.runID,
			)
			return
		}

		observability.AddCacheHits(len(pages))
		missing = missingCells
	}

	fillStart := time.Now()

	if len(missing) == 0 {
		missing = nil
	}

	jobs := make(chan string, e.queueSize)
	results := make(chan result, len(missing))

	workerN := e.maxWorkers
	if workerN <= 0 {
		workerN = 8
	}
	var wg sync.WaitGroup
	wg.Add(workerN)

	for range workerN {
		go func() {
			defer wg.Done()
			for cell := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				res := e.fetchCell(ctx, q, cell, resToUse, ttl)
				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for _, c := range missing {
		select {
		case jobs <- c:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}
	}
	close(jobs)
	wg.Wait()
	close(results)

	fetched := make([][]byte, 0, len(missing))
	var errs []error
	for rres := range results {
		if rres.err != nil {
			errs = append(errs, rres.err)
			continue
		}
		if len(rres.body) > 0 {
			fetched = append(fetched, rres.body)
		}
	}

	observability.AddCacheMisses(len(missing))

	for _, b := range fetched {
		pages = append(pages, composer.ShardPage{Body: b, CacheStatus: composer.CacheMiss})
	}

	if len(errs) > 0 {
		var msg strings.Builder
		msg.WriteString("one or more upstream errors (")
		msg.WriteString(fmt.Sprintf("%d/%d cells failed): ", len(errs), len(missing)))
		for i, ferr := range errs {
			if i > 0 {
				msg.WriteString("; ")
			}
			msg.WriteString(ferr.Error())
		}

		e.logger.Error("cache upstream errors during fill",
			"scenario", "cache",
			"layer", q.Layer,
			"res_to_use", resToUse,
			"cells", len(cells),
			"missing", len(missing),
			"err_count", len(errs),
			"ttl_used", ttl.String(),
			"decision", decisionLabel(dec.Type),
			"reason", string(reason),
			"run_id", e.runID,
			"sample_err", errs[0].Error(),
		)

		http.Error(w, msg.String(), http.StatusBadGateway)
		return
	}

	req := composer.Request{
		Query:        composer.QueryParams{Limit: 0, Offset: 0},
		Pages:        pages,
		AcceptHeader: r.Header.Get("Accept"),
		OutputFormat: r.URL.Query().Get("outputFormat"),
	}
	res, err := composer.Compose(r.Context(), e.eng, req)
	if err != nil {
		e.logger.Error("cache compose error on partial-miss (feature-centric)",
			"scenario", "cache",
			"layer", q.Layer,
			"res_to_use", resToUse,
			"cells", len(cells),
			"index_hits", indexHitCount,
			"index_misses", indexMissCount,
			"missing_cells", len(missing),
			"ttl_used", ttl.String(),
			"decision", decisionLabel(dec.Type),
			"reason", string(reason),
			"run_id", e.runID,
			"err", err,
		)
		http.Error(w, "compose error: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", res.ContentType)
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(res.Body)

	observability.ObserveSpatialRead("miss", false)
	e.logger.Info("cache partial-miss (feature-centric)",
		"layer", q.Layer,
		"res_to_use", resToUse,
		"cells", len(cells),
		"index_hits", indexHitCount,
		"index_misses", indexMissCount,
		"missing_cells", len(missing),
		"unique_ids", len(allIDs),
		"ttl_used", ttl.String(),
		"fill_dur", time.Since(fillStart).String(),
		"total_dur", time.Since(start).String(),
		"decision", decisionLabel(dec.Type),
		"reason", string(reason),
		"run_id", e.runID,
	)
}

func (e *Engine) cellsForRes(q model.QueryRequest, res int) (model.Cells, error) {
	switch {
	case q.Polygon != nil:
		c, err := e.mapr.CellsForPolygon(*q.Polygon, res)
		if err != nil {
			return nil, fmt.Errorf("h3 polygon cells: %w", err)
		}
		return c, nil
	case q.BBox != nil:
		c, err := e.mapr.CellsForBBox(*q.BBox, res)
		if err != nil {
			return nil, fmt.Errorf("h3 bbox cells: %w", err)
		}
		return c, nil
	default:
		return nil, errors.New("neither bbox nor polygon provided")
	}
}

func (e *Engine) ttlFor(layer string) time.Duration {
	if layer == "" {
		return e.ttlDefault
	}
	if d, ok := e.ttlMap[layer]; ok {
		return d
	}
	parts := strings.Split(layer, ":")
	if len(parts) == 2 {
		if d, ok := e.ttlMap[parts[1]]; ok {
			return d
		}
	}
	return e.ttlDefault
}

func (e *Engine) fetchCell(ctx context.Context, q model.QueryRequest, cell string, res int, ttl time.Duration) result {
	key := keys.Key(q.Layer, res, cell, q.Filters)

	if e.http == nil || e.owsURL == nil {
		return result{
			cell: cell,
			key:  key,
			err:  fmt.Errorf("cache fetchCell: http client or owsURL not configured"),
		}
	}

	cellPolyJSON, err := cellPolygonGeoJSON(cell)
	if err != nil {
		return result{cell: cell, key: key, err: fmt.Errorf("cell %s polygon: %w", cell, err)}
	}

	perQ := model.QueryRequest{
		Layer:   q.Layer,
		Polygon: &model.Polygon{GeoJSON: cellPolyJSON},
		Filters: q.Filters,
	}
	params := ogc.BuildGetFeatureParams(perQ)

	ctxReq, cancel := context.WithTimeout(ctx, e.opTimeout)
	defer cancel()
	u := *e.owsURL
	u.RawQuery = params.Encode()

	req, _ := http.NewRequestWithContext(ctxReq, http.MethodGet, u.String(), nil)
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := e.http.Do(req)
	dur := time.Since(start)
	observability.ObserveUpstreamLatency("geoserver_cell", dur.Seconds())

	if err != nil {
		return result{cell: cell, key: key, err: fmt.Errorf("cell %s fetch: %w", cell, err)}
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			e.logger.Warn("close response body", "err", cerr)
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return result{cell: cell, key: key, err: fmt.Errorf("cell %s status=%d body=%q", cell, resp.StatusCode, strings.TrimSpace(string(b)))}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result{cell: cell, key: key, err: fmt.Errorf("cell %s read: %w", cell, err)}
	}

	if e.fs != nil && e.idx != nil {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(body, &root); err != nil {
			e.logger.Warn("cache v2: parse FeatureCollection root failed",
				"layer", q.Layer,
				"res", res,
				"cell", cell,
				"err", err,
			)
		} else {
			featuresRaw, ok := root["features"]
			if !ok {
				e.logger.Warn("cache v2: FeatureCollection missing features array",
					"layer", q.Layer,
					"res", res,
					"cell", cell,
				)
			} else {
				var feats []json.RawMessage
				if err := json.Unmarshal(featuresRaw, &feats); err != nil {
					e.logger.Warn("cache v2: decode features array failed",
						"layer", q.Layer,
						"res", res,
						"cell", cell,
						"err", err,
					)
				} else {
					t := max(ttl, 0)

					// Known-empty cell: record sentinel so we don't refetch later.
					if len(feats) == 0 {
						if err := e.idx.SetIDs(ctx, q.Layer, res, cell, model.Filters(q.Filters),
							[]string{cellindex.EmptyMarkerID}, t); err != nil {
							e.logger.Warn("cache v2: cell index set empty failed",
								"layer", q.Layer,
								"res", res,
								"cell", cell,
								"err", err,
							)
						} else {
							e.logger.Debug("cache v2 marked empty cell",
								"layer", q.Layer,
								"res", res,
								"cell", cell,
							)
						}
					} else {
						// Non-empty: dedupe and normalize feature IDs with cheaper decoding.
						featsMap := make(map[string][]byte, len(feats))
						ids := make([]string, 0, len(feats))

						type minimalFeature struct {
							ID       json.RawMessage `json:"id"`
							Geometry json.RawMessage `json:"geometry"`
						}

						for i, fr := range feats {
							var f minimalFeature
							if err := json.Unmarshal(fr, &f); err != nil {
								e.logger.Warn("cache v2: feature parse failed",
									"layer", q.Layer,
									"res", res,
									"cell", cell,
									"idx", i,
									"err", err,
								)
								continue
							}

							var normID string

							if len(bytes.TrimSpace(f.ID)) > 0 {
								cid, err := geojsonagg.CanonicalIDKey(f.ID)
								if err != nil {
									e.logger.Warn("cache v2: invalid feature id, skipping id-based key",
										"layer", q.Layer,
										"res", res,
										"cell", cell,
										"idx", i,
										"err", err,
									)
								} else {
									normID = cid
								}
							}

							if normID == "" {
								gh, err := geojsonagg.GeometryHash(f.Geometry, geojsonagg.DefaultGeomPrecision)
								if err != nil {
									e.logger.Warn("cache v2: geometry hash failed, skipping feature",
										"layer", q.Layer,
										"res", res,
										"cell", cell,
										"idx", i,
										"err", err,
									)
									continue
								}
								normID = gh
							}

							if _, exists := featsMap[normID]; !exists {
								featsMap[normID] = fr
							}
							ids = append(ids, normID)
						}

						if len(featsMap) > 0 && len(ids) > 0 {
							if err := e.fs.PutFeatures(ctx, q.Layer, featsMap, t); err != nil {
								e.logger.Warn("cache v2: feature store put failed",
									"layer", q.Layer,
									"res", res,
									"cell", cell,
									"err", err,
								)
							} else if err := e.idx.SetIDs(ctx, q.Layer, res, cell, model.Filters(q.Filters), ids, t); err != nil {
								e.logger.Warn("cache v2: cell index set failed",
									"layer", q.Layer,
									"res", res,
									"cell", cell,
									"err", err,
								)
							} else {
								e.logger.Debug("cache v2 filled cell",
									"layer", q.Layer,
									"res", res,
									"cell", cell,
									"feature_count", len(featsMap),
									"index_ids", len(ids),
								)
							}
						}
					}
				}
			}
		}
	}

	return result{cell: cell, key: key, body: body, err: nil}
}

func cellPolygonGeoJSON(cellStr string) (string, error) {
	var c h3.Cell
	if err := c.UnmarshalText([]byte(cellStr)); err != nil {
		return "", fmt.Errorf("parse cell: %w", err)
	}
	if !c.IsValid() {
		return "", fmt.Errorf("invalid h3 cell %q", cellStr)
	}
	b, err := c.Boundary()
	if err != nil {
		return "", fmt.Errorf("boundary: %w", err)
	}
	if len(b) < 3 {
		return "", fmt.Errorf("degenerate boundary for %s", cellStr)
	}
	coords := make([]string, 0, len(b)+1)
	for _, ll := range b {
		coords = append(coords, fmt.Sprintf("[%.8f,%.8f]", ll.Lng, ll.Lat))
	}
	coords = append(coords, coords[0])
	return `{"type":"Polygon","coordinates":[[` + strings.Join(coords, ",") + `]]}`, nil
}

type hotReadOnly struct{ w *metricswrap.WithMetrics }

func (e *Engine) Hotness() interface{ Reset(...string) } {
	if e == nil || e.hot == nil {
		return nil
	}
	return e.hot
}

func (h hotReadOnly) Score(cell string) float64 {
	if h.w == nil {
		return 0
	}
	return h.w.Score(cell)
}

func decisionLabel(t adaptive.DecisionType) string {
	switch t {
	case adaptive.DecisionBypass:
		return "bypass"
	case adaptive.DecisionServeOnlyIfFresh:
		return "serve_only_if_fresh"
	default:
		return "fill"
	}
}
