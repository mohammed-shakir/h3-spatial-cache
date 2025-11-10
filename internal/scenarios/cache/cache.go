package cache

import (
	"context"
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
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
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
	owsURL          *url.URL
	http            *http.Client
	ttlDefault      time.Duration
	ttlMap          map[string]time.Duration
	maxWorkers      int
	queueSize       int
	opTimeout       time.Duration
	adaptiveEnabled bool
	adaptiveDryRun  bool
	serveFreshOnly  bool
	decider         adaptive.Decider
	hot             *metricswrap.WithMetrics
	runID           string
}

func init() {
	scenarios.Register("cache", newCache)
}

// creates cache scenario query handler
func newCache(cfg config.Config, logger *slog.Logger, _ executor.Interface) (router.QueryHandler, error) {
	rc, err := redisstore.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		return nil, fmt.Errorf("redis client: %w", err)
	}
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

		owsURL: u,
		http:   httpclient.NewOutbound(),

		ttlDefault: cfg.CacheTTLDefault,
		ttlMap:     cfg.CacheTTLOvr,

		maxWorkers: cfg.CacheFillMaxWorkers,
		queueSize:  cfg.CacheFillQueue,
		opTimeout:  cfg.CacheOpTimeout,

		adaptiveEnabled: cfg.AdaptiveEnabled,
		adaptiveDryRun:  cfg.AdaptiveDryRun,
		serveFreshOnly:  cfg.AdaptiveServeOnlyIfFresh,
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

	// map footprint to cells at base resolution
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

	// hotness hooks (cheap, sharded)
	if e.adaptiveEnabled && e.hot != nil {
		for _, c := range cells {
			e.hot.Inc(c)
			observability.ObserveHotnessValueSample(c, e.hot.Score(c))
		}
	}

	// decision
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
			"run_id", e.runID, "layer", q.Layer,
			"decision", decisionLabel(dec.Type), "reason", string(reason),
			"resolution", dec.Resolution, "ttl", dec.TTL.String(),
			"cells", len(cells),
			"dry_run", e.adaptiveDryRun,
			"dur", time.Since(decideStart).String())
	}

	// apply resolution and TTL based on decision
	resToUse := e.res
	if applyDecision {
		resToUse = dec.Resolution
	}
	ttl := e.ttlFor(q.Layer)
	if applyDecision && dec.TTL > 0 {
		ttl = dec.TTL
	}

	// remap cells if resolution changed
	if resToUse != e.res {
		cells, err = e.cellsForRes(q, resToUse)
		if err != nil {
			http.Error(w, "failed to compute cells for adaptive resolution", http.StatusBadRequest)
			return
		}
	}

	// choose path based on decision type
	useLookup := true
	writeFill := true
	serveOnlyIfFresh := false

	switch dec.Type {
	case adaptive.DecisionBypass:
		if applyDecision {
			useLookup = false
			writeFill = false
			ttl = 0
		}
	case adaptive.DecisionServeOnlyIfFresh:
		if applyDecision {
			serveOnlyIfFresh = true
		}
	case adaptive.DecisionFill:
		// normal path
	}

	// build keys for the selected resolution
	keysList := make([]string, 0, len(cells))
	for _, c := range cells {
		keysList = append(keysList, keys.Key(q.Layer, resToUse, c, q.Filters))
	}

	// cache lookup
	var hits map[string][]byte
	if useLookup {
		m, err := e.store.MGet(keysList)
		if err != nil {
			e.logger.Warn("cache mget error, continuing with fetch path", "err", err)
			hits = map[string][]byte{}
		} else {
			hits = m
		}
	} else {
		hits = map[string][]byte{}
	}

	// separate hits/misses
	pages := make([]composer.ShardPage, 0, len(keysList))
	missing := make([]string, 0, len(keysList))
	for i, k := range keysList {
		if v, ok := hits[k]; ok && len(v) > 0 {
			pages = append(pages, composer.ShardPage{Body: v, CacheStatus: composer.CacheHit})
			continue
		}
		missing = append(missing, cells[i])
	}

	if serveOnlyIfFresh && len(missing) > 0 {
		http.Error(w, "fresh content required but not fully cached", http.StatusPreconditionFailed)
		return
	}

	if len(missing) == 0 {
		req := composer.Request{
			Query: composer.QueryParams{Limit: 0, Offset: 0},
			Pages: pages, AcceptHeader: r.Header.Get("Accept"),
			OutputFormat: r.URL.Query().Get("outputFormat"),
		}
		res, err := composer.Compose(r.Context(), e.eng, req)
		if err != nil {
			http.Error(w, "compose error: "+err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", res.ContentType)
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(res.Body)
		observability.AddCacheHits(len(pages))
		e.logger.Info("cache full-hit",
			"layer", q.Layer, "res", resToUse,
			"cells", len(cells), "hits", len(pages), "misses", 0,
			"ttl_used", ttl.String(),
			"dur", time.Since(start).String(),
			"decision", decisionLabel(dec.Type), "reason", string(reason), "run_id", e.runID)
		return
	}

	fillStart := time.Now()
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
				res := e.fetchCell(ctx, q, cell, resToUse)
				if res.err == nil && len(res.body) > 0 && writeFill {
					_ = e.store.Set(res.key, res.body, ttl)
				}
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

	var fetched [][]byte
	var errs []error
	for r := range results {
		if r.err != nil {
			errs = append(errs, r.err)
		} else if len(r.body) > 0 {
			fetched = append(fetched, r.body)
		}
	}

	observability.AddCacheMisses(len(missing))

	for _, b := range fetched {
		pages = append(pages, composer.ShardPage{Body: b, CacheStatus: composer.CacheMiss})
	}

	if len(errs) > 0 {
		msg := strings.Builder{}
		msg.WriteString("one or more upstream errors (")
		msg.WriteString(fmt.Sprintf("%d/%d cells failed): ", len(errs), len(missing)))
		for i, e := range errs {
			if i > 0 {
				msg.WriteString("; ")
			}
			msg.WriteString(e.Error())
		}
		http.Error(w, msg.String(), http.StatusBadGateway)
		return
	}

	req := composer.Request{
		Query: composer.QueryParams{Limit: 0, Offset: 0},
		Pages: pages, AcceptHeader: r.Header.Get("Accept"),
		OutputFormat: r.URL.Query().Get("outputFormat"),
	}
	res, err := composer.Compose(r.Context(), e.eng, req)
	if err != nil {
		http.Error(w, "compose error: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", res.ContentType)
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(res.Body)
	e.logger.Info("cache partial-miss",
		"layer", q.Layer, "res", resToUse,
		"cells", len(cells), "hits", len(pages)-len(fetched), "misses", len(missing),
		"ttl_used", ttl.String(),
		"fill_dur", time.Since(fillStart).String(),
		"total_dur", time.Since(start).String(),
		"decision", decisionLabel(dec.Type), "reason", string(reason), "run_id", e.runID)
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

// fetches a single h3 cell from geoserver
func (e *Engine) fetchCell(ctx context.Context, q model.QueryRequest, cell string, res int) result {
	key := keys.Key(q.Layer, res, cell, q.Filters)

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

// read-only hotness view handed to the decider
type hotReadOnly struct{ w *metricswrap.WithMetrics }

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
