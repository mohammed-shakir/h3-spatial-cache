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

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
	cacheiface "github.com/mohammed-shakir/h3-spatial-cache/internal/cache"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/httpclient"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
)

type Engine struct {
	logger *slog.Logger
	res    int

	mapr *h3mapper.Mapper
	agg  aggregate.Interface

	store cacheiface.Interface

	owsURL *url.URL
	http   *http.Client

	ttlDefault time.Duration
	ttlMap     map[string]time.Duration

	maxWorkers int
	queueSize  int
	opTimeout  time.Duration
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
	return &Engine{
		logger: logger,
		res:    cfg.H3Res,

		mapr: h3mapper.New(),
		agg:  geojsonagg.New(true),

		store: newCacheAdapter(rc, cfg.CacheOpTimeout),

		owsURL: u,
		http:   httpclient.NewOutbound(),

		ttlDefault: cfg.CacheTTLDefault,
		ttlMap:     cfg.CacheTTLOvr,

		maxWorkers: cfg.CacheFillMaxWorkers,
		queueSize:  cfg.CacheFillQueue,
		opTimeout:  cfg.CacheOpTimeout,
	}, nil
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
	cells, err := e.cellsFor(q)
	if err != nil {
		e.logger.Error("h3 mapping failed", "err", err)
		http.Error(w, "failed to map query footprint", http.StatusBadRequest)
		return
	}
	if len(cells) == 0 {
		e.writeJSON(w, []byte(`{"type":"FeatureCollection","features":[]}`))
		return
	}

	// build redis keys for all h3 cells
	keysList := make([]string, 0, len(cells))
	for _, c := range cells {
		keysList = append(keysList, keys.Key(q.Layer, e.res, c, q.Filters))
	}

	hits, err := e.store.MGet(keysList)
	if err != nil {
		e.logger.Warn("cache mget error, continuing with fetch path", "err", err)
		hits = map[string][]byte{}
	}

	// separate hits and misses
	parts := make([][]byte, 0, len(hits))
	missing := make([]string, 0, len(keysList))
	for i, k := range keysList {
		if v, ok := hits[k]; ok && len(v) > 0 {
			parts = append(parts, v)
			continue
		}
		missing = append(missing, cells[i])
	}

	if len(missing) == 0 {
		body, mErr := e.agg.Merge(parts)
		if mErr != nil {
			http.Error(w, "failed to merge cached parts: "+mErr.Error(), http.StatusBadGateway)
			return
		}
		e.writeJSON(w, body)
		observability.IncCacheHit("cache")
		e.logger.Info("cache full-hit",
			"layer", q.Layer, "res", e.res,
			"cells", len(cells), "hits", len(parts), "misses", 0,
			"ttl_used", e.ttlFor(q.Layer).String(),
			"dur", time.Since(start).String())
		return
	}

	fillStart := time.Now()

	ttl := e.ttlFor(q.Layer)
	jobs := make(chan string, e.queueSize)
	results := make(chan result, len(missing))

	workerN := e.maxWorkers
	if workerN <= 0 {
		workerN = 8
	}
	var wg sync.WaitGroup
	wg.Add(workerN)

	// worker pool getting missing h3 cells and filling cache
	for range workerN {
		go func() {
			defer wg.Done()
			for cell := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				res := e.fetchCell(ctx, q, cell)
				if res.err == nil && len(res.body) > 0 {
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

	observability.IncCacheMiss("cache")

	parts = append(parts, fetched...)
	body, mErr := e.agg.Merge(parts)
	if mErr != nil {
		http.Error(w, "failed to merge parts: "+mErr.Error(), http.StatusBadGateway)
		return
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

	e.writeJSON(w, body)
	e.logger.Info("cache partial-miss filled",
		"layer", q.Layer, "res", e.res,
		"cells", len(cells), "hits", len(parts), "misses", len(missing),
		"ttl_used", ttl.String(),
		"fill_dur", time.Since(fillStart).String(),
		"total_dur", time.Since(start).String())
}

func (e *Engine) writeJSON(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (e *Engine) cellsFor(q model.QueryRequest) (model.Cells, error) {
	switch {
	case q.Polygon != nil:
		c, err := e.mapr.CellsForPolygon(*q.Polygon, e.res)
		if err != nil {
			return nil, fmt.Errorf("h3 polygon cells: %w", err)
		}
		return c, nil
	case q.BBox != nil:
		c, err := e.mapr.CellsForBBox(*q.BBox, e.res)
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
func (e *Engine) fetchCell(ctx context.Context, q model.QueryRequest, cell string) result {
	key := keys.Key(q.Layer, e.res, cell, q.Filters)

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
