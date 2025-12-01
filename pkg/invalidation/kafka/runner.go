package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/cellindex"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation"
)

type HotnessResetter interface {
	Reset(cells ...string)
}

type Mapper interface {
	CellsForBBox(bbox model.BBox, res int) (model.Cells, error)
	CellsForPolygon(poly model.Polygon, res int) (model.Cells, error)
}

type CellIndex interface {
	DelCells(ctx context.Context, layer string, res int, cells []string, filters model.Filters) error
}

type Runner struct {
	log      *slog.Logger
	cfg      InvalidationConfig
	cache    cache.Interface
	mapper   Mapper
	resRange []int
	idx      CellIndex
	ms       *metricSet
	ver      *versionDedupe
	assigned atomic.Bool
	assignMu sync.RWMutex
	assign   map[int32]struct{}
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	hot      HotnessResetter
}

type Options struct {
	Logger    *slog.Logger
	Register  prometheus.Registerer
	ResRange  []int
	Hotness   HotnessResetter
	CellIndex cellindex.CellIndex
}

func New(cfg InvalidationConfig, c cache.Interface, m Mapper, opts Options) *Runner {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	r := &Runner{
		log:      opts.Logger,
		cfg:      cfg,
		cache:    c,
		mapper:   m,
		resRange: opts.ResRange,
		ms:       newMetricSet(opts.Register),
		ver:      newVersionDedupe(8192),
		assign:   map[int32]struct{}{},
		hot:      opts.Hotness,
		idx:      opts.CellIndex,
	}
	if len(r.resRange) == 0 {
		r.resRange = []int{8}
	}
	return r
}

func (r *Runner) Start(ctx context.Context) error {
	if r.cfg.Driver != DriverKafka || !r.cfg.Enabled {
		r.log.Info("invalidation runner disabled", "driver", r.cfg.Driver, "enabled", r.cfg.Enabled)
		return nil
	}
	if r.cache == nil {
		return errors.New("kafka runner: cache dependency is required")
	}

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_5_0_0
	cfg.Consumer.Group.Session.Timeout = r.cfg.SessionTimeout
	cfg.Consumer.Group.Heartbeat.Interval = r.cfg.Heartbeat
	cfg.Consumer.Group.Rebalance.Timeout = r.cfg.RebalanceTimeout
	if r.cfg.InitialOldest {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	cfg.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup(r.cfg.Brokers, r.cfg.GroupID, cfg)
	if err != nil {
		return fmt.Errorf("consumer group: %w", err)
	}

	h := &groupHandler{
		setup: func(sess sarama.ConsumerGroupSession) {
			claims := sess.Claims()
			r.assignMu.Lock()
			r.assigned.Store(true)
			r.assign = map[int32]struct{}{}
			for _, parts := range claims {
				for _, p := range parts {
					r.assign[p] = struct{}{}
				}
			}
			r.assignMu.Unlock()
		},
		cleanup: func(sarama.ConsumerGroupSession) {
			r.assignMu.Lock()
			r.assigned.Store(false)
			r.assign = map[int32]struct{}{}
			r.assignMu.Unlock()
		},
		process: r.handleMessage,
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() {
			if err := group.Close(); err != nil {
				r.log.Error("kafka consumer group close", "err", err)
			}
		}()

		for {
			if err := group.Consume(ctx, []string{r.cfg.Topic}, h); err != nil {
				r.log.Error("kafka consume error", "err", err)
				select {
				case <-time.After(2 * time.Second):
				case <-ctx.Done():
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for err := range group.Errors() {
			r.log.Error("kafka group error", "err", err)
		}
	}()

	r.log.Info("kafka invalidation runner started",
		"topic", r.cfg.Topic, "group", r.cfg.GroupID, "brokers", r.cfg.Brokers)
	return nil
}

func (r *Runner) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	r.log.Info("kafka invalidation runner stopped")
}

func (r *Runner) Readiness() (ready bool, partitions []int32) {
	if !r.assigned.Load() {
		return false, nil
	}
	r.assignMu.RLock()
	defer r.assignMu.RUnlock()
	for p := range r.assign {
		partitions = append(partitions, p)
	}
	return true, partitions
}

func (r *Runner) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	start := time.Now()

	if !msg.Timestamp.IsZero() {
		lag := time.Since(msg.Timestamp).Seconds()
		r.ms.lagGauge.Set(lag)
		observability.SetInvalidationLagSeconds(lag)
	}

	var w WireEvent
	if err := json.Unmarshal(msg.Value, &w); err == nil && (w.Key != "" || len(w.H3Cells) > 0) {
		ts := w.TS
		if ts.IsZero() {
			ts = msg.Timestamp
		}
		err := r.applyWire(ctx, w, ts)
		r.observe(w.Op, err, time.Since(start))
		if err == nil && w.Layer != "" && !ts.IsZero() {
			observability.SetLayerInvalidatedAt(w.Layer, ts)
		}
		return err
	}

	var ev invalidation.Event
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		r.ms.msgs.WithLabelValues("error").Inc()
		return fmt.Errorf("decode: %w", err)
	}
	if err := ev.Validate(); err != nil {
		r.ms.msgs.WithLabelValues("error").Inc()
		return fmt.Errorf("validate: %w", err)
	}
	ts := msg.Timestamp
	err := r.applySpatial(ctx, ev)
	r.observe(ev.Op, err, time.Since(start))
	if err == nil && ev.Layer != "" && !ts.IsZero() {
		observability.SetLayerInvalidatedAt(ev.Layer, ts)
	}
	return err
}

func (r *Runner) observe(op string, err error, dur time.Duration) {
	if op == "" {
		op = "unknown"
	}
	if err != nil {
		r.ms.msgs.WithLabelValues("error").Inc()
	} else {
		r.ms.msgs.WithLabelValues("ok").Inc()
	}
	r.ms.proc.WithLabelValues(op).Observe(dur.Seconds())
}

func (r *Runner) applyWire(ctx context.Context, w WireEvent, _ time.Time) error {
	var keysToDel []string
	appliedSet := make(map[string]struct{})

	res := r.resRange
	if len(w.Resolutions) > 0 {
		res = w.Resolutions
	}

	if w.Key != "" {
		keysToDel = append(keysToDel, w.Key)
	} else {
		for _, cell := range w.H3Cells {
			for _, rr := range res {
				keysToDel = append(keysToDel, keys.Key(w.Layer, rr, cell, ""))
			}
		}
	}

	applied := 0
	perCell := 1
	if len(w.H3Cells) > 0 {
		perCell = len(res)
	}
	for i, k := range keysToDel {
		if !r.ver.shouldApply(k, w.Version) {
			r.ms.apply.WithLabelValues("skip_version").Inc()
			continue
		}
		applied++
		if len(w.H3Cells) > 0 {
			cellIdx := i / perCell
			if cellIdx >= 0 && cellIdx < len(w.H3Cells) {
				appliedSet[w.H3Cells[cellIdx]] = struct{}{}
			}
		}
	}
	if applied == 0 {
		return nil
	}

	if err := r.cache.Del(keysToDel...); err != nil {
		return fmt.Errorf("redis del (%d keys): %w", len(keysToDel), err)
	}
	r.ms.apply.WithLabelValues("delete").Add(float64(applied))

	if r.idx != nil && len(appliedSet) > 0 && w.Layer != "" {
		cells := make([]string, 0, len(appliedSet))
		for c := range appliedSet {
			cells = append(cells, c)
		}

		for _, rr := range res {
			if err := r.idx.DelCells(ctx, w.Layer, rr, cells, ""); err != nil {
				r.log.Warn("cell index delete failed during wire invalidation",
					"layer", w.Layer,
					"res", rr,
					"cells", len(cells),
					"err", err,
				)
			}
		}
	}

	if r.hot != nil && len(appliedSet) > 0 {
		uniq := make([]string, 0, len(appliedSet))
		for c := range appliedSet {
			uniq = append(uniq, c)
		}
		r.hot.Reset(uniq...)
	}
	return nil
}

func (r *Runner) applySpatial(ctx context.Context, ev invalidation.Event) error {
	cellRes := 0
	for _, rr := range r.resRange {
		if rr > cellRes {
			cellRes = rr
		}
	}
	var cells model.Cells
	switch {
	case ev.BBox != nil:
		b := model.BBox{X1: ev.BBox.X1, Y1: ev.BBox.Y1, X2: ev.BBox.X2, Y2: ev.BBox.Y2, SRID: ev.BBox.SRID}
		c, err := r.mapper.CellsForBBox(b, cellRes)
		if err != nil {
			return fmt.Errorf("CellsForBBox: %w", err)
		}
		cells = c
	default:
		c, err := r.mapper.CellsForPolygon(model.Polygon{GeoJSON: string(ev.Geometry)}, cellRes)
		if err != nil {
			return fmt.Errorf("CellsForPolygon: %w", err)
		}
		cells = c
	}
	if len(cells) == 0 {
		return nil
	}

	var ks []string
	for _, rr := range r.resRange {
		for _, c := range cells {
			ks = append(ks, keys.Key(ev.Layer, rr, c, ""))
		}
	}
	if err := r.cache.Del(ks...); err != nil {
		return fmt.Errorf("redis del (%d keys): %w", len(ks), err)
	}
	r.ms.apply.WithLabelValues("delete").Add(float64(len(ks)))

	if r.idx != nil && ev.Layer != "" {
		for _, rr := range r.resRange {
			if err := r.idx.DelCells(ctx, ev.Layer, rr, []string(cells), ""); err != nil {
				r.log.Warn("cell index delete failed during spatial invalidation",
					"layer", ev.Layer,
					"res", rr,
					"cells", len(cells),
					"err", err,
				)
			}
		}
	}

	if r.hot != nil {
		r.hot.Reset(cells...)
	}
	return nil
}

type groupHandler struct {
	setup   func(sarama.ConsumerGroupSession)
	cleanup func(sarama.ConsumerGroupSession)
	process func(context.Context, *sarama.ConsumerMessage) error
}

func (h *groupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	if h.setup != nil {
		h.setup(sess)
	}
	return nil
}

func (h *groupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	if h.cleanup != nil {
		h.cleanup(sess)
	}
	return nil
}

func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := sess.Context()
	for msg := range claim.Messages() {
		if err := h.process(ctx, msg); err != nil {
			return err
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}
