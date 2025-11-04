package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	obs "github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation"
	mylog "github.com/mohammed-shakir/h3-spatial-cache/internal/logger"
)

type CellMapper interface {
	CellsForBBox(bbox model.BBox, res int) (model.Cells, error)
	CellsForPolygon(poly model.Polygon, res int) (model.Cells, error)
}

type HotnessResetter interface {
	Reset(cells ...string)
}

type Consumer struct {
	cfg      Config
	logger   *slog.Logger
	cache    cache.Interface
	mapper   CellMapper
	hot      HotnessResetter
	resRange []int
	zlog     *zerolog.Logger
}

func New(cfg Config, logger *slog.Logger, c cache.Interface, mapper CellMapper, hot HotnessResetter, resRange []int) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		cfg:      cfg,
		logger:   logger,
		cache:    c,
		mapper:   mapper,
		hot:      hot,
		resRange: resRange,
	}
}

// consumes invalidation events from kafka and processing them
func (c *Consumer) Start(ctx context.Context) error {
	if c.cache == nil || c.mapper == nil {
		return errors.New("kafkaconsumer: missing dependencies (cache/mapper)")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Consumer.Group.Session.Timeout = c.cfg.SessionTimeout
	cfg.Consumer.Group.Heartbeat.Interval = c.cfg.Heartbeat
	cfg.Consumer.Group.Rebalance.Timeout = c.cfg.RebalanceTimeout
	if c.cfg.InitialOffsetOldest {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	cfg.Consumer.Offsets.AutoCommit.Enable = true

	group, err := sarama.NewConsumerGroup(c.cfg.Brokers, c.cfg.GroupID, cfg)
	if err != nil {
		return fmt.Errorf("create consumer group: %w", err)
	}
	defer func() { _ = group.Close() }()

	base := mylog.WithComponent(context.Background(), "kafka_consumer")
	zl := mylog.Build(mylog.Config{
		Level:     "info",
		Scenario:  "baseline",
		Component: "kafka_consumer",
	}, nil)
	c.zlog = mylog.FromContext(base, &zl)

	handler := &groupHandler{process: c.ProcessOne}

	c.logger.Info("kafka invalidation consumer starting",
		"brokers", c.cfg.Brokers, "topic", c.cfg.Topic, "group", c.cfg.GroupID)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("kafka invalidation consumer shutting down")
			return nil
		default:
			if err := group.Consume(ctx, []string{c.cfg.Topic}, handler); err != nil {
				c.logger.Error("consumer error", "err", err)
				c.zlog.Error().Err(err).
					Strs("brokers", c.cfg.Brokers).
					Str("topic", c.cfg.Topic).
					Msg("kafka consumer error")
				time.Sleep(2 * time.Second)
			}
		}
	}
}

// process a single invalidation event message
func (c *Consumer) ProcessOne(ctx context.Context, msg *sarama.ConsumerMessage) error {
	start := time.Now()

	var ev invalidation.Event
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		obs.IncKafkaConsumerError("decode")
		obs.ObserveUpstreamLatency("kafka_decode", time.Since(start).Seconds())

		mylog.FromContext(ctx, c.zlog).Error().
			Str("kind", "decode").
			Str("topic", msg.Topic).
			Int32("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Msg("kafka error")

		return fmt.Errorf("json decode: %w", err)
	}

	cells, err := c.cellsForEvent(ev)
	if err != nil {
		obs.ObserveInvalidation(ev.Op, ev.Layer, 0, time.Since(start), err)
		return fmt.Errorf("derive cells: %w", err)
	}
	if len(cells) == 0 {
		obs.ObserveInvalidation(ev.Op, ev.Layer, 0, time.Since(start), nil)
		c.logger.Debug("no cells to invalidate (skipping)", "layer", ev.Layer, "op", ev.Op)
		return nil
	}

	delKeys := make([]string, 0, len(cells)*len(c.resRange))
	for _, res := range c.resRange {
		for _, cell := range cells {
			delKeys = append(delKeys, keys.Key(ev.Layer, res, cell, ""))
		}
	}

	if err := c.cache.Del(delKeys...); err != nil {
		obs.IncKafkaConsumerError("redis_del")
		obs.ObserveInvalidation(ev.Op, ev.Layer, 0, time.Since(start), err)

		mylog.FromContext(ctx, c.zlog).Error().
			Str("kind", "redis_del").
			Str("topic", msg.Topic).
			Int32("partition", msg.Partition).
			Int("keys", len(delKeys)).
			Msg("kafka error")

		return fmt.Errorf("redis del: %w", err)
	}

	if c.hot != nil {
		c.hot.Reset([]string(cells)...)
	}

	obs.ObserveInvalidation(ev.Op, ev.Layer, len(delKeys), time.Since(start), nil)
	c.logger.Debug("invalidated keys",
		"layer", ev.Layer, "op", ev.Op, "cells", len(cells), "keys", len(delKeys))

	mylog.FromContext(ctx, c.zlog).Info().
		Str("event", "invalidation").
		Str("op", ev.Op).Str("layer", ev.Layer).
		Int("cells", len(cells)).Int("keys", len(delKeys)).
		Msg("invalidated keys")

	return nil
}

// choose mapping method based on event content
func (c *Consumer) cellsForEvent(ev invalidation.Event) (model.Cells, error) {
	res := bestRes(c.resRange)
	switch {
	case ev.BBox != nil:
		cells, err := c.mapper.CellsForBBox(toModelBBox(*ev.BBox), res)
		if err != nil {
			return nil, fmt.Errorf("CellsForBBox: %w", err)
		}
		return cells, nil
	case len(ev.Geometry) > 0:
		cells, err := c.mapper.CellsForPolygon(model.Polygon{GeoJSON: string(ev.Geometry)}, res)
		if err != nil {
			return nil, fmt.Errorf("CellsForPolygon: %w", err)
		}
		return cells, nil
	default:
		return nil, fmt.Errorf("unsupported event: missing bbox/geometry")
	}
}

func bestRes(resRange []int) int {
	best := 0
	for _, r := range resRange {
		if r > best {
			best = r
		}
	}
	if best == 0 {
		best = 8
	}
	return best
}

func toModelBBox(b invalidation.BBox) model.BBox {
	return model.BBox{
		X1: b.X1,
		Y1: b.Y1,
		X2: b.X2,
		Y2: b.Y2,
	}
}
