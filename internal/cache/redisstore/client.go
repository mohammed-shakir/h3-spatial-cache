// Package redisstore wraps Redis client operations used by the cache.
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	maintnotifications "github.com/redis/go-redis/v9/maintnotifications"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
)

type Option func(*redis.Options)

func WithPoolSize(n int) Option {
	return func(o *redis.Options) { o.PoolSize = n }
}

func WithMinIdleConns(n int) Option {
	return func(o *redis.Options) { o.MinIdleConns = n }
}

func WithDialTimeout(d time.Duration) Option {
	return func(o *redis.Options) { o.DialTimeout = d }
}

func WithReadTimeout(d time.Duration) Option {
	return func(o *redis.Options) { o.ReadTimeout = d }
}

func WithWriteTimeout(d time.Duration) Option {
	return func(o *redis.Options) { o.WriteTimeout = d }
}

type Client struct {
	rdb *redis.Client
}

func New(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	if addr == "" {
		return nil, errors.New("redis address is required")
	}

	ro := &redis.Options{
		Addr:         addr,
		PoolSize:     64,
		MinIdleConns: 4,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	}
	for _, f := range opts {
		f(ro)
	}

	rdb := redis.NewClient(ro)

	start := time.Now()
	err := rdb.Ping(ctx).Err()
	observability.ObserveCacheOp("ping", err, time.Since(start).Seconds())
	if err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

// MGet returns a map of found keys to their values
func (c *Client) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	start := time.Now()
	defer func() { /* metrics recorded in the return below */ }()
	if len(keys) == 0 {
		observability.ObserveCacheOp("mget", nil, time.Since(start).Seconds())
		return map[string][]byte{}, nil
	}

	vals, err := c.rdb.MGet(ctx, keys...).Result()
	observability.ObserveCacheOp("mget", err, time.Since(start).Seconds())
	if err != nil {
		return nil, fmt.Errorf("redis MGET %d keys: %w", len(keys), err)
	}

	out := make(map[string][]byte, len(vals))
	hits := 0
	for i, v := range vals {
		if v == nil {
			continue // missing key
		}
		hits++
		switch t := v.(type) {
		case string:
			out[keys[i]] = []byte(t)
		case []byte:
			out[keys[i]] = t
		default:
			out[keys[i]] = fmt.Append(nil, t)
		}
	}
	if miss := len(keys) - hits; hits > 0 {
		observability.AddCacheHits(hits)
		if miss > 0 {
			observability.AddCacheMisses(miss)
		}
	} else if len(keys) > 0 {
		observability.AddCacheMisses(len(keys))
	}
	return out, nil
}

func (c *Client) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	start := time.Now()
	err := c.rdb.Set(ctx, key, val, ttl).Err()
	observability.ObserveCacheOp("set", err, time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("redis SET %q: %w", key, err)
	}
	return nil
}

func (c *Client) Del(ctx context.Context, keys ...string) error {
	start := time.Now()
	err := c.rdb.Del(ctx, keys...).Err()
	observability.ObserveCacheOp("del", err, time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("redis DEL %d keys: %w", len(keys), err)
	}
	return nil
}

func (c *Client) Close() error {
	if err := c.rdb.Close(); err != nil {
		return fmt.Errorf("redis close: %w", err)
	}
	return nil
}

func (c *Client) MSetWithTTL(
	ctx context.Context,
	kv map[string][]byte,
	ttl time.Duration,
) error {
	start := time.Now()
	if len(kv) == 0 {
		observability.ObserveCacheOp("mset", nil, time.Since(start).Seconds())
		return nil
	}

	_, err := c.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for k, v := range kv {
			if err := p.Set(ctx, k, v, ttl).Err(); err != nil {
				return fmt.Errorf("redis MSET pipeline SET %q: %w", k, err)
			}
		}
		return nil
	})

	observability.ObserveCacheOp("mset", err, time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("redis MSET %d keys (pipeline): %w", len(kv), err)
	}
	return nil
}
