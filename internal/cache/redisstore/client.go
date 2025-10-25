package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

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

// returns a map of found keys to their values
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
	for i, v := range vals {
		if v == nil {
			continue // missing key
		}
		switch t := v.(type) {
		case string:
			out[keys[i]] = []byte(t)
		case []byte:
			out[keys[i]] = t
		default:
			out[keys[i]] = fmt.Append(nil, t)
		}
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
