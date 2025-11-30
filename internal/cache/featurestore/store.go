package featurestore

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
)

type FeatureStore interface {
	MGetFeatures(ctx context.Context, layer string, ids []string) (map[string][]byte, error)

	PutFeatures(ctx context.Context, layer string, feats map[string][]byte, ttl time.Duration) error
}

type redisFeatureStore struct {
	cli        *redisstore.Client
	defaultTTL time.Duration
}

func NewRedisStore(cli *redisstore.Client, defaultTTL time.Duration) FeatureStore {
	return &redisFeatureStore{
		cli:        cli,
		defaultTTL: defaultTTL,
	}
}

func (s *redisFeatureStore) MGetFeatures(
	ctx context.Context,
	layer string,
	ids []string,
) (map[string][]byte, error) {
	if len(ids) == 0 {
		return map[string][]byte{}, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = featureKey(layer, id)
	}

	raw, err := s.cli.MGet(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("featurestore redis MGET %d keys: %w", len(keys), err)
	}

	out := make(map[string][]byte, len(raw))
	for i, id := range ids {
		k := keys[i]
		if v, ok := raw[k]; ok {
			out[id] = v
		}
	}
	return out, nil
}

func (s *redisFeatureStore) PutFeatures(
	ctx context.Context,
	layer string,
	feats map[string][]byte,
	ttl time.Duration,
) error {
	if len(feats) == 0 {
		return nil
	}

	t := ttl
	if t <= 0 {
		t = s.defaultTTL
	}

	for id, body := range feats {
		k := featureKey(layer, id)
		if err := s.cli.Set(ctx, k, body, t); err != nil {
			return fmt.Errorf("featurestore redis SET %q: %w", k, err)
		}
	}
	return nil
}

func featureKey(layer, id string) string {
	layerKey := sanitizeLayer(strings.TrimSpace(layer))
	normID := strings.TrimSpace(id)
	return "feat:" + layerKey + ":" + normID
}

func sanitizeLayer(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	var prev rune
	for _, r := range s {
		var out rune
		switch {
		case isASCIIWhitespace(r):
			out = '_'
		case isAlphaNum(r) || r == ':' || r == '_' || r == '-':
			out = r
		default:
			out = '-'
		}
		if (out == '_' || out == '-') && out == prev {
			continue
		}
		b.WriteRune(out)
		prev = out
	}
	return b.String()
}

func isASCIIWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f'
}

func isAlphaNum(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		unicode.IsDigit(r)
}
