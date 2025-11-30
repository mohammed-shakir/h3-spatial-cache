package cellindex

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

type CellIndex interface {
	GetIDs(ctx context.Context, layer string, res int, cell string, filters model.Filters) ([]string, error)

	SetIDs(ctx context.Context, layer string, res int, cell string, filters model.Filters, ids []string, ttl time.Duration) error
}

type redisCellIndex struct {
	cli *redisstore.Client
}

func NewRedisIndex(cli *redisstore.Client) CellIndex {
	return &redisCellIndex{cli: cli}
}

func (ci *redisCellIndex) GetIDs(
	ctx context.Context,
	layer string,
	res int,
	cell string,
	filters model.Filters,
) ([]string, error) {
	key := keys.CellIndexKey(layer, res, cell, filters)

	rawMap, err := ci.cli.MGet(ctx, []string{key})
	if err != nil {
		return nil, fmt.Errorf("cellindex redis MGET: %w", err)
	}
	raw, ok := rawMap[key]
	if !ok || len(raw) == 0 {
		return nil, nil
	}

	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, fmt.Errorf("cellindex decode ids: %w", err)
	}
	return ids, nil
}

func (ci *redisCellIndex) SetIDs(
	ctx context.Context,
	layer string,
	res int,
	cell string,
	filters model.Filters,
	ids []string,
	ttl time.Duration,
) error {
	key := keys.CellIndexKey(layer, res, cell, filters)

	if len(ids) == 0 {
		if err := ci.cli.Del(ctx, key); err != nil {
			return fmt.Errorf("cellindex redis DEL %q: %w", key, err)
		}
		return nil
	}

	uniq := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}

	payload, err := json.Marshal(uniq)
	if err != nil {
		return fmt.Errorf("cellindex encode ids: %w", err)
	}

	if err := ci.cli.Set(ctx, key, payload, ttl); err != nil {
		return fmt.Errorf("cellindex redis SET %q: %w", key, err)
	}
	return nil
}
