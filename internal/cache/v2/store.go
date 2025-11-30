package v2

import (
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/cellindex"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/featurestore"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
)

type Store struct {
	Features featurestore.FeatureStore
	Cells    cellindex.CellIndex
}

func NewRedisStore(cli *redisstore.Client, defaultTTL time.Duration) *Store {
	return &Store{
		Features: featurestore.NewRedisStore(cli, defaultTTL),
		Cells:    cellindex.NewRedisIndex(cli),
	}
}
