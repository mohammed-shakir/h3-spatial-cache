package composer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
)

type GeoJSONV2Adapter struct {
	Agg *geojsonagg.Aggregator
}

func NewGeoJSONV2Adapter(agg *geojsonagg.Aggregator) *GeoJSONV2Adapter {
	return &GeoJSONV2Adapter{Agg: agg}
}

func (a *GeoJSONV2Adapter) MergeWithQuery(_ context.Context, q QueryParams, pages []ShardPage) ([]byte, error) {
	req := geojsonagg.Request{
		Query: geojsonagg.Query{
			StartIndex: q.Offset,
			Limit:      q.Limit,
			Sort:       convertSortKeys(q.Sort),
		},
		Shards: make([]geojsonagg.ShardPage, 0, len(pages)),
	}

	for i, page := range pages {
		p := page.Body

		var root map[string]json.RawMessage
		if err := json.Unmarshal(p, &root); err != nil {
			return nil, fmt.Errorf("part %d: parse json: %w", i, err)
		}
		featuresRaw, ok := root["features"]
		if !ok {
			return nil, fmt.Errorf(`part %d: missing required member "features"`, i)
		}
		var feats []json.RawMessage
		if err := json.Unmarshal(featuresRaw, &feats); err != nil {
			return nil, fmt.Errorf(`part %d: "features" must be an array: %w`, i, err)
		}

		fromCache := page.CacheStatus == CacheHit

		req.Shards = append(req.Shards, geojsonagg.ShardPage{
			Meta:     geojsonagg.ShardMeta{FromCache: fromCache, ID: fmt.Sprintf("part-%d", i)},
			Features: feats,
		})
	}

	out, _, err := a.Agg.MergeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("geojsonagg merge: %w", err)
	}
	return out, nil
}

func convertSortKeys(in []SortKey) []geojsonagg.SortKey {
	if len(in) == 0 {
		return nil
	}
	out := make([]geojsonagg.SortKey, len(in))
	for i := range in {
		dir := geojsonagg.Asc
		if in[i].Desc {
			dir = geojsonagg.Desc
		}
		out[i] = geojsonagg.SortKey{
			Property:  in[i].Property,
			Direction: dir,
		}
	}
	return out
}
