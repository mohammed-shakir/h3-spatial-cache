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

func (a *GeoJSONV2Adapter) MergeWithQuery(
	_ context.Context,
	q QueryParams,
	pages []ShardPage,
) ([]byte, error) {
	req := geojsonagg.Request{
		Query: geojsonagg.Query{
			StartIndex: q.Offset,
			Limit:      q.Limit,
			Sort:       convertSortKeys(q.Sort),
		},
		Shards: make([]geojsonagg.ShardPage, 0, len(pages)),
	}

	type fcRoot struct {
		Features []json.RawMessage `json:"features"`
	}

	for i, page := range pages {
		var feats []json.RawMessage

		if page.Features != nil {
			feats = page.Features
		} else {
			if len(page.Body) == 0 {
				continue
			}

			var root fcRoot
			if err := json.Unmarshal(page.Body, &root); err != nil {
				return nil, fmt.Errorf("part %d: parse json: %w", i, err)
			}
			if root.Features == nil {
				return nil, fmt.Errorf(`part %d: missing required member "features"`, i)
			}
			feats = root.Features
		}

		if len(feats) == 0 {
			continue
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
