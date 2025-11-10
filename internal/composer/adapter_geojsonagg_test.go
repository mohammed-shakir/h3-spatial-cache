package composer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
)

func Test_GeoJSONV2Adapter_SortLimitParity(t *testing.T) {
	// two shards, mixed order, expect stable sort + limit applied once
	shard1 := []byte(`{"type":"FeatureCollection","features":[
	 {"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{"score":2,"ts":"2020-01-01T00:00:00Z"}},
	 {"type":"Feature","geometry":{"type":"Point","coordinates":[0,1]},"properties":{"score":1,"ts":"2020-01-01T00:00:00Z"}}
	]}`)
	shard2 := []byte(`{"type":"FeatureCollection","features":[
	 {"type":"Feature","geometry":{"type":"Point","coordinates":[1,0]},"properties":{"score":1,"ts":"2020-01-02T00:00:00Z"}},
	 {"type":"Feature","geometry":{"type":"Point","coordinates":[1,1]},"properties":{"score":3,"ts":"2020-01-01T00:00:00Z"}}
	]}`)

	agg := geojsonagg.NewAdvanced()
	eng := Engine{V2: NewGeoJSONV2Adapter(agg)}

	req := Request{
		Query: QueryParams{
			Sort: []SortKey{
				{Property: "score", Desc: false},
				{Property: "ts", Desc: false},
			},
			Limit:  3,
			Offset: 1,
		},
		Pages: []ShardPage{
			{Body: shard1, CacheStatus: CacheHit},
			{Body: shard2, CacheStatus: CacheMiss},
		},
		AcceptHeader: "application/geo+json",
	}

	res, err := Compose(context.Background(), eng, req)
	if err != nil {
		t.Fatal(err)
	}
	if ct := res.ContentType; ct != "application/geo+json" {
		t.Fatalf("content-type = %s", ct)
	}
	var out struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(res.Body, &out); err != nil {
		t.Fatalf("parse output: %v", err)
	}
	if len(out.Features) != 3 {
		t.Fatalf("want 3 features after limit/offset, got %d", len(out.Features))
	}
}
