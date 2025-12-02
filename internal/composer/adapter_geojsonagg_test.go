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

func Test_GeoJSONV2Adapter_UsesShardFeaturesWhenPresent(t *testing.T) {
	agg := geojsonagg.NewAdvanced()
	eng := Engine{V2: NewGeoJSONV2Adapter(agg)}

	feat1 := json.RawMessage(`{"type":"Feature","id":"a","geometry":{"type":"Point","coordinates":[0,0]},"properties":{"name":"a"}}`)
	feat2 := json.RawMessage(`{"type":"Feature","id":"b","geometry":{"type":"Point","coordinates":[1,1]},"properties":{"name":"b"}}`)

	req := Request{
		Query: QueryParams{
			Limit:  0,
			Offset: 0,
		},
		Pages: []ShardPage{
			{
				Features:    []json.RawMessage{feat1, feat2},
				CacheStatus: CacheHit,
			},
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
	if got := len(out.Features); got != 2 {
		t.Fatalf("features len=%d want 2", got)
	}
}

func Test_GeoJSONV2Adapter_UsesFeaturesSlice(t *testing.T) {
	agg := geojsonagg.NewAdvanced()
	eng := Engine{V2: NewGeoJSONV2Adapter(agg)}

	f1 := json.RawMessage(`{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{"score":1}}`)
	f2 := json.RawMessage(`{"type":"Feature","geometry":{"type":"Point","coordinates":[1,0]},"properties":{"score":2}}`)

	req := Request{
		Query: QueryParams{
			Sort: []SortKey{
				{Property: "score", Desc: false},
			},
			Limit:  0,
			Offset: 0,
		},
		Pages: []ShardPage{
			{
				Features:    []json.RawMessage{f2, f1},
				CacheStatus: CacheHit,
			},
		},
		AcceptHeader: "application/geo+json",
	}

	res, err := Compose(context.Background(), eng, req)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(res.Body, &out); err != nil {
		t.Fatalf("parse output: %v", err)
	}
	if len(out.Features) != 2 {
		t.Fatalf("want 2 features, got %d", len(out.Features))
	}
	var f struct {
		Properties map[string]any `json:"properties"`
	}
	_ = json.Unmarshal(out.Features[0], &f)
	if v, ok := f.Properties["score"].(float64); !ok || v != 1 {
		t.Fatalf("first feature score=%v want 1", f.Properties["score"])
	}
}
