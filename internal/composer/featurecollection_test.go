package composer

import (
	"encoding/json"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
)

func TestBuildFeatureCollectionShard_EmptySlice(t *testing.T) {
	b, err := BuildFeatureCollectionShard(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fc struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(b, &fc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if fc.Type != "FeatureCollection" {
		t.Fatalf("type=%q want FeatureCollection", fc.Type)
	}
	if len(fc.Features) != 0 {
		t.Fatalf("features len=%d want 0", len(fc.Features))
	}
}

func TestBuildFeatureCollectionShard_TwoFeatures(t *testing.T) {
	f1 := []byte(`{"type":"Feature","geometry":null,"properties":{"name":"a"}}`)
	f2 := []byte(`{"type":"Feature","geometry":null,"properties":{"name":"b"}}`)

	b, err := BuildFeatureCollectionShard([][]byte{f1, f2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fc struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(b, &fc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if fc.Type != "FeatureCollection" {
		t.Fatalf("type=%q want FeatureCollection", fc.Type)
	}
	if got := len(fc.Features); got != 2 {
		t.Fatalf("features len=%d want 2", got)
	}

	getName := func(raw json.RawMessage) string {
		var obj struct {
			Properties map[string]any `json:"properties"`
		}
		_ = json.Unmarshal(raw, &obj)
		if obj.Properties == nil {
			return ""
		}
		if v, ok := obj.Properties["name"].(string); ok {
			return v
		}
		return ""
	}
	if n := getName(fc.Features[0]); n != "a" {
		t.Fatalf("first feature name=%q want a", n)
	}
	if n := getName(fc.Features[1]); n != "b" {
		t.Fatalf("second feature name=%q want b", n)
	}
}

func TestBuildFeatureCollectionShard_InvalidJSON(t *testing.T) {
	_, err := BuildFeatureCollectionShard([][]byte{[]byte("{oops}")})
	if err == nil {
		t.Fatalf("expected error for invalid JSON, got nil")
	}
}

func TestBuildFeatureCollectionShard_RoundTripViaGeoJSONAgg(t *testing.T) {
	f1 := []byte(`{"type":"Feature","geometry":null,"properties":{"name":"a"}}`)
	f2 := []byte(`{"type":"Feature","geometry":null,"properties":{"name":"b"}}`)

	shard, err := BuildFeatureCollectionShard([][]byte{f1, f2})
	if err != nil {
		t.Fatalf("BuildFeatureCollectionShard: %v", err)
	}

	agg := geojsonagg.New(false)
	out, err := agg.Merge([][]byte{shard})
	if err != nil {
		t.Fatalf("agg.Merge: %v", err)
	}

	var fc struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(out, &fc); err != nil {
		t.Fatalf("unmarshal merged: %v", err)
	}
	if fc.Type != "FeatureCollection" {
		t.Fatalf("merged type=%q want FeatureCollection", fc.Type)
	}
	if got := len(fc.Features); got != 2 {
		t.Fatalf("merged features len=%d want 2", got)
	}
}
