package invalidation

import (
	"encoding/json"
	"testing"
	"time"
)

func mustTS() time.Time { return time.Date(2025, 10, 26, 12, 30, 45, 0, time.UTC) }

func TestEvent_Validate_BBoxAndPolygonMutualExclusion(t *testing.T) {
	ev := Event{
		Version: 1, Op: "update", Layer: "demo:places", TS: mustTS(),
		BBox:     &BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
		Geometry: json.RawMessage(`{"type":"Polygon","coordinates":[[[11,55],[12,55],[12,56],[11,56],[11,55]]]}`),
	}
	if err := ev.Validate(); err == nil {
		t.Fatalf("expected error when both bbox and geometry are set")
	}
}

func TestEvent_Validate_BBoxHappyPath(t *testing.T) {
	ev := Event{
		Version: 1, Op: "delete", Layer: "demo:places", TS: mustTS(),
		BBox: &BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestEvent_Validate_PolygonHappyPath(t *testing.T) {
	ev := Event{
		Version: 1, Op: "insert", Layer: "demo:places", TS: mustTS(),
		Geometry: json.RawMessage(`{"type":"Polygon","coordinates":[[[11,55],[12,55],[12,56],[11,56],[11,55]]]}`),
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestEvent_Validate_RejectsBadBBox(t *testing.T) {
	ev := Event{
		Version: 1, Op: "update", Layer: "demo:places", TS: mustTS(),
		BBox: &BBox{X1: 11, Y1: 55, X2: 11, Y2: 56, SRID: "EPSG:4326"},
	}
	if err := ev.Validate(); err == nil {
		t.Fatalf("expected error for non-increasing bbox")
	}
}
