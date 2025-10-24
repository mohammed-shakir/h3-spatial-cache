package h3mapper

import (
	"reflect"
	"sort"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

func TestBBox_HappyPath_SortedUnique(t *testing.T) {
	m := New()
	bb := model.BBox{X1: 17.95, Y1: 59.30, X2: 18.15, Y2: 59.40, SRID: "EPSG:4326"}

	cells, err := m.CellsForBBox(bb, 8)
	if err != nil {
		t.Fatalf("CellsForBBox err: %v", err)
	}
	if len(cells) == 0 {
		t.Fatalf("expected non-empty cells for bbox")
	}
	if !sort.StringsAreSorted([]string(cells)) {
		t.Fatalf("cells must be sorted")
	}
	if hasDups(cells) {
		t.Fatalf("cells must be de-duplicated")
	}
}

func TestPolygon_SubsetOfBBoxAndDeterministic(t *testing.T) {
	m := New()
	bb := model.BBox{X1: 17.95, Y1: 59.30, X2: 18.15, Y2: 59.40, SRID: "EPSG:4326"}

	polyJSON := `{"type":"Polygon","coordinates":[[
		[18.00,59.32],[18.12,59.32],[18.12,59.38],[18.00,59.38],[18.00,59.32]
	]]}`
	res := 9
	cp, err := m.CellsForPolygon(model.Polygon{GeoJSON: polyJSON}, res)
	if err != nil {
		t.Fatalf("polygon: %v", err)
	}
	cb, err := m.CellsForBBox(bb, res)
	if err != nil {
		t.Fatalf("bbox: %v", err)
	}
	// polygon should be subset of bbox
	if len(cp) == 0 {
		t.Fatalf("expected non-empty polygon coverage")
	}
	if !sort.StringsAreSorted([]string(cp)) || hasDups(cp) {
		t.Fatalf("polygon cells must be sorted + unique")
	}
	cp2, err := m.CellsForPolygon(model.Polygon{GeoJSON: polyJSON}, res)
	if err != nil {
		t.Fatalf("polygon second call: %v", err)
	}
	if !reflect.DeepEqual(cp, cp2) {
		t.Fatalf("expected identical output for identical input")
	}
	if len(cp) > len(cb) {
		t.Fatalf("polygon coverage larger than bbox coverage (unexpected)")
	}
}

func TestBounds_InvalidResolutionAndDegeneratePolygon(t *testing.T) {
	m := New()
	bb := model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"}

	// resolution bounds check
	if _, err := m.CellsForBBox(bb, -1); err == nil {
		t.Fatalf("expected error for res=-1")
	}
	if _, err := m.CellsForBBox(bb, 16); err == nil {
		t.Fatalf("expected error for res=16")
	}

	// invalid polygon with no coordinates
	p := model.Polygon{GeoJSON: `{"type":"Polygon","coordinates":[[]]}`}
	if _, err := m.CellsForPolygon(p, 8); err == nil {
		t.Fatalf("expected error for degenerate polygon")
	}
}

func hasDups(s []string) bool {
	seen := map[string]struct{}{}
	for _, v := range s {
		if _, ok := seen[v]; ok {
			return true
		}
		seen[v] = struct{}{}
	}
	return false
}
