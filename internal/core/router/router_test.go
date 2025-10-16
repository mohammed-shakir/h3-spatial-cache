package router

import (
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

func TestParseBBOX_Valid(t *testing.T) {
	bb, err := parseBBOX("11.0,55.0,12.0,56.0,EPSG:4326")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"}
	if bb != want {
		t.Fatalf("got %+v want %+v", bb, want)
	}
}

func TestParseBBOX_InvalidSRID(t *testing.T) {
	_, err := parseBBOX("11,55,12,56,EPSG:3857")
	if err == nil {
		t.Fatal("expected error for SRID")
	}
}

func TestParsePolygon_TypeChecks(t *testing.T) {
	// valid polygon
	_, err := parsePolygon(`{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// valid multipolygon
	_, err = parsePolygon(`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,1],[0,0]]]]}`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// invalid type
	_, err = parsePolygon(`{"type":"LineString","coordinates":[[0,0],[1,1]]}`)
	if err == nil {
		t.Fatal("expected error for non-polygon type")
	}
}
