package router

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseQueryRequest_PolygonPrecedence(t *testing.T) {
	poly := `{"type":"Polygon","coordinates":[[[11,55],[12,55],[12,56],[11,56],[11,55]]]}`
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:places")
	q.Set("bbox", "11,55,12,56,EPSG:4326")
	q.Set("polygon", poly)
	req.URL.RawQuery = q.Encode()

	got, warn, err := ParseQueryRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if warn == "" {
		t.Fatalf("expected non-empty warning when both bbox and polygon provided")
	}
	if got.Polygon == nil {
		t.Fatalf("expected Polygon to be set")
	}
	if got.BBox != nil {
		t.Fatalf("expected BBox to be dropped when polygon present")
	}
}

func TestParseQueryRequest_InvalidFilters(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:places")
	q.Set("filters", "name = 'x'; DROP TABLE places")
	req.URL.RawQuery = q.Encode()

	_, _, err := ParseQueryRequest(req)
	if err == nil {
		t.Fatalf("expected error for unsafe filters")
	}
}

func TestParseBBOX_InvalidGeometry(t *testing.T) {
	if _, err := parseBBOX("11,55,11,56,EPSG:4326"); err == nil {
		t.Fatalf("expected error for non-increasing bbox coordinates")
	}
}
