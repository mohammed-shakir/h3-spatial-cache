package ogc

import (
	"net/url"
	"strings"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

func TestBuildGetFeatureParams_WithBBox(t *testing.T) {
	q := model.QueryRequest{
		Layer: "demo:NR_polygon",
		BBox:  &model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	v := BuildGetFeatureParams(q)
	assertHas := func(k, want string) {
		if got := v.Get(k); got != want {
			t.Fatalf("param %q got %q want %q", k, got, want)
		}
	}
	assertHas("service", "WFS")
	assertHas("request", "GetFeature")
	assertHas("typeNames", "demo:NR_polygon")
	assertHas("bbox", "11.000000,55.000000,12.000000,56.000000,EPSG:4326")
}

func TestBuildGetFeatureParams_WithPolygon(t *testing.T) {
	poly := `{"type":"Polygon","coordinates":[[[11,55],[12,55],[12,56],[11,56],[11,55]]]}`
	q := model.QueryRequest{
		Layer:   "demo:NR_polygon",
		Polygon: &model.Polygon{GeoJSON: poly},
		Filters: "name <> ''",
	}
	v := BuildGetFeatureParams(q)
	cql := v.Get("cql_filter")
	if !strings.Contains(cql, "INTERSECTS(geom, SRID=4326;POLYGON") || !strings.Contains(cql, "name <> ''") {
		t.Fatalf("expected polygon INTERSECTS combined with filters; got %q", cql)
	}
	if got := v.Get("bbox"); got != "" {
		t.Fatalf("bbox must be empty when polygon is provided; got %q", got)
	}
}

func TestOWSEndpoint(t *testing.T) {
	base := "http://localhost:8080/geoserver"
	want := "http://localhost:8080/geoserver/ows"
	if got := OWSEndpoint(base); got != want {
		t.Fatalf("OWSEndpoint got %q want %q", got, want)
	}
	if _, err := url.Parse(OWSEndpoint(base)); err != nil {
		t.Fatalf("invalid URL from OWSEndpoint: %v", err)
	}
}
