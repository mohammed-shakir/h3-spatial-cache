package baseline

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
)

type fakeExec struct {
	lastQ       model.QueryRequest
	wroteStatus int
	lastParams  url.Values
}

// records the last query request and writes a 204 No Content response
func (f *fakeExec) ForwardWFS(_ context.Context, w http.ResponseWriter, _ *http.Request, q model.QueryRequest) {
	f.lastQ = q
	f.lastParams = ogc.BuildGetFeatureParams(q)
	f.wroteStatus = http.StatusNoContent
	w.WriteHeader(f.wroteStatus)
}

func equalValues(a, b url.Values) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
	}
	return true
}

func TestBaseline_H3ContextForBBox_IsPopulated_AndTransparent(t *testing.T) {
	cfg := config.FromEnv()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fx := &fakeExec{}

	e, err := newBaseline(cfg, logger, fx)
	if err != nil {
		t.Fatalf("newBaseline: %v", err)
	}

	inQ := model.QueryRequest{
		Layer: "demo:places",
		BBox:  &model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	wantParams := ogc.BuildGetFeatureParams(inQ)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rr := httptest.NewRecorder()

	e.HandleQuery(context.Background(), rr, req, inQ)

	// confirm h3 metadata was added
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d want 204", rr.Code)
	}
	if fx.lastQ.H3Res != cfg.H3Res {
		t.Fatalf("H3Res=%d want %d", fx.lastQ.H3Res, cfg.H3Res)
	}
	if len(fx.lastQ.Cells) == 0 {
		t.Fatalf("expected non-empty Cells for bbox")
	}
	if !equalValues(fx.lastParams, wantParams) {
		t.Fatalf("upstream params changed unexpectedly.\n got: %s\nwant: %s", fx.lastParams.Encode(), wantParams.Encode())
	}
}

func TestBaseline_H3ContextForPolygon_IsPopulated_AndTransparent(t *testing.T) {
	cfg := config.FromEnv()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fx := &fakeExec{}

	e, err := newBaseline(cfg, logger, fx)
	if err != nil {
		t.Fatalf("newBaseline: %v", err)
	}

	poly := `{"type":"Polygon","coordinates":[[[11,55],[12,55],[12,56],[11,56],[11,55]]]}`
	inQ := model.QueryRequest{
		Layer:   "demo:places",
		Polygon: &model.Polygon{GeoJSON: poly},
		Filters: "name <> ''",
	}
	wantParams := ogc.BuildGetFeatureParams(inQ)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rr := httptest.NewRecorder()

	e.HandleQuery(context.Background(), rr, req, inQ)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d want 204", rr.Code)
	}
	if fx.lastQ.H3Res != cfg.H3Res {
		t.Fatalf("H3Res=%d want %d", fx.lastQ.H3Res, cfg.H3Res)
	}
	if len(fx.lastQ.Cells) == 0 {
		t.Fatalf("expected non-empty Cells for polygon")
	}
	if !equalValues(fx.lastParams, wantParams) {
		t.Fatalf("upstream params changed unexpectedly.\n got: %s\nwant: %s", fx.lastParams.Encode(), wantParams.Encode())
	}
}
