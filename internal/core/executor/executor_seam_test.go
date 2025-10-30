package executor

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
)

type upstreamRecorder struct {
	mu         sync.Mutex
	lastPath   string
	lastQuery  url.Values
	lastHeader http.Header
	lastBody   []byte
}

func (u *upstreamRecorder) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	u.mu.Lock()
	u.lastPath = r.URL.Path
	u.lastQuery = r.URL.Query()
	u.lastHeader = r.Header.Clone()
	u.lastBody = body
	u.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (u *upstreamRecorder) snapshot() (string, url.Values, http.Header) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastPath, u.lastQuery, u.lastHeader
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

func TestExecutor_ForwardWFS_BBox(t *testing.T) {
	up := &upstreamRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(up.handler))
	defer srv.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exec, err := New(logger, nil, srv.URL+"/ows")
	if err != nil {
		t.Fatalf("executor.New: %v", err)
	}

	q := model.QueryRequest{
		Layer: "demo:NR_polygon",
		BBox:  &model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	wantQuery := ogc.BuildGetFeatureParams(q)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rr := httptest.NewRecorder()
	exec.ForwardWFS(context.Background(), rr, req, q)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	if strings.TrimSpace(rr.Body.String()) != `{"ok":true}` {
		t.Fatalf("unexpected body: %q", rr.Body.String())
	}

	path, gotQuery, hdr := up.snapshot()
	if path != "/ows" {
		t.Fatalf("upstream path=%q want /ows", path)
	}
	if !equalValues(gotQuery, wantQuery) {
		t.Fatalf("mismatched query.\n got: %v\nwant: %v", gotQuery.Encode(), wantQuery.Encode())
	}
	if got := hdr.Get("Accept"); got != "application/json" {
		t.Fatalf("missing/invalid Accept header: %q", got)
	}
}

func TestExecutor_ForwardWFS_PolygonWins(t *testing.T) {
	up := &upstreamRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(up.handler))
	defer srv.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	exec, err := New(logger, nil, srv.URL+"/ows")
	if err != nil {
		t.Fatalf("executor.New: %v", err)
	}

	poly := `{"type":"Polygon","coordinates":[[[11,55],[12,55],[12,56],[11,56],[11,55]]]}`
	q := model.QueryRequest{
		Layer:   "demo:NR_polygon",
		BBox:    &model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
		Polygon: &model.Polygon{GeoJSON: poly},
	}

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rr := httptest.NewRecorder()
	exec.ForwardWFS(context.Background(), rr, req, q)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	if strings.TrimSpace(rr.Body.String()) != `{"ok":true}` {
		t.Fatalf("unexpected body: %q", rr.Body.String())
	}

	path, gotQuery, hdr := up.snapshot()
	if path != "/ows" {
		t.Fatalf("upstream path=%q want /ows", path)
	}
	if got := gotQuery.Get("bbox"); got != "" {
		t.Fatalf("bbox should not be forwarded when polygon present; got %q", got)
	}
	if got := gotQuery.Get("cql_filter"); got == "" {
		t.Fatalf("expected cql_filter to be present for polygon case")
	}
	if got := gotQuery.Get("cql_filter"); !strings.Contains(got, "SRID=4326;") {
		t.Fatalf("expected CQL geometry literal to include SRID=4326; got %q", got)
	}
	if got := hdr.Get("Accept"); got != "application/json" {
		t.Fatalf("missing/invalid Accept header: %q", got)
	}
}
