package baseline

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
	simpledec "github.com/mohammed-shakir/h3-spatial-cache/internal/decision/simple"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
)

type execRecorder struct {
	lastQ       model.QueryRequest
	wroteStatus int
	lastParams  url.Values
}

func (f *execRecorder) ForwardWFS(_ context.Context, w http.ResponseWriter, _ *http.Request, q model.QueryRequest) {
	f.lastQ = q
	f.lastParams = ogc.BuildGetFeatureParams(q)
	f.wroteStatus = http.StatusNoContent
	w.WriteHeader(f.wroteStatus)
}

func equalValues2(a, b url.Values) bool {
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

type fakeHot struct {
	seen map[string]struct{}
	thr  float64
}

func (f *fakeHot) Inc(cell string) {
	if f.seen == nil {
		f.seen = map[string]struct{}{}
	}
	f.seen[cell] = struct{}{}
}

func (f *fakeHot) Score(cell string) float64 {
	if _, ok := f.seen[cell]; ok {
		return f.thr
	}
	return 0
}

func (f *fakeHot) Reset(cells ...string) {
	for _, c := range cells {
		delete(f.seen, c)
	}
}

var _ hotness.Interface = (*fakeHot)(nil)

func TestBaseline_Decision_DoesNotAlterParams_AndLogsShouldCache(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	exec := &execRecorder{}
	res := 8
	mapr := h3mapper.New()
	thr := 10.0
	hot := &fakeHot{thr: thr}
	dec := simpledec.New(hot, thr, res, res, res, mapr)

	e := &Engine{
		logger: logger,
		exec:   exec,
		res:    res,
		mapr:   mapr,
		hot:    hot,
		dec:    dec,
		thr:    thr,
	}

	inQ := model.QueryRequest{
		Layer: "demo:places",
		BBox:  &model.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	wantParams := ogc.BuildGetFeatureParams(inQ)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rr := httptest.NewRecorder()
	e.HandleQuery(req.Context(), rr, req, inQ)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d want 204", rr.Code)
	}
	if !equalValues2(exec.lastParams, wantParams) {
		t.Fatalf("upstream params changed unexpectedly.\n got: %s\nwant: %s", exec.lastParams.Encode(), wantParams.Encode())
	}

	logs := buf.String()
	if !strings.Contains(logs, "shouldCache=true") {
		t.Fatalf("expected debug logs to contain shouldCache=true; got:\n%s", logs)
	}
}
