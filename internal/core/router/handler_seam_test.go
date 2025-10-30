package router

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
)

type fakeHandler struct {
	lastQ model.QueryRequest
}

func (f *fakeHandler) HandleQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	f.lastQ = q
	w.WriteHeader(http.StatusNoContent)
}

func TestHandleQuery_SeamDispatch(t *testing.T) {
	cfg := config.FromEnv()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	h := &fakeHandler{}
	hdl := HandleQuery(logger, cfg, h)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	q.Set("bbox", "11.0,55.0,12.0,56.0,EPSG:4326")
	req.URL.RawQuery = q.Encode()

	rr := httptest.NewRecorder()
	hdl(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 from fake handler, got %d", rr.Code)
	}
	if h.lastQ.Layer != "demo:NR_polygon" || h.lastQ.BBox == nil {
		t.Fatalf("handler did not receive parsed query correctly: %+v", h.lastQ)
	}
}
