package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

// fake executor that streams a tiny GML payload
type fakeExec struct{}

func (f fakeExec) FetchGetFeature(ctx context.Context, q model.QueryRequest) ([]byte, string, error) {
	return nil, "", nil
}
func (f fakeExec) ForwardGetFeature(w http.ResponseWriter, r *http.Request, q model.QueryRequest) {}
func (f fakeExec) ForwardGetFeatureFormat(w http.ResponseWriter, r *http.Request, q model.QueryRequest, accept string) {
	w.Header().Set("Content-Type", accept)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"/>`))
}

func TestGMLStreaming_On(t *testing.T) {
	e := &Engine{
		gmlStreaming: true,
		exec:         fakeExec{},
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/query?layer=topp:states", nil)
	req.Header.Set("Accept", "application/gml+xml; version=3.2")

	q := model.QueryRequest{Layer: "topp:states"}
	e.HandleQuery(context.Background(), rr, req, q)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/gml+xml; version=3.2" {
		t.Fatalf("expected GML content type, got %q", ct)
	}
	if len(rr.Body.Bytes()) == 0 {
		t.Fatalf("expected streamed body")
	}
}

func TestGMLStreaming_Off406(t *testing.T) {
	e := &Engine{
		gmlStreaming: false,
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/query?layer=topp:states", nil)
	req.Header.Set("Accept", "application/gml+xml; version=3.2")

	q := model.QueryRequest{Layer: "topp:states"}
	e.HandleQuery(context.Background(), rr, req, q)

	if rr.Code != http.StatusNotAcceptable {
		t.Fatalf("expected 406, got %d", rr.Code)
	}
	if vary := rr.Header().Get("Vary"); vary != "Accept" {
		t.Fatalf("expected Vary: Accept, got %q", vary)
	}
}
