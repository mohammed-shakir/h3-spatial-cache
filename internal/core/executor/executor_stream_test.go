package executor

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/httpclient"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

func TestForwardGetFeature_StripsHopByHop_AndForwardsVary(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("Vary", "Accept")
		w.Header().Set("Content-Type", "application/geo+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[]}`))
	}))
	defer up.Close()

	exec, err := New(slog.Default(), httpclient.NewOutbound(), up.URL)
	if err != nil {
		t.Fatalf("executor init: %v", err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/query", nil)

	exec.ForwardGetFeature(rr, req, model.QueryRequest{Layer: "roads"})

	res := rr.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	if res.Header.Get("Connection") != "" {
		t.Fatalf("expected hop-by-hop Connection header to be stripped")
	}
	if res.Header.Get("Vary") != "Accept" {
		t.Fatalf("expected Vary: Accept to be forwarded")
	}
	if res.Header.Get("Content-Type") == "" {
		t.Fatalf("expected Content-Type to be forwarded")
	}
}
