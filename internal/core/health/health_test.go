package health

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveness_Handler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	Liveness()(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type=%q want text/plain", ct)
	}
	if got := strings.TrimSpace(rr.Body.String()); got != "ok" {
		t.Fatalf("body=%q want ok", got)
	}
}
