package baseline

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/composer"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/router"
	simpledec "github.com/mohammed-shakir/h3-spatial-cache/internal/decision/simple"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
)

type noHot struct{}

func (noHot) Inc(string)           {}
func (noHot) Score(string) float64 { return 0 }
func (noHot) Reset(...string)      {}

type streamExec struct{ forwardCalled bool }

func (f *streamExec) FetchGetFeature(_ context.Context, _ model.QueryRequest) ([]byte, string, error) {
	return []byte(`{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":null}]}`),
		"application/geo+json", nil
}

func (f *streamExec) ForwardGetFeature(w http.ResponseWriter, _ *http.Request, _ model.QueryRequest) {
	f.forwardCalled = true
	w.Header().Set("Content-Type", "application/geo+json")
	w.Header().Add("Vary", "Accept")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[]}`))
}

func (f *streamExec) ForwardGetFeatureFormat(w http.ResponseWriter, r *http.Request, q model.QueryRequest, _ string) {
	f.ForwardGetFeature(w, r, q)
}

func newTestHandler(stream bool, exec *streamExec) router.QueryHandler {
	return &Engine{
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		exec:           exec,
		res:            8,
		mapr:           nil,
		hot:            noHot{},
		dec:            simpledec.New(noHot{}, 0, 8, 8, 8, h3mapper.New()),
		thr:            0,
		eng:            composerEngine(),
		streamUpstream: stream,
	}
}

func composerEngine() (eng composer.Engine) {
	eng.V2 = &passAdapter{}
	return eng
}

type passAdapter struct{}

func (*passAdapter) MergeWithQuery(_ context.Context, _ composer.QueryParams, parts [][]byte) ([]byte, error) {
	if len(parts) == 0 {
		return []byte(`{"type":"FeatureCollection","features":[]}`), nil
	}
	return parts[0], nil
}

func TestBaselineStreaming_On_UsesProxy(t *testing.T) {
	fx := &streamExec{}
	h := newTestHandler(true, fx)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := model.QueryRequest{Layer: "roads"}

	h.HandleQuery(context.Background(), w, r, q)

	if !fx.forwardCalled {
		t.Fatalf("expected ForwardGetFeature to be called when stream flag is on")
	}
	res := w.Result()
	if got := res.Header.Get("Content-Type"); got == "" {
		t.Fatalf("expected Content-Type to be set by proxy path")
	}
	if vary := res.Header.Get("Vary"); vary != "Accept" {
		t.Fatalf("expected Vary: Accept to pass through, got %q", vary)
	}
}

func TestBaselineStreaming_Off_UsesBuffered(t *testing.T) {
	fx := &streamExec{}
	h := newTestHandler(false, fx)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := model.QueryRequest{Layer: "roads"}

	h.HandleQuery(context.Background(), w, r, q)

	if fx.forwardCalled {
		t.Fatalf("did not expect ForwardGetFeature to be called when stream flag is off")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from buffered baseline, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct == "" {
		t.Fatalf("expected Content-Type on buffered baseline response")
	}
}
