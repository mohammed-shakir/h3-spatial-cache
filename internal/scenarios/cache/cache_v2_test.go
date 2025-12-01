package cache

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

type recordingFeatureStore struct {
	mu    sync.Mutex
	calls []recordingFSCall
}

type recordingFSCall struct {
	layer string
	feats map[string][]byte
	ttl   time.Duration
}

func (r *recordingFeatureStore) MGetFeatures(ctx context.Context, layer string, ids []string) (map[string][]byte, error) {
	return map[string][]byte{}, nil
}

func (r *recordingFeatureStore) PutFeatures(
	ctx context.Context,
	layer string,
	feats map[string][]byte,
	ttl time.Duration,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cp := make(map[string][]byte, len(feats))
	for k, v := range feats {
		cp[k] = append([]byte(nil), v...)
	}
	r.calls = append(r.calls, recordingFSCall{
		layer: layer,
		feats: cp,
		ttl:   ttl,
	})
	return nil
}

type recordingCellIndex struct {
	mu    sync.Mutex
	calls []recordingIdxCall
}

type recordingIdxCall struct {
	layer   string
	res     int
	cell    string
	filters model.Filters
	ids     []string
	ttl     time.Duration
}

func (r *recordingCellIndex) GetIDs(
	ctx context.Context,
	layer string,
	res int,
	cell string,
	filters model.Filters,
) ([]string, error) {
	return nil, nil
}

func (r *recordingCellIndex) SetIDs(
	ctx context.Context,
	layer string,
	res int,
	cell string,
	filters model.Filters,
	ids []string,
	ttl time.Duration,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cp := append([]string(nil), ids...)
	r.calls = append(r.calls, recordingIdxCall{
		layer:   layer,
		res:     res,
		cell:    cell,
		filters: filters,
		ids:     cp,
		ttl:     ttl,
	})
	return nil
}

func newTestEngineForV2(t *testing.T, body string, fs *recordingFeatureStore, idx *recordingCellIndex) *Engine {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test url: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return &Engine{
		logger: logger,
		res:    7,
		minRes: 7,
		maxRes: 7,
		fs:     fs,
		idx:    idx,
		owsURL: u,
		http:   srv.Client(),
		// opTimeout used for upstream calls
		opTimeout: 2 * time.Second,
	}
}

func TestFetchCell_PopulatesFeatureStoreAndIndex_WithIDs(t *testing.T) {
	fs := &recordingFeatureStore{}
	idx := &recordingCellIndex{}

	body := `{"type":"FeatureCollection","features":[` +
		`{"type":"Feature","id":"foo","geometry":null,"properties":{"name":"a"}},` +
		`{"type":"Feature","id":2,"geometry":null,"properties":{"name":"b"}}` +
		`]}`

	e := newTestEngineForV2(t, body, fs, idx)

	ctx := context.Background()
	q := model.QueryRequest{Layer: "demo:layer"}
	cell := "892a100d2b3ffff" // valid test cell used elsewhere
	res := 7
	ttl := 90 * time.Second

	r := e.fetchCell(ctx, q, cell, res, ttl)
	if r.err != nil {
		t.Fatalf("fetchCell err: %v", r.err)
	}
	if len(r.body) == 0 {
		t.Fatalf("expected non-empty body from fetchCell")
	}

	if len(fs.calls) != 1 {
		t.Fatalf("expected 1 PutFeatures call, got %d", len(fs.calls))
	}
	if len(idx.calls) != 1 {
		t.Fatalf("expected 1 SetIDs call, got %d", len(idx.calls))
	}

	fsCall := fs.calls[0]
	if fsCall.layer != q.Layer {
		t.Fatalf("PutFeatures layer=%q want %q", fsCall.layer, q.Layer)
	}
	if fsCall.ttl != ttl {
		t.Fatalf("PutFeatures ttl=%v want %v", fsCall.ttl, ttl)
	}

	// IDs should be canonicalized (string/number)
	if _, ok := fsCall.feats["s:foo"]; !ok {
		t.Fatalf("expected feature with id s:foo in feature store")
	}
	if _, ok := fsCall.feats["n:2"]; !ok {
		t.Fatalf("expected feature with id n:2 in feature store")
	}

	idxCall := idx.calls[0]
	if idxCall.layer != q.Layer || idxCall.res != res || idxCall.cell != cell {
		t.Fatalf("SetIDs context mismatch: %+v", idxCall)
	}
	if len(idxCall.ids) != 2 {
		t.Fatalf("SetIDs ids len=%d want 2", len(idxCall.ids))
	}
	if idxCall.ids[0] != "s:foo" || idxCall.ids[1] != "n:2" {
		t.Fatalf("SetIDs ids=%v want [s:foo n:2]", idxCall.ids)
	}
	if idxCall.ttl != ttl {
		t.Fatalf("SetIDs ttl=%v want %v", idxCall.ttl, ttl)
	}
}

func TestFetchCell_PopulatesFeatureStoreAndIndex_FallbackGeometryHash(t *testing.T) {
	fs := &recordingFeatureStore{}
	idx := &recordingCellIndex{}

	geom := `{"type":"Point","coordinates":[18.00000001,59.30000001]}`
	body := `{"type":"FeatureCollection","features":[` +
		`{"type":"Feature","geometry":` + geom + `,"properties":{"name":"a"}},` +
		`{"type":"Feature","geometry":` + geom + `,"properties":{"name":"b"}}` +
		`]}`

	e := newTestEngineForV2(t, body, fs, idx)

	ctx := context.Background()
	q := model.QueryRequest{Layer: "demo:layer2"}
	cell := "892a100d2b3ffff"
	res := 7
	ttl := 60 * time.Second

	r := e.fetchCell(ctx, q, cell, res, ttl)
	if r.err != nil {
		t.Fatalf("fetchCell err: %v", r.err)
	}

	if len(fs.calls) != 1 {
		t.Fatalf("expected 1 PutFeatures call, got %d", len(fs.calls))
	}
	if len(idx.calls) != 1 {
		t.Fatalf("expected 1 SetIDs call, got %d", len(idx.calls))
	}

	// Compute expected geometry hash with same precision.
	expectedID, err := geojsonagg.GeometryHash([]byte(geom), geojsonagg.DefaultGeomPrecision)
	if err != nil {
		t.Fatalf("GeometryHash: %v", err)
	}

	fsCall := fs.calls[0]
	if len(fsCall.feats) != 1 {
		t.Fatalf("expected 1 unique feature in feature store (dedup by geom), got %d", len(fsCall.feats))
	}
	if _, ok := fsCall.feats[expectedID]; !ok {
		t.Fatalf("expected feature keyed by %q in feature store, got keys=%v", expectedID, keysOf(fsCall.feats))
	}

	idxCall := idx.calls[0]
	if len(idxCall.ids) == 0 {
		t.Fatalf("expected at least 1 id in cell index")
	}
	if idxCall.ids[0] != expectedID {
		t.Fatalf("first index id=%q want %q", idxCall.ids[0], expectedID)
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestFetchCell_MultiResolution_SafeReuseOfFeatures(t *testing.T) {
	fs := &recordingFeatureStore{}
	idx := &recordingCellIndex{}

	body := `{"type":"FeatureCollection","features":[` +
		`{"type":"Feature","id":"foo","geometry":null,"properties":{"name":"a"}},` +
		`{"type":"Feature","id":"bar","geometry":null,"properties":{"name":"b"}}` +
		`]}`

	e := newTestEngineForV2(t, body, fs, idx)

	ctx := context.Background()
	q := model.QueryRequest{Layer: "demo:layer"}
	cell := "892a100d2b3ffff"
	ttl := 2 * time.Minute

	// coarse res
	resCoarse := 6
	r1 := e.fetchCell(ctx, q, cell, resCoarse, ttl)
	if r1.err != nil {
		t.Fatalf("fetchCell coarse err: %v", r1.err)
	}

	// fine res
	resFine := 8
	r2 := e.fetchCell(ctx, q, cell, resFine, ttl)
	if r2.err != nil {
		t.Fatalf("fetchCell fine err: %v", r2.err)
	}

	// We should have written to feature store twice, but with the same IDs.
	if len(fs.calls) != 2 {
		t.Fatalf("expected 2 PutFeatures calls (coarse+fine), got %d", len(fs.calls))
	}
	firstIDs := keysOf(fs.calls[0].feats)
	secondIDs := keysOf(fs.calls[1].feats)

	if !reflect.DeepEqual(firstIDs, secondIDs) {
		t.Fatalf("feature IDs differ between resolutions; coarse=%v fine=%v", firstIDs, secondIDs)
	}

	// Cell index entries must differ by resolution, but both contain the same IDs.
	if len(idx.calls) != 2 {
		t.Fatalf("expected 2 SetIDs calls, got %d", len(idx.calls))
	}
	if idx.calls[0].res == idx.calls[1].res {
		t.Fatalf("cell index calls must use different resolutions")
	}
	if !reflect.DeepEqual(idx.calls[0].ids, idx.calls[1].ids) {
		t.Fatalf("cell index IDs differ between resolutions; coarse=%v fine=%v",
			idx.calls[0].ids, idx.calls[1].ids)
	}
}
