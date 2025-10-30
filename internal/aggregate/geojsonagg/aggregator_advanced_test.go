package geojsonagg

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func loadJSON[T any](t *testing.T, path string) T {
	t.Helper()
	clean := filepath.Clean(path)
	b, err := os.ReadFile(clean)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("unmarshal %s: %v\n%s", path, err, string(b))
	}
	return v
}

func Test_MergeRequest_DedupByID_ThenGeometry(t *testing.T) {
	agg := NewAdvanced()
	req := loadJSON[Request](t, filepath.Join("..", "..", "..", "testdata", "aggregator", "dedup_id_then_geom", "input.json"))
	out, diag, err := agg.MergeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	var fc struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(out, &fc); err != nil {
		t.Fatalf("bad FC: %v", err)
	}

	got := make([]string, 0, len(fc.Features))
	for _, f := range fc.Features {
		var obj struct {
			Properties map[string]any `json:"properties"`
		}
		_ = json.Unmarshal(f, &obj)
		if nm, ok := obj.Properties["name"].(string); ok {
			got = append(got, nm)
		}
	}
	want := loadJSON[[]string](t, filepath.Join("..", "..", "..", "testdata", "aggregator", "dedup_id_then_geom", "expected_names.json"))
	if !slices.Equal(got, want) {
		t.Fatalf("names=%v want %v", got, want)
	}

	if diag.DedupByID == 0 || diag.TotalOut == 0 {
		t.Fatalf("unexpected diag: %+v", diag)
	}
}

func Test_MergeRequest_SortAcrossCells_WithLimitOffset(t *testing.T) {
	agg := NewAdvanced()
	req := loadJSON[Request](t, filepath.Join("..", "..", "..", "testdata", "aggregator", "sort_numeric_time_limit", "input.json"))
	out, _, err := agg.MergeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	var fc struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(out, &fc); err != nil {
		t.Fatalf("bad FC: %v", err)
	}

	got := make([]string, 0, len(fc.Features))
	for _, f := range fc.Features {
		var obj struct {
			Properties map[string]any `json:"properties"`
		}
		_ = json.Unmarshal(f, &obj)
		got = append(got, obj.Properties["name"].(string))
	}
	want := loadJSON[[]string](t, filepath.Join("..", "..", "..", "testdata", "aggregator", "sort_numeric_time_limit", "expected_names.json"))
	if !slices.Equal(got, want) {
		t.Fatalf("names=%v want %v", got, want)
	}
}

func Test_MergeRequest_HitClassifications(t *testing.T) {
	agg := NewAdvanced()
	for _, c := range []string{"full_hit", "partial_hit", "miss"} {
		req := loadJSON[Request](t, filepath.Join("..", "..", "..", "testdata", "aggregator", "hit_classes", c, "input.json"))
		_, diag, err := agg.MergeRequest(req)
		if err != nil {
			t.Fatalf("%s: %v", c, err)
		}
		if string(diag.HitClass) != c {
			t.Fatalf("got %s want %s", diag.HitClass, c)
		}
	}
}
