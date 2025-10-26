package geojsonagg

import (
	"encoding/json"
	"slices"
	"testing"
)

// builds a FeatureCollection json from the json strings
func mustFC(features ...string) []byte {
	s := `{"type":"FeatureCollection","features":[` + join(features) + `]}`
	return []byte(s)
}

func join(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

// generates a simple feature object with optional id and name
func feat(idJSON, name string) string {
	idPart := ""
	if idJSON != "" {
		idPart = `"id":` + idJSON + `,`
	}
	return `{"type":"Feature",` + idPart + `"geometry":null,"properties":{"name":"` + name + `"}}`
}

type merged struct {
	Type     string            `json:"type"`
	Features []json.RawMessage `json:"features"`
}

func parseOut(t *testing.T, b []byte) merged {
	t.Helper()
	var m merged
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal merged output: %v\n%s", err, string(b))
	}
	return m
}

func featureNames(t *testing.T, feats []json.RawMessage) []string {
	t.Helper()
	names := make([]string, 0, len(feats))
	for i, f := range feats {
		var obj struct {
			Type       string                 `json:"type"`
			Properties map[string]any         `json:"properties"`
			ID         any                    `json:"id,omitempty"`
			Geometry   map[string]any         `json:"geometry"`
			Extra      map[string]json.Number `json:"-"`
		}
		if err := json.Unmarshal(f, &obj); err != nil {
			t.Fatalf("feature %d unmarshal: %v", i, err)
		}
		if obj.Type != "Feature" {
			t.Fatalf("feature %d type=%q want Feature", i, obj.Type)
		}
		if obj.Properties == nil {
			names = append(names, "")
		} else {
			if v, ok := obj.Properties["name"].(string); ok {
				names = append(names, v)
			} else {
				names = append(names, "")
			}
		}
	}
	return names
}

func TestMerge_EmptyInput_ReturnsEmptyFeatureCollection(t *testing.T) {
	agg := New(true)
	out, err := agg.Merge(nil)
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	got := parseOut(t, out)
	if got.Type != "FeatureCollection" {
		t.Fatalf("type=%q want FeatureCollection", got.Type)
	}
	if len(got.Features) != 0 {
		t.Fatalf("features len=%d want 0", len(got.Features))
	}
}

func TestMerge_SinglePart_PreservesFeatures(t *testing.T) {
	agg := New(false)

	f1 := feat(`"A"`, "a")
	f2 := feat(`1`, "b")
	part := mustFC(f1, f2)

	out, err := agg.Merge([][]byte{part})
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	got := parseOut(t, out)
	if got.Type != "FeatureCollection" {
		t.Fatalf("type=%q want FeatureCollection", got.Type)
	}
	names := featureNames(t, got.Features)
	want := []string{"a", "b"}
	if !slices.Equal(names, want) {
		t.Fatalf("names=%v want %v", names, want)
	}
}

func TestMerge_MultipleParts_DeterministicOrder(t *testing.T) {
	agg := New(false)
	p1 := mustFC(
		feat(`"A"`, "a"),
		feat(``, "b"),
	)
	p2 := mustFC(
		feat(`"C"`, "c"),
	)
	p3 := mustFC(
		feat(`2`, "d"),
		feat(``, "e"),
	)

	out, err := agg.Merge([][]byte{p1, p2, p3})
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	got := parseOut(t, out)
	names := featureNames(t, got.Features)
	want := []string{"a", "b", "c", "d", "e"}
	if !slices.Equal(names, want) {
		t.Fatalf("names=%v want %v", names, want)
	}
}

func TestMerge_MalformedInputs_AreRejected(t *testing.T) {
	agg := New(false)

	tests := []struct {
		name string
		in   [][]byte
	}{
		{"not-json", [][]byte{[]byte("{oops]")}},
		{"wrong-type", [][]byte{[]byte(`{"type":"Feature","geometry":null}`)}},
		{"no-features", [][]byte{[]byte(`{"type":"FeatureCollection"}`)}},
		{"features-not-array", [][]byte{[]byte(`{"type":"FeatureCollection","features":{}}`)}},
		{"feature-not-object", [][]byte{[]byte(`{"type":"FeatureCollection","features":[true]}`)}},
		{"feature-not-Feature", [][]byte{[]byte(`{"type":"FeatureCollection","features":[{"type":"Polygon"}]}`)}},
		{"feature-bad-id-type", [][]byte{[]byte(`{"type":"FeatureCollection","features":[{"type":"Feature","id":{"x":1}}]}`)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := agg.Merge(tc.in); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestMerge_DedupByID_OptionalBehavior(t *testing.T) {
	p1 := mustFC(feat(`"A"`, "a1"))
	p2 := mustFC(feat(`"A"`, "a2"))

	aggDedup := New(true)
	out, err := aggDedup.Merge([][]byte{p1, p2})
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	names := featureNames(t, parseOut(t, out).Features)
	if want := []string{"a1"}; !slices.Equal(names, want) {
		t.Fatalf("dedup names=%v want %v", names, want)
	}

	aggNo := New(false)
	out2, err := aggNo.Merge([][]byte{p1, p2})
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	names2 := featureNames(t, parseOut(t, out2).Features)
	if want := []string{"a1", "a2"}; !slices.Equal(names2, want) {
		t.Fatalf("no-dedup names=%v want %v", names2, want)
	}
}

func TestMerge_NoDedupForFeaturesWithoutID(t *testing.T) {
	agg := New(true)

	p1 := mustFC(feat("", "x"))
	p2 := mustFC(feat("", "x-dup"))

	out, err := agg.Merge([][]byte{p1, p2})
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	names := featureNames(t, parseOut(t, out).Features)
	if want := []string{"x", "x-dup"}; !slices.Equal(names, want) {
		t.Fatalf("names=%v want %v", names, want)
	}
}
