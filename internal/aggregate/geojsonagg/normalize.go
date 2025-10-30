package geojsonagg

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

// computes a hash of the given GeoJSON geometry
func GeometryHash(geomRaw json.RawMessage, precision int) (string, error) {
	if len(bytes.TrimSpace(geomRaw)) == 0 || bytes.Equal(geomRaw, []byte("null")) {
		return "gh:null", nil
	}
	var g any
	if err := json.Unmarshal(geomRaw, &g); err != nil {
		return "", fmt.Errorf("parse geometry: %w", err)
	}
	normalized, err := normalizeGeometry(g, precision)
	if err != nil {
		return "", fmt.Errorf("normalize geometry: %w", err)
	}
	buf, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("marshal normalized geometry: %w", err)
	}
	sum := sha256.Sum256(buf)
	return fmt.Sprintf("gh:%x", sum[:]), nil
}

func normalizeGeometry(g any, precision int) (any, error) {
	m, ok := g.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("geometry must be object")
	}
	typ, _ := m["type"].(string)
	coords := m["coordinates"]
	switch typ {
	case "Point":
		return map[string]any{"type": "Point", "coordinates": roundPos(coords, precision)}, nil
	case "MultiPoint":
		return map[string]any{"type": "MultiPoint", "coordinates": roundPosArray(coords, precision)}, nil
	case "LineString":
		return map[string]any{"type": "LineString", "coordinates": roundPosArray(coords, precision)}, nil
	case "MultiLineString":
		return map[string]any{"type": "MultiLineString", "coordinates": roundPosArray2(coords, precision)}, nil
	case "Polygon":
		rings := roundPosArray2(coords, precision)
		rings = orientPolygonRings(rings)
		return map[string]any{"type": "Polygon", "coordinates": rings}, nil
	case "MultiPolygon":
		mp := roundPosArray3(coords, precision)
		for i := range mp {
			mp[i] = orientPolygonRings(mp[i])
		}
		sort.Slice(mp, func(i, j int) bool { return lex3(mp[i], mp[j]) < 0 })
		return map[string]any{"type": "MultiPolygon", "coordinates": mp}, nil
	case "GeometryCollection":
		arr, _ := m["geometries"].([]any)
		out := make([]any, 0, len(arr))
		for _, gi := range arr {
			ng, err := normalizeGeometry(gi, precision)
			if err != nil {
				return nil, err
			}
			out = append(out, ng)
		}
		sort.Slice(out, func(i, j int) bool {
			bi, _ := json.Marshal(out[i])
			bj, _ := json.Marshal(out[j])
			return bytes.Compare(bi, bj) < 0
		})
		return map[string]any{"type": "GeometryCollection", "geometries": out}, nil
	default:
		return m, nil
	}
}

func roundPos(v any, p int) []any {
	a, _ := v.([]any)
	if len(a) == 0 {
		return nil
	}
	out := make([]any, len(a))
	for i := range a {
		if f, ok := a[i].(float64); ok {
			out[i] = roundFloat(f, p)
		} else {
			out[i] = a[i]
		}
	}
	return out
}

func roundPosArray(v any, p int) [][]any {
	a, _ := v.([]any)
	out := make([][]any, len(a))
	for i := range a {
		out[i] = roundPos(a[i], p)
	}
	return out
}

func roundPosArray2(v any, p int) [][][]any {
	a, _ := v.([]any)
	out := make([][][]any, len(a))
	for i := range a {
		out[i] = roundPosArray(a[i], p)
	}
	return out
}

func roundPosArray3(v any, p int) [][][][]any {
	a, _ := v.([]any)
	out := make([][][][]any, len(a))
	for i := range a {
		out[i] = roundPosArray2(a[i], p)
	}
	return out
}

func roundFloat(x float64, p int) float64 {
	f := math.Pow(10, float64(p))
	return math.Round(x*f) / f
}

func orientPolygonRings(rings [][][]any) [][][]any {
	if len(rings) == 0 {
		return rings
	}
	out := make([][][]any, len(rings))
	for i := range rings {
		r := rings[i]
		if isCCW(r) {
			if i == 0 {
				out[i] = r
			} else {
				out[i] = reverseRing(r)
			}
		} else {
			if i == 0 {
				out[i] = reverseRing(r)
			} else {
				out[i] = r
			}
		}
	}
	return out
}

// returns true if the ring is counter-clockwise
func isCCW(r [][]any) bool {
	var area float64
	for i := 0; i+1 < len(r); i++ {
		x1, _ := r[i][0].(float64)
		y1, _ := r[i][1].(float64)
		x2, _ := r[i+1][0].(float64)
		y2, _ := r[i+1][1].(float64)
		area += (x2 - x1) * (y2 + y1)
	}
	return area < 0
}

func reverseRing(r [][]any) [][]any {
	n := len(r)
	out := make([][]any, n)
	for i := range r {
		out[i] = r[n-1-i]
	}
	return out
}

func lex3(a, b [][][]any) int {
	ba, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Compare(ba, bb)
}
