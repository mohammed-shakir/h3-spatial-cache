package geojsonagg

import (
	"fmt"
	"testing"
)

func fcWithN(n int, prefix string, withIDs bool) []byte {
	j := `{"type":"FeatureCollection","features":[`
	for i := range n {
		if i > 0 {
			j += ","
		}
		if withIDs {
			j += fmt.Sprintf(`{"type":"Feature","id":"%s-%d","geometry":null,"properties":{"name":"%s-%d"}}`, prefix, i, prefix, i)
		} else {
			j += fmt.Sprintf(`{"type":"Feature","geometry":null,"properties":{"name":"%s-%d"}}`, prefix, i)
		}
	}
	j += "]}"
	return []byte(j)
}

func makeParts(parts, featsPerPart int, withIDs bool) [][]byte {
	out := make([][]byte, parts)
	for i := range parts {
		out[i] = fcWithN(featsPerPart, fmt.Sprintf("p%d", i), withIDs)
	}
	return out
}

func benchMerge(b *testing.B, parts, featsPerPart int, dedup bool) {
	agg := New(dedup)
	in := makeParts(parts, featsPerPart, true)
	b.ReportAllocs()

	for b.Loop() {
		if _, err := agg.Merge(in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMerge_Small(b *testing.B) {
	for _, parts := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("parts=%d_feats=8", parts), func(b *testing.B) {
			benchMerge(b, parts, 8, true)
		})
	}
}

func BenchmarkMerge_Medium(b *testing.B) {
	for _, parts := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("parts=%d_feats=64", parts), func(b *testing.B) {
			benchMerge(b, parts, 64, true)
		})
	}
}
