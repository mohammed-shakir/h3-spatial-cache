package geojsonagg

import (
	"encoding/json"
	"fmt"
	"testing"
)

// creates a FeatureCollection with n features
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

// benchmarks merging parts with given number of features each
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

func BenchmarkMergeRequest_KWay(b *testing.B) {
	agg := NewAdvanced()
	K := 8
	N := 512
	req := Request{Query: Query{
		Sort:  []SortKey{{Property: "rank", Direction: Asc}},
		Limit: 0, StartIndex: 0,
	}}
	req.Shards = make([]ShardPage, K)
	for k := range K {
		req.Shards[k] = ShardPage{Meta: ShardMeta{FromCache: true, ID: fmt.Sprintf("s%d", k)}}
		for i := range N {
			feat := fmt.Sprintf(`{"type":"Feature","id":"%d-%d","geometry":null,"properties":{"rank":%d,"name":"%d-%d"}}`, k, i, k*N+i, k, i)
			req.Shards[k].Features = append(req.Shards[k].Features, json.RawMessage(feat))
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := agg.MergeRequest(req); err != nil {
			b.Fatal(err)
		}
	}
}
