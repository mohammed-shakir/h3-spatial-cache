package geojsonagg

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type Aggregator struct {
	EnableDedup   bool
	GeomPrecision int
	Prefetch      int
}

const DefaultGeomPrecision = 7

func NewAdvanced() *Aggregator {
	return &Aggregator{
		EnableDedup:   true,
		GeomPrecision: DefaultGeomPrecision,
	}
}

// MergeRequest merges the given request's shards into a single GeoJSON FeatureCollection
func (a *Aggregator) MergeRequest(req Request) ([]byte, Diagnostics, error) {
	diag := Diagnostics{}
	if len(req.Shards) == 0 {
		out := []byte(`{"type":"FeatureCollection","features":[]}`)
		diag.HitClass = Miss
		return out, diag, nil
	}

	cached := 0
	for _, s := range req.Shards {
		if s.Meta.FromCache {
			cached++
		}
	}
	switch {
	case cached == 0:
		diag.HitClass = Miss
	case cached == len(req.Shards):
		diag.HitClass = FullHit
	default:
		diag.HitClass = PartialHit
	}

	iters := make([]*featIter, 0, len(req.Shards))
	for si := range req.Shards {
		it := &featIter{
			shardIdx:   si,
			features:   req.Shards[si].Features,
			geomHashes: req.Shards[si].GeomHashes,
			pos:        0,
			getCmp:     func(f featureParsed) []cmpValue { return extractSortTuple(f, req.Query.Sort) },
		}
		iters = append(iters, it)
	}

	h := &featHeap{sort: req.Query.Sort}
	heap.Init(h)
	for _, it := range iters {
		if f, ok := it.next(); ok {
			heap.Push(h, f)
		}
	}

	seenID := map[string]struct{}{}
	seenGH := map[string]struct{}{}
	var outFeatures []json.RawMessage
	outFeatures = make([]json.RawMessage, 0, 128)

	skipped := 0
	emitted := 0
	start := req.Query.StartIndex
	limit := max(req.Query.Limit, 0)
	if start < 0 {
		start = 0
	}

	for h.Len() > 0 {
		fp := heap.Pop(h).(featureParsed)
		diag.TotalIn++

		if a.EnableDedup {
			if len(fp.idRaw) > 0 {
				key, idErr := canonicalIDKey(fp.idRaw)
				if idErr != nil {
					return nil, diag, fmt.Errorf("invalid feature id: %w", idErr)
				}
				if key != "" {
					if _, ok := seenID[key]; ok {
						diag.DedupByID++
						if f, ok := fp.iter.next(); ok {
							heap.Push(h, f)
						}
						continue
					}
					seenID[key] = struct{}{}
				}
			} else {
				if fp.geomHash == "" {
					gh, err := GeometryHash(fp.geomRaw, a.GeomPrecision)
					if err != nil {
						return nil, diag, fmt.Errorf("geom hash: %w", err)
					}
					fp.geomHash = gh
				}
				if _, ok := seenGH[fp.geomHash]; ok {
					diag.DedupByGH++
					if f, ok := fp.iter.next(); ok {
						heap.Push(h, f)
					}
					continue
				}
				seenGH[fp.geomHash] = struct{}{}
			}
		}

		switch {
		case skipped < start:
			skipped++
		case limit == 0 || emitted < limit:
			outFeatures = append(outFeatures, fp.raw)
			emitted++
		}

		if f, ok := fp.iter.next(); ok {
			heap.Push(h, f)
		}
	}
	diag.TotalOut = len(outFeatures)

	out := struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}{
		Type:     "FeatureCollection",
		Features: outFeatures,
	}
	buf, err := json.Marshal(out)
	if err != nil {
		return nil, diag, fmt.Errorf("marshal output: %w", err)
	}
	return buf, diag, nil
}

type featureParsed struct {
	raw      json.RawMessage
	idRaw    json.RawMessage
	geomRaw  json.RawMessage
	sortVals []cmpValue
	geomHash string
	iter     *featIter
	shardIdx int
	localIdx int
}

type featIter struct {
	shardIdx   int
	features   []json.RawMessage
	geomHashes []string
	pos        int
	getCmp     func(featureParsed) []cmpValue
}

// returns the next featureParsed from the iterator
func (it *featIter) next() (featureParsed, bool) {
	if it.pos >= len(it.features) {
		return featureParsed{}, false
	}
	raw := it.features[it.pos]
	it.pos++

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		panic(fmt.Errorf("feature parse shard=%d idx=%d: %w", it.shardIdx, it.pos-1, err))
	}
	fp := featureParsed{
		raw:      raw,
		idRaw:    obj["id"],
		geomRaw:  obj["geometry"],
		shardIdx: it.shardIdx,
		localIdx: it.pos - 1,
		iter:     it,
	}

	if len(it.geomHashes) > 0 && fp.localIdx < len(it.geomHashes) {
		fp.geomHash = it.geomHashes[fp.localIdx]
	}

	fp.sortVals = it.getCmp(fp)
	return fp, true
}

type featHeap struct {
	items []featureParsed
	sort  []SortKey
}

func (h featHeap) Len() int { return len(h.items) }
func (h featHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	c := compareTuples(a.sortVals, b.sortVals, h.sort)
	if c != 0 {
		return c < 0
	}
	if a.shardIdx != b.shardIdx {
		return a.shardIdx < b.shardIdx
	}
	return a.localIdx < b.localIdx
}
func (h featHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *featHeap) Push(x any)   { h.items = append(h.items, x.(featureParsed)) }
func (h *featHeap) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

func extractSortTuple(fp featureParsed, keys []SortKey) []cmpValue {
	if len(keys) == 0 {
		return nil
	}
	var obj struct {
		Properties map[string]any             `json:"properties"`
		ID         any                        `json:"id,omitempty"`
		Geometry   map[string]any             `json:"geometry"`
		Extra      map[string]json.RawMessage `json:"-"`
	}
	_ = json.Unmarshal(fp.raw, &obj)
	props := obj.Properties

	out := make([]cmpValue, len(keys))
	for i, k := range keys {
		var v any
		if props != nil {
			v = props[k.Property]
		}
		out[i] = coerceCmpValue(v, k.TypeHint)
	}
	return out
}

// represents a value for comparison during sorting
func coerceCmpValue(v any, hint string) cmpValue {
	if v == nil {
		return cmpValue{kind: kindNull, null: true}
	}
	switch hint {
	case "number":
		if f, ok := toFloat(v); ok {
			return cmpValue{kind: kindNumber, n: f}
		}
		return cmpValue{kind: kindString, s: fmt.Sprintf("%v", v)}
	case "time":
		if t, ok := toTime(v); ok {
			return cmpValue{kind: kindTime, t: t}
		}
		return cmpValue{kind: kindString, s: fmt.Sprintf("%v", v)}
	case "string":
		return cmpValue{kind: kindString, s: fmt.Sprintf("%v", v)}
	}

	if t, ok := toTime(v); ok {
		return cmpValue{kind: kindTime, t: t}
	}
	if f, ok := toFloat(v); ok {
		return cmpValue{kind: kindNumber, n: f}
	}
	return cmpValue{kind: kindString, s: fmt.Sprintf("%v", v)}
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		return math.NaN(), false
	default:
		return math.NaN(), false
	}
}

func toTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return ts, true
		}
		if ts, err := time.Parse(time.RFC3339, t); err == nil {
			return ts, true
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}

func compareTuples(a, b []cmpValue, keys []SortKey) int {
	for i := range keys {
		dir := 1
		if keys[i].Direction == Desc {
			dir = -1
		}
		if a[i].null != b[i].null {
			if keys[i].Nulls == NullsFirst {
				if a[i].null {
					return -1
				}
				return 1
			}
			if a[i].null {
				return 1
			}
			return -1
		}
		if a[i].null {
			continue
		}

		switch a[i].kind {
		case kindNumber:
			if a[i].n < b[i].n {
				return -1 * dir
			}
			if a[i].n > b[i].n {
				return 1 * dir
			}
		case kindTime:
			if a[i].t.Before(b[i].t) {
				return -1 * dir
			}
			if a[i].t.After(b[i].t) {
				return 1 * dir
			}
		default:
			if c := strings.Compare(a[i].s, b[i].s); c != 0 {
				return c * dir
			}
		}
	}
	return 0
}

// Merge merges multiple GeoJSON FeatureCollection parts into a single FeatureCollection
func (a *Aggregator) Merge(parts [][]byte) ([]byte, error) {
	req := Request{
		Query:  Query{},
		Shards: make([]ShardPage, 0, len(parts)),
	}
	for i, p := range parts {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(p, &root); err != nil {
			return nil, fmt.Errorf("part %d: parse json: %w", i, err)
		}

		var typ string
		tRaw, ok := root["type"]
		if !ok {
			return nil, fmt.Errorf(`part %d: missing required member "type"`, i)
		}
		if err := json.Unmarshal(tRaw, &typ); err != nil {
			return nil, fmt.Errorf(`part %d: parse "type": %w`, i, err)
		}
		if typ != "FeatureCollection" {
			return nil, fmt.Errorf(`part %d: type is %q (want "FeatureCollection")`, i, typ)
		}

		featuresRaw, ok := root["features"]
		if !ok {
			return nil, fmt.Errorf(`part %d: missing required member "features"`, i)
		}
		var feats []json.RawMessage
		if err := json.Unmarshal(featuresRaw, &feats); err != nil {
			return nil, fmt.Errorf(`part %d: "features" must be an array: %w`, i, err)
		}

		for j, fr := range feats {
			var fobj map[string]json.RawMessage
			if err := json.Unmarshal(fr, &fobj); err != nil {
				return nil, fmt.Errorf("part %d feature %d: not a JSON object: %w", i, j, err)
			}
			var ftype string
			tr, ok := fobj["type"]
			if !ok {
				return nil, fmt.Errorf(`part %d feature %d: missing "type"`, i, j)
			}
			if err := json.Unmarshal(tr, &ftype); err != nil {
				return nil, fmt.Errorf(`part %d feature %d: parse "type": %w`, i, j, err)
			}
			if ftype != "Feature" {
				return nil, fmt.Errorf(`part %d feature %d: type is %q (want "Feature")`, i, j, ftype)
			}
			if idRaw, ok := fobj["id"]; ok && len(idRaw) > 0 {
				if _, err := canonicalIDKey(idRaw); err != nil {
					return nil, fmt.Errorf("part %d feature %d: invalid id: %w", i, j, err)
				}
			}
		}

		req.Shards = append(req.Shards, ShardPage{
			Meta:     ShardMeta{FromCache: false, ID: fmt.Sprintf("part-%d", i)},
			Features: feats,
		})
	}

	out, _, err := a.MergeRequest(req)
	return out, err
}

func New(dedup bool) *Aggregator {
	return &Aggregator{
		EnableDedup:   dedup,
		GeomPrecision: DefaultGeomPrecision,
	}
}

// returns a canonical string key for the given raw JSON id value
func canonicalIDKey(idRaw json.RawMessage) (string, error) {
	trim := strings.TrimSpace(string(idRaw))
	if trim == "" {
		return "", nil
	}
	dec := json.NewDecoder(strings.NewReader(trim))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", fmt.Errorf("parse id: %w", err)
	}
	switch t := v.(type) {
	case string:
		return "s:" + t, nil
	case json.Number:
		return "n:" + t.String(), nil
	default:
		return "", fmt.Errorf("id must be string or number (got %T)", v)
	}
}

func CanonicalIDKey(idRaw json.RawMessage) (string, error) {
	return canonicalIDKey(idRaw)
}
