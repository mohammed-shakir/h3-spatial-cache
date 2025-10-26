package geojsonagg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate"
)

type Aggregator struct {
	DeduplicateByID bool
}

var _ aggregate.Interface = (*Aggregator)(nil)

func New(dedup bool) *Aggregator {
	return &Aggregator{DeduplicateByID: dedup}
}

func (a *Aggregator) Merge(parts [][]byte) ([]byte, error) {
	if len(parts) == 0 {
		return []byte(`{"type":"FeatureCollection","features":[]}`), nil
	}

	out := struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}{
		Type:     "FeatureCollection",
		Features: make([]json.RawMessage, 0, 128),
	}

	seen := map[string]struct{}{} // used for deduplication by id if enabled

	for i, p := range parts {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(p, &root); err != nil {
			return nil, fmt.Errorf("part %d: parse json: %w", i, err)
		}

		var typ string
		if tRaw, ok := root["type"]; !ok {
			return nil, fmt.Errorf(`part %d: missing required member "type"`, i)
		} else if err := json.Unmarshal(tRaw, &typ); err != nil {
			return nil, fmt.Errorf(`part %d: parse "type": %w`, i, err)
		} else if typ != "FeatureCollection" {
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
			if tr, ok := fobj["type"]; !ok {
				return nil, fmt.Errorf(`part %d feature %d: missing "type"`, i, j)
			} else if err := json.Unmarshal(tr, &ftype); err != nil {
				return nil, fmt.Errorf(`part %d feature %d: parse "type": %w`, i, j, err)
			} else if ftype != "Feature" {
				return nil, fmt.Errorf(`part %d feature %d: type is %q (want "Feature")`, i, j, ftype)
			}

			// deduplicate by id if enabled
			if idRaw, ok := fobj["id"]; ok && len(idRaw) > 0 {
				key, idErr := canonicalIDKey(idRaw)
				if idErr != nil {
					return nil, fmt.Errorf("part %d feature %d: invalid id: %w", i, j, idErr)
				}
				if a.DeduplicateByID && key != "" {
					if _, dup := seen[key]; dup {
						continue
					}
					seen[key] = struct{}{}
				}
			}

			out.Features = append(out.Features, fr)
		}
	}

	buf, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal merged FeatureCollection: %w", err)
	}
	return buf, nil
}

// parse id to allow both string and number types
func canonicalIDKey(idRaw json.RawMessage) (string, error) {
	trim := strings.TrimSpace(string(idRaw))
	if trim == "" {
		return "", nil
	}

	dec := json.NewDecoder(bytes.NewReader(idRaw))
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
