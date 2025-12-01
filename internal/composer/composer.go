package composer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
)

type SortKey struct {
	Property string
	Desc     bool
}

type QueryParams struct {
	FiltersRaw []byte
	Sort       []SortKey
	Limit      int
	Offset     int
}

type CacheStatus int

const (
	CacheMiss CacheStatus = iota
	CacheHit
)

type ShardPage struct {
	Body        []byte
	CacheStatus CacheStatus
}

type HitClass string

const (
	HitClassFull    HitClass = "full_hit"
	HitClassPartial HitClass = "partial_hit"
	HitClassMiss    HitClass = "miss"
)

func classifyHit(pages []ShardPage) HitClass {
	if len(pages) == 0 {
		return HitClassMiss
	}
	allHit := true
	anyHit := false
	for _, p := range pages {
		if p.CacheStatus == CacheHit {
			anyHit = true
		} else {
			allHit = false
		}
	}
	switch {
	case allHit:
		return HitClassFull
	case anyHit:
		return HitClassPartial
	default:
		return HitClassMiss
	}
}

type Format int

const (
	FormatGeoJSON Format = iota
	FormatGML32
)

type NegotiationInput struct {
	AcceptHeader  string
	OutputFormat  string
	DefaultFormat Format
}

type Negotiation struct {
	Format      Format
	ContentType string
}

// NegotiateFormat determines the output format and content type
func NegotiateFormat(in NegotiationInput) Negotiation {
	of := strings.ToLower(strings.TrimSpace(in.OutputFormat))
	switch {
	case strings.HasPrefix(of, "application/geo+json"),
		of == "geojson",
		of == "json",
		strings.HasPrefix(of, "application/json"):
		return Negotiation{Format: FormatGeoJSON, ContentType: "application/geo+json"}

	case strings.Contains(of, "gml"):
		return Negotiation{Format: FormatGML32, ContentType: "application/gml+xml; version=3.2"}
	}

	ah := strings.ToLower(in.AcceptHeader)
	bestQ := -1.0
	best := Negotiation{}
	for part := range strings.SplitSeq(ah, ",") {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}
		mt := token
		params := ""
		if i := strings.Index(token, ";"); i >= 0 {
			mt = strings.TrimSpace(token[:i])
			params = token[i+1:]
		}
		q := 1.0
		for p := range strings.SplitSeq(params, ";") {
			p = strings.TrimSpace(p)
			if after, ok := strings.CutPrefix(p, "q="); ok {
				if v, err := strconv.ParseFloat(after, 64); err == nil {
					q = v
				}
			}
		}
		var cand *Negotiation
		switch {
		case mt == "*/*":
			if in.DefaultFormat == FormatGML32 {
				tmp := Negotiation{Format: FormatGML32, ContentType: "application/gml+xml; version=3.2"}
				cand = &tmp
			} else {
				tmp := Negotiation{Format: FormatGeoJSON, ContentType: "application/geo+json"}
				cand = &tmp
			}
		case mt == "application/geo+json" || mt == "application/json" || strings.Contains(mt, "geo+json"):
			tmp := Negotiation{Format: FormatGeoJSON, ContentType: "application/geo+json"}
			cand = &tmp
		case mt == "application/gml+xml" || strings.Contains(mt, "gml"):
			tmp := Negotiation{Format: FormatGML32, ContentType: "application/gml+xml; version=3.2"}
			cand = &tmp
		}
		if cand != nil && q > bestQ {
			bestQ = q
			best = *cand
		}
	}
	if bestQ >= 0 {
		return best
	}

	if in.DefaultFormat == FormatGML32 {
		return Negotiation{Format: FormatGML32, ContentType: "application/gml+xml; version=3.2"}
	}
	return Negotiation{Format: FormatGeoJSON, ContentType: "application/geo+json"}
}

type AggregatorV2 interface {
	MergeWithQuery(ctx context.Context, q QueryParams, pages []ShardPage) ([]byte, error)
}

type AggregatorV1 = aggregate.Interface

type Engine struct {
	V2 AggregatorV2
	V1 AggregatorV1
}

// merges the given parts using the configured aggregator
func (e Engine) merge(ctx context.Context, q QueryParams, pages []ShardPage) ([]byte, error) {
	if e.V2 != nil {
		b, err := e.V2.MergeWithQuery(ctx, q, pages)
		if err != nil {
			return nil, fmt.Errorf("aggregator v2 merge: %w", err)
		}
		return b, nil
	}
	if e.V1 != nil {
		parts := make([][]byte, 0, len(pages))
		for _, p := range pages {
			parts = append(parts, p.Body)
		}
		b, err := e.V1.Merge(parts)
		if err != nil {
			return nil, fmt.Errorf("aggregator v1 merge: %w", err)
		}
		return b, nil
	}
	return nil, errors.New("no aggregator provided")
}

type Request struct {
	Query        QueryParams
	Pages        []ShardPage
	AcceptHeader string
	OutputFormat string
}

type Result struct {
	StatusCode  int
	Body        []byte
	ContentType string
	HitClass    HitClass
}

// Compose merges the given shard pages into a single response
func Compose(ctx context.Context, eng Engine, req Request) (Result, error) {
	t0 := time.Now()
	if len(req.Pages) == 0 {
		neg := NegotiateFormat(NegotiationInput{
			AcceptHeader:  req.AcceptHeader,
			OutputFormat:  req.OutputFormat,
			DefaultFormat: FormatGeoJSON,
		})
		empty := []byte(`{"type":"FeatureCollection","features":[]}`)
		observability.ObserveSpatialResponse(string(HitClassMiss), formatString(neg.Format), time.Since(t0).Seconds())
		return Result{StatusCode: http.StatusOK, Body: empty, ContentType: neg.ContentType, HitClass: HitClassMiss}, nil
	}

	neg := NegotiateFormat(NegotiationInput{
		AcceptHeader:  req.AcceptHeader,
		OutputFormat:  req.OutputFormat,
		DefaultFormat: FormatGeoJSON,
	})

	merged, err := eng.merge(ctx, req.Query, req.Pages)
	if err != nil {
		return Result{}, fmt.Errorf("aggregate merge: %w", err)
	}

	switch neg.Format {
	case FormatGeoJSON:
		res := Result{
			StatusCode:  http.StatusOK,
			Body:        merged,
			ContentType: neg.ContentType,
			HitClass:    classifyHit(req.Pages),
		}
		observability.ObserveSpatialResponse(string(res.HitClass), formatString(neg.Format), time.Since(t0).Seconds())
		return res, nil

	case FormatGML32:
		return Result{}, fmt.Errorf("GML 3.2 output not enabled")
	default:
		return Result{}, fmt.Errorf("unsupported format")
	}
}

func BuildFeatureCollectionShard(features [][]byte) ([]byte, error) {
	type fc struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}

	out := fc{
		Type:     "FeatureCollection",
		Features: make([]json.RawMessage, 0, len(features)),
	}

	for i, f := range features {
		if !json.Valid(f) {
			return nil, fmt.Errorf("feature %d: invalid JSON", i)
		}
		out.Features = append(out.Features, json.RawMessage(f))
	}

	buf, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal FeatureCollection: %w", err)
	}
	return buf, nil
}

func formatString(f Format) string {
	switch f {
	case FormatGML32:
		return "gml"
	default:
		return "geojson"
	}
}
