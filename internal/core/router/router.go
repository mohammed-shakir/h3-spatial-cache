package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
)

// receives validated query requests and serves them
type QueryHandler interface {
	HandleQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest)
}

// validates input query params and calls the handler
func HandleQuery(logger *slog.Logger, _ config.Config, h QueryHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}

		q, warn, err := ParseQueryRequest(r)
		if warn != "" {
			logger.Warn(warn)
		}
		if err != nil {
			http.Error(sw, err.Error(), http.StatusBadRequest)
			observability.ObserveHTTP(r.Method, "/query", http.StatusBadRequest, time.Since(start).Seconds())
			return
		}

		h.HandleQuery(r.Context(), sw, r, q)
		observability.ObserveHTTP(r.Method, "/query", sw.code, time.Since(start).Seconds())
	}
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func ParseQueryRequest(r *http.Request) (model.QueryRequest, string, error) {
	var warn string

	layer := strings.TrimSpace(r.URL.Query().Get("layer"))
	if layer == "" {
		return model.QueryRequest{}, "", errors.New("missing required parameter: layer")
	}

	rawBBox := strings.TrimSpace(r.URL.Query().Get("bbox"))
	rawPoly := strings.TrimSpace(r.URL.Query().Get("polygon"))
	filters := strings.TrimSpace(r.URL.Query().Get("filters"))

	// drop bbox if polygon is given (polygon wins)
	if rawBBox != "" && rawPoly != "" {
		warn = "both bbox and polygon supplied; preferring polygon"
		rawBBox = ""
	}

	var bbox *model.BBox
	if rawBBox != "" {
		bb, err := parseBBOX(rawBBox)
		if err != nil {
			return model.QueryRequest{}, warn, fmt.Errorf("invalid bbox: %w", err)
		}
		bbox = &bb
	}

	var poly *model.Polygon
	if rawPoly != "" {
		p, err := parsePolygon(rawPoly)
		if err != nil {
			return model.QueryRequest{}, warn, fmt.Errorf("invalid polygon: %w", err)
		}
		poly = &p
	}

	if filters != "" && !isSafeCQL(filters) {
		return model.QueryRequest{}, warn, errors.New("invalid or disallowed cql_filter")
	}

	return model.QueryRequest{
		Layer:   layer,
		BBox:    bbox,
		Polygon: poly,
		Filters: filters,
	}, warn, nil
}

func parseBBOX(bboxParam string) (model.BBox, error) {
	parts := strings.Split(bboxParam, ",")
	if len(parts) != 5 {
		return model.BBox{}, errors.New("expected 5 comma-separated values: x1,y1,x2,y2,EPSG:4326")
	}
	xMin, err := parseFloat(parts[0])
	if err != nil {
		return model.BBox{}, fmt.Errorf("x1: %w", err)
	}
	yMin, err := parseFloat(parts[1])
	if err != nil {
		return model.BBox{}, fmt.Errorf("y1: %w", err)
	}
	xMax, err := parseFloat(parts[2])
	if err != nil {
		return model.BBox{}, fmt.Errorf("x2: %w", err)
	}
	yMax, err := parseFloat(parts[3])
	if err != nil {
		return model.BBox{}, fmt.Errorf("y2: %w", err)
	}

	srid := strings.ToUpper(strings.TrimSpace(parts[4]))
	if srid != "EPSG:4326" {
		return model.BBox{}, fmt.Errorf("only EPSG:4326 is supported at this stage (got %q)", srid)
	}

	if !(xMin >= -180 && xMin <= 180 && xMax >= -180 && xMax <= 180) {
		return model.BBox{}, errors.New("longitude must be in [-180,180]")
	}
	if !(yMin >= -90 && yMin <= 90 && yMax >= -90 && yMax <= 90) {
		return model.BBox{}, errors.New("latitude must be in [-90,90]")
	}
	if xMax <= xMin || yMax <= yMin {
		return model.BBox{}, errors.New("coordinates must satisfy x2>x1 and y2>y1")
	}
	return model.BBox{X1: xMin, Y1: yMin, X2: xMax, Y2: yMax, SRID: srid}, nil
}

func parseFloat(v string) (float64, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0, fmt.Errorf("parse float: %w", err)
	}
	return f, nil
}

var safeCQLPattern = regexp.MustCompile(`^[\w\s\=\>\<\!\(\)\.\,\'\"\-]+$`)

func isSafeCQL(s string) bool {
	if len(s) > 500 {
		return false
	}
	return safeCQLPattern.MatchString(s)
}

func parsePolygon(raw string) (model.Polygon, error) {
	var tmp struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(raw), &tmp); err != nil {
		return model.Polygon{}, fmt.Errorf("parse json: %w", err)
	}
	t := strings.TrimSpace(tmp.Type)
	switch t {
	case "Polygon", "MultiPolygon":
		return model.Polygon{GeoJSON: raw}, nil
	default:
		return model.Polygon{}, fmt.Errorf(`unsupported GeoJSON "type": %q (must be Polygon or MultiPolygon)`, t)
	}
}
