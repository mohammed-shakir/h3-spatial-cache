package router

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/model"
)

// validates user input for /query and returns a normalized request
func ParseQueryRequest(r *http.Request) (model.QueryRequest, string, error) {
	var warn string

	layer := strings.TrimSpace(r.URL.Query().Get("layer"))
	if layer == "" {
		return model.QueryRequest{}, "", errors.New("missing required parameter: layer")
	}

	rawBBox := strings.TrimSpace(r.URL.Query().Get("bbox"))
	filters := strings.TrimSpace(r.URL.Query().Get("filters"))
	var bbox *model.BBox

	// drop bbox if filters are supplied
	if rawBBox != "" && filters != "" {
		warn = "both bbox and filters supplied; dropping bbox (KVP shorthand filters are mutually exclusive)"
		rawBBox = ""
	}

	if rawBBox != "" {
		bb, err := parseBBOX(rawBBox)
		if err != nil {
			return model.QueryRequest{}, "", fmt.Errorf("invalid bbox: %w", err)
		}
		bbox = &bb
	}

	if filters != "" && !isSafeCQL(filters) {
		return model.QueryRequest{}, "", errors.New("invalid or disallowed cql_filter")
	}

	return model.QueryRequest{
		Layer:   layer,
		BBox:    bbox,
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

// encodes a wfs query to values (useful for tests). Example:
func BuildQuery(q model.QueryRequest) url.Values {
	params := url.Values{}
	params.Set("service", "WFS")
	params.Set("version", "2.0.0")
	params.Set("request", "GetFeature")
	params.Set("typeNames", q.Layer)
	if q.BBox != nil {
		params.Set("bbox", fmt.Sprintf("%.6f,%.6f,%.6f,%.6f,%s",
			q.BBox.X1, q.BBox.Y1, q.BBox.X2, q.BBox.Y2, q.BBox.SRID))
	}
	if q.Filters != "" {
		params.Set("cql_filter", q.Filters)
	}
	params.Set("outputFormat", "application/json")
	return params
}
