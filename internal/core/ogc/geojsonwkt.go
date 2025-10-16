package ogc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func GeoJSONToWKT(geojson string) (string, error) {
	var v struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	}
	if err := json.Unmarshal([]byte(geojson), &v); err != nil {
		return "", fmt.Errorf("parse geojson: %w", err)
	}
	switch strings.TrimSpace(v.Type) {
	case "Polygon":
		var rings [][][]float64
		if err := json.Unmarshal(v.Coordinates, &rings); err != nil {
			return "", fmt.Errorf("parse polygon coords: %w", err)
		}
		return polygonToWKT(rings)
	case "MultiPolygon":
		var polys [][][][]float64
		if err := json.Unmarshal(v.Coordinates, &polys); err != nil {
			return "", fmt.Errorf("parse multipolygon coords: %w", err)
		}
		return multiPolygonToWKT(polys)
	default:
		return "", fmt.Errorf("unsupported type %q", v.Type)
	}
}

func polygonToWKT(rings [][][]float64) (string, error) {
	if len(rings) == 0 {
		return "", errors.New("empty polygon")
	}
	outRings := make([]string, 0, len(rings))
	for _, ring := range rings {
		if len(ring) < 4 {
			return "", errors.New("polygon ring has <4 points")
		}
		var pts []string
		for _, xy := range ring {
			if len(xy) != 2 {
				return "", errors.New("coordinate must be [x,y]")
			}
			pts = append(pts, fmt.Sprintf("%.8f %.8f", xy[0], xy[1]))
		}
		outRings = append(outRings, fmt.Sprintf("(%s)", strings.Join(pts, ", ")))
	}
	return fmt.Sprintf("POLYGON(%s)", strings.Join(outRings, ", ")), nil
}

func multiPolygonToWKT(polys [][][][]float64) (string, error) {
	if len(polys) == 0 {
		return "", errors.New("empty multipolygon")
	}
	parts := make([]string, 0, len(polys))
	for _, poly := range polys {
		wkt, err := polygonToWKT(poly)
		if err != nil {
			return "", err
		}
		// strip "POLYGON" wrapper to embed into MULTIPOLYGON
		body := strings.TrimPrefix(wkt, "POLYGON")
		parts = append(parts, body)
	}
	return fmt.Sprintf("MULTIPOLYGON(%s)", strings.Join(parts, ", ")), nil
}
