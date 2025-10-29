package invalidation

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Event struct {
	Version   int             `json:"version"`
	Op        string          `json:"op"`
	Layer     string          `json:"layer"`
	TS        time.Time       `json:"ts"`
	FeatureID any             `json:"feature_id,omitempty"`
	Source    string          `json:"source,omitempty"`
	BBox      *BBox           `json:"bbox,omitempty"`
	Geometry  json.RawMessage `json:"geometry,omitempty"`
}

type BBox struct {
	X1   float64 `json:"x1"`
	Y1   float64 `json:"y1"`
	X2   float64 `json:"x2"`
	Y2   float64 `json:"y2"`
	SRID string  `json:"srid"`
}

func (e Event) Validate() error {
	if e.Version != 1 {
		return fmt.Errorf("version must be 1")
	}
	switch e.Op {
	case "insert", "update", "delete":
	default:
		return fmt.Errorf("op must be insert|update|delete")
	}
	if strings.TrimSpace(e.Layer) == "" {
		return fmt.Errorf("layer is required")
	}
	if e.TS.IsZero() {
		return fmt.Errorf("ts is required")
	}
	hasBBox := e.BBox != nil
	hasGeom := len(e.Geometry) > 0
	if hasBBox == hasGeom {
		return fmt.Errorf("exactly one of bbox or geometry is required")
	}
	if hasBBox {
		bb := *e.BBox
		if bb.SRID != "EPSG:4326" {
			return fmt.Errorf("bbox.srid must be EPSG:4326")
		}
		if !(bb.X1 >= -180 && bb.X1 <= 180 && bb.X2 >= -180 && bb.X2 <= 180) {
			return fmt.Errorf("bbox longitude out of range")
		}
		if !(bb.Y1 >= -90 && bb.Y1 <= 90 && bb.Y2 >= -90 && bb.Y2 <= 90) {
			return fmt.Errorf("bbox latitude out of range")
		}
		if !(bb.X2 > bb.X1 && bb.Y2 > bb.Y1) {
			return fmt.Errorf("bbox must satisfy x2>x1 and y2>y1")
		}
		return nil
	}
	// quick GeoJSON Polygon/MultiPolygon header check
	var hdr struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(e.Geometry, &hdr); err != nil {
		return fmt.Errorf("geometry parse: %w", err)
	}
	if hdr.Type != "Polygon" && hdr.Type != "MultiPolygon" {
		return fmt.Errorf("geometry.type must be Polygon or MultiPolygon")
	}
	return nil
}
