package h3mapper

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	h3 "github.com/uber/h3-go/v4"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

type Mapper struct{}

func New() *Mapper { return &Mapper{} }

func (m *Mapper) CellsForBBox(bb model.BBox, res int) (model.Cells, error) {
	if err := validateRes(res); err != nil {
		return nil, err
	}
	// convert bbox to rectangular GeoLoop for H3 polyfill
	outer := h3.GeoLoop{
		{Lat: bb.Y1, Lng: bb.X1},
		{Lat: bb.Y1, Lng: bb.X2},
		{Lat: bb.Y2, Lng: bb.X2},
		{Lat: bb.Y2, Lng: bb.X1},
	}
	return polyfillOne(outer, nil, res)
}

func (m *Mapper) CellsForPolygon(poly model.Polygon, res int) (model.Cells, error) {
	if err := validateRes(res); err != nil {
		return nil, err
	}

	var hdr struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(poly.GeoJSON), &hdr); err != nil {
		return nil, fmt.Errorf("parse geojson: %w", err)
	}

	switch hdr.Type {
	case "Polygon":
		var tmp struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		}
		if err := json.Unmarshal([]byte(poly.GeoJSON), &tmp); err != nil {
			return nil, fmt.Errorf("parse polygon coords: %w", err)
		}
		if len(tmp.Coordinates) == 0 {
			return nil, errors.New("empty polygon")
		}
		outer := toLoop(tmp.Coordinates[0])
		if len(outer) < 4 {
			return nil, errors.New("outer ring has < 4 vertices")
		}
		// handle inner rings (holes) if any
		var holes []h3.GeoLoop
		for i := 1; i < len(tmp.Coordinates); i++ {
			h := toLoop(tmp.Coordinates[i])
			if len(h) < 4 {
				return nil, fmt.Errorf("hole %d has < 4 vertices", i-1)
			}
			holes = append(holes, h)
		}
		return polyfillOne(outer, holes, res)

	case "MultiPolygon":
		var tmp struct {
			Type        string          `json:"type"`
			Coordinates [][][][]float64 `json:"coordinates"`
		}
		if err := json.Unmarshal([]byte(poly.GeoJSON), &tmp); err != nil {
			return nil, fmt.Errorf("parse multipolygon coords: %w", err)
		}
		if len(tmp.Coordinates) == 0 {
			return nil, errors.New("empty multipolygon")
		}
		seen := make(map[string]struct{})
		var out []string
		for pi, polyRings := range tmp.Coordinates {
			if len(polyRings) == 0 {
				return nil, fmt.Errorf("polygon %d is empty", pi)
			}
			outer := toLoop(polyRings[0])
			if len(outer) < 4 {
				return nil, fmt.Errorf("polygon %d outer ring has < 4 vertices", pi)
			}
			var holes []h3.GeoLoop
			for i := 1; i < len(polyRings); i++ {
				h := toLoop(polyRings[i])
				if len(h) < 4 {
					return nil, fmt.Errorf("polygon %d hole %d has < 4 vertices", pi, i-1)
				}
				holes = append(holes, h)
			}
			// deduplicate overlapping cells across multipolygon parts
			cells, err := polyfillOne(outer, holes, res)
			if err != nil {
				return nil, err
			}
			for _, c := range cells {
				if _, ok := seen[c]; !ok {
					seen[c] = struct{}{}
					out = append(out, c)
				}
			}
		}
		sort.Strings(out)
		return out, nil

	default:
		return nil, fmt.Errorf("unsupported GeoJSON type: %s", hdr.Type)
	}
}

func validateRes(res int) error {
	if res < 0 || res > 15 {
		return fmt.Errorf("invalid H3 resolution %d (must be 0..15)", res)
	}
	return nil
}

func toLoop(coords [][]float64) h3.GeoLoop {
	loop := make(h3.GeoLoop, 0, len(coords))
	for _, xy := range coords {
		if len(xy) != 2 {
			continue
		}
		loop = append(loop, h3.LatLng{Lat: xy[1], Lng: xy[0]})
	}
	// remove duplicated last point if present
	if len(loop) >= 2 {
		last := loop[len(loop)-1]
		first := loop[0]
		if last.Lat == first.Lat && last.Lng == first.Lng {
			loop = loop[:len(loop)-1]
		}
	}
	return loop
}

func polyfillOne(outer h3.GeoLoop, holes []h3.GeoLoop, res int) (model.Cells, error) {
	if len(outer) < 4 {
		return nil, errors.New("outer ring has < 4 vertices")
	}
	poly := h3.GeoPolygon{
		GeoLoop: outer,
		Holes:   holes,
	}

	indexes, err := h3.PolygonToCells(poly, res)
	if err != nil {
		return nil, fmt.Errorf("h3 polyfill: %w", err)
	}

	out := make([]string, 0, len(indexes))
	seen := make(map[string]struct{}, len(indexes))
	for _, idx := range indexes {
		s := idx.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	// return sorted cell ids
	sort.Strings(out)
	return out, nil
}
