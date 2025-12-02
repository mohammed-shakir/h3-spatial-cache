// Package model defines core domain types shared across the service.
package model

import "fmt"

type BBox struct {
	X1, Y1 float64
	X2, Y2 float64
	SRID   string
}

// String representation matching wfs/wms bbox format
func (b BBox) String() string {
	return fmt.Sprintf("%.6f,%.6f,%.6f,%.6f,%s", b.X1, b.Y1, b.X2, b.Y2, b.SRID)
}

type Polygon struct {
	GeoJSON string
}

type Cells []string

type QueryRequest struct {
	Layer   string
	BBox    *BBox
	Polygon *Polygon
	Filters string
	H3Res   int
	Cells   Cells
}

type Filters string
