package model

import "fmt"

type BBox struct {
	X1, Y1 float64
	X2, Y2 float64
	SRID   string
}

func (b BBox) String() string {
	return fmt.Sprintf("%.6f,%.6f,%.6f,%.6f,%s", b.X1, b.Y1, b.X2, b.Y2, b.SRID)
}

// polygon is a GeoJSON string
type Polygon struct {
	GeoJSON string
}

// list of h3 cell ids
type Cells []string

type QueryRequest struct {
	Layer   string
	BBox    *BBox
	Polygon *Polygon
	Filters string
	H3Res   int
	Cells   Cells
}
