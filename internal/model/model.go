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

type QueryRequest struct {
	Layer   string
	BBox    *BBox
	Filters string
}
