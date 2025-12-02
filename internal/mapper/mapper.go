// Package mapper converts between geometric coordinates and H3 cells.
package mapper

import (
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

type Interface interface {
	CellsForBBox(bb model.BBox, res int) (model.Cells, error)
	CellsForPolygon(poly model.Polygon, res int) (model.Cells, error)
}
