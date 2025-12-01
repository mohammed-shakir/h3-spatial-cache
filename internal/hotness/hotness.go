// Package hotness tracks request hotness and cache temperature metrics.
package hotness

type Interface interface {
	Inc(cell string)
	Score(cell string) float64
	Reset(cells ...string)
}
