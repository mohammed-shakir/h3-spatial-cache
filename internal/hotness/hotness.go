package hotness

type Interface interface {
	Inc(cell string)
	Score(cell string) float64
	Reset(cells ...string)
}
