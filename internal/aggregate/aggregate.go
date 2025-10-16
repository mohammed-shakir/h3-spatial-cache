package aggregate

type Interface interface {
	Merge(parts [][]byte) ([]byte, error)
}
