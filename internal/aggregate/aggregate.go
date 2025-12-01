// Package aggregate defines feature aggregation interfaces and orchestration helpers.
package aggregate

type Interface interface {
	Merge(parts [][]byte) ([]byte, error)
}
