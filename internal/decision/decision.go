// Package decision defines interfaces and helpers for caching decisions.
package decision

type LoadStats struct{}

type Interface interface {
	ShouldCache(cells []string) bool
	SelectResolution(load LoadStats) int
}
