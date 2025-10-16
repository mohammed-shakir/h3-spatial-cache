package decision

type LoadStats struct{}

type Interface interface {
	ShouldCache(cells []string) bool
	SelectResolution(load LoadStats) int
}
