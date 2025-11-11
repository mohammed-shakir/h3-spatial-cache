package main

import "testing"

func TestMatrixExpansionCount(t *testing.T) {
	c := cfg{
		Scenarios:     []string{"baseline", "cache"},
		H3ResList:     []int{7, 8, 9},
		TTLs:          []string{"30s", "60s"},
		Hots:          []string{"5", "10"},
		Invalidations: []string{"ttl", "kafka"},
		OutRoot:       t.TempDir(),
		DryRun:        true,
	}
	if err := dryRun(c); err != nil {
		t.Fatalf("dry-run: %v", err)
	}
}
