package kafka

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

type versionDedupe struct {
	mu  sync.Mutex
	lru *lru.Cache[string, uint64]
}

func newVersionDedupe(size int) *versionDedupe {
	if size <= 0 {
		size = 4096
	}
	c, _ := lru.New[string, uint64](size)
	return &versionDedupe{lru: c}
}

// returns true if v is greater than last seen
func (d *versionDedupe) shouldApply(key string, v uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if last, ok := d.lru.Get(key); ok {
		if v <= last {
			return false
		}
	}
	d.lru.Add(key, v)
	return true
}
