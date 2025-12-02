// Package expdecay implements an exponential decay model for hotness scores.
package expdecay

import (
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
)

const numShards = 64

type Tracker struct {
	HalfLife time.Duration

	now func() time.Time

	shards [numShards]shard
}

type shard struct {
	mu sync.RWMutex
	m  map[string]*counter
}

type counter struct {
	score float64
	last  time.Time
}

var _ hotness.Interface = (*Tracker)(nil)

func New(halfLife time.Duration) *Tracker {
	if halfLife <= 0 {
		halfLife = time.Minute
	}
	t := &Tracker{HalfLife: halfLife, now: time.Now}
	for i := range t.shards {
		t.shards[i].m = make(map[string]*counter)
	}
	return t
}

func (t *Tracker) Inc(cell string) {
	if cell == "" {
		return
	}
	s := t.pick(cell)
	n := t.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	c := s.m[cell]
	if c == nil {
		s.m[cell] = &counter{score: 1, last: n}
		return
	}
	dt := n.Sub(c.last).Seconds()
	// apply exponential decay to the existing score before incrementing
	c.score = decay(c.score, dt, t.HalfLife.Seconds()) + 1.0
	c.last = n
}

func (t *Tracker) Score(cell string) float64 {
	if cell == "" {
		return 0
	}
	s := t.pick(cell)
	n := t.now()

	s.mu.RLock()
	c := s.m[cell]
	if c == nil {
		s.mu.RUnlock()
		return 0
	}
	score, last := c.score, c.last
	s.mu.RUnlock()

	// apply exponential decay to the existing score
	dt := n.Sub(last).Seconds()
	return decay(score, dt, t.HalfLife.Seconds())
}

func (t *Tracker) Reset(cells ...string) {
	for _, cell := range cells {
		if cell == "" {
			continue
		}
		s := t.pick(cell)
		s.mu.Lock()
		delete(s.m, cell)
		s.mu.Unlock()
	}
}

func decay(score, dt, halfLife float64) float64 {
	if score == 0 || dt <= 0 || halfLife <= 0 {
		return score
	}
	lambda := math.Ln2 / halfLife
	// apply exponential decay (e^(-λt))
	return score * math.Exp(-lambda*dt)
}

func (t *Tracker) pick(cell string) *shard {
	h := xxhash.Sum64String(cell)
	idx := h & (uint64(len(t.shards)) - 1)
	return &t.shards[idx]
}

func (t *Tracker) Size() int {
	total := 0
	for i := range t.shards {
		t.shards[i].mu.RLock()
		total += len(t.shards[i].m)
		t.shards[i].mu.RUnlock()
	}
	return total
}
