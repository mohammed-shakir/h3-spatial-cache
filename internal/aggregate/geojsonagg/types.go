package geojsonagg

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Direction int

const (
	Asc Direction = iota
	Desc
)

// UnmarshalJSON accepts both string and int representations
func (d *Direction) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		switch strings.ToLower(s) {
		case "asc":
			*d = Asc
		case "desc":
			*d = Desc
		default:
			return fmt.Errorf("invalid direction %q (want asc|desc)", s)
		}
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err == nil {
		*d = Direction(n)
		return nil
	}
	return fmt.Errorf("direction must be string or int")
}

type NullsPolicy int

const (
	NullsLast NullsPolicy = iota
	NullsFirst
)

// UnmarshalJSON enables nulls ordering policy in sort keys
func (n *NullsPolicy) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		switch strings.ToLower(s) {
		case "first":
			*n = NullsFirst
		case "last", "":
			*n = NullsLast
		default:
			return fmt.Errorf("invalid nulls policy %q (want first|last)", s)
		}
		return nil
	}
	var i int
	if err := json.Unmarshal(b, &i); err == nil {
		*n = NullsPolicy(i)
		return nil
	}
	return fmt.Errorf("nulls policy must be string or int")
}

type SortKey struct {
	Property  string      `json:"property"`
	Direction Direction   `json:"direction"`
	Nulls     NullsPolicy `json:"nulls,omitempty"`
	TypeHint  string      `json:"typeHint,omitempty"`
}

type Query struct {
	Filters    map[string]any `json:"filters,omitempty"`
	Sort       []SortKey      `json:"sort,omitempty"`
	Limit      int            `json:"limit,omitempty"`
	StartIndex int            `json:"startIndex,omitempty"`
}

type HitClass string

const (
	FullHit    HitClass = "full_hit"
	PartialHit HitClass = "partial_hit"
	Miss       HitClass = "miss"
)

type ShardMeta struct {
	FromCache bool   `json:"fromCache"`
	ID        string `json:"id,omitempty"`
}

type ShardPage struct {
	Meta       ShardMeta         `json:"meta"`
	Features   []json.RawMessage `json:"features"`
	GeomHashes []string          `json:"geomHashes,omitempty"`
}

type Request struct {
	Query  Query       `json:"query"`
	Shards []ShardPage `json:"shards"`
}

type Diagnostics struct {
	HitClass  HitClass `json:"hit_class"`
	TotalIn   int      `json:"total_in"`
	TotalOut  int      `json:"total_out"`
	DedupByID int      `json:"dedup_by_id"`
	DedupByGH int      `json:"dedup_by_geom"`
}

type valueKind int

const (
	kindNull valueKind = iota
	kindString
	kindNumber
	kindTime
)

type cmpValue struct {
	kind valueKind
	s    string
	n    float64
	t    time.Time
	null bool
}
