package kafka

import "time"

type WireEvent struct {
	Key         string    `json:"key,omitempty"`
	Layer       string    `json:"layer,omitempty"`
	H3Cells     []string  `json:"h3_cells,omitempty"`
	Resolutions []int     `json:"res,omitempty"`
	Version     uint64    `json:"version"`
	TS          time.Time `json:"ts"`
	Op          string    `json:"op,omitempty"`
}
