package health

import (
	"encoding/json"
	"net/http"
)

type ReadinessReporter interface {
	Readiness() (ready bool, partitions []int32)
}

func Readiness(rr ReadinessReporter) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		type resp struct {
			Status     string  `json:"status"`
			Partitions []int32 `json:"partitions,omitempty"`
		}
		ready, parts := rr.Readiness()
		out := resp{Status: "not_ready"}
		if ready {
			out.Status = "ready"
			out.Partitions = parts
		}
		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(out)
	}
}
