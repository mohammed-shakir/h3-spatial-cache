package scenarios_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/executor"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/baseline"
)

func TestRegistry_FallbackToBaseline(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.FromEnv()

	exec, err := executor.New(logger, nil, "http://example.com/ows")
	if err != nil {
		t.Fatalf("executor.New failed: %v", err)
	}

	h, err := scenarios.New("totally-unknown", cfg, logger, exec)
	if err != nil || h == nil {
		t.Fatalf("expected fallback to baseline, got err=%v h=%v", err, h)
	}
}
