package cache

import (
	"testing"
	"time"
)

func TestTTLFor_PrefixFallbackAndExact(t *testing.T) {
	e := &Engine{
		ttlDefault: 30 * time.Second,
		ttlMap: map[string]time.Duration{
			"places":     time.Minute,
			"demo:roads": 10 * time.Second,
		},
	}

	if got := e.ttlFor(""); got != 30*time.Second {
		t.Fatalf("default ttl=%v", got)
	}
	if got := e.ttlFor("demo:roads"); got != 10*time.Second {
		t.Fatalf("exact ttl=%v", got)
	}
	if got := e.ttlFor("ns:places"); got != time.Minute {
		t.Fatalf("prefix ttl=%v", got)
	}
	if got := e.ttlFor("demo:unknown"); got != 30*time.Second {
		t.Fatalf("missing override ttl=%v", got)
	}
}
