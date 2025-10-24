package keys

import (
	"regexp"
	"strings"
	"testing"
	"unicode"
)

func TestDeterminism_SameInputsSameKey(t *testing.T) {
	k1 := Key("demo:places", 8, "892a100d2b3ffff", "name='Stockholm' AND type IN('city','town')")
	k2 := Key("demo:places", 8, "892a100d2b3ffff", "name='Stockholm' AND type IN('city','town')")
	if k1 != k2 {
		t.Fatalf("determinism failed:\n k1=%s\n k2=%s", k1, k2)
	}
}

func TestNormalization_SpacingVariantsProduceSameKey(t *testing.T) {
	fA := "  name  =    'Stockholm'   AND  type IN('city','town')  "
	fB := "name='Stockholm' AND type IN ( 'city' , 'town' )"
	k1 := Key(" demo:places ", 8, "892a100d2b3ffff", fA)
	k2 := Key("demo:places", 8, "892a100d2b3ffff", fB)
	if k1 != k2 {
		t.Fatalf("normalized keys differ:\n k1=%s\n k2=%s", k1, k2)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9:_=\-]+$`).MatchString(k1) {
		t.Fatalf("key contains disallowed characters: %s", k1)
	}
}

func TestDifference_DifferentFiltersAreDifferent(t *testing.T) {
	f1 := "a=1 AND b=2"
	f2 := "b=2 AND a=1"
	k1 := Key("demo:places", 8, "892a100d2b3ffff", f1)
	k2 := Key("demo:places", 8, "892a100d2b3ffff", f2)
	if k1 == k2 {
		t.Fatalf("different filters must produce different keys")
	}
}

func TestUnicodeSafety_NoPanicAndHashSuffixPresent(t *testing.T) {
	f := "name = 'Göteborg' AND note = '雪'"
	k := Key("demo:places", 8, "892a100d2b3ffff", f)

	for _, r := range k {
		if r > unicode.MaxASCII {
			t.Fatalf("non-ASCII rune leaked into key: %q in %s", r, k)
		}
	}

	m := regexp.MustCompile(`:f=([0-9a-f]{16})$`).FindStringSubmatch(k)
	if len(m) != 2 {
		t.Fatalf("missing or invalid :f=<hex64> suffix in key: %s", k)
	}

	if !strings.Contains(k, ":filters=") {
		t.Fatalf("missing filters= segment in key: %s", k)
	}
}
