// Package keys defines Redis key formats used by the caching layer.
package keys

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/cespare/xxhash/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

// Key generate a cache key for the given parameters
func Key(layer string, res int, cell, filters string) string {
	layerNorm := sanitizeLayer(strings.TrimSpace(layer))
	filterText := normalizeFilters(filters)
	filterSafe := sanitizeForKey(filterText)

	const maxFilterTextLen = 160
	if len(filterSafe) > maxFilterTextLen {
		filterSafe = filterSafe[:maxFilterTextLen]
	}

	sum := xxhash.Sum64String(filterText)

	return fmt.Sprintf("%s:%d:%s:filters=%s:f=%016x", layerNorm, res, cell, filterSafe, sum)
}

// normalize spacing around operators
func normalizeFilters(s string) string {
	if s == "" {
		return ""
	}
	s = collapseASCIIWhitespace(strings.TrimSpace(s))
	re := regexp.MustCompile(`\s*([=<>!\.,\(\)])\s*`)
	return re.ReplaceAllString(s, "$1")
}

func sanitizeForKey(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))

	var prev rune
	for _, r := range s {
		out := rune(0)
		switch {
		case r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f':
			out = '_'
		case isAlphaNum(r) || r == ':' || r == '_' || r == '-' || r == '=':
			out = r
		default:
			out = '-'
		}
		if (out == '_' || out == '-') && out == prev {
			continue
		}
		b.WriteRune(out)
		prev = out
	}
	return b.String()
}

func sanitizeLayer(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	var prev rune
	for _, r := range s {
		out := rune(0)
		switch {
		case r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f':
			out = '_'
		case isAlphaNum(r) || r == ':' || r == '_' || r == '-':
			out = r
		default:
			out = '-'
		}
		if (out == '_' || out == '-') && out == prev {
			continue
		}
		b.WriteRune(out)
		prev = out
	}
	return b.String()
}

func collapseASCIIWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	wasWS := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f' {
			if !wasWS {
				b.WriteByte(' ')
				wasWS = true
			}
			continue
		}
		b.WriteRune(r)
		wasWS = false
	}
	return strings.TrimSpace(b.String())
}

func isAlphaNum(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		unicode.IsDigit(r)
}

func CellIndexKey(layer string, res int, cell string, filters model.Filters) string {
	base := Key(layer, res, cell, string(filters))
	return "idx:" + base
}
