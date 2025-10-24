package h3mapper

import (
	"fmt"
	"sort"

	h3 "github.com/uber/h3-go/v4"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

func (m *Mapper) ToParent(cell string, parentRes int) (string, error) {
	if err := validateRes(parentRes); err != nil {
		return "", err
	}
	var c h3.Cell
	if err := c.UnmarshalText([]byte(cell)); err != nil {
		return "", fmt.Errorf("parse cell: %w", err)
	}

	if !c.IsValid() {
		return "", fmt.Errorf("invalid h3 cell %q", cell)
	}
	curRes := c.Resolution()
	if parentRes > curRes {
		return "", fmt.Errorf("parentRes %d must be <= cell resolution %d", parentRes, curRes)
	}
	if parentRes == curRes {
		return cell, nil
	}

	// traverse up to the requested parent resolution
	p, err := c.Parent(parentRes)
	if err != nil {
		return "", fmt.Errorf("h3 parent: %w", err)
	}
	return p.String(), nil
}

func (m *Mapper) ToChildren(cell string, childRes int) (model.Cells, error) {
	if err := validateRes(childRes); err != nil {
		return nil, err
	}
	var c h3.Cell
	if err := c.UnmarshalText([]byte(cell)); err != nil {
		return nil, fmt.Errorf("parse cell: %w", err)
	}

	if !c.IsValid() {
		return nil, fmt.Errorf("invalid h3 cell %q", cell)
	}
	curRes := c.Resolution()
	if childRes < curRes {
		return nil, fmt.Errorf("childRes %d must be >= cell resolution %d", childRes, curRes)
	}
	if childRes == curRes {
		return model.Cells{cell}, nil
	}

	kids, err := c.Children(childRes)
	if err != nil {
		return nil, fmt.Errorf("h3 children: %w", err)
	}

	seen := make(map[string]struct{}, len(kids))
	out := make([]string, 0, len(kids))
	for _, k := range kids {
		s := k.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	// return sorted children
	sort.Strings(out)
	return out, nil
}
