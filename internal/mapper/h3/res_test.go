package h3mapper

import (
	"reflect"
	"slices"
	"sort"
	"testing"

	h3 "github.com/uber/h3-go/v4"
)

func TestHierarchy_RoundTrip_ParentChildren(t *testing.T) {
	m := New()

	baseRes := 8
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 59.3293, Lng: 18.0686}, baseRes)
	if err != nil {
		t.Fatalf("LatLngToCell: %v", err)
	}
	cellStr := cell.String()

	parentStr, err := m.ToParent(cellStr, baseRes-1)
	if err != nil {
		t.Fatalf("ToParent: %v", err)
	}

	children, err := m.ToChildren(parentStr, baseRes)
	if err != nil {
		t.Fatalf("ToChildren: %v", err)
	}
	// make sure the original cell is among the children (amogos)
	if !contains(children, cellStr) {
		t.Fatalf("children at res=%d did not include original cell %s", baseRes, cellStr)
	}
	if len(children) == 0 {
		t.Fatalf("expected non-empty children for parent %s", parentStr)
	}
	if !sort.StringsAreSorted([]string(children)) {
		t.Fatalf("children must be sorted")
	}
}

func TestHierarchy_IdempotenceAndDeterminism(t *testing.T) {
	m := New()

	baseRes := 7
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 55.6050, Lng: 13.0038}, baseRes)
	if err != nil {
		t.Fatalf("LatLngToCell: %v", err)
	}
	cellStr := cell.String()

	p, err := m.ToParent(cellStr, baseRes)
	if err != nil {
		t.Fatalf("ToParent same-res: %v", err)
	}
	if p != cellStr {
		t.Fatalf("expected ToParent same-res to return input cell")
	}

	kids, err := m.ToChildren(cellStr, baseRes)
	if err != nil {
		t.Fatalf("ToChildren same-res: %v", err)
	}
	if len(kids) != 1 || kids[0] != cellStr {
		t.Fatalf("expected ToChildren same-res to return [%s], got %v", cellStr, kids)
	}

	k1, _ := m.ToChildren(cellStr, baseRes+1)
	k2, _ := m.ToChildren(cellStr, baseRes+1)
	if !reflect.DeepEqual(k1, k2) {
		t.Fatalf("expected identical children slices for repeated calls")
	}
	if !sort.StringsAreSorted([]string(k1)) {
		t.Fatalf("children must be sorted")
	}
}

func TestHierarchy_BadTransitions(t *testing.T) {
	m := New()
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 57.7089, Lng: 11.9746}, 9)
	if err != nil {
		t.Fatalf("LatLngToCell: %v", err)
	}
	cellStr := cell.String()

	// invalidate upward and downward resolution transitions
	if _, err := m.ToParent(cellStr, 10); err == nil {
		t.Fatalf("expected error for parentRes > current res")
	}

	if _, err := m.ToChildren(cellStr, 8); err == nil {
		t.Fatalf("expected error for childRes < current res")
	}
}

func contains(xs []string, v string) bool {
	return slices.Contains(xs, v)
}
