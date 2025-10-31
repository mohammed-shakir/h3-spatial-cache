package composer

import "testing"

func TestNegotiateFormat_OrderOfPrecedence(t *testing.T) {
	neg := NegotiateFormat(NegotiationInput{
		OutputFormat:  "application/json",
		AcceptHeader:  "application/gml+xml",
		DefaultFormat: FormatGeoJSON,
	})
	if neg.Format != FormatGeoJSON {
		t.Fatalf("outputFormat must win; got %v", neg.Format)
	}

	neg = NegotiateFormat(NegotiationInput{
		OutputFormat:  "",
		AcceptHeader:  "application/gml+xml;q=0.9,application/json;q=0.5",
		DefaultFormat: FormatGeoJSON,
	})
	if neg.Format != FormatGML32 {
		t.Fatalf("expected GML32 via Accept")
	}
}
