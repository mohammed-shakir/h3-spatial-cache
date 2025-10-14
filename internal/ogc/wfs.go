package ogc

import (
	"net/url"
	"strings"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/model"
)

func OWSEndpoint(geoServerBase string) string {
	return strings.TrimRight(geoServerBase, "/") + "/ows"
}

func BuildGetFeatureParams(q model.QueryRequest) url.Values {
	params := url.Values{}
	params.Set("service", "WFS")
	params.Set("version", "2.0.0")
	params.Set("request", "GetFeature")
	params.Set("typeNames", q.Layer)
	if q.BBox != nil {
		params.Set("bbox", q.BBox.String())
	}
	if q.Filters != "" {
		params.Set("cql_filter", q.Filters)
	}
	params.Set("outputFormat", "application/json")
	return params
}
