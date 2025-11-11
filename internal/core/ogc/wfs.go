package ogc

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

func OWSEndpoint(geoServerBase string) string {
	return strings.TrimRight(geoServerBase, "/") + "/ows"
}

func BuildGetFeatureParams(q model.QueryRequest) url.Values {
	return BuildGetFeatureParamsFormat(q, "application/json")
}

func BuildGetFeatureParamsFormat(q model.QueryRequest, outputFormat string) url.Values {
	params := url.Values{}
	params.Set("service", "WFS")
	params.Set("version", "2.0.0")
	params.Set("request", "GetFeature")
	params.Set("typeNames", q.Layer)
	if q.BBox != nil && q.Polygon == nil {
		params.Set("bbox", q.BBox.String())
	}
	// prefer polygon over bbox and combine with filters if both present
	if q.Polygon != nil {
		if wkt, err := GeoJSONToWKT(q.Polygon.GeoJSON); err != nil {
			if q.Filters != "" {
				params.Set("cql_filter", q.Filters)
			}
		} else {
			cql := fmt.Sprintf("INTERSECTS(geom, %s)", wkt)
			if q.Filters != "" {
				cql = fmt.Sprintf("(%s) AND (%s)", q.Filters, cql)
			}
			params.Set("cql_filter", cql)
		}
	} else if q.Filters != "" {
		params.Set("cql_filter", q.Filters)
	}
	if strings.TrimSpace(outputFormat) == "" {
		outputFormat = "application/json"
	}
	params.Set("outputFormat", outputFormat)
	return params
}
