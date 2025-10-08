package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type bbox struct {
	X1, Y1 float64
	X2, Y2 float64
	SRID   string // spatial reference id
}

// accepts client requests, builds wfs GetFeature requests,
// forwards the requests to geoserver, and streams the responses back to client
func queryHandler(logger *slog.Logger, geoserverBaseURL string, httpClient *http.Client) http.Handler {
	trimmedGeoServerBase := strings.TrimRight(geoserverBaseURL, "/")
	owsEndpoint := trimmedGeoServerBase + "/ows"

	// returned handler (process each request from client)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		layerName := strings.TrimSpace(r.URL.Query().Get("layer"))

		if layerName == "" {
			http.Error(w, "missing required parameter: layer", http.StatusBadRequest)
			return
		}

		// if bbox is provided, parse and validate it
		// if filters (KVP shorthand) is provided, use it instead and ignore bbox
		rawBBoxParam := strings.TrimSpace(r.URL.Query().Get("bbox"))
		var hasBBox bool
		var boundingBox bbox
		var err error
		if rawBBoxParam != "" {
			boundingBox, err = parseBBOX(rawBBoxParam)
			if err != nil {
				http.Error(w, "invalid bbox: "+err.Error(), http.StatusBadRequest)
				return
			}
			hasBBox = true
		} else {
			hasBBox = false
		}

		filterParam := strings.TrimSpace(r.URL.Query().Get("filters"))
		if filterParam != "" {
			if hasBBox {
				logger.Warn("both bbox and filters supplied; dropping bbox (KVP shorthand filters are mutually exclusive)",
					"layer", layerName, "bbox", rawBBoxParam)
				hasBBox = false
			}
			if !isSafeCQL(filterParam) {
				logger.Warn("rejected potentially unsafe cql_filter", "value", filterParam)
				http.Error(w, "invalid or disallowed cql_filter", http.StatusBadRequest)
				return
			}
		}

		// build wfs GetFeature request
		queryParams := url.Values{}
		queryParams.Set("service", "WFS")
		queryParams.Set("version", "2.0.0")
		queryParams.Set("request", "GetFeature")
		queryParams.Set("typeNames", layerName)

		if hasBBox {
			queryParams.Set("bbox", fmt.Sprintf("%.6f,%.6f,%.6f,%.6f,%s",
				boundingBox.X1, boundingBox.Y1, boundingBox.X2, boundingBox.Y2, boundingBox.SRID))
		}

		if filterParam != "" {
			queryParams.Set("cql_filter", filterParam)
		}

		queryParams.Set("outputFormat", "application/json")

		logger.Info("forward WFS GetFeature",
			"layer", layerName,
			"geoserver", trimmedGeoServerBase)

		targetURL, err := url.Parse(owsEndpoint)
		if err != nil {
			http.Error(w, "invalid GeoServer URL: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Define the reverse proxy
		rt := http.RoundTripper(http.DefaultTransport)
		if httpClient != nil && httpClient.Transport != nil {
			rt = httpClient.Transport
		}
		proxy := &httputil.ReverseProxy{
			Transport: rt,
			Director: func(req *http.Request) {
				q := req.URL.Query()
				q.Set("service", "WFS")
				q.Set("version", "2.0.0")
				q.Set("request", "GetFeature")
				q.Set("typeNames", layerName)

				if hasBBox {
					q.Set("bbox", fmt.Sprintf("%.6f,%.6f,%.6f,%.6f,%s",
						boundingBox.X1, boundingBox.Y1, boundingBox.X2, boundingBox.Y2, boundingBox.SRID))
				}
				if filterParam != "" {
					q.Set("cql_filter", filterParam)
				}
				q.Set("outputFormat", "application/json")

				req.URL.Scheme = targetURL.Scheme
				req.URL.Host = targetURL.Host
				req.URL.Path = targetURL.Path
				req.URL.RawQuery = q.Encode()

				req.Header.Set("Accept", "application/json")
			},
			ModifyResponse: func(resp *http.Response) error {
				logger.Info("forward done",
					"status", resp.StatusCode,
					"duration_ms", time.Since(startTime).Milliseconds())
				return nil
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				logger.Error("reverse proxy error", "err", err)
				http.Error(w, "upstream proxy error: "+err.Error(), http.StatusBadGateway)
			},
		}

		proxy.ServeHTTP(w, r)
	})
}

func parseBBOX(bboxParam string) (bbox, error) {
	parts := strings.Split(bboxParam, ",")
	if len(parts) != 5 {
		return bbox{}, errors.New("expected 5 comma-separated values: x1,y1,x2,y2,EPSG:4326")
	}

	xMin, err := parseFloat(parts[0])
	if err != nil {
		return bbox{}, fmt.Errorf("x1: %w", err)
	}

	yMin, err := parseFloat(parts[1])
	if err != nil {
		return bbox{}, fmt.Errorf("y1: %w", err)
	}

	xMax, err := parseFloat(parts[2])
	if err != nil {
		return bbox{}, fmt.Errorf("x2: %w", err)
	}

	yMax, err := parseFloat(parts[3])
	if err != nil {
		return bbox{}, fmt.Errorf("y2: %w", err)
	}

	// validate spatial reference id
	srid := strings.ToUpper(strings.TrimSpace(parts[4]))
	if srid != "EPSG:4326" {
		return bbox{}, fmt.Errorf("only EPSG:4326 is supported at this stage (got %q)", srid)
	}

	if !(xMin >= -180 && xMin <= 180 && xMax >= -180 && xMax <= 180) {
		return bbox{}, errors.New("longitude must be in [-180,180]")
	}
	if !(yMin >= -90 && yMin <= 90 && yMax >= -90 && yMax <= 90) {
		return bbox{}, errors.New("latitude must be in [-90,90]")
	}
	if xMax <= xMin || yMax <= yMin {
		return bbox{}, errors.New("coordinates must satisfy x2>x1 and y2>y1")
	}
	return bbox{X1: xMin, Y1: yMin, X2: xMax, Y2: yMax, SRID: srid}, nil
}

// trim whitespace and convert to float
func parseFloat(value string) (float64, error) {
	parsedValue, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("parse float: %w", err)
	}
	return parsedValue, nil
}

var safeCQLPattern = regexp.MustCompile(`^[\w\s\=\>\<\!\(\)\.\,\'\"\-]+$`)

func isSafeCQL(s string) bool {
	if len(s) > 500 {
		return false
	}
	return safeCQLPattern.MatchString(s)
}
