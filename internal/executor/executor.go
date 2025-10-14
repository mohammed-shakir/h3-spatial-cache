package executor

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/ogc"
)

type Executor struct {
	logger   *slog.Logger
	client   *http.Client
	owsURL   *url.URL
	startNow func() time.Time // for tests
}

func New(logger *slog.Logger, client *http.Client, ows string) *Executor {
	u, _ := url.Parse(ows)
	return &Executor{
		logger:   logger,
		client:   client,
		owsURL:   u,
		startNow: time.Now,
	}
}

// proxies a query to GeoServer /ows and streams the response
func (e *Executor) ForwardGetFeature(w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	params := ogc.BuildGetFeatureParams(q)
	start := e.startNow()

	rt := http.RoundTripper(http.DefaultTransport)
	if e.client != nil && e.client.Transport != nil {
		rt = e.client.Transport
	}

	proxy := &httputil.ReverseProxy{
		Transport: rt,
		Director: func(req *http.Request) {
			req.URL.Scheme = e.owsURL.Scheme
			req.URL.Host = e.owsURL.Host
			req.URL.Path = e.owsURL.Path
			req.URL.RawQuery = params.Encode()
			req.Header.Set("Accept", "application/json")
		},
		ModifyResponse: func(resp *http.Response) error {
			e.logger.Info("forward done",
				"status", resp.StatusCode,
				"duration_ms", time.Since(start).Milliseconds())
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			e.logger.Error("reverse proxy error", "err", err)
			http.Error(w, "upstream proxy error: "+err.Error(), http.StatusBadGateway)
		},
	}

	e.logger.Info("forward WFS GetFeature",
		"layer", q.Layer,
		"geoserver_ows", e.owsURL.String())
	proxy.ServeHTTP(w, r)
}
