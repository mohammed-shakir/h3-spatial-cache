package executor

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
)

type Interface interface {
	ForwardWFS(ctx context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest)
}

type Executor struct {
	logger   *slog.Logger
	client   *http.Client
	owsURL   *url.URL
	startNow func() time.Time // for tests
}

func New(logger *slog.Logger, client *http.Client, ows string) (*Executor, error) {
	u, err := url.Parse(ows)
	if err != nil {
		return nil, fmt.Errorf("parse ows url: %w", err)
	}
	return &Executor{
		logger:   logger,
		client:   client,
		owsURL:   u,
		startNow: time.Now,
	}, nil
}

// proxies a wfs request to GeoServer /ows and streams the response
func (e *Executor) ForwardWFS(_ context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	params := ogc.BuildGetFeatureParams(q)
	start := e.startNow()

	rt := http.RoundTripper(http.DefaultTransport)
	if e.client != nil && e.client.Transport != nil {
		rt = e.client.Transport
	}

	proxy := &httputil.ReverseProxy{
		Transport: rt,

		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL.Scheme = e.owsURL.Scheme
			p.Out.URL.Host = e.owsURL.Host
			p.Out.URL.Path = e.owsURL.Path
			p.Out.URL.RawPath = e.owsURL.EscapedPath()
			p.Out.URL.RawQuery = params.Encode()
			p.Out.Host = e.owsURL.Host
			p.Out.Header.Set("Accept", "application/json")
			p.SetXForwarded()
		},

		ModifyResponse: func(resp *http.Response) error {
			dur := time.Since(start)
			e.logger.Debug("forward done",
				"status", resp.StatusCode,
				"duration", dur.String())
			observability.ObserveUpstreamLatency("geoserver", dur.Seconds())
			return nil
		},

		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			e.logger.Error("reverse proxy error", "err", err)
			http.Error(w, "upstream proxy error: "+err.Error(), http.StatusBadGateway)
		},
	}

	e.logger.Debug("forward WFS GetFeature",
		"layer", q.Layer,
		"geoserver_ows", e.owsURL.String())

	proxy.ServeHTTP(w, r)
}

func (e *Executor) ForwardGetFeature(w http.ResponseWriter, r *http.Request, q model.QueryRequest) {
	e.ForwardWFS(r.Context(), w, r, q)
}
