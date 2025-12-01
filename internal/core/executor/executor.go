// Package executor coordinates executing upstream HTTP requests and streaming responses.
package executor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/ogc"
)

type Interface interface {
	FetchGetFeature(ctx context.Context, q model.QueryRequest) ([]byte, string, error)
	ForwardGetFeature(w http.ResponseWriter, r *http.Request, q model.QueryRequest)
	ForwardGetFeatureFormat(w http.ResponseWriter, r *http.Request, q model.QueryRequest, accept string)
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

// ForwardWFS proxies a wfs request to GeoServer /ows and streams the response
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

func (e *Executor) ForwardWFSWithFormat(_ context.Context, w http.ResponseWriter, r *http.Request, q model.QueryRequest, accept string) {
	if strings.TrimSpace(accept) == "" {
		accept = "application/json"
	}
	params := ogc.BuildGetFeatureParamsFormat(q, accept)
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
			p.Out.Header.Set("Accept", accept)
			p.SetXForwarded()
		},
		ModifyResponse: func(resp *http.Response) error {
			dur := time.Since(start)
			e.logger.Debug("forward done", "status", resp.StatusCode, "duration", dur.String())
			observability.ObserveUpstreamLatency("geoserver", dur.Seconds())
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			e.logger.Error("reverse proxy error", "err", err)
			http.Error(w, "upstream proxy error: "+err.Error(), http.StatusBadGateway)
		},
	}

	e.logger.Debug("forward WFS GetFeature (format)",
		"layer", q.Layer, "accept", accept, "geoserver_ows", e.owsURL.String())
	proxy.ServeHTTP(w, r)
}

func (e *Executor) ForwardGetFeatureFormat(w http.ResponseWriter, r *http.Request, q model.QueryRequest, accept string) {
	e.ForwardWFSWithFormat(r.Context(), w, r, q, accept)
}

func (e *Executor) FetchGetFeature(ctx context.Context, q model.QueryRequest) ([]byte, string, error) {
	params := ogc.BuildGetFeatureParams(q)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.owsURL.String(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	u := *e.owsURL
	u.RawQuery = params.Encode()
	req.URL = &u
	req.Host = e.owsURL.Host
	req.Header.Set("Accept", "application/json")

	start := e.startNow()
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	dur := time.Since(start)
	observability.ObserveUpstreamLatency("geoserver", dur.Seconds())

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, "", fmt.Errorf("upstream status %d: %s", resp.StatusCode, string(b))
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read body: %w", err)
	}
	return b, resp.Header.Get("Content-Type"), nil
}
