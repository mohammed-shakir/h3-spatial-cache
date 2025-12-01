// Package metrics exposes Prometheus metrics for the service.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type BuildInfo struct {
	Version   string
	Revision  string
	Branch    string
	BuildDate string
}

type Config struct {
	Enabled bool
	Addr    string
	Path    string
	Build   BuildInfo
}

type Provider struct {
	reg       *prometheus.Registry
	buildInfo *prometheus.GaugeVec
}

func Init(cfg Config) *Provider {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	build := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_build_info",
			Help: "Build info for this binary (value is always 1).",
		},
		[]string{"version", "revision", "branch", "build_date"},
	)
	reg.MustRegister(build)
	v := cfg.Build
	if v.Version == "" {
		v.Version = "dev"
	}
	build.WithLabelValues(v.Version, v.Revision, v.Branch, v.BuildDate).Set(1)

	return &Provider{reg: reg, buildInfo: build}
}

func (p *Provider) Handler() http.Handler {
	return promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{})
}

func (p *Provider) Register(cs ...prometheus.Collector) {
	for _, c := range cs {
		p.reg.MustRegister(c)
	}
}

func (p *Provider) Registerer() prometheus.Registerer { return p.reg }
