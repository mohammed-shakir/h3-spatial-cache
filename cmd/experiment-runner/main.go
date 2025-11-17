package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	urlpkg "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type opt struct {
	Scenario     string
	H3Res        int
	TTL          string
	HotThreshold string
	Invalidation string
}

type cfg struct {
	PromURL       string
	TargetURL     string
	Layer         string
	Duration      time.Duration
	Concurrency   int
	BBoxes        int
	OutRoot       string
	DryRun        bool
	Scenarios     []string
	H3ResList     []int
	TTLs          []string
	Hots          []string
	Invalidations []string
	CentroidsPath string
	ClearCache    bool
}

func main() {
	c := parseFlags()
	if c.DryRun {
		if err := dryRun(c); err != nil {
			log.Fatalf("dry-run: %v", err)
		}
		return
	}
	if err := runAll(c); err != nil {
		log.Fatalf("runner: %v", err)
	}
}

func parseFlags() cfg {
	var c cfg
	var scenarios, h3res, ttls, hots, invs string

	flag.StringVar(&c.PromURL, "prom", "http://localhost:9090", "Prometheus base URL")
	flag.StringVar(&c.TargetURL, "target", "http://localhost:8090/query", "Middleware /query URL")
	flag.StringVar(&c.Layer, "layer", "demo:NR_polygon", "Layer (WFS typeNames)")
	flag.DurationVar(&c.Duration, "duration", 2*time.Minute, "Per-combo load duration")
	flag.IntVar(&c.Concurrency, "concurrency", 32, "Loadgen concurrency")
	flag.IntVar(&c.BBoxes, "bboxes", 128, "Distinct BBOXes")
	flag.StringVar(&c.OutRoot, "out", "results", "Output root dir")
	flag.BoolVar(&c.DryRun, "dry-run", false, "Only create directory tree; no services")
	flag.StringVar(&c.CentroidsPath, "centroids", "", "Optional centroid CSV file (id,lon,lat) to forward to loadgen")
	flag.StringVar(&scenarios, "scenarios", "baseline,cache", "Scenarios CSV")
	flag.StringVar(&h3res, "h3res", "7,8,9", "H3 resolutions CSV")
	flag.StringVar(&ttls, "ttls", "30s,60s", "TTLs CSV (Cache TTL Default)")
	flag.StringVar(&hots, "hots", "5,10", "Hot thresholds CSV")
	flag.StringVar(&invs, "invalidations", "ttl,kafka", "Invalidation modes CSV")
	flag.BoolVar(&c.ClearCache, "clear-cache", true, "Flush Redis before each cache scenario run")

	flag.Parse()

	c.Scenarios = splitCSV(scenarios)
	for _, s := range splitCSV(h3res) {
		var n int
		if _, err := fmt.Sscanf(s, "%d", &n); err == nil {
			c.H3ResList = append(c.H3ResList, n)
		}
	}
	c.TTLs = splitCSV(ttls)
	c.Hots = splitCSV(hots)
	c.Invalidations = splitCSV(invs)
	return c
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		if x := strings.TrimSpace(p); x != "" {
			out = append(out, x)
		}
	}
	return out
}

func runAll(c cfg) error {
	tstamp := time.Now().UTC().Format("20060102_150405Z")
	root := filepath.Join(c.OutRoot, tstamp)
	if err := os.MkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("mkdir results root: %w", err)
	}

	if err := preflightPorts(); err != nil {
		return fmt.Errorf("pre-flight port check failed: %w", err)
	}

	for _, sc := range c.Scenarios {
		for _, res := range c.H3ResList {
			for _, ttl := range c.TTLs {
				for _, hot := range c.Hots {
					for _, inv := range c.Invalidations {
						one := opt{
							Scenario: sc, H3Res: res,
							TTL: ttl, HotThreshold: hot, Invalidation: inv,
						}
						if err := runOne(c, root, one); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func bundleDir(root string, o opt) string {
	return filepath.Join(root,
		fmt.Sprintf("%s-r%d-ttl%s-hot%s-inv%s",
			o.Scenario, o.H3Res, sanitize(o.TTL), sanitize(o.HotThreshold), o.Invalidation))
}

func sanitize(s string) string {
	return strings.NewReplacer(":", "", "/", "-", ",", "_").Replace(s)
}

func runOne(c cfg, root string, o opt) error {
	dir := bundleDir(root, o)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("mkdir combo dir: %w", err)
	}
	if c.DryRun {
		return nil
	}

	if c.ClearCache && o.Scenario == "cache" {
		if err := clearRedis(); err != nil {
			return fmt.Errorf("clear redis before scenario=%s: %w", o.Scenario, err)
		}
	}

	env := os.Environ()
	env = set(env, "SCENARIO", o.Scenario)
	env = set(env, "H3_RES", fmt.Sprintf("%d", o.H3Res))
	env = set(env, "CACHE_TTL_DEFAULT", o.TTL)
	env = set(env, "HOT_THRESHOLD", o.HotThreshold)
	switch o.Invalidation {
	case "kafka":
		env = set(env, "INVALIDATION_ENABLED", "true")
		env = set(env, "INVALIDATION_DRIVER", "kafka")
	default:
		env = set(env, "INVALIDATION_ENABLED", "false")
		env = set(env, "INVALIDATION_DRIVER", "none")
	}

	app := exec.Command("go", "run", "./cmd/middleware")
	app.Env = env
	app.Stdout = mustFile(filepath.Join(dir, "middleware.stdout.log"))
	app.Stderr = mustFile(filepath.Join(dir, "middleware.stderr.log"))
	app.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := app.Start(); err != nil {
		return fmt.Errorf("start middleware: %w", err)
	}

	defer func() {
		if app.Process == nil {
			return
		}

		if pgid, err := syscall.Getpgid(app.Process.Pid); err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else {
			_ = app.Process.Kill()
		}
		_ = app.Wait()
	}()

	if err := waitReady("http://localhost:8090/healthz", 30*time.Second); err != nil {
		return fmt.Errorf("middleware not ready: %w", err)
	}

	go func() {
		f := filepath.Join(dir, "docker_stats.csv")
		_ = runCaptureStats(f, "postgis", "geoserver")
	}()

	outPrefix := filepath.Join(dir, o.Scenario)

	args := []string{
		"run", "./cmd/baseline-loadgen",
		"-target", c.TargetURL,
		"-layer", c.Layer,
		"-concurrency", fmt.Sprintf("%d", c.Concurrency),
		"-duration", c.Duration.String(),
		"-bboxes", fmt.Sprintf("%d", c.BBoxes),
		"-out", outPrefix,
		"-append-ts=false",
	}

	if strings.TrimSpace(c.CentroidsPath) != "" {
		args = append(args, "-centroids", c.CentroidsPath)
	}

	// #nosec G204 -- constructing argv for a fixed binary; no shell expansion, flags are static.
	load := exec.Command("go", args...)

	load.Stdout = mustFile(filepath.Join(dir, "loadgen.stdout.log"))
	load.Stderr = mustFile(filepath.Join(dir, "loadgen.stderr.log"))
	start := time.Now().UTC()
	if err := load.Run(); err != nil {
		return fmt.Errorf("loadgen: %w", err)
	}
	end := time.Now().UTC()

	if err := queryPrometheus(c.PromURL, dir, o, start, end); err != nil {
		_ = os.WriteFile(filepath.Join(dir, "prom_errors.txt"),
			[]byte(err.Error()), 0o600)
	}

	return nil
}

func set(env []string, k, v string) []string {
	prefix := k + "="
	for i := range env {
		if strings.HasPrefix(env[i], prefix) {
			env[i] = prefix + v
			return env
		}
	}
	return append(env, prefix+v)
}

func waitReady(readyURL string, timeout time.Duration) error {
	u, err := urlpkg.Parse(readyURL)
	if err != nil || u.Scheme != "http" || (u.Host != "localhost:8090" && u.Host != "127.0.0.1:8090") {
		return fmt.Errorf("invalid ready URL: %q", readyURL)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// #nosec G107 -- URL is validated above and restricted to localhost:8090.
		resp, err := http.Get(u.String())
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.New("timeout waiting for readiness")
}

func mustFile(path string) *os.File {
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	return f
}

func runCaptureStats(path string, containers ...string) error {
	if len(containers) == 0 {
		return nil
	}
	args := append([]string{"./scripts/capture-stats.sh"}, containers...)
	// #nosec G204 -- trusted local script; argv passed directly to exec without shell interpolation.
	cmd := exec.Command("bash", args...)
	cmd.Stdout = mustFile(path)
	cmd.Stderr = mustFile(strings.TrimSuffix(path, ".csv") + ".log")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("capture-stats start: %w", err)
	}
	return nil
}

type oneQuery struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
	URL  string `json:"url"`
}

func queryPrometheus(base, dir string, o opt, start, end time.Time) error {
	base = strings.TrimRight(base, "/")
	window := end.Sub(start).Round(time.Second)
	esc := urlpkg.QueryEscape

	sc := o.Scenario

	qP50 := fmt.Sprintf(`histogram_quantile(0.50, sum by (le) (increase(spatial_response_duration_seconds_bucket{scenario="%s"}[%ds])))`, sc, int(window.Seconds()))
	qP95 := fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (increase(spatial_response_duration_seconds_bucket{scenario="%s"}[%ds])))`, sc, int(window.Seconds()))
	qP99 := fmt.Sprintf(`histogram_quantile(0.99, sum by (le) (increase(spatial_response_duration_seconds_bucket{scenario="%s"}[%ds])))`, sc, int(window.Seconds()))

	qHit := fmt.Sprintf(`(
  sum(increase(spatial_response_total{hit_class=~"full_hit|partial_hit",scenario="%s"}[%ds]))
) / clamp_min(sum(increase(spatial_response_total{scenario="%s"}[%ds])), 1e-9)`, sc, int(window.Seconds()), sc, int(window.Seconds()))

	qStale := fmt.Sprintf(`(
  sum(increase(spatial_reads_total{stale="true",scenario="%s"}[%ds]))
) / clamp_min(sum(increase(spatial_reads_total{scenario="%s"}[%ds])), 1e-9)`, sc, int(window.Seconds()), sc, int(window.Seconds()))

	qRedisMem := `sum(redis_memory_used_bytes)`
	qPgCPU := `sum by (instance) (rate(process_cpu_seconds_total{job=~"postgres.*"}[1m]))`

	queries := []oneQuery{
		{"p50_latency_s", qP50, base + "/api/v1/query?query=" + esc(qP50)},
		{"p95_latency_s", qP95, base + "/api/v1/query?query=" + esc(qP95)},
		{"p99_latency_s", qP99, base + "/api/v1/query?query=" + esc(qP99)},
		{"hit_ratio", qHit, base + "/api/v1/query?query=" + esc(qHit)},
		{"staleness_ratio", qStale, base + "/api/v1/query?query=" + esc(qStale)},
		{"redis_memory_used_bytes_sum", qRedisMem, base + "/api/v1/query?query=" + esc(qRedisMem)},
		{"postgres_cpu_rate", qPgCPU, base + "/api/v1/query?query=" + esc(qPgCPU)},
	}
	b, _ := json.MarshalIndent(queries, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "promql_queries.json"), b, 0o600)

	type promResp struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
		Error  string          `json:"error,omitempty"`
	}

	httpCli := http.Client{Timeout: 8 * time.Second}
	results := make(map[string]json.RawMessage, len(queries))

	for _, q := range queries {
		resp, err := httpCli.Get(q.URL)
		if err != nil {
			return fmt.Errorf("prom query %s: %w", q.Name, err)
		}
		var rr promResp
		dec := json.NewDecoder(resp.Body)
		_ = dec.Decode(&rr)
		_ = resp.Body.Close()
		if rr.Status != "success" {
			results[q.Name] = json.RawMessage(`{"error": "` + rr.Error + `"}`)
			continue
		}
		results[q.Name] = rr.Data
	}
	js, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "prom_results.json"), js, 0o600); err != nil {
		return fmt.Errorf("write prom_results.json: %w", err)
	}
	return nil
}

func preflightPorts() error {
	httpAddr := os.Getenv("ADDR")
	if strings.TrimSpace(httpAddr) == "" {
		httpAddr = ":8090"
	}

	if err := checkPortAvailable(httpAddr); err != nil {
		return err
	}

	metricsEnabled := strings.ToLower(strings.TrimSpace(os.Getenv("METRICS_ENABLED"))) == "true"
	if metricsEnabled {
		metricsAddr := os.Getenv("METRICS_ADDR")
		if strings.TrimSpace(metricsAddr) == "" {
			metricsAddr = ":9100"
		}
		if err := checkPortAvailable(metricsAddr); err != nil {
			return err
		}
	}

	return nil
}

func checkPortAvailable(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("port %s is already in use: %w", addr, err)
	}
	_ = ln.Close()
	return nil
}

func clearRedis() error {
	addr := os.Getenv("REDIS_ADDR")
	if strings.TrimSpace(addr) == "" {
		addr = "localhost:6379"
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dial redis %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte("FLUSHALL\r\n")); err != nil {
		return fmt.Errorf("write FLUSHALL to redis %s: %w", addr, err)
	}

	return nil
}

func dryRun(c cfg) error {
	tstamp := time.Now().UTC().Format("20060102_150405Z")
	root := filepath.Join(c.OutRoot, tstamp)
	for _, sc := range c.Scenarios {
		for _, res := range c.H3ResList {
			for _, ttl := range c.TTLs {
				for _, hot := range c.Hots {
					for _, inv := range c.Invalidations {
						dir := bundleDir(root, opt{
							Scenario: sc, H3Res: res, TTL: ttl, HotThreshold: hot, Invalidation: inv,
						})
						if err := os.MkdirAll(dir, 0o750); err != nil {
							return fmt.Errorf("mkdir combo dir: %w", err)
						}
					}
				}
			}
		}
	}
	fmt.Println("created:", root)
	return nil
}
