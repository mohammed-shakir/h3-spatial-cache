package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	TargetURL       string
	LayerName       string
	Concurrency     int
	Duration        time.Duration
	ZipfS           float64
	ZipfV           float64
	BBoxCount       int
	OutputPrefix    string
	RequestTimeout  time.Duration
	AppendTimestamp bool
	TimestampFormat string
	CentroidFile    string
}

func loadConfig() Config {
	var cfg Config
	flag.StringVar(&cfg.TargetURL, "target", "http://localhost:8090/query", "Baseline server /query URL")
	flag.StringVar(&cfg.LayerName, "layer", "demo:NR_polygon", "Layer (WFS typeNames)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 32, "Concurrent workers")
	flag.DurationVar(&cfg.Duration, "duration", 60*time.Second, "Test duration")
	flag.Float64Var(&cfg.ZipfS, "zipf-s", 1.3, "Zipf parameter s (>1)")
	flag.Float64Var(&cfg.ZipfV, "zipf-v", 1.0, "Zipf parameter v (>=1)")
	flag.IntVar(&cfg.BBoxCount, "bboxes", 128, "Distinct BBOXes in pool")
	flag.StringVar(&cfg.OutputPrefix, "out", "results/baseline", "Output file prefix (JSON/CSV)")
	flag.DurationVar(&cfg.RequestTimeout, "timeout", 10*time.Second, "Per-request timeout")
	flag.BoolVar(&cfg.AppendTimestamp, "append-ts", true, "Append timestamp to output prefix")
	flag.StringVar(&cfg.TimestampFormat, "ts-format", "iso", "Timestamp format: iso|unix|none")
	flag.StringVar(&cfg.CentroidFile, "centroids", "", "Optional centroid CSV file (id,lon,lat) to drive BBOXes")
	flag.Parse()
	return cfg
}

type BBox struct{ X1, Y1, X2, Y2 float64 }

// String returns the bbox in "minx,miny,maxx,maxy,EPSG:4326" format.
func (b BBox) String() string {
	return fmt.Sprintf("%.5f,%.5f,%.5f,%.5f,EPSG:4326", b.X1, b.Y1, b.X2, b.Y2)
}

// creates a mix of "hot" and "cold" bounding boxes for testing.
func makeBBoxes(count int, r *rand.Rand) []BBox {
	centers := [][2]float64{
		{18.0686, 59.3293}, // Stockholm
		{11.9746, 57.7089}, // Göteborg
		{13.0038, 55.6050}, // Malmö
		{22.1547, 65.5848}, // Luleå
	}
	bboxes := make([]BBox, 0, count)

	hotBoxCount := int(math.Max(8, float64(count/4))) // at least 8 hot boxes

	// generate "hot" boxes around centers
	for i := range hotBoxCount {
		c := centers[i%len(centers)]                                              // cycle through centers
		dx, dy := (r.Float64()-0.5)*0.20, (r.Float64()-0.5)*0.20                  // random offset
		w, h := 0.12+r.Float64()*0.08, 0.12+r.Float64()*0.08                      // random size
		lon, lat := c[0]+dx, c[1]+dy                                              // apply offset
		bboxes = append(bboxes, BBox{lon - w/2, lat - h/2, lon + w/2, lat + h/2}) // create box
	}

	// generate remaining "cold" boxes randomly over sweden
	for len(bboxes) < count {
		lon := 11 + r.Float64()*(24-11)                                           // random lon
		lat := 55 + r.Float64()*(66-55)                                           // random lat
		w, h := 0.2*r.Float64()+0.05, 0.2*r.Float64()+0.05                        // random size
		bboxes = append(bboxes, BBox{lon - w/2, lat - h/2, lon + w/2, lat + h/2}) // create box
	}
	return bboxes
}

type Centroid struct {
	ID  string
	Lon float64
	Lat float64
}

func loadCentroidsCSV(path string) ([]Centroid, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("open centroids: %w", err)
	}
	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)

	// Read header
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	colIdx := map[string]int{}
	for i, h := range header {
		colIdx[strings.ToLower(strings.TrimSpace(h))] = i
	}

	idIdx, okID := colIdx["id"]
	lonIdx, okLon := colIdx["lon"]
	latIdx, okLat := colIdx["lat"]
	if !okID || !okLon || !okLat {
		return nil, fmt.Errorf("centroid csv: expected columns id,lon,lat; got %v", header)
	}

	var out []Centroid
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}

		id := strings.TrimSpace(rec[idIdx])
		lonStr := strings.TrimSpace(rec[lonIdx])
		latStr := strings.TrimSpace(rec[latIdx])

		if id == "" || lonStr == "" || latStr == "" {
			continue
		}

		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			return nil, fmt.Errorf("parse lon %q: %w", lonStr, err)
		}
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			return nil, fmt.Errorf("parse lat %q: %w", latStr, err)
		}

		out = append(out, Centroid{ID: id, Lon: lon, Lat: lat})
	}

	return out, nil
}

func makeBBoxesFromCentroids(centroids []Centroid, count int) []BBox {
	if len(centroids) == 0 || count <= 0 {
		return nil
	}
	if count > len(centroids) {
		count = len(centroids)
	}

	const halfSize = 0.02 // degrees (~2.2km at mid-latitudes)

	bboxes := make([]BBox, 0, count)
	for i := range count {
		c := centroids[i%len(centroids)]
		bboxes = append(bboxes, BBox{
			X1: c.Lon - halfSize,
			Y1: c.Lat - halfSize,
			X2: c.Lon + halfSize,
			Y2: c.Lat + halfSize,
		})
	}
	return bboxes
}

// request result (one sample per request)
type sample struct {
	Timestamp time.Time
	Latency   time.Duration
	Status    int
	ErrorMsg  string
	BoxIndex  int
	BBoxStr   string
}

type summary struct {
	StartTime     time.Time `json:"start"`
	EndTime       time.Time `json:"end"`
	DurationSec   float64   `json:"duration_sec"`
	TotalRequests int64     `json:"total"`
	SuccessCount  int64     `json:"success"`
	ErrorCount    int64     `json:"errors"`
	ThroughputRPS float64   `json:"throughput_rps"`
	P50Ms         float64   `json:"p50_ms"`
	P95Ms         float64   `json:"p95_ms"`
	P99Ms         float64   `json:"p99_ms"`
	Concurrency   int       `json:"concurrency"`
	ZipfS         float64   `json:"zipf_s"`
	ZipfV         float64   `json:"zipf_v"`
	BBoxes        int       `json:"bboxes"`
	TargetURL     string    `json:"target"`
	LayerName     string    `json:"layer"`
}

type aggregatedResult struct {
	total   int64
	success int64
	errors  int64
	latMs   []float64
}

func main() {
	cfg := loadConfig()
	if err := os.MkdirAll(filepath.Dir(cfg.OutputPrefix), 0o750); err != nil {
		log.Fatalf("mkdir results: %v", err)
	}

	prefix := cfg.OutputPrefix
	if cfg.AppendTimestamp {
		switch strings.ToLower(cfg.TimestampFormat) {
		case "none":
		case "unix":
			prefix = fmt.Sprintf("%s_%d", prefix, time.Now().Unix())
		default: // "iso"
			prefix = fmt.Sprintf("%s_%s", prefix, time.Now().UTC().Format("20060102_150405Z"))
		}
	}

	// precompute random workload
	seed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(seed))

	var bboxes []BBox
	if strings.TrimSpace(cfg.CentroidFile) != "" {
		centroids, err := loadCentroidsCSV(cfg.CentroidFile)
		if err != nil {
			log.Printf("WARN: failed to load centroids from %q: %v; falling back to synthetic BBOXes", cfg.CentroidFile, err)
		} else {
			bboxes = makeBBoxesFromCentroids(centroids, cfg.BBoxCount)
			log.Printf("using %d centroid-driven BBOXes from %s", len(bboxes), cfg.CentroidFile)
		}
	}

	// fallback if centroids disabled or failed
	if len(bboxes) == 0 {
		bboxes = makeBBoxes(cfg.BBoxCount, r)
		log.Printf("using %d synthetic BBOXes", len(bboxes))
	}

	if len(bboxes) == 0 {
		log.Fatalf("no BBOXes generated")
	}

	imax := uint64(len(bboxes)) - 1

	// HTTP client for load generation
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 4 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:          1024,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   4 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: cfg.RequestTimeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	// Prepare output files
	csvPath := prefix + "_samples.csv"
	jsonPath := prefix + "_summary.json"
	csvFile, err := os.Create(filepath.Clean(csvPath))
	if err != nil {
		log.Printf("open csv: %v", err)
		return
	}
	defer func() { _ = csvFile.Close() }()
	csvWriter := csv.NewWriter(csvFile)

	// Collects results asynchronously
	samplesChan := make(chan sample, 4096)
	resultsChan := make(chan aggregatedResult, 1)
	go func() {
		_ = csvWriter.Write([]string{"timestamp", "latency_ms", "status", "error", "bbox_idx", "bbox"})
		var total, successCount, errorCount int64
		latencies := make([]float64, 0, 1<<20)
		for s := range samplesChan {
			total++
			if s.ErrorMsg == "" && s.Status >= 200 && s.Status < 300 {
				successCount++
				latencies = append(latencies, float64(s.Latency.Microseconds())/1000.0)
			} else {
				errorCount++
			}
			_ = csvWriter.Write([]string{
				s.Timestamp.UTC().Format(time.RFC3339Nano),
				fmt.Sprintf("%.3f", float64(s.Latency.Microseconds())/1000.0),
				fmt.Sprintf("%d", s.Status),
				s.ErrorMsg,
				fmt.Sprintf("%d", s.BoxIndex),
				s.BBoxStr,
			})
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			log.Printf("csv flush error: %v", err)
		}
		resultsChan <- aggregatedResult{total: total, success: successCount, errors: errorCount, latMs: latencies}
	}()

	startTime := time.Now()
	log.Printf("loadgen start target=%s layer=%s dur=%s conc=%d zipf(s=%.2f,v=%.2f) bboxes=%d centroids=%s",
		cfg.TargetURL, cfg.LayerName, cfg.Duration, cfg.Concurrency, cfg.ZipfS, cfg.ZipfV, cfg.BBoxCount, cfg.CentroidFile)

	var wg sync.WaitGroup
	wg.Add(cfg.Concurrency)

	for workerID := range cfg.Concurrency {
		go func(id int) {
			defer wg.Done()

			rWorker := rand.New(rand.NewSource(seed + int64(id) + 1))
			zipfDist := rand.NewZipf(rWorker, cfg.ZipfS, cfg.ZipfV, imax)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				v := zipfDist.Uint64()
				if v > uint64(math.MaxInt) {
					continue
				}
				idx := int(v)
				if idx >= len(bboxes) {
					continue
				}
				box := bboxes[idx]

				u, _ := url.Parse(cfg.TargetURL)
				q := u.Query()
				q.Set("layer", cfg.LayerName)
				q.Set("bbox", box.String())
				u.RawQuery = q.Encode()

				startReq := time.Now()
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
				req.Header.Set("Accept", "application/json")
				resp, err := httpClient.Do(req)
				latency := time.Since(startReq)

				result := sample{
					Timestamp: startReq,
					Latency:   latency,
					Status:    0,
					ErrorMsg:  "",
					BoxIndex:  idx,
					BBoxStr:   box.String(),
				}

				if err != nil {
					result.ErrorMsg = err.Error()
				} else {
					result.Status = resp.StatusCode
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						result.ErrorMsg = fmt.Sprintf("status=%d", resp.StatusCode)
					}
				}

				select {
				case samplesChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}(workerID)
	}

	// close samples channel
	go func() {
		<-ctx.Done()
		wg.Wait()
		close(samplesChan)
	}()

	aggResult := <-resultsChan
	endTime := time.Now()
	elapsed := endTime.Sub(startTime).Seconds()

	sort.Float64s(aggResult.latMs)
	p50 := percentile(aggResult.latMs, 50)
	p95 := percentile(aggResult.latMs, 95)
	p99 := percentile(aggResult.latMs, 99)

	runSummary := summary{
		StartTime:     startTime.UTC(),
		EndTime:       endTime.UTC(),
		DurationSec:   elapsed,
		TotalRequests: aggResult.total,
		SuccessCount:  aggResult.success,
		ErrorCount:    aggResult.errors,
		ThroughputRPS: float64(aggResult.total) / elapsed,
		P50Ms:         p50,
		P95Ms:         p95,
		P99Ms:         p99,
		Concurrency:   cfg.Concurrency,
		ZipfS:         cfg.ZipfS,
		ZipfV:         cfg.ZipfV,
		BBoxes:        cfg.BBoxCount,
		TargetURL:     cfg.TargetURL,
		LayerName:     cfg.LayerName,
	}

	jsonFile, err := os.Create(filepath.Clean(jsonPath))
	if err == nil {
		enc := json.NewEncoder(jsonFile)
		enc.SetIndent("", "  ")
		_ = enc.Encode(runSummary)
		_ = jsonFile.Close()
	}

	log.Printf("done: total=%d succ=%d err=%d thr=%.2f rps p50=%.1fms p95=%.1fms p99=%.1fms",
		aggResult.total, aggResult.success, aggResult.errors, runSummary.ThroughputRPS, p50, p95, p99)
	log.Printf("wrote %s and %s", jsonPath, csvPath)
}

func percentile(sortedValues []float64, p float64) float64 {
	if len(sortedValues) == 0 {
		return math.NaN()
	}
	if p <= 0 {
		return sortedValues[0]
	}
	if p >= 100 {
		return sortedValues[len(sortedValues)-1]
	}
	k := (p / 100.0) * float64(len(sortedValues)-1)
	f := math.Floor(k)
	i := int(f)
	if i >= len(sortedValues)-1 {
		return sortedValues[len(sortedValues)-1]
	}
	d := k - f
	return sortedValues[i]*(1-d) + sortedValues[i+1]*d
}
