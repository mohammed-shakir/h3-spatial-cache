# Monitoring and Metrics

This document explains how to bring up the observability stack, how
metrics/logs are wired, and what to look at when running experiments.

## Table of Contents

- [Monitoring and Metrics](#monitoring-and-metrics)
  - [1. Endpoints](#1-endpoints)
  - [2. Metrics wiring](#2-metrics-wiring)
  - [3. Dashboards and PromQL](#3-dashboards-and-promql)
    - [3.1 Scenario and cache behaviour](#31-scenario-and-cache-behaviour)
    - [3.2 Hot keys and TTLs](#32-hot-keys-and-ttls)
  - [4. Logs and Loki](#4-logs-and-loki)
  - [5. Experiment artifacts and correlation](#5-experiment-artifacts-and-correlation)
    - [5.1 Loadgen and experiment outputs](#51-loadgen-and-experiment-outputs)
    - [5.2 Docker stats (infra load)](#52-docker-stats-infra-load)

> Prereqs: `./scripts/dev-up.sh` has been run; base containers
(PostGIS, GeoServer, Redis, Kafka, Prometheus, Grafana, Loki, promtail)
are available. Middleware is running (either via `go run` or Compose).

## 1. Endpoints

- **Middleware metrics:** `http://localhost:9100/metrics`
- **Prometheus UI:** `http://localhost:9090`
- **Grafana UI:** `http://localhost:3000`
- **Middleware HTTP:** `http://localhost:8090`

## 2. Metrics wiring

The middleware exports Prometheus metrics when `METRICS_ENABLED=true`:

- HTTP & scenario-level:
  - `spatial_response_duration_seconds`: histogram of response latencies
  (labels: `scenario`, `hit_class`, `layer`, `status`)
  - `spatial_response_total`: counter of responses (labels include `scenario`, `hit_class`).

- Cache:
  - `spatial_reads_total{cache="hit|miss"}`: counts underlying spatial DB reads.
  - `spatial_cache_hits_total`: counts cache hits (full + partial).
  - `spatial_cache_hot_keys`: gauge of number of hot keys in Redis.
  - `redis_operation_duration_seconds`: histogram of Redis op latencies
  (labels: `operation`).

- Invalidation:
  - `invalidation_events_total`: counts invalidation events processed.
  - `spatial_invalidation_total`: counts spatial keys invalidated.
  - `invalidation_lag_seconds`: histogram of lag between event time and
  processing time.

Prometheus scrapes the middleware at `:9100` (see `deploy/compose/prometheus/prometheus.yml`).
Grafana auto-loads the “Spatial Cache – Starter” dashboard (see `deploy/compose/grafana/provisioning/dashboards/`).

## 3. Dashboards and PromQL

### 3.1 Scenario and cache behaviour

In Grafana, use the **scenario** and **hit_class** variables to split:

- Baseline vs cache.
- `full_hit` vs `partial_hit` vs `miss`.

Useful PromQL snippets (via Prometheus UI or Grafana panels):

- **P95 latency (all scenarios):**

  ```promql
  histogram_quantile(
    0.95,
    sum by (le, scenario) (
      rate(spatial_response_duration_seconds_bucket[5m])
    )
  )
  ```

- **Hit ratio (cache scenario):**

  ```promql
  sum(rate(spatial_response_total{scenario="cache", hit_class="full_hit"}[5m]))
  /
  sum(rate(spatial_response_total{scenario="cache"}[5m]))
  ```

- **Stale ratio (if you track stale=true):**

  ```promql
  sum(rate(spatial_reads_total{stale="true"}[5m]))
  /
  sum(rate(spatial_reads_total[5m]))
  ```

### 3.2 Hot keys and TTLs

- **Hot keys gauge** (Grafana panel): Uses `spatial_cache_hot_keys`.
- To inspect TTLs in Redis directly, use a tool like `valkey-cli`:

  ```bash
  valkey-cli --scan --pattern 'demo:NR_polygon*' | head
  valkey-cli TTL "<some-key>"
  ```

## 4. Logs and Loki

Logs from middleware and other containers are shipped via promtail → Loki → Grafana.

In Grafana:

1. Open **Explore → Loki**.
2. Filter by labels like `component="middleware"` or `container="middleware"`.
3. Search for:

   - `cache full-hit`, `cache partial-miss`, `cache miss`
   - `adaptive_decision`
   - `kafka invalidation` / `spatial_invalidation_total`

## 5. Experiment artifacts and correlation

### 5.1 Loadgen and experiment outputs

- **Baseline loadgen:**

  ```bash
  go run ./cmd/baseline-loadgen \
    -target http://localhost:8090/query \
    -layer demo:NR_polygon \
    -duration 20s \
    -concurrency 32 \
    -zipf-s 1.3 \
    -zipf-v 1.0 \
    -bboxes 1024 \
    -timeout 5s \
    -centroids data/NR_polygon_centroids.csv \
    -out results/baseline \
    -append-ts=true \
    -ts-format=iso
  ```

  (If you do not have the data/ folder, remove the `-centroids` flag to use
  random bboxes)

  or use default parameters:

  ```bash
  go run ./cmd/baseline-loadgen -out results/baseline
  ```

  This produces: `results/<prefix>_samples.csv` and `<prefix>_summary.json`
  (p50/p95/p99, throughput).

- **Experiment runner:**

  You can run the full matrix directly:

  ```bash
  go run ./cmd/experiment-runner \
    -prom http://localhost:9090 \
    -target http://localhost:8090/query \
    -layer demo:NR_polygon \
    -duration 20s \
    -concurrency 32 \
    -zipf-s 1.3 \
    -zipf-v 1.0 \
    -bboxes 1024 \
    -centroids data/NR_polygon_centroids.csv \
    -out results \
    -scenarios baseline,cache \
    -h3res 7,8,9 \
    -ttls 30s,60s \
    -hots 5,10 \
    -invalidations ttl,kafka
  ```

  or for a focused test (e.g., just one configuration):

  ```bash
  go run ./cmd/experiment-runner \
    -prom http://localhost:9090 \
    -target http://localhost:8090/query \
    -layer demo:NR_polygon \
    -duration 20s \
    -concurrency 32 \
    -zipf-s 1.3 \
    -zipf-v 1.0 \
    -bboxes 1024 \
    -centroids data/NR_polygon_centroids.csv \
    -out results \
    -scenarios cache \
    -h3res 8 \
    -ttls 60s \
    -hots 10 \
    -invalidations ttl
  ```

  (If you do not have the data/ folder, remove the `-centroids` flag to use
  random bboxes)

  This produces a directory tree under:

  ```text
  results/<timestamp>/<scenario>/<config>/
  ```

  with logs, PromQL results, summaries, and config metadata.

### 5.2 Docker stats (infra load)

To correlate system load (CPU/mem) with cache behavior:

```bash
./scripts/capture-stats.sh geoserver postgis > results/docker_stats_$(date +%s).csv
```

This runs until you Ctrl+C.
