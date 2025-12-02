# Monitoring and Metrics

This document explains how to bring up the observability stack, how
metrics/logs are wired, and what to look at when running experiments.

## Table of Contents

- [Monitoring and Metrics](#monitoring-and-metrics)
  - [1. Endpoints](#1-endpoints)
  - [2. Metrics wiring](#2-metrics-wiring)
  - [3. Dashboards and PromQL](#3-dashboards-and-promql)
    - [3.1 Scenario and cache behaviour](#31-scenario-and-cache-behaviour)
    - [3.2 Hotness and TTLs](#32-hotness-and-ttls)
  - [4. Logs and Loki](#4-logs-and-loki)
  - [5. Experiment artifacts and correlation](#5-experiment-artifacts-and-correlation)
    - [5.1 Loadgen and experiment outputs](#51-loadgen-and-experiment-outputs)
    - [5.2 Docker stats (infra load)](#52-docker-stats-infra-load)

> Prereqs: `./scripts/dev-up.sh` has been run; base containers
> (PostGIS, GeoServer, Redis, Kafka, Prometheus, Grafana, Loki, promtail)
> are available. Middleware is running (either via `go run` or Compose).

## 1. Endpoints

- **Middleware metrics:** `http://localhost:9100/metrics`
- **Prometheus UI:** `http://localhost:9090`
- **Grafana UI:** `http://localhost:3000`
- **Middleware HTTP:** `http://localhost:8090`

## 2. Metrics wiring

The middleware exports Prometheus metrics when `METRICS_ENABLED=true`:

- **HTTP & scenario-level:**
  - `spatial_response_duration_seconds`: histogram of /query response latencies
    (labels: `scenario` (baseline/cache), `hit_class` (full_hit/partial_hit/miss),
    `format` (geojson/gml)).
  - `spatial_response_total`: counter of responses (labels include `scenario`,
    `hit_class`, `format`).

- **Cache & Redis:**
  - `spatial_reads_total{outcome="hit|miss"}`: counts cache-served vs
    backend-served reads.
  - `spatial_cache_hits_total` / `spatial_cache_misses_total`: counts hits and misses
    from the cache engine’s perspective.
  - `redis_operation_duration_seconds`: histogram of Redis op latencies
    (labels: `op="ping|mget|set|del|mset"`, `status="ok|error"`).

- **Adaptive & hotness:**
  - `adaptive_decisions_total`: counts adaptive decisions
    (labels: `decision="fill|bypass|serve_only_if_fresh"`, `reason="..."`).
  - Hotness gauges/counters for sampled H3 cells (used to visualize which cells
    are “hot” and how that changes over time).

- **Invalidation & freshness:**
  - `invalidation_events_total`: counts invalidation messages processed from Kafka
    (labels: `status="ok|error"`).
  - `invalidation_applied_total`: counts concrete actions taken (labels include
    `action="delete|skip_version"`).
  - `spatial_invalidation_lag_seconds`: gauge of lag between event time and
    processing time.
  - `spatial_fresh_reject_total`: counts HTTP 412 responses when “serve only if fresh”
    is enabled but cache cannot safely serve fresh data (labels: `reason="miss|stale"`).

Prometheus scrapes the middleware metrics server at `METRICS_ADDR` (default `:9090`)
and `METRICS_PATH` (default `/metrics`); see `deploy/compose/prometheus/prometheus.yml`.
Grafana auto-loads the “Spatial Cache – Starter” dashboard
(see `deploy/compose/grafana/provisioning/dashboards/`).

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

### 3.2 Hotness and TTLs

The adaptive module exposes hotness-related metrics so you can see which H3 cells
are considered “hot” and how that changes over time. Look for panels based on:

- `adaptive_decisions_total` (to see how many queries are treated as fill/bypass/serve_only_if_fresh).
- Hotness gauges/counters (to see relative hotness for sampled cells or regions).

TTL is still enforced by Redis expiry. To inspect actual TTLs in Redis directly,
use a tool like `valkey-cli`:

```bash
valkey-cli --scan # to list keys
valkey-cli TTL "<some-key>" # to check TTL for a specific key (in seconds)
valkey-cli --raw GET '<some-key>' | jq . # to see the cached value
```

This lets you confirm that hot regions are assigned longer TTLs (e.g. “hot” tier)
and cold regions shorter or no cache, in line with your adaptive config.

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
