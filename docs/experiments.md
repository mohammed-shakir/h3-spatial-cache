# Automated Experiments

This guide shows how to reproduce latency, throughput,
and cache-effectiveness results using the **experiment runner**.

## Experiment matrix

By default, the runner sweeps all combinations of the
following parameters (you can override them via flags):

- `SCENARIO ∈ {baseline, cache}`
- `H3_RES ∈ {7,8,9}`
- `CACHE_TTL_DEFAULT ∈ {30s,60s}`
- `HOT_THRESHOLD ∈ {5,10}`
- `INVALIDATION ∈ {ttl,kafka}`
  _(Kafka enables `INVALIDATION_ENABLED=true`, while `ttl` keeps it off.)_

For each combination, the runner:

1. Starts the middleware with those environment variables.
2. Applies a Zipfian load for `DURATION` (default **2 minutes**).
3. Queries Prometheus for latency, hit ratio, and staleness.
4. Stores logs, metrics, and raw samples under:

```
results/<timestamp>/<scenario>-r<R>-ttl<T>-hot<H>-inv<I>/
```

## Run

### Run the experiment runner

You can run the full matrix directly:

```bash
go run ./cmd/experiment-runner \
  -prom http://localhost:9090 \
  -target http://localhost:8090/query \
  -layer demo:NR_polygon \
  -duration 2m \
  -concurrency 32 \
  -bboxes 128 \
  -out results \
  -scenarios baseline,cache \
  -h3res 7,8,9 \
  -ttls 30s,60s \
  -hots 5,10 \
  -invalidations ttl,kafka
```

### Run a smaller sweep

For a focused test (e.g., just one configuration):

```bash
go run ./cmd/experiment-runner \
  -prom http://localhost:9090 \
  -target http://localhost:8090/query \
  -layer demo:NR_polygon \
  -duration 1m \
  -concurrency 16 \
  -bboxes 64 \
  -out results \
  -scenarios cache \
  -h3res 8 \
  -ttls 60s \
  -hots 10 \
  -invalidations ttl
```

### 4. View results

After each run, check the generated `results/<timestamp>/.../` directories for:

- `middleware.stdout.log` / `middleware.stderr.log`
- `loadgen.stdout.log` / `loadgen.stderr.log`
- `<scenario>_samples.csv`, `<scenario>_summary.json`
- `promql_queries.json`, `prom_results.json`
