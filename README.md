# Adaptive Caching of Spatial Queries in PostGIS Using H3

A Go middleware that intercepts GeoServer/PostGIS requests, buckets spatial
footprints into **H3** hex cells, serves hot regions from **Redis**, and keeps
results fresh via **TTL + Kafka**–driven invalidations.

## Table of contents

- [Adaptive Caching of Spatial Queries in PostGIS Using H3](#adaptive-caching-of-spatial-queries-in-postgis-using-h3)
  - [Description](#description)
  - [Repository Structure](#repository-structure)
  - [Prerequisites](#prerequisites)
    - [Required](#required)
    - [Recommended Tools](#recommended-tools)
  - [Quick start](#quick-start)
    - [Environment Setup](#environment-setup)
      - [Database Seed](#database-seed)
    - [Start the Services](#start-the-services)
    - [Run The App](#run-the-app)
      - [Locally](#locally)
      - [Inside Docker](#inside-docker)
    - [Load Testing](#load-testing)
    - [Test Queries](#test-queries)
    - [Stop the Services](#stop-the-services)
    - [Testing](#testing)
    - [Lint](#lint)
  - [License](#license)

## Description

Modern spatial data platforms built on **GeoServer** and **PostGIS** frequently serve
overlapping, high-frequency queries for geographical “hot” regions (e.g., urban centers).

This uneven query distribution leads to:

- Repeated, expensive database work
- High CPU and I/O pressure on PostGIS
- Longer map rendering times
- Slower user latency

Caching is a common fix, but simple spatial caching often covers too much or
can't keep up with changing hotspots. Systems like **Uber’s H3** use a grid that
can handle different query sizes in a compact way. Earlier work often adds H3
indices into the database schema for spatial grouping or uses **Redis** for
generic web caching to reduce backend load.

However, the combined use of:

- Real-time hotspot detection via **H3** (external to PostGIS)
- Adaptive caching of those hotspots in **Redis**
- Precise invalidation driven by data-change events (e.g., via **Kafka**)

as a middleware layer has not, to the best of current
knowledge, been thoroughly tested in the literature.

This project focuses on combining spatial indexing, caching strategy design, and
system performance modeling to test and validate this specific architectural approach.

## Repository Structure

- `cmd/`: Main binaries/applications.
- `internal/core/`: Core middleware components (HTTP server, router, executor,
config, observability, OGC adapter, etc.).
- `internal/scenarios/`: Scenario-specific behavior used for experiments
(baseline, cache, etc.).
- `internal/*`: Shared internal building blocks (cache, mapper, hotness,
decision engine, metrics, invalidation, logger, etc.) used by core and scenarios.
- `pkg/`: Reusable modules that can be imported by multiple binaries.
- `config/`: Scenario/runtime configuration files that select which scenario
and invalidation mode to run.
- `deploy/`: Docker stuff + monitoring stuff.
- `docs/`: Documentation.
- `integration/`: Integration tests.
- `scripts/`: Helper scripts (start/stop dev stack, capture stats,
seed database, etc.).
- `testdata/`: Test fixtures (e.g. GeoJSON aggregator inputs/expected outputs).
- `results/`: Load test and experiment outputs.

## Prerequisites

### Required

- [**Docker Engine**](https://docs.docker.com/engine/install)
- [**Docker Compose v2**](https://docs.docker.com/compose/install)
- [**Go (>= 1.24)**](https://go.dev/doc/install)
- [**Git**](https://git-scm.com/downloads)

### Recommended Tools

- [**pgAdmin 4**](https://www.pgadmin.org/download)
- [**RedisInsight**](https://redis.io/insight/)
- [**Kafka UI**](https://github.com/provectus/kafka-ui)
- [**valkey-cli**](https://valkey.io/topics/cli/)

## Quick start

### Environment Setup

Make sure you have a correct environment file (.env) in `deploy/compose/`. You
can copy the example file (`.env.example`) and modify it as needed:

```bash
cp deploy/compose/.env.example deploy/compose/.env
```

Generate a kafka cluster id and set it in the .env file:

```bash
docker run --rm apache/kafka:3.8.0 /opt/kafka/bin/kafka-storage.sh random-uuid
```

Create the `results/` directory to store load test results:

```bash
mkdir -p results
```

#### Database Seed

The dataset (`scripts/seed/10-metria.sql`) is **not included** in this repository.
Make sure you have that file in place before starting the services.

Also, if you want to run the load generator and experiment runner with the
centroids of the seed data, make sure you have the `data/` folder.

### Start the Services

To start the services, you can use the provided scripts or run the commands
manually (The helper script uses docker compose under the hood):

```bash
./scripts/dev-up.sh
```

This script:

- Starts all services defined in `docker-compose.yml`, including:
  - **PostGIS**
  - **GeoServer**
  - **Redis**
  - **Kafka**
  - **Prometheus**
  - **Grafana**
  - **Loki**
  - **Promtail**
  - **Alertmanager**
  - (Optional) the `app` middleware (only if run with `--profile app`)

- Waits until **PostGIS**, **Redis**, **Kafka**, and **GeoServer** are *healthy*.

- Ensures the Kafka topic (default: `spatial-updates`) is created.

- Configures GeoServer:

  - Creates the workspace (default: `demo`)
  - Creates/updates the PostGIS datastore
  - Publishes all available feature types (tables) as GeoServer layers

After the script completes, you can verify running containers with:

```bash
docker ps
```

### Run The App

You can run the app either locally or inside the docker container.

#### Locally

```bash
set -o allexport; . deploy/compose/.env; set +o allexport
go run ./cmd/middleware -scenario baseline
```

You can change the `-scenario` flag to test different scenarios (`baseline` or `cache`).

#### Inside Docker

```bash
docker compose -f deploy/compose/docker-compose.yml \
  --env-file deploy/compose/.env \
  --profile app \
  up --build app
```

Rebuild the container after code changes:

```bash
docker compose -f deploy/compose/docker-compose.yml build app
docker compose -f deploy/compose/docker-compose.yml up app
```

### Load Testing

Then run the load generator:

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

Or run the experiment-runner to do multiple runs with different scenarios.
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

> NOTE: When running the experiment-runner, do not run the middleware, because
the experiment-runner starts its own instance of the middleware internally.

Optionally, you can also capture container cpu/memory stats during the load test:

```bash
./scripts/capture-stats.sh geoserver postgis > results/docker_stats_$(date -u +%Y%m%d_%H%M%SZ).csv
```

The load test results will be saved in the `results/` directory.

### Test Queries

You can test both BBOX and Polygon requests through the middleware:

```bash
# BBOX request
curl -s 'http://localhost:8090/query?layer=demo:NR_polygon&bbox=17.98,59.32,18.01,59.34,EPSG:4326'

# Polygon request
curl -G "http://localhost:8090/query" \
  --data-urlencode 'layer=demo:NR_polygon' \
  --data-urlencode 'polygon={"type":"Polygon","coordinates":[[[17.98,59.32],[18.01,59.32],[18.01,59.34],[17.98,59.34],[17.98,59.32]]]}'
```

### Stop the Services

When you are finished, you can stop the services using the provided script:

```bash
# Remove containers and volumes
./scripts/dev-down.sh

# Remove containers only (keep volumes)
./scripts/dev-down.sh --keep-volumes
```

Nuke docker:

```bash
docker kill $(docker ps -q) 2>/dev/null; docker system prune -af --volumes
```

### Testing

Run all unit tests:

```bash
go test ./...
```

### Lint

Use `golangci-lint` to lint the code (uses the `.golangci.yml` config):

```bash
golangci-lint run
golangci-lint run --fix
```

and to clean up dependencies:

```bash
go mod tidy
```

## License

This project is licensed under the **Business Source License 1.1** (see
`LICENSE`).
