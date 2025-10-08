# Adaptive Caching of Spatial Queries in PostGIS Using H3

A Go middleware that intercepts GeoServer/PostGIS requests, buckets spatial
footprints into **H3** hex cells, serves hot regions from **Redis**, and keeps
results fresh via **TTL + Kafka**–driven invalidations.

## Table of contents

* [Description](#description)
* [Prerequisites](#prerequisites)
  * [Required](#required)
  * [Recommended Tools](#recommended-tools)
* [Quick start](#quick-start)
  * [Environment Setup](#environment-setup)
  * [Start the Services](#start-the-services)
  * [Run The App](#run-the-app)
    * [Locally](#locally)
    * [Inside Docker](#inside-docker)
  * [Stop the Services](#stop-the-services)
* [License](#license)

## Description

Modern spatial data platforms built on **GeoServer** and **PostGIS** frequently serve
overlapping, high-frequency queries for geographical “hot” regions (e.g., urban centers).

This uneven query distribution leads to:

* Repeated, expensive database work
* High CPU and I/O pressure on PostGIS
* Longer map rendering times
* Slower user latency

Caching is a common fix, but simple spatial caching often covers too much or
can't keep up with changing hotspots. Systems like **Uber’s H3** use a grid that
can handle different query sizes in a compact way. Earlier work often adds H3
indices into the database schema for spatial grouping or uses **Redis** for
generic web caching to reduce backend load.

However, the combined use of:

* Real-time hotspot detection via **H3** (external to PostGIS)
* Adaptive caching of those hotspots in **Redis**
* Precise invalidation driven by data-change events (e.g., via **Kafka**)

as a middleware layer has not, to the best of current
knowledge, been thoroughly tested in the literature.

This project focuses on combining spatial indexing, caching strategy design, and
system performance modeling to test and validate this specific architectural approach.

## Prerequisites

### Required

* [**Docker Engine**](https://docs.docker.com/engine/install)
* [**Docker Compose v2**](https://docs.docker.com/compose/install)
* [**Go (>= 1.24)**](https://go.dev/doc/install)
* [**Git**](https://git-scm.com/downloads)

### Recommended Tools

* [**pgAdmin 4**](https://www.pgadmin.org/download)
* [**RedisInsight**](https://redis.io/insight/)
* [**Kafka UI**](https://github.com/provectus/kafka-ui)

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

### Start the Services

To start the services, you can use the provided scripts or run the commands
manually (The helper script uses docker compose under the hood):

```bash
./scripts/dev-up.sh
```

This script:

* Starts **PostGIS**, **GeoServer**, **Redis**, and **Kafka** containers.
* Creates Kafka topic.
* Configures GeoServer by creating a workspace, datastore, and layer.

### Run The App

You can run the app either locally or inside the docker container.

#### Locally

```bash
set -o allexport; . deploy/compose/.env; set +o allexport
go run ./cmd/baseline-server
```

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
-layer demo:places \
-concurrency 32 \
-duration 60s \
-zipf-s 1.3 -zipf-v 1.0 \
-bboxes 128 \
-out results/baseline_$(date +%s)
```

Optionally, you can capture container cpu/memory stats during the load test:

```bash
./scripts/capture-stats.sh geoserver postgis > results/docker_stats_$(date +%s).csv
```

The load test results will be saved in the `results/` directory.

### Stop the Services

When you are finished, you can stop the services using the provided script:

```bash
# Remove containers and volumes
./scripts/dev-down.sh

# Remove containers only (keep volumes)
./scripts/dev-down.sh --keep-volumes
```

### Lint

Use `golangci-lint` to lint the code (uses the `.golangci.yml` config):

```bash
golangci-lint run
golangci-lint run --fix
```

## License

This project is licensed under the **Business Source License 1.1** (see
`LICENSE`).
