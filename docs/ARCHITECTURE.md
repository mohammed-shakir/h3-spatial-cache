# System Architecture

<!--toc:start-->
- [System Architecture](#system-architecture)
  - [1. HTTP server and router](#1-http-server-and-router)
    - [1.1 What it does](#11-what-it-does)
    - [1.2 Router responsibilities](#12-router-responsibilities)
  - [2. Scenario engine layer](#2-scenario-engine-layer)
    - [2.1 Baseline engine](#21-baseline-engine)
    - [2.2 Cache engine](#22-cache-engine)
  - [3. H3 mapping and sharding](#3-h3-mapping-and-sharding)
    - [3.1 Sharding idea](#31-sharding-idea)
  - [4. Redis cache layer](#4-redis-cache-layer)
    - [4.1 Key format](#41-key-format)
    - [4.2 Value format](#42-value-format)
    - [4.3 TTL and adaptive tiers](#43-ttl-and-adaptive-tiers)
  - [5. GeoServer and PostGIS](#5-geoserver-and-postgis)
    - [5.1 GeoServer](#51-geoserver)
    - [5.2 PostGIS](#52-postgis)
    - [5.3 Executor responsibilities](#53-executor-responsibilities)
  - [6. Kafka invalidation path](#6-kafka-invalidation-path)
<!--toc:end-->

## 1. HTTP server and router

### 1.1 What it does

The server listens on `ADDR` which is configured to be on port `:8090`, and it
registers the following HTTP endpoints:

- `/query` – main API.
- `/healthz` – liveness check (process up?).
- `/health/ready` – readiness check (e.g. Kafka consumer healthy?).
- `/metrics` – Prometheus exported metrics.

### 1.2 Router responsibilities

For `/query`:

1. Parse query parameters:
   - `layer` (e.g. `demo:NR_polygon`)
   - `bbox` **or** `polygon` (geometry)
   - optional filters, format parameter, etc.

2. Normalize inputs:
    - Ensure consistent `EPSG` string.
    - Convert strings to internal types.

3. Choose scenario:
    - Based on env or flag: `baseline` or `cache`.

The router itself doesn’t care about caching/GeoServer, it just forwards
to the selected scenario engine.

## 2. Scenario engine layer

This is where “baseline vs cache” happens, both baseline and cache
implement the same interface:

  ```go
  type QueryHandler interface {
      Handle(ctx, req) (Response, error)
  }
  ```

The router calls the chosen handler.

### 2.1 Baseline engine

- Uses H3 mapping and hotness **only for metrics**.
- Calls executor once per request to get full polygon result (executor
  handles GeoServer talk).
- No Redis, no invalidation.

### 2.2 Cache engine

- Does the full decision pipeline:

  - H3 mapping.
  - Hotness update.
  - Adaptive decision.
  - Redis MGET.
  - Decide cache hit/partial/miss.
  - Call executor per cell.
  - Compose results.

Both eventually end up calling composer (which builds GeoJSON FeatureCollection)
to generate the final response.

## 3. H3 mapping and sharding

This is the spatial "grid" logic, where the input is a bbox/polygon (geometry),
and the output is a list of H3 cell IDs at some resolution (7/8/9…). This
lets us treat a big polygon as a bunch of smaller tiles, each tile can be
cached independently, and overlapping queries can share cached tiles.

### 3.1 Sharding idea

Instead of caching “entire polygon result”, we cache
“per-cell results”, so lets say we have two queries:

- Query 1: cells A B C D
- Query 2: cells C D E F

Since we cache per-cell, query 1 warms cache for A/B/C/D, then query 2 can reuse
C and D, only fetching E and F. This is the core advantage of H3-based caching.

## 4. Redis cache layer

### 4.1 Key format

Keys look like:

```text
layer:res:h3cell:filters=<normalized>:f=<xxhash>
```

Where:

- `layer`: `demo:NR_polygon`
- `res`: H3 resolution (e.g. `r7`)
- `h3cell`: H3 index.
- `filters`: serialized filters (sorted, normalized).
- `f=<xxhash>`: hash of filters to keep key length manageable and unique.

### 4.2 Value format

Values are binary-ish, there is the **header**: `"SC1"` (protocol version) +
8-byte timestamp, and a **body**: the serialized GeoJSON for that tile.
We have the timestamp in order to compare against the last Kafka invalidation
time, and TTL expiration. If value timestamp is older than a relevant
invalidation TS, we treat it as stale.

### 4.3 TTL and adaptive tiers

- `CACHE_TTL_DEFAULT`: baseline TTL.
- `CACHE_TTL_OVERRIDES`: per-layer customized TTL.

Adaptive logic can modify TTL to shorter/longer based on hotness, so hot
regions → longer TTL, while cold regions → shorter TTL or no cache.

## 5. GeoServer and PostGIS

### 5.1 GeoServer

GeoServer acts as a **WFS server**, it exposes the PostGIS data as OGC
endpoints (`/geoserver/ows?service=WFS&version=1.1.0&request=GetFeature&...`),
and the middleware's executor talks to GeoServer, not directly to PostGIS.

### 5.2 PostGIS

PostGIS is the actual spatial database with seeded data (`NR_polygon`, etc).
The SQL seed scripts in `scripts/seed/*.sql` populate it. GeoServer is
configured to use this DB and schema.

### 5.3 Executor responsibilities

The executor translates internal querys (layer, bbox/polygon, filters)
into GeoServer WFS request, it handles response streaming (GeoJSON, GML),
and surfaces errors consistently back to the middleware.

## 6. Kafka invalidation path

The invalidation path looks like:

```text
[PostGIS change] 
  → producer app 
  → Kafka topic "spatial-updates"
  → middleware Kafka runner
  → map event bbox/keys to H3
  → Redis DEL + hotness reset
  → update invalidated-at timestamps
```

So Kafka gives us **decoupling**, because the database writers don't need to
know about Redis, and the middleware just subscribes to “spatial-updates”.
