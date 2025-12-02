# System Architecture

## Table of Contents

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
    - [4.1 Key spaces](#41-key-spaces)
    - [4.2 Value formats](#42-value-formats)
    - [4.3 TTL and adaptive tiers](#43-ttl-and-adaptive-tiers)
  - [5. GeoServer and PostGIS](#5-geoserver-and-postgis)
    - [5.1 GeoServer](#51-geoserver)
    - [5.2 PostGIS](#52-postgis)
    - [5.3 Executor responsibilities](#53-executor-responsibilities)
  - [6. Kafka invalidation path](#6-kafka-invalidation-path)

## 1. HTTP server and router

### 1.1 What it does

The server process listens on two HTTP ports:

- The **main API server** listens on `ADDR` (configured to `:8090`) and exposes:
  - `/query` – main API.
  - `/healthz` – liveness check (process up?).
  - `/health/ready` – readiness check (e.g. Kafka consumer healthy?).

- A separate **metrics server** is started when `METRICS_ENABLED=true`:
  - `METRICS_ADDR` (default `:9090`)
  - `METRICS_PATH` (default `/metrics`)
  - exposes Prometheus metrics for the middleware.

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
  HandleQuery(ctx, w, r, queryRequest) // simplified
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
  - H3 mapping (at a base resolution with configurable min/max for adaptation).
  - Hotness update (per-cell exponential decay).
  - Adaptive decision (resolution, TTL tier, and whether to fill/bypass/serve-only-if-fresh).
  - Feature-centric cache read:
    - Look up per-cell feature IDs via a **cell index**.
    - Fetch unique feature payloads via a **feature store**.
  - Decide full hit / partial hit / miss (or adaptive bypass).
  - For missing cells, call GeoServer per cell (or per sub-query) and:
    - populate the feature store,
    - update the cell index.
  - Compose results using an advanced aggregator that merges feature shards
    with global sort/limit/offset and deduplication by ID/geometry.

Both scenarios eventually end up calling the composer (which builds a GeoJSON
FeatureCollection) to generate the final response.

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

### 4.1 Key spaces

Redis is used for several related key spaces:

1. **Base cell key** (shared/legacy form)

   ```text
     <layerNorm>:<res>:<h3cell>:filters=<sanitized>:f=<xxhash>
   ```

   Where:
   - `layerNorm`: sanitized layer name (e.g. `demo:NR_polygon` with
     spaces/punctuation normalized).
   - `res`: H3 resolution as integer (e.g. `7`).
   - `h3cell`: H3 index string.
   - `filters`: normalized, whitespace-collapsed filter expression, truncated
     to a safe length.
   - `f=<xxhash>`: 64-bit hash of the normalized filter string to keep keys
     unique but bounded.

   This “base key” is used as a building block by other key spaces.

2. **Cell index keys**

   ```text
   idx:<base-key>
   ```

   These map a `(layer, res, cell, filters)` combination to a **list of feature IDs**
   (or an explicit “empty” marker). They are used by the cell-index component to
   quickly discover which features belong to a given cell.

3. **Feature store keys**

   ```text
   feat:<sanitized-layer>:<canonical-feature-id-or-geom-hash>
   ```

   These store **individual GeoJSON features**. The ID part is either:
   - a canonicalized GeoJSON `id` (string or number), or
   - a geometry hash (e.g. `gh:<hash>`) derived from the feature geometry
     when no valid ID exists.

### 4.2 Value formats

Two value types are used in the feature-centric cache:

1. **Cell index values**
   - JSON array of strings:

     ```json
     ["s:123", "n:456", "gh:abc123...", "__EMPTY__"]
     ```

   - Each element is a normalized feature identifier:
     - `s:...` for string IDs,
     - `n:...` for numeric IDs,
     - `gh:...` for geometry hashes.
   - A special sentinel `__EMPTY__` means “we checked this cell and it is empty”.
     This lets us distinguish “known empty” from “no cache entry yet”.

2. **Feature store values**
   - Raw GeoJSON **Feature** objects (the feature JSON itself).
   - There is no extra header; TTL and freshness are driven by Redis expiry +
     layer-level invalidation metadata.

Older “per-cell blob” entries that include an `"SC1"` header + timestamp + tile GeoJSON
still exist for legacy paths, but the main cache read path now uses the
feature store + cell index instead of reading per-cell blobs directly.

### 4.3 TTL and adaptive tiers

- `CACHE_TTL_DEFAULT`: baseline TTL.
- `CACHE_TTL_OVERRIDES`: per-layer customized TTL.

Adaptive logic can modify TTL to shorter/longer based on hotness, so hot
regions have longer TTL, while cold regions have shorter TTL or no cache. The
chosen TTL is applied consistently to both the feature store entries and
the corresponding cell index entries for a given fill.

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
