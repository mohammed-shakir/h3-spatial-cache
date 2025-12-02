# Scenarios

All scenarios share the same input (HTTP request, layer, bbox/polygon, filters),
they all produce the same kind of output (GeoJSON FeatureCollection / possibly GML),
but the difference is how they get the data and what logic they apply.
There are 2 scenarios implemented for this middleware:

- `baseline`: no cache, just pass-through, but with tracking.
- `cache`: uses a Redis-backed **feature-centric cache** (feature store + cell index)
  plus hotness and adaptive decisions.
- Then "sub-scenarios"/modes layered on top of `cache`:
  - Full cache hit
  - Partial cache hit
  - Cache miss
  - Kafka invalidation
  - Hotness & adaptive caching
  - Monitoring/metrics

## Table of Contents

- [Scenarios](#scenarios)
  - [1. Baseline (no caching)](#1-baseline-no-caching)
    - [Step by step for baseline](#step-by-step-for-baseline)
    - [How to test the baseline](#how-to-test-the-baseline)
  - [2. Cache scenario](#2-cache-scenario)
    - [2.1 Full cache hit (cache scenario, best case)](#21-full-cache-hit-cache-scenario-best-case)
      - [Step by step for full cache hit](#step-by-step-for-full-cache-hit)
      - [How to test the full cache hit](#how-to-test-the-full-cache-hit)
    - [2.2 Partial cache hit (realistic case)](#22-partial-cache-hit-realistic-case)
      - [Step by step for partial cache hit](#step-by-step-for-partial-cache-hit)
      - [How to test the partial cache hit](#how-to-test-the-partial-cache-hit)
    - [2.3 Cache miss (cold or bypass)](#23-cache-miss-cold-or-bypass)
      - [Step by step for cache miss](#step-by-step-for-cache-miss)
      - [How to test the cache miss](#how-to-test-the-cache-miss)
    - [2.4 Data update and Kafka-driven invalidation](#24-data-update-and-kafka-driven-invalidation)
      - [The Event](#the-event)
      - [Step by step for Kafka invalidation](#step-by-step-for-kafka-invalidation)
      - [How to test Kafka invalidation](#how-to-test-kafka-invalidation)
    - [2.5 Hotness tracking and adaptive caching](#25-hotness-tracking-and-adaptive-caching)
      - [Key concepts](#key-concepts)
      - [Step by step for hotness and adaptive caching](#step-by-step-for-hotness-and-adaptive-caching)
      - [How to test hotness and adaptive caching](#how-to-test-hotness-and-adaptive-caching)

## 1. Baseline (no caching)

The baseline scenario is simple, it does not use any caching mechanism.
It just takes the request from the client, it calculates which h3 cells the query
area touches (for tracking purposes), it forwards the request to GeoServer,
GeoServer queries PostGIS, gets the data, sends it back to the middleware,
then back to the client.

**Pipeline**:

```bash
Client -> Middleware (tracking) -> GeoServer -> PostGIS -> Client
```

### Step by step for baseline

1. **Client sends `/query`**

   Example:

   ```bash
   curl -s 'http://localhost:8090/query?layer=demo:NR_polygon&bbox=17.98,59.32,18.01,59.34,EPSG:4326'
   ```

2. **Router validates input**

   Checks that the `layer` exists, either `bbox` or `polygon` is present and has
   valid format, and normalizes the input, so maybe trims spaces,
   uppercases SRID like `EPSG:4326`, etc.

3. **Baseline engine runs**

   It calculates which H3 cells the query area touches. This is done only for
   logging, so it doesn’t store or reuse results, it just records "this query
   touched cells A, B, C...". Each cell gets its hotness score updated using
   exponential decay, where new queries bump the score up and old scores decay
   over time so hotspots cool down if not used. But in baseline, hotness is only
   recorded and not used for caching.

4. **Executor calls GeoServer**
   - Converts the query (layer + bbox/polygon) into a GeoServer WFS request.
   - GeoServer forwards that to PostGIS.
   - GeoServer returns GeoJSON/GML back to the middleware.

5. **Composer wraps the result**
   - Ensures the response is a valid **FeatureCollection** with consistent structure.
   - This makes sure baseline and cache scenarios return the same shape.

6. **Response is sent back to client.**

### How to test the baseline

- Run baseline:

  ```bash
  set -o allexport; . deploy/compose/.env; set +o allexport
  go run ./cmd/middleware -scenario baseline
  ```

- Run some queries or loadgen:

  ```bash
  # BBOX request
  curl -s 'http://localhost:8090/query?layer=demo:NR_polygon&bbox=17.98,59.32,18.01,59.34,EPSG:4326'

  # Polygon request
  curl -G "http://localhost:8090/query" \
    --data-urlencode 'layer=demo:NR_polygon' \
    --data-urlencode 'polygon={"type":"Polygon","coordinates":[[[17.98,59.32],[18.01,59.32],[18.01,59.34],[17.98,59.34],[17.98,59.32]]]}'
  ```

After sending a request, you can check the middleware logs. It will show:

- **Incoming HTTP request**: confirms that the middleware received a `/query` request.
- **H3 mapping**: shows the request footprint (bbox/polygon) converted to H3 cells.
- **Hotness updates**: shows how hotness scores are updated for cells (for
  analysis only; baseline still never uses cache).

## 2. Cache scenario

### 2.1 Full cache hit (cache scenario, best case)

It has seen the region before, and the H3 cells are already in Redis.
So we can answer entirely from cache. No GeoServer, no PostGIS for this request.
So for a full hit, every hex cell needed should already be in Redis.

**Pipeline**:

```bash
Client -> Middleware -> Redis (feature store + cell index) -> Client
```

#### Step by step for full cache hit

1. **Same initial steps**
   - `/query` → router validates → cache scenario chosen.

2. **Map query footprint → H3 cells**
   - The engine figures out which H3 cells cover the requested geometry.
   - Uses a base resolution, possibly adjusted by the adaptive decider within
     the configured `[H3ResMin, H3ResMax]` range.

3. **Lookup in the cell index**
   - For all cells, it calls the cell index (backed by Redis) to get the list of
     feature IDs per `(layer, res, cell, filters)`.
   - Cells are split into:
     - **Index hits**: have IDs (or the explicit “empty” marker).
     - **Index misses**: have no entry yet.

4. **Fetch feature payloads from the feature store**
   - From all index hits, it collects the **unique feature IDs** and calls the
     feature store (also Redis-backed) to fetch the GeoJSON feature payloads in
     one batched read.

5. **Assemble cached shard pages**
   - For each cell with IDs:
     - Rebuilds the list of feature JSON objects.
     - Tracks geometry hashes when IDs are hash-based.
     - Builds a shard page:
       - `CacheStatus = CacheHit`
       - `Features = []json.RawMessage`
       - `GeomHashes = []string` (optional, for dedup).
   - If **all cells** are covered by the feature store (no index misses), this
     is a full feature-centric hit.

6. **Freshness check (optional)**
   - If “serve only if fresh” is enabled and there has been an invalidation for
     this layer since the cache was populated, the engine may return HTTP 412
     instead of serving from cache.
   - Otherwise, cached data is considered acceptable and used directly.

7. **Compose and return**
   - The composer calls the advanced GeoJSON aggregator with the cached shards.
   - The aggregator merges features, applies global sort/limit/offset, and
     deduplicates by feature ID or geometry hash.
   - Response is returned to the client without any GeoServer/PostGIS calls.

#### How to test the full cache hit

Warm the region first, so it needs to be hit several times
(you can run the load generator). Then check Redis (install a tool like `valkey-cli`):

```bash
valkey-cli --scan
```

You should see:

- `idx:` keys for the H3 cells (cell index),
- `feat:` keys for individual features in those cells.

You can also check metrics for cache hits:

```bash
curl -s http://localhost:8090/metrics | grep spatial_cache_hits_total
curl -s http://localhost:8090/metrics | grep 'hit_class="full_hit"'
```

### 2.2 Partial cache hit (realistic case)

Some of the needed cells are in cache, some are not. Use cache for what we
have, and fetch only the missing ones from GeoServer. This is what happens when
the workload “moves” a bit, or you have large bboxes.

**Pipeline**:

```bash
Client
-> Middleware (cache)
-> Redis (feature store + cell index) + GeoServer/PostGIS for misses
-> Redis (feature/index fill)
-> Client
```

#### Step by step for partial cache hit

1. **Same initial steps**
   - `/query` → router → cache scenario → map to H3 cells (possibly at an
     adaptive resolution).

2. **Read from cell index**
   - The engine queries the cell index for all cells.
   - Cells are split into:
     - **Hit group**: cells with IDs (or an explicit empty marker).
     - **Miss group**: cells with no index entry.

3. **Fetch features for hit cells**
   - For IDs belonging to hit cells, it calls the feature store once to get all
     unique features.
   - Builds shard pages (`CacheStatus = CacheHit`) for cells that can be fully
     reconstructed from the feature store.

4. **Freshness enforcement**
   - If “serve only if fresh” is enabled and:
     - any cells are missing, or
     - the layer has been invalidated since cache population,
       the engine returns HTTP 412 and does **not** call GeoServer.

5. **Fetch only the missing cells from GeoServer**
   - For cells in the **miss group**, it issues per-cell WFS requests to
     GeoServer.
   - Each cell response is parsed:
     - individual features are written to the feature store,
     - the list of normalized IDs is written to the cell index.
   - The raw per-cell FeatureCollections are kept as “miss shard pages”
     (`CacheStatus = CacheMiss`) for this request.

6. **Composer merges both sources**
   - Cached pages (feature-based) and freshly fetched pages (body-based) are
     all passed to the aggregator.
   - The aggregator merges them into a single FeatureCollection, with global
     sort/limit/offset and deduplication.

7. **Return response**
   - Client sees a single FeatureCollection; internally we have partially
     served from cache and partially from GeoServer.

#### How to test the partial cache hit

- Warm a small area (so some cells are cached).
- Then query a slightly bigger region that overlaps warm and cold cells.
- Check Redis key count: it should **grow over time** as new cells appear.
- Metrics should show partial hits and decreasing latency over more runs.

### 2.3 Cache miss (cold or bypass)

We either have nothing cached for this area, or the decider told us not to use
cache. So we fetch everything from GeoServer and then optionally cache the result.”

Two main causes:

1. **Cold start**: First-ever query for that area/resolution/filter.
2. **Bypass decision**: Adaptive logic decides “don’t cache this” (e.g.,
   too cold, not worth memory or risk of staleness).

**Pipeline**:

```bash
Client -> Middleware -> GeoServer/PostGIS -> Redis -> Client
```

#### Step by step for cache miss

1. **Same initial steps**
   - `/query` → router → cache scenario → map H3 cells.

2. **Check feature-centric cache**
   - Cell index query returns no IDs for any cells (cold start), or the
     feature store cannot supply required features.
   - Effectively, all cells are misses from the feature-centric perspective.

3. **Adaptive decision**
   - The decider returns either:
     - `DecisionBypass`: do not use cache for this request, or
     - `DecisionFill`: fetch data and populate cache.

4. **Bypass path**
   - For `DecisionBypass`, the engine:
     - calls the executor once with the original query,
     - does **not** update feature store or cell index,
     - passes the result to composer and returns it to the client.

5. **Fill path**
   - For `DecisionFill`, the engine:
     - treats all cells as “missing”,
     - runs the same per-cell fill logic as in the partial-hit path:
       - `fetchCell` → GeoServer per cell,
       - parse features,
       - write to feature store and cell index with the chosen TTL,
       - use per-cell bodies as miss shard pages for this request.

6. **Compose & return**
   - All shard pages for this request are passed to the aggregator and returned
     as a single FeatureCollection.
   - On the next query for the same region, the feature-centric cache can be
     used.

#### How to test the cache miss

Query a new region you haven’t hit before, you should see:

- Miss logs.
- After that, keys appear in Redis.
- Second query to the same region gives partial/full hit, and improved latency.

### 2.4 Data update and Kafka-driven invalidation

When the data in PostGIS changes, we send a message to Kafka. The
middleware consumes it and deletes any affected cache entries so clients
don’t get stale results. This connects database writes to cache invalidation.

**Pipeline**:

```bash
PostGIS -> Kafka -> Middleware -> Redis
```

#### The Event

Example event gets sent into Kafka:

```json
{
  "version": 1,
  "op": "update",
  "layer": "demo:NR_polygon",
  "ts": "2024-01-01T00:00:00Z",
  "bbox": {
    "x1": 11,
    "y1": 55,
    "x2": 12,
    "y2": 56,
    "srid": "EPSG:4326"
  }
}
```

- `op`: Type of change (update/insert/delete).
- `layer`: Which layer’s data changed.
- `bbox`: Spatial area that changed.
- `ts`: When the change happened.

#### Step by step for Kafka invalidation

1. **Event consumption**
   - Consumer receives a JSON event, it can be:
     - **WireEvent with explicit keys**: Event already knows exact cache keys.
     - **Spatial event**: Event has geometry/bbox; middleware has
       to re-map it to H3.

2. **Determine affected H3 cells**

   For spatial events, map the bbox or polygon to H3 cells and build the cache key
   prefixes for those cells.

3. **Delete Redis keys (cache + index)**

   For each affected cell and each configured resolution:
   - build the base cache key using the standard `<layer:res:cell:filters...>` format,
   - delete any corresponding **cell cache entries**,
   - delete any corresponding **cell index entries** (`idx:` keys).
     Hotness for those cells is reset so they no longer appear hot after data changes.

4. **Update "invalidated-at" timestamps**

   Internally, it keeps track of the latest invalidation time per layer. The
   feature store itself does not carry per-entry timestamps; instead, the cache
   engine uses the layer’s “invalidated-at” timestamp together with Redis TTLs
   to decide whether it is safe to serve from cache when “serve only if fresh”
   is enabled.

5. **Next time a query hits that area**

   Even if some Redis entries survive, they may be treated as stale depending on
   the layer’s invalidation time and freshness settings. Fresh results are fetched
   and cached again with new TTLs as needed.

#### How to test Kafka invalidation

1. Warm a region (so it has keys).

2. Send an invalidation event affecting that region.

3. Check:
   - Redis keys vanish for that area.
   - Next query to that region is a cache miss & refill.
   - Stale read metrics remain low.

### 2.5 Hotness tracking and adaptive caching

Not all areas are equal. Some regions are 'hot' (lots of queries),
others are 'cold'. We’d like to cache **hot** regions more aggressively
(maybe finer H3, longer TTL), and waste less memory on cold ones.”

#### Key concepts

- **Hotness per H3 cell**
  - Every time a cell is touched by a query, its hotness counter is increased.
  - Hotness decays over time: if there are no new queries, score drops.
  - This captures “recent popularity”, not just total hits.

- **Threshold and half-life**
  - `HOT_THRESHOLD`: if hotness > threshold ⇒ treat cell as hot.
  - `HOT_HALF_LIFE`: how fast scores decay (e.g. 1 minute).

- **Adaptive decisions**
  - The decider sees hotness and chooses:
    - Which **H3 resolution** to use for this query.
    - Which **TTL tier** to apply (e.g. cold/warm/hot).
    - Whether to:
      - **fill** the cache,
      - **bypass** it, or
      - **serve only if fresh** (fail with 412 if freshness cannot be guaranteed).
  - In **dry-run mode**, it logs decisions but doesn’t actually change behavior.
  - In **live mode**, it changes mapping & TTLs for real.

#### Step by step for hotness and adaptive caching

1. Query comes in, and H3 cells are computed.

2. For each cell:
   - Update exp-decay hotness.
   - The decider checks the new score.

3. Decider chooses:
   - Keep same resolution or switch to a different one.
   - Cache behaviour:
     - `fill` (use cache + possibly write),
     - `bypass` (go straight to GeoServer),
     - `serve_only_if_fresh` (serve from cache only if it is fresh; else 412).
   - TTL length (e.g. cold vs warm vs hot tier).

4. Cache keys and TTLs are chosen according to these decisions, and both the
   feature store and cell index entries use the selected TTL when written.

5. Cache and metrics reflect these choices.

#### How to test hotness and adaptive caching

- Run a highly skewed workload (Zipf).
- Watch:
  - `adaptive_decisions_total` increasing, broken down by `decision` and `reason`.
  - Hotness metrics to confirm that a small set of cells dominate the traffic.
  - TTLs for hot regions becoming longer (`valkey-cli TTL <key>`) compared to
    cold regions.
