# Scenarios

<!--toc:start-->
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
<!--toc:end-->

All scenarios share the same input (HTTP request, layer, bbox/polygon, filters),
they all produce the same kind of output (GeoJSON FeatureCollection / possibly GML),
but the difference is how they get the data and what logic they apply.
There are 2 scenarios implemented for this middleware:

- `baseline`: no cache, just pass-through, but with tracking.
- `cache`: uses redis + hotness + adaptive decisions.
- Then "sub-scenarios"/modes layered on top of `cache`:
  - Full cache hit
  - Partial cache hit
  - Cache miss
  - Kafka invalidation
  - Hotness & adaptive caching
  - Monitoring/metrics

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

After sending a request, you can check the middleware logs. It will show 3 things:

- **Incoming HTTP request**: So it confirms that the middleware received a
  /query request.
- **H3 mapping**: Shows the request footprint (bbox/polygon) converted to H3 cells.
- **Cache decision**: In baseline, it will never cache, but it will still print
  the result of the hotness-based decision engine

## 2. Cache scenario

### 2.1 Full cache hit (cache scenario, best case)

It has seen the region before, and the H3 cells are already in Redis.
So we can answer entirely from cache. No GeoServer, no PostGIS for this request.
So for a full hit, every hex cell needed should already be in Redis.

**Pipeline**:

```bash
Client -> Middleware -> Redis -> Client
```

#### Step by step for full cache hit

1. **Same initial steps**

    - `/query` → router validates → cache scenario chosen.

2. **Map query footprint → H3 cells**

    - The engine figures out which H3 cells cover the requested geometry.
    - Uses the chosen resolution (e.g. res 7/8/9).

3. **Build Redis keys**

    - For each cell, it builds a key like:

      ```text
      demo:NR_polygon:res7:<h3cell>:filters=<normalized>:f=<hash>
      ```

    - The hash ensures that if filters change,
    we don’t accidentally reuse a wrong page.

4. **Do a batch read (MGET) from Redis**

    - We do a single MGET call to get all the keys (so all needed h3 cells to
    cover the query area).

5. **Check freshness**

    For each value, it has a header: `SC1 + [timestamp] + payload (GeoJSON data)`
    (SCI is just a label, and it stands for "spatial cache 1").
    Middleware compares that timestamp with the latest Kafka invalidation timestamp
    for that layer/region: if a cache entry is older than the invalidation, it is
    considered stale. A full hit requires all cells present and fresh.

6. **If full hit: compose directly**

    The composer just stitches together the cached "pages" (each cell's GeoJSON),
    and there is no call to GeoServer at all.

7. **Return response**

#### How to test the full cache hit

Warm the region first, so it needs to be hit several times
(you can run the load generator). Then check Redis (install a tool like `valkey-cli`):

  ```bash
  valkey-cli --scan --pattern 'demo:NR_polygon*'
  ```

You should see cached keys for the H3 cells.

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
Client -> Middleware -> Redis + GeoServer/PostGIS -> Redis -> Client
```

#### Step by step for partial cache hit

1. **Same initial steps**
    - `/query` → router → cache scenario → map to H3 cells.

2. **Build Redis keys**

3. **Split cells into two groups**

    - **Hit group**: cells with fresh entries in Redis.
    - **Miss group**: cells that are missing or stale.

4. **Fetch only the missing cells from GeoServer**

    - It issues GeoServer requests per missing cell (or per chunk).
    - Results come back as GeoJSON.

5. **Write newly fetched cells into Redis**

    - Each new cell is stored with:
      - SC1 header timestamp.
      - A TTL chosen by the TTL rules (baseline or adaptive).

6. **Composer merges both sources**

   - Cached + freshly fetched data are merged into the final FeatureCollection.

7. **Return response**

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
    - `/query` → router → cache scenario → map H3 cells → build keys.

2. **Check Redis (MGET)**
    - `MGET` returns **nothing** (or all stale) → all cells are misses.

3. **Decider check**:

    If `DecisionBypass` and “serve only if fresh” is enabled, it might return HTTP
    412 (Precondition Failed) to the client to indicate that it cannot safely serve
    stale data. Otherwise, it calls GeoServer to fetch all data, stores each
    cell in Redis, and returns a normal 200 response.

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
    "x1": 11, "y1": 55, "x2": 12, "y2": 56,
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

3. **Delete Redis keys**

    For each affected cell, run `DEL` to remove cache entries. Reset hotness for
    those cells so they don’t look artificially hot.

4. **Update "invalidated-at" timestamps**

    Internally, it keeps track of the latest invalidation time per layer or region.
    The pattern is that a cache entry has a timestamp in its SC1 header, while the
    middleware knows the last invalidation time for each layer. If the cache
    timestamp is earlier than the last invalidation time, the entry is considered
    stale.

5. **Next time a query hits that area**

    Even if a key survived, it’s treated as stale and refetched.
    Fresh result is cached with a new timestamp and TTL.

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
    - Which **H3 resolution** to use.
    - Which **TTL tier** to apply for that cell (short/medium/long).
  - In **dry-run mode**, it logs decisions but doesn’t actually change behavior.
  - In **live mode**, it changes mapping & TTLs for real.

#### Step by step for hotness and adaptive caching

1. Query comes in, and H3 cells are computed.

2. For each cell:
    - Update exp-decay hotness.
    - The decider checks the new score.

3. Decider chooses:
    - Keep same resolution or switch to a different one.
    - Cache or bypass.
    - TTL length (e.g. 30s vs 5min).

4. Cache keys and TTLs are chosen according to these decisions.

5. Cache and metrics reflect these choices.

#### How to test hotness and adaptive caching

- Run a highly skewed workload (Zipf).
- Watch:
  - `adaptive_decisions_total` increasing.
  - Number of “hot keys” rising for hot areas.
  - TTLs for hot regions becoming longer (`redis-cli TTL <key>`).
