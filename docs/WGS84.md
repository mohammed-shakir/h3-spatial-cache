# Convert to WGS84 polygon

To convert a polygon in the database to WGS84 (EPSG:4326),
first find the nvrid of the polygon you want to convert,
then you can run the following SQL query in PostGIS:

```sql
SELECT
  nvrid,
  ST_AsGeoJSON(
    ST_Transform(geom, 4326),
    6
  ) AS geom_wgs84
FROM shakir."NR_polygon"
WHERE nvrid = '2010953';
```

This returns something like:

```json
{"type":"Polygon","coordinates":[[[13.301289,61.766764],...,[13.301289,61.766764]]]}
```

Then you can use this WGS84 polygon to make queries to the middleware:

```bash
curl -G "http://localhost:8090/query" \
  --data-urlencode 'layer=demo:NR_polygon' \
  --data-urlencode 'polygon=<THE WGS84 POLYGON JSON HERE>'
```

## BBOX

If you want a BBOX from the DB polygon instead, you can use:

```sql
WITH wgs AS (
  SELECT ST_Transform(geom, 4326) AS g
  FROM shakir."NR_polygon"
  WHERE nvrid = '2010953'
)
SELECT 
  ST_XMin(g), ST_YMin(g),
  ST_XMax(g), ST_YMax(g)
FROM wgs;
```

Use the coordinates to make a BBOX query:

```bash
curl -s 'http://localhost:8090/query?layer=demo:NR_polygon&bbox=<MINX>,<MINY>,<MAXX>,<MAXY>,EPSG:4326'
```
