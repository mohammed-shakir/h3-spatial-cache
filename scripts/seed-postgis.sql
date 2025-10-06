-- For first db init
CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS places;
CREATE TABLE places (
  id   SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  geom geometry(Point, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_places_geom ON places USING GIST (geom);

INSERT INTO places (name, geom) VALUES
('Luleå',       ST_SetSRID(ST_MakePoint(22.1547, 65.5848), 4326)),
('Stockholm',   ST_SetSRID(ST_MakePoint(18.0686, 59.3293), 4326)),
('Göteborg',    ST_SetSRID(ST_MakePoint(11.9746, 57.7089), 4326)),
('Malmö',       ST_SetSRID(ST_MakePoint(13.0038, 55.6050), 4326));
