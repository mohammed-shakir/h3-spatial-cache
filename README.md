# Adaptive Caching of Spatial Queries in PostGIS Using H3

## Description

A Go middleware that intercepts GeoServer/PostGIS requests, buckets spatial footprints into **H3** hex cells, serves hot regions from **Redis**, and keeps results fresh via **TTL + Kafka**–driven invalidations.

---

## Prerequisites

- [**Docker Engine**](https://docs.docker.com/engine/install/) and [**Docker Compose v2**](https://docs.docker.com/compose/install/)
- [**Go**](https://go.dev/doc/install)
- [**H3 Go bindings**](https://github.com/uber/h3-go)
- [**Git**](https://git-scm.com/downloads)

---

## Running the program

**TODO**
