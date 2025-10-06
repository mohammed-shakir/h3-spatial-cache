#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$ROOT_DIR/deploy/compose"

cd "$COMPOSE_DIR"

# Load .env
set -a
[ -f .env ] && . ./.env || true
set +a

# Default values
: "${GEOSERVER_PORT:=8080}"
: "${KAFKA_TOPIC:=spatial-updates}"
: "${POSTGRES_DB:=gis}"
: "${POSTGRES_USER:=gis}"
: "${POSTGRES_PASSWORD:=gis}"

echo "Docker compose up"
docker compose up -d --build

wait_healthy() {
	local svc="$1"
	echo -n "Waiting for $svc to be healthy"
	for _ in {1..90}; do
		status="$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo 'starting')"
		if [[ "$status" == "healthy" ]]; then
			echo "$svc is healthy"
			return 0
		fi
		echo -n "."
		sleep 2
	done
	echo "Timed out"
	docker compose ps
	exit 1
}

wait_healthy postgis
wait_healthy redis
wait_healthy kafka
wait_healthy geoserver

# Kafka
echo "Ensure kafka topic exists: ${KAFKA_TOPIC}"
docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--create --if-not-exists --replication-factor 1 --partitions 1 \
	--topic "${KAFKA_TOPIC}" >/dev/null 2>&1 || true

# Configure geoserver
GS="http://localhost:${GEOSERVER_PORT}/geoserver"
BASIC_AUTH="admin:geoserver"

echo "Create workspace demo"
curl -fsS -u "$BASIC_AUTH" -X POST -H "Content-type: application/json" \
	-d '{"workspace":{"name":"demo"}}' \
	"$GS/rest/workspaces" || true

echo "Create PostGIS store pg in workspace demo"
curl -fsS -u "$BASIC_AUTH" -X POST -H "Content-type: application/json" \
	-d @- "$GS/rest/workspaces/demo/datastores" <<JSON || true
{
  "dataStore": {
    "name": "pg",
    "connectionParameters": {
      "host": "postgis",
      "port": "5432",
      "database": "${POSTGRES_DB}",
      "user": "${POSTGRES_USER}",
      "passwd": "${POSTGRES_PASSWORD}",
      "dbtype": "postgis",
      "schema": "public"
    }
  }
}
JSON

echo "Publish featuretype places"
curl -fsS -u "$BASIC_AUTH" -X POST -H "Content-type: application/json" \
	-d '{"featureType":{"name":"places","nativeName":"places","srs":"EPSG:4326"}}' \
	"$GS/rest/workspaces/demo/datastores/pg/featuretypes" || true

WFS_URL="${GS}/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=demo:places&outputFormat=application/json&count=2"
echo "WFS call:"
curl -fsS "$WFS_URL" | head -c 1024
echo

echo "Done"
echo "Try this WFS (GeoJSON):"
echo "$WFS_URL"
