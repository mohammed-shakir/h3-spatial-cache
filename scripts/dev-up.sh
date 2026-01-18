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
: "${KAFKA_TOPIC:=spatial-invalidation}"
: "${KAFKA_PARTITIONS:=6}"
: "${KAFKA_REPLICATION_FACTOR:=1}"
: "${POSTGRES_DB:=gis}"
: "${POSTGRES_USER:=gis}"
: "${POSTGRES_PASSWORD:=gis}"
: "${WS:=demo}"
: "${STORE:=pg}"
: "${PG_SCHEMA:=shakir}"

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
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:9092 \
	--create --if-not-exists \
	--topic "${KAFKA_TOPIC}" \
	--partitions "${KAFKA_PARTITIONS}" \
	--replication-factor "${KAFKA_REPLICATION_FACTOR}" >/dev/null 2>&1 || true

# Configure geoserver
GS="http://localhost:${GEOSERVER_PORT}/geoserver"
BASIC_AUTH="admin:geoserver"

if ! curl -fs -u "$BASIC_AUTH" "$GS/rest/workspaces/${WS}.json" >/dev/null; then
	echo "Create workspace ${WS}"
	curl -fsS -u "$BASIC_AUTH" -X POST -H "Content-type: application/json" \
		-d "{\"workspace\":{\"name\":\"${WS}\"}}" \
		"$GS/rest/workspaces"
else
	echo "Workspace ${WS} already exists"
fi

DS_PAYLOAD=$(
	cat <<JSON
{
  "dataStore": {
    "name": "${STORE}",
    "enabled": true,
    "connectionParameters": {
      "entry": [
        {"@key":"host","$":"postgis"},
        {"@key":"port","$":"5432"},
        {"@key":"database","$":"${POSTGRES_DB}"},
        {"@key":"schema","$":"${PG_SCHEMA}"},
        {"@key":"user","$":"${POSTGRES_USER}"},
        {"@key":"passwd","$":"${POSTGRES_PASSWORD}"},
        {"@key":"dbtype","$":"postgis"}
      ]
    }
  }
}
JSON
)

if ! curl -fs -u "$BASIC_AUTH" "$GS/rest/workspaces/${WS}/datastores/${STORE}.json" >/dev/null; then
	echo "Create datastore ${STORE} in ${WS}"
	echo "$DS_PAYLOAD" | curl -fsS -u "$BASIC_AUTH" -X POST -H "Content-type: application/json" \
		-d @- "$GS/rest/workspaces/${WS}/datastores"
else
	echo "Update datastore ${STORE} in ${WS}"
	echo "$DS_PAYLOAD" | curl -fsS -u "$BASIC_AUTH" -X PUT -H "Content-type: application/json" \
		-d @- "$GS/rest/workspaces/${WS}/datastores/${STORE}.json"
fi

AVAILABLE=$(curl -fs -u "$BASIC_AUTH" \
	"$GS/rest/workspaces/${WS}/datastores/${STORE}/featuretypes.json?list=available" |
	jq -r '.list.string[]?')

for FT in $AVAILABLE; do
	if curl -fs -u "$BASIC_AUTH" "$GS/rest/layers/${WS}:${FT}.json" >/dev/null; then
		echo "Layer ${WS}:${FT} already published"
	else
		echo "Publishing ${FT}"
		curl -fsS -u "$BASIC_AUTH" -X POST -H "Content-type: application/json" \
			-d "{\"featureType\":{\"name\":\"${FT}\",\"srs\":\"EPSG:3006\"}}" \
			"$GS/rest/workspaces/${WS}/datastores/${STORE}/featuretypes"
	fi
done

CONFIGURED=$(
	curl -fs -u "$BASIC_AUTH" \
		"$GS/rest/workspaces/${WS}/datastores/${STORE}/featuretypes.json?list=configured" |
		jq -r '.list.string[]?'
)
echo "Configured feature types in ${WS}/${STORE}:"
echo "$CONFIGURED"

FIRST_FT=$(echo "$AVAILABLE" | head -n1 || true)
if [[ -n "${FIRST_FT:-}" ]]; then
	WFS_URL="${GS}/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=${WS}:${FIRST_FT}&outputFormat=application/json&count=1"
	echo "WFS call:"
	curl -fsS "$WFS_URL" | head -c 1024 || true
	echo
	echo "Try this WFS (GeoJSON):"
	echo "$WFS_URL"
else
	echo "No unpublished tables found in datastore ${STORE} (schema ${PG_SCHEMA})."
fi
