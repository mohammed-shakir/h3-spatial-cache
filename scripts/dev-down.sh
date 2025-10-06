#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$ROOT_DIR/deploy/compose"

KEEP_VOLUMES="${1:-}"

if [[ "$KEEP_VOLUMES" == "--keep-volumes" ]]; then
	echo "Stopping containers (keeping volumes)"
	docker compose --env-file "$COMPOSE_DIR/.env" -f "$COMPOSE_DIR/docker-compose.yml" down
else
	echo "Stopping containers and removing volumes"
	docker compose --env-file "$COMPOSE_DIR/.env" -f "$COMPOSE_DIR/docker-compose.yml" down -v
fi
