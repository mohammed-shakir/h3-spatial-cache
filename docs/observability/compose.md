# Observability via docker compose (prometheus + grafana + loki)

This brings up prometheus, grafana, loki, and promtail.
It scrapes the middleware's `/metrics` and takes its container logs.

```bash
./scripts/dev-up.sh

# start observability
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.yml \
  up -d prometheus grafana loki promtail

# (optional) run middleware inside compose
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.yml --profile app \
  up -d app
```
