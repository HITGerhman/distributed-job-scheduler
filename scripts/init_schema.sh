#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE_FILE="$ROOT_DIR/deploy/docker-compose.yml"
SCHEMA_FILE="$ROOT_DIR/schema.sql"

if [[ ! -f "$SCHEMA_FILE" ]]; then
  echo "schema file not found: $SCHEMA_FILE" >&2
  exit 1
fi

# Ensure mysql service is up.
docker compose -f "$COMPOSE_FILE" up -d mysql >/dev/null

# Apply schema using container env MYSQL_ROOT_PASSWORD.
docker compose -f "$COMPOSE_FILE" exec -T mysql sh -lc 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD"' < "$SCHEMA_FILE"

echo "schema applied: $SCHEMA_FILE"
