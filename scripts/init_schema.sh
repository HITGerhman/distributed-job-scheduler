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

# Apply schema using container env MYSQL_ROOT_PASSWORD without command-line password warnings.
docker compose -f "$COMPOSE_FILE" exec -T mysql sh -lc 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot' < "$SCHEMA_FILE"

ensure_column() {
  local table_name=$1
  local column_name=$2
  local column_ddl=$3

  local exists
  exists=$(docker compose -f "$COMPOSE_FILE" exec -T mysql sh -lc \
    "MYSQL_PWD=\"\$MYSQL_ROOT_PASSWORD\" mysql -N -B -uroot -e \"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='DJS' AND table_name='${table_name}' AND column_name='${column_name}';\"")

  if [[ "$exists" == "0" ]]; then
    docker compose -f "$COMPOSE_FILE" exec -T mysql sh -lc \
      "MYSQL_PWD=\"\$MYSQL_ROOT_PASSWORD\" mysql -uroot -e \"USE DJS; ALTER TABLE ${table_name} ADD COLUMN ${column_name} ${column_ddl};\""
  fi
}

ensure_column jobs max_retries "INT UNSIGNED NOT NULL DEFAULT 2 AFTER timeout_seconds"
ensure_column jobs retry_backoff_seconds "INT UNSIGNED NOT NULL DEFAULT 2 AFTER max_retries"
ensure_column jobs max_retry_backoff_seconds "INT UNSIGNED NOT NULL DEFAULT 30 AFTER retry_backoff_seconds"
ensure_column jobs heartbeat_timeout_seconds "INT UNSIGNED NOT NULL DEFAULT 15 AFTER max_retry_backoff_seconds"

ensure_column job_instances attempt "INT UNSIGNED NOT NULL DEFAULT 0 AFTER status"
ensure_column job_instances max_attempts "INT UNSIGNED NOT NULL DEFAULT 1 AFTER attempt"
ensure_column job_instances retry_backoff_seconds "INT UNSIGNED NOT NULL DEFAULT 2 AFTER max_attempts"
ensure_column job_instances max_retry_backoff_seconds "INT UNSIGNED NOT NULL DEFAULT 30 AFTER retry_backoff_seconds"
ensure_column job_instances heartbeat_timeout_seconds "INT UNSIGNED NOT NULL DEFAULT 15 AFTER max_retry_backoff_seconds"
ensure_column job_instances next_retry_at "DATETIME(3) NULL AFTER heartbeat_timeout_seconds"
ensure_column job_instances last_heartbeat_at "DATETIME(3) NULL AFTER next_retry_at"

echo "schema applied: $SCHEMA_FILE"
