#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/deploy/docker-compose.yml}"
MASTER_URLS="${MASTER_URLS:-http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082}"
WORKER_URL="${WORKER_URL:-http://127.0.0.1:8083}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-mysql}"
MYSQL_DB="${MYSQL_DB:-DJS}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-172600}"
MONGO_CONTAINER="${MONGO_CONTAINER:-mongo}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-1}"

timestamp() {
  date '+%H:%M:%S'
}

log_step() {
  printf '\n[%s] %s\n' "$(timestamp)" "$*"
}

log_info() {
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

pause_if_needed() {
  if [[ "${DEMO_PAUSE:-0}" == "1" ]]; then
    read -r -p "Press Enter to continue... "
  fi
}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

master_urls() {
  local IFS=','
  local urls=()
  read -r -a urls <<<"$MASTER_URLS"
  printf '%s\n' "${urls[@]}"
}

json_number() {
  local body="$1"
  local key="$2"
  printf '%s\n' "$body" | grep -o "\"${key}\":[0-9]\+" | head -n1 | cut -d: -f2 || true
}

json_string() {
  local body="$1"
  local key="$2"
  printf '%s\n' "$body" | sed -n "s/.*\"${key}\":\"\\([^\"]*\\)\".*/\\1/p" | head -n1
}

json_bool() {
  local body="$1"
  local key="$2"
  printf '%s\n' "$body" | grep -o "\"${key}\":\(true\|false\)" | head -n1 | cut -d: -f2 || true
}

wait_until() {
  local timeout_secs="$1"
  shift

  local deadline=$(( $(date +%s) + timeout_secs ))
  while true; do
    if "$@"; then
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      return 1
    fi
    sleep "$POLL_INTERVAL_SECS"
  done
}

mysql_query() {
  local sql="$1"
  docker exec -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" \
    mysql -uroot -NB "$MYSQL_DB" -e "$sql" | tr -d '\r'
}

find_leader_url() {
  local url health
  while IFS= read -r url; do
    health="$(curl -fsS "$url/healthz" 2>/dev/null || true)"
    if [[ -n "$health" && "$(json_bool "$health" "is_leader")" == "true" ]]; then
      printf '%s\n' "$url"
      return 0
    fi
  done < <(master_urls)
  return 1
}

leader_id() {
  local leader_url health
  leader_url="$(find_leader_url)" || return 1
  health="$(curl -fsS "$leader_url/healthz")"
  json_string "$health" "master_id"
}

observed_leader_id() {
  local url health observed
  while IFS= read -r url; do
    health="$(curl -fsS "$url/healthz" 2>/dev/null || true)"
    if [[ -z "$health" ]]; then
      continue
    fi
    observed="$(json_string "$health" "leader_id")"
    if [[ -n "$observed" ]]; then
      printf '%s\n' "$observed"
      return 0
    fi
    if [[ "$(json_bool "$health" "is_leader")" == "true" ]]; then
      json_string "$health" "master_id"
      return 0
    fi
  done < <(master_urls)
  return 1
}
