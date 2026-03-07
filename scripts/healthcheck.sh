#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

master_health_ready() {
  local url="$1"
  curl -fsS "$url/healthz" >/dev/null 2>&1
}

worker_health_ready() {
  curl -fsS "$WORKER_URL/healthz" >/dev/null 2>&1
}

leader_ready() {
  find_leader_url >/dev/null 2>&1
}

metric_ready() {
  local url="$1"
  local metric_name="$2"
  curl -fsS "$url/metrics" | grep -q "^${metric_name}"
}

log_step "docker compose services"
compose ps

log_step "waiting for master health endpoints"
while IFS= read -r url; do
  if ! wait_until 30 master_health_ready "$url"; then
    echo "master health check failed: $url" >&2
    exit 1
  fi
  log_info "master health ok: $url"
done < <(master_urls)

log_step "waiting for worker health endpoint"
if ! wait_until 30 worker_health_ready; then
  echo "worker health check failed: $WORKER_URL" >&2
  exit 1
fi
worker_health="$(curl -fsS "$WORKER_URL/healthz")"
log_info "worker health ok: id=$(json_string "$worker_health" "worker_id") mongo_available=$(json_bool "$worker_health" "mongo_available")"

log_step "waiting for elected leader"
if ! wait_until 30 leader_ready; then
  echo "leader election did not converge in time" >&2
  exit 1
fi
leader_url="$(find_leader_url)"
leader_health="$(curl -fsS "$leader_url/healthz")"
log_info "leader ready: id=$(json_string "$leader_health" "master_id") url=$leader_url workers=$(json_number "$leader_health" "workers")"

log_step "checking metrics endpoints"
if ! metric_ready "$leader_url" "djs_master_is_leader"; then
  echo "master metrics missing expected metric at $leader_url" >&2
  exit 1
fi
if ! metric_ready "$WORKER_URL" "djs_worker_mongo_available"; then
  echo "worker metrics missing expected metric at $WORKER_URL" >&2
  exit 1
fi
log_info "metrics ok: master=$leader_url/metrics worker=$WORKER_URL/metrics"

log_step "healthcheck complete"
