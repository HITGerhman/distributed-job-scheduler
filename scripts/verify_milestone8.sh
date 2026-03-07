#!/usr/bin/env bash

set -euo pipefail

MASTER_URLS="${MASTER_URLS:-http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082}"
WORKER_URL="${WORKER_URL:-http://127.0.0.1:8083}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-mysql}"
MONGO_CONTAINER="${MONGO_CONTAINER:-mongo}"
MYSQL_DB="${MYSQL_DB:-DJS}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-172600}"
TRIGGERS="${TRIGGERS:-100}"
PARALLELISM="${PARALLELISM:-50}"
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-60}"
EXPECTED_LOGS_PER_RUN="${EXPECTED_LOGS_PER_RUN:-4}"

find_leader_url() {
  local url health
  IFS=',' read -r -a urls <<<"$MASTER_URLS"
  for url in "${urls[@]}"; do
    health="$(curl -fsS "$url/healthz" 2>/dev/null || true)"
    if [[ -n "$health" && "$health" == *'"is_leader":true'* ]]; then
      printf '%s\n' "$url"
      return 0
    fi
  done

  for url in "${urls[@]}"; do
    if curl -fsS "$url/healthz" >/dev/null 2>&1; then
      printf '%s\n' "$url"
      return 0
    fi
  done
  return 1
}

metric_value() {
  local url="$1"
  local name="$2"
  curl -fsS "$url/metrics" | awk -v metric="$name" '$1 == metric { print $2; found=1; exit } END { if (!found) print 0 }'
}

mysql_query() {
  local sql="$1"
  docker exec -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" \
    mysql -uroot -NB "$MYSQL_DB" -e "$sql" | tail -n 1
}

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
leader_url="$(find_leader_url)"

if [[ -z "$leader_url" ]]; then
  echo "failed to locate an available master leader" >&2
  exit 1
fi

baseline_success="$(metric_value "$leader_url" 'djs_master_job_terminal_total{status="success"}')"
baseline_retries="$(metric_value "$leader_url" 'djs_master_retry_total{reason="execution_failure"}')"
baseline_pending_high="$(metric_value "$leader_url" 'djs_master_queue_high_watermark{state="pending"}')"
baseline_running_high="$(metric_value "$leader_url" 'djs_master_queue_high_watermark{state="running"}')"
baseline_log_entries="$(metric_value "$WORKER_URL" 'djs_worker_log_batch_entries_total')"
baseline_log_flushes="$(metric_value "$WORKER_URL" 'djs_worker_log_batch_flush_total')"
baseline_log_dropped="$(metric_value "$WORKER_URL" 'djs_worker_log_dropped_total')"

create_payload="$(cat <<EOF
{"name":"milestone8-verify-${stamp}","cron_expr":"@manual","command":"sh","args":["-c","echo out; echo err >&2"],"enabled":false}
EOF
)"

create_resp="$(curl -fsS -X POST "$leader_url/jobs" -H 'Content-Type: application/json' -d "$create_payload")"
job_id="$(printf '%s\n' "$create_resp" | grep -o '"id":[0-9]*' | head -n1 | cut -d: -f2)"

if [[ -z "$job_id" ]]; then
  echo "failed to parse job id from response: $create_resp" >&2
  exit 1
fi

trigger_url="$leader_url/jobs/$job_id/trigger"
export trigger_url
start_ms="$(date +%s%3N)"
seq "$TRIGGERS" | xargs -P "$PARALLELISM" -I{} sh -c 'curl -fsS -X POST "$trigger_url" >/dev/null'
end_ms="$(date +%s%3N)"
elapsed_ms="$((end_ms - start_ms))"

deadline="$(( $(date +%s) + WAIT_TIMEOUT_SECS ))"
while :; do
  total_count="$(mysql_query "SELECT COUNT(*) FROM job_instances WHERE job_id=${job_id};")"
  terminal_count="$(mysql_query "SELECT COUNT(*) FROM job_instances WHERE job_id=${job_id} AND status IN ('SUCCESS','FAILED','KILLED');")"
  if [[ "$total_count" == "$TRIGGERS" && "$terminal_count" == "$TRIGGERS" ]]; then
    break
  fi
  if (( "$(date +%s)" >= deadline )); then
    echo "timeout waiting for job instances to finish: job_id=$job_id total=$total_count terminal=$terminal_count" >&2
    exit 1
  fi
  sleep 1
done

success_count="$(mysql_query "SELECT COUNT(*) FROM job_instances WHERE job_id=${job_id} AND status='SUCCESS';")"
distinct_slots="$(mysql_query "SELECT COUNT(DISTINCT scheduled_at) FROM job_instances WHERE job_id=${job_id};")"
mongo_logs="$(docker exec "$MONGO_CONTAINER" mongosh --quiet "mongodb://127.0.0.1:27017/${MYSQL_DB}" --eval "db.job_logs.countDocuments({job_id: ${job_id}})")"

after_success="$(metric_value "$leader_url" 'djs_master_job_terminal_total{status="success"}')"
after_pending_high="$(metric_value "$leader_url" 'djs_master_queue_high_watermark{state="pending"}')"
after_running_high="$(metric_value "$leader_url" 'djs_master_queue_high_watermark{state="running"}')"
after_log_entries="$(metric_value "$WORKER_URL" 'djs_worker_log_batch_entries_total')"
after_log_flushes="$(metric_value "$WORKER_URL" 'djs_worker_log_batch_flush_total')"
after_log_dropped="$(metric_value "$WORKER_URL" 'djs_worker_log_dropped_total')"
worker_mongo_available="$(metric_value "$WORKER_URL" 'djs_worker_mongo_available')"

expected_logs="$((TRIGGERS * EXPECTED_LOGS_PER_RUN))"
success_delta="$((after_success - baseline_success))"
log_entries_delta="$((after_log_entries - baseline_log_entries))"
log_flushes_delta="$((after_log_flushes - baseline_log_flushes))"
log_dropped_delta="$((after_log_dropped - baseline_log_dropped))"

printf 'leader_url=%s\n' "$leader_url"
printf 'job_id=%s\n' "$job_id"
printf 'trigger_count=%s\n' "$TRIGGERS"
printf 'parallelism=%s\n' "$PARALLELISM"
printf 'submit_elapsed_ms=%s\n' "$elapsed_ms"
printf 'mysql_success=%s\n' "$success_count"
printf 'distinct_slots=%s\n' "$distinct_slots"
printf 'mongo_logs=%s\n' "$mongo_logs"
printf 'master_success_delta=%s\n' "$success_delta"
printf 'master_retries_baseline=%s\n' "$baseline_retries"
printf 'master_pending_high_before=%s\n' "$baseline_pending_high"
printf 'master_pending_high_after=%s\n' "$after_pending_high"
printf 'master_running_high_before=%s\n' "$baseline_running_high"
printf 'master_running_high_after=%s\n' "$after_running_high"
printf 'worker_log_entries_delta=%s\n' "$log_entries_delta"
printf 'worker_log_flushes_delta=%s\n' "$log_flushes_delta"
printf 'worker_log_dropped_delta=%s\n' "$log_dropped_delta"
printf 'worker_mongo_available=%s\n' "$worker_mongo_available"

if [[ "$success_count" != "$TRIGGERS" ]]; then
  echo "unexpected success count: want=$TRIGGERS got=$success_count" >&2
  exit 1
fi
if [[ "$distinct_slots" != "$TRIGGERS" ]]; then
  echo "unexpected distinct scheduled_at count: want=$TRIGGERS got=$distinct_slots" >&2
  exit 1
fi
if [[ "$mongo_logs" != "$expected_logs" ]]; then
  echo "unexpected mongo log count: want=$expected_logs got=$mongo_logs" >&2
  exit 1
fi
if [[ "$success_delta" != "$TRIGGERS" ]]; then
  echo "unexpected success metric delta: want=$TRIGGERS got=$success_delta" >&2
  exit 1
fi
if [[ "$log_entries_delta" != "$expected_logs" ]]; then
  echo "unexpected log entry metric delta: want=$expected_logs got=$log_entries_delta" >&2
  exit 1
fi
if [[ "$log_dropped_delta" != "0" ]]; then
  echo "worker dropped logs during verification: delta=$log_dropped_delta" >&2
  exit 1
fi
if [[ "$worker_mongo_available" != "1" ]]; then
  echo "worker mongo availability metric is not healthy: value=$worker_mongo_available" >&2
  exit 1
fi
