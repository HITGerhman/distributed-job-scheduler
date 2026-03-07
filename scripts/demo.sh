#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

mode="${1:-full}"
leader_service=""
cron_job_id=""

create_shell_job() {
  local base_url="$1"
  local name="$2"
  local cron_expr="$3"
  local shell_script="$4"

  curl -fsS -X POST "$base_url/jobs" \
    -H 'Content-Type: application/json' \
    -d @- <<EOF
{"name":"$name","cron_expr":"$cron_expr","command":"sh","args":["-c","$shell_script"]}
EOF
}

get_instance_json() {
  local base_url="$1"
  local instance_id="$2"
  curl -fsS "$base_url/job-instances/$instance_id"
}

wait_for_instance_status() {
  local base_url="$1"
  local instance_id="$2"
  local want_status="$3"
  local timeout_secs="${4:-30}"
  local deadline=$(( $(date +%s) + timeout_secs ))
  local response=""
  local status=""

  while true; do
    response="$(get_instance_json "$base_url" "$instance_id" 2>/dev/null || true)"
    if [[ -n "$response" ]]; then
      status="$(json_string "$response" "status")"
      if [[ "$status" == "$want_status" ]]; then
        printf '%s\n' "$response"
        return 0
      fi
    fi
    if (( $(date +%s) >= deadline )); then
      echo "instance $instance_id did not reach status $want_status in time; last_response=${response:-<empty>}" >&2
      return 1
    fi
    sleep 1
  done
}

cleanup() {
  local exit_code=$?

  if [[ -n "$leader_service" ]]; then
    log_info "restarting stopped leader service: $leader_service"
    compose start "$leader_service" >/dev/null || true
  fi

  if [[ -n "$cron_job_id" ]]; then
    log_info "disabling demo cron job: $cron_job_id"
    mysql_query "UPDATE jobs SET enabled=0 WHERE id=${cron_job_id};" >/dev/null || true
  fi

  exit "$exit_code"
}
trap cleanup EXIT

show_cluster_summary() {
  log_step "cluster summary"
  while IFS= read -r url; do
    local health
    health="$(curl -fsS "$url/healthz")"
    log_info "$url role=$(json_string "$health" "role") leader=$(json_bool "$health" "is_leader") master_id=$(json_string "$health" "master_id") workers=$(json_number "$health" "workers")"
  done < <(master_urls)
  local worker_health
  worker_health="$(curl -fsS "$WORKER_URL/healthz")"
  log_info "$WORKER_URL worker_id=$(json_string "$worker_health" "worker_id") mongo_available=$(json_bool "$worker_health" "mongo_available")"
}

manual_job_demo() {
  local current_leader create_resp job_id trigger_resp instance_id instance_json worker_id

  current_leader="$(find_leader_url)"
  log_step "manual job demo: create -> dispatch -> success"
  create_resp="$(create_shell_job "$current_leader" \
    "demo-manual-$(date -u +%H%M%S)" \
    "@manual" \
    'echo hello-from-djs; echo dispatched-to-worker >&2')"
  job_id="$(json_number "$create_resp" "id")"
  log_info "created manual job: job_id=$job_id"

  trigger_resp="$(curl -fsS -X POST "$current_leader/jobs/$job_id/trigger")"
  instance_id="$(json_number "$trigger_resp" "job_instance_id")"
  log_info "triggered instance: instance_id=$instance_id"

  instance_json="$(wait_for_instance_status "$current_leader" "$instance_id" "SUCCESS" 20)"
  worker_id="$(json_string "$instance_json" "worker_id")"
  log_info "instance reached SUCCESS on worker=$worker_id"
  log_info "instance payload: $instance_json"
  log_info "log tail:"
  tail -n 5 "$ROOT_DIR/logs/job_${job_id}_instance_${instance_id}.log"
  pause_if_needed
}

kill_job_demo() {
  local current_leader create_resp job_id trigger_resp instance_id instance_json

  current_leader="$(find_leader_url)"
  log_step "kill demo: running instance -> kill -> KILLED"
  create_resp="$(create_shell_job "$current_leader" \
    "demo-kill-$(date -u +%H%M%S)" \
    "@manual" \
    "trap '' TERM; while :; do echo tick; sleep 1; done")"
  job_id="$(json_number "$create_resp" "id")"
  log_info "created kill job: job_id=$job_id"

  trigger_resp="$(curl -fsS -X POST "$current_leader/jobs/$job_id/trigger")"
  instance_id="$(json_number "$trigger_resp" "job_instance_id")"
  log_info "triggered long-running instance: instance_id=$instance_id"

  instance_json="$(wait_for_instance_status "$current_leader" "$instance_id" "RUNNING" 20)"
  log_info "instance is RUNNING: $instance_json"

  curl -fsS -X POST "$current_leader/job-instances/$instance_id/kill" \
    -H 'Content-Type: application/json' \
    -d '{"reason":"demo kill"}' >/dev/null
  instance_json="$(wait_for_instance_status "$current_leader" "$instance_id" "KILLED" 20)"
  log_info "instance reached KILLED: $instance_json"
  log_info "kill log tail:"
  tail -n 8 "$ROOT_DIR/logs/job_${job_id}_instance_${instance_id}.log"
  pause_if_needed
}

failover_demo() {
  local current_leader_url current_leader_id create_resp job_id initial_count next_count new_leader_id attempt

  current_leader_url="$(find_leader_url)"
  current_leader_id="$(leader_id)"
  log_step "failover demo: cron scheduling survives leader switch"
  create_resp="$(create_shell_job "$current_leader_url" \
    "demo-cron-$(date -u +%H%M%S)" \
    '*/5 * * * * *' \
    'echo cron-alive')"
  job_id="$(json_number "$create_resp" "id")"
  cron_job_id="$job_id"
  log_info "created cron job: job_id=$job_id current_leader=$current_leader_id"

  initial_count=0
  for attempt in $(seq 1 20); do
    initial_count="$(mysql_query "SELECT COUNT(*) FROM job_instances WHERE job_id=${job_id};" | tail -n1)"
    if [[ -n "$initial_count" && "$initial_count" =~ ^[0-9]+$ && "$initial_count" -gt 0 ]]; then
      break
    fi
    sleep 1
  done
  if [[ -z "$initial_count" || ! "$initial_count" =~ ^[0-9]+$ || "$initial_count" -le 0 ]]; then
    echo "cron job $job_id did not start in time" >&2
    return 1
  fi
  log_info "cron has started firing: instances=$initial_count"

  leader_service="$current_leader_id"
  compose stop "$leader_service" >/dev/null
  log_info "stopped leader service: $leader_service"

  new_leader_id=""
  for attempt in $(seq 1 20); do
    new_leader_id="$(observed_leader_id 2>/dev/null || true)"
    if [[ -n "$new_leader_id" && "$new_leader_id" != "$current_leader_id" ]]; then
      break
    fi
    sleep 1
  done
  if [[ -z "$new_leader_id" || "$new_leader_id" == "$current_leader_id" ]]; then
    echo "leader did not change from $current_leader_id in time" >&2
    return 1
  fi

  next_count="$initial_count"
  for attempt in $(seq 1 20); do
    next_count="$(mysql_query "SELECT COUNT(*) FROM job_instances WHERE job_id=${job_id};" | tail -n1)"
    if [[ -n "$next_count" && "$next_count" =~ ^[0-9]+$ && "$next_count" -gt "$initial_count" ]]; then
      break
    fi
    sleep 1
  done
  if [[ -z "$next_count" || ! "$next_count" =~ ^[0-9]+$ || "$next_count" -le "$initial_count" ]]; then
    echo "cron job $job_id did not continue after failover in time" >&2
    return 1
  fi

  log_info "new leader elected: $new_leader_id"
  log_info "cron continued after failover: before=$initial_count after=$next_count"

  compose start "$leader_service" >/dev/null
  log_info "restarted old leader service: $leader_service"
  leader_service=""

  mysql_query "UPDATE jobs SET enabled=0 WHERE id=${job_id};" >/dev/null
  log_info "disabled cron demo job: $job_id"
  cron_job_id=""
  pause_if_needed
}

main() {
  log_step "demo mode: $mode"
  "$SCRIPT_DIR/healthcheck.sh"
  show_cluster_summary
  pause_if_needed

  case "$mode" in
    full)
      manual_job_demo
      kill_job_demo
      failover_demo
      ;;
    failover)
      failover_demo
      ;;
    *)
      echo "unsupported demo mode: $mode" >&2
      exit 1
      ;;
  esac

  log_step "demo completed"
}

main "$@"
