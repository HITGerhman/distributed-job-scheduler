#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF'
usage: scripts/experiments/run_failover_trial.sh [-config configs/local.yaml] [-run-dir runtime/experiments/...]

Start 2 masters + 1 worker with isolated temp configs, schedule one near-future mock job,
kill the current leader, and emit a JSON summary for takeover and first post-failover dispatch.
EOF
}

BASE_CONFIG="configs/local.yaml"
RUN_DIR=""
TIMEZONE="Asia/Shanghai"
STARTUP_TIMEOUT_SEC=60
FAILOVER_TIMEOUT_SEC=45
DISPATCH_TIMEOUT_SEC=90
KILL_BEFORE_SEC=3
MIN_SLOT_LEAD_SEC=25
PAYLOAD_DURATION_MS=1000
KILL_SIGNAL="KILL"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -config)
      BASE_CONFIG="$2"
      shift 2
      ;;
    -run-dir)
      RUN_DIR="$2"
      shift 2
      ;;
    -timezone)
      TIMEZONE="$2"
      shift 2
      ;;
    -startup-timeout)
      STARTUP_TIMEOUT_SEC="$2"
      shift 2
      ;;
    -failover-timeout)
      FAILOVER_TIMEOUT_SEC="$2"
      shift 2
      ;;
    -dispatch-timeout)
      DISPATCH_TIMEOUT_SEC="$2"
      shift 2
      ;;
    -kill-before-sec)
      KILL_BEFORE_SEC="$2"
      shift 2
      ;;
    -kill-signal)
      KILL_SIGNAL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd bash go python3 date kill

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "${RUN_DIR}" ]]; then
  RUN_DIR="runtime/experiments/failover-${RUN_ID}"
fi

MASTER1_ID="exp-master-1"
MASTER2_ID="exp-master-2"
WORKER_ID="exp-worker-1"
MASTER1_GRPC="127.0.0.1:28080"
MASTER2_GRPC="127.0.0.1:28081"
WORKER_GRPC="127.0.0.1:29090"
MASTER1_HTTP="127.0.0.1:38080"
MASTER2_HTTP="127.0.0.1:38081"
WORKER_HTTP="127.0.0.1:39090"

CONFIG_DIR="${RUN_DIR}/configs"
STDOUT_DIR="${RUN_DIR}/stdout"
RESULT_DIR="${RUN_DIR}/results"
LOG_DIR="${RUN_DIR}/logs"
BIN_DIR="${RUN_DIR}/bin"
mkdir -p "${CONFIG_DIR}" "${STDOUT_DIR}" "${RESULT_DIR}" "${LOG_DIR}" "${BIN_DIR}"

MASTER1_CONFIG="${CONFIG_DIR}/master-1.yaml"
MASTER2_CONFIG="${CONFIG_DIR}/master-2.yaml"
WORKER_CONFIG="${CONFIG_DIR}/worker-1.yaml"
CONTROL_CONFIG="${CONFIG_DIR}/control.yaml"

render_config \
  --base "${BASE_CONFIG}" \
  --out "${MASTER1_CONFIG}" \
  --set "app.id=${MASTER1_ID}" \
  --set "app.role=master" \
  --set "grpc.master_listen=${MASTER1_GRPC}" \
  --set "grpc.master_advertise=${MASTER1_GRPC}" \
  --set "observability.master_http_listen=${MASTER1_HTTP}" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false" \
  --set "scheduling.create_interval=250ms" \
  --set "scheduling.dispatch_interval=250ms" \
  --set "scheduling.reconcile_interval=500ms" \
  --set "scheduling.lookahead=0s"

render_config \
  --base "${BASE_CONFIG}" \
  --out "${MASTER2_CONFIG}" \
  --set "app.id=${MASTER2_ID}" \
  --set "app.role=master" \
  --set "grpc.master_listen=${MASTER2_GRPC}" \
  --set "grpc.master_advertise=${MASTER2_GRPC}" \
  --set "observability.master_http_listen=${MASTER2_HTTP}" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false" \
  --set "scheduling.create_interval=250ms" \
  --set "scheduling.dispatch_interval=250ms" \
  --set "scheduling.reconcile_interval=500ms" \
  --set "scheduling.lookahead=0s"

render_config \
  --base "${BASE_CONFIG}" \
  --out "${WORKER_CONFIG}" \
  --set "app.id=${WORKER_ID}" \
  --set "app.role=worker" \
  --set "grpc.worker_listen=${WORKER_GRPC}" \
  --set "grpc.worker_advertise=${WORKER_GRPC}" \
  --set "observability.worker_http_listen=${WORKER_HTTP}" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false"

render_config \
  --base "${BASE_CONFIG}" \
  --out "${CONTROL_CONFIG}" \
  --set "app.id=exp-control" \
  --set "app.role=control" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false"

(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/master" ./cmd/master)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/worker" ./cmd/worker)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/control" ./cmd/control)

MASTER1_PID=""
MASTER2_PID=""
WORKER_PID=""
cleanup() {
  [[ -n "${MASTER1_PID}" ]] && safe_kill "${MASTER1_PID}" TERM
  [[ -n "${MASTER2_PID}" ]] && safe_kill "${MASTER2_PID}" TERM
  [[ -n "${WORKER_PID}" ]] && safe_kill "${WORKER_PID}" TERM
  [[ -n "${MASTER1_PID}" ]] && wait_for_process_exit "${MASTER1_PID}"
  [[ -n "${MASTER2_PID}" ]] && wait_for_process_exit "${MASTER2_PID}"
  [[ -n "${WORKER_PID}" ]] && wait_for_process_exit "${WORKER_PID}"
}
trap cleanup EXIT

(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/master" -config "${MASTER1_CONFIG}"
) >"${STDOUT_DIR}/master-1.stdout" 2>&1 &
MASTER1_PID=$!

(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/master" -config "${MASTER2_CONFIG}"
) >"${STDOUT_DIR}/master-2.stdout" 2>&1 &
MASTER2_PID=$!

(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/worker" -config "${WORKER_CONFIG}"
) >"${STDOUT_DIR}/worker-1.stdout" 2>&1 &
WORKER_PID=$!

MASTER1_LOG="${LOG_DIR}/master-${MASTER1_ID}.log"
MASTER2_LOG="${LOG_DIR}/master-${MASTER2_ID}.log"
WORKER_LOG="${LOG_DIR}/worker-${WORKER_ID}.log"

wait_log_event --file "${WORKER_LOG}" --event startup --timeout-sec "${STARTUP_TIMEOUT_SEC}" >/dev/null
leader_json="$(wait_log_event --file "${MASTER1_LOG}" --file "${MASTER2_LOG}" --event leader_acquired --timeout-sec "${STARTUP_TIMEOUT_SEC}")"
leader_file="$(printf '%s' "${leader_json}" | json_query file)"
leader_id="$(printf '%s' "${leader_json}" | json_query node_id)"
leader_acquired_ts="$(printf '%s' "${leader_json}" | json_query ts)"

if [[ "${leader_file}" == "${MASTER1_LOG}" ]]; then
  LEADER_PID="${MASTER1_PID}"
  STANDBY_ID="${MASTER2_ID}"
  STANDBY_LOG="${MASTER2_LOG}"
else
  LEADER_PID="${MASTER2_PID}"
  STANDBY_ID="${MASTER1_ID}"
  STANDBY_LOG="${MASTER1_LOG}"
fi

slot_json="$(next_slot_plan "${TIMEZONE}" "${MIN_SLOT_LEAD_SEC}")"
slot_utc="$(printf '%s' "${slot_json}" | json_query slot_utc)"
slot_local="$(printf '%s' "${slot_json}" | json_query slot_local)"
cron_expr="$(printf '%s' "${slot_json}" | json_query cron)"
seconds_until_slot="$(printf '%s' "${slot_json}" | json_query seconds_until_slot)"

job_name="failover-${RUN_ID}"
payload="{\"kind\":\"mock\",\"duration_ms\":${PAYLOAD_DURATION_MS},\"result_summary\":{\"run_id\":\"${RUN_ID}\"}}"
(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/control" \
    -config "${CONTROL_CONFIG}" \
    -action create-job \
    -name "${job_name}" \
    -cron "${cron_expr}" \
    -timezone "${TIMEZONE}" \
    -payload "${payload}" \
    -timeout 30 \
    -max-retries 0 \
    -retry-backoff 0 \
    -allow-concurrent=true
) >"${RESULT_DIR}/create-job.json" 2>"${STDOUT_DIR}/control.stderr"

sleep_before_kill="$(python3 - "${seconds_until_slot}" "${KILL_BEFORE_SEC}" <<'PY'
import sys
seconds_until_slot = float(sys.argv[1])
kill_before = float(sys.argv[2])
value = seconds_until_slot - kill_before
print(max(0.0, value))
PY
)"
sleep "${sleep_before_kill}"

kill_ts="$(now_utc_iso)"
safe_kill "${LEADER_PID}" "${KILL_SIGNAL}"

new_leader_json="$(wait_log_event --file "${STANDBY_LOG}" --event leader_acquired --after-ts "${kill_ts}" --timeout-sec "${FAILOVER_TIMEOUT_SEC}")"
new_leader_ts="$(printf '%s' "${new_leader_json}" | json_query ts)"
dispatch_json="$(wait_log_event --file "${STANDBY_LOG}" --event dispatch_attempted --after-ts "${kill_ts}" --timeout-sec "${DISPATCH_TIMEOUT_SEC}")"
dispatch_ts="$(printf '%s' "${dispatch_json}" | json_query ts)"
worker_started_json="$(wait_log_event --file "${WORKER_LOG}" --event task_started --after-ts "${kill_ts}" --timeout-sec "${DISPATCH_TIMEOUT_SEC}")"
worker_started_ts="$(printf '%s' "${worker_started_json}" | json_query ts)"

takeover_ms="$(iso_diff_ms "${kill_ts}" "${new_leader_ts}")"
kill_to_dispatch_ms="$(iso_diff_ms "${kill_ts}" "${dispatch_ts}")"
slot_to_dispatch_ms="$(iso_diff_ms "${slot_utc}" "${dispatch_ts}")"
slot_to_started_ms="$(iso_diff_ms "${slot_utc}" "${worker_started_ts}")"

export RUN_ID RUN_DIR BASE_CONFIG LEADER_ID="${leader_id}" LEADER_ACQUIRED_TS="${leader_acquired_ts}" STANDBY_ID KILL_SIGNAL KILL_TS="${kill_ts}" NEW_LEADER_TS="${new_leader_ts}" DISPATCH_TS="${dispatch_ts}" WORKER_STARTED_TS="${worker_started_ts}" TAKEOVER_MS="${takeover_ms}" KILL_TO_DISPATCH_MS="${kill_to_dispatch_ms}" SLOT_UTC="${slot_utc}" SLOT_LOCAL="${slot_local}" SLOT_TO_DISPATCH_MS="${slot_to_dispatch_ms}" SLOT_TO_STARTED_MS="${slot_to_started_ms}" JOB_NAME="${job_name}" CRON_EXPR="${cron_expr}" LEADER_LOG="${leader_file}" STANDBY_LOG WORKER_LOG
python3 - <<'PY'
import json
import os

payload = {
    "run_id": os.environ["RUN_ID"],
    "run_dir": os.environ["RUN_DIR"],
    "mode": "failover_trial",
    "base_config": os.environ["BASE_CONFIG"],
    "leader_id_before_kill": os.environ["LEADER_ID"],
    "leader_acquired_before_kill_ts": os.environ["LEADER_ACQUIRED_TS"],
    "standby_id": os.environ["STANDBY_ID"],
    "kill_signal": os.environ["KILL_SIGNAL"],
    "kill_ts": os.environ["KILL_TS"],
    "new_leader_ts": os.environ["NEW_LEADER_TS"],
    "first_dispatch_after_kill_ts": os.environ["DISPATCH_TS"],
    "worker_started_after_kill_ts": os.environ["WORKER_STARTED_TS"],
    "takeover_ms": int(os.environ["TAKEOVER_MS"]),
    "kill_to_first_dispatch_ms": int(os.environ["KILL_TO_DISPATCH_MS"]),
    "slot_utc": os.environ["SLOT_UTC"],
    "slot_local": os.environ["SLOT_LOCAL"],
    "slot_to_dispatch_ms": int(os.environ["SLOT_TO_DISPATCH_MS"]),
    "slot_to_worker_started_ms": int(os.environ["SLOT_TO_STARTED_MS"]),
    "job_name": os.environ["JOB_NAME"],
    "cron_expr": os.environ["CRON_EXPR"],
    "logs": {
        "leader_before_kill": os.environ["LEADER_LOG"],
        "standby": os.environ["STANDBY_LOG"],
        "worker": os.environ["WORKER_LOG"],
    },
}
summary_path = os.path.join(os.environ["RUN_DIR"], "results", "summary.json")
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False, indent=2)
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
