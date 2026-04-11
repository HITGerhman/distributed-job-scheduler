#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF'
usage: scripts/experiments/run_minute_burst.sh [-config configs/local.yaml] [-run-dir runtime/experiments/...]

Start 1 master + 1 worker with isolated temp configs, create a burst of one-off-ish cron jobs
for the same upcoming minute slot, and emit a JSON summary with dispatch/start/finish counts.
EOF
}

BASE_CONFIG="configs/local.yaml"
RUN_DIR=""
TIMEZONE="Asia/Shanghai"
JOB_COUNT=20
PAYLOAD_DURATION_MS=50
SAMPLE_WINDOW_SEC=10
STARTUP_TIMEOUT_SEC=60
MIN_SLOT_LEAD_SEC=25

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
    -job-count)
      JOB_COUNT="$2"
      shift 2
      ;;
    -payload-duration-ms)
      PAYLOAD_DURATION_MS="$2"
      shift 2
      ;;
    -sample-window-sec)
      SAMPLE_WINDOW_SEC="$2"
      shift 2
      ;;
    -startup-timeout)
      STARTUP_TIMEOUT_SEC="$2"
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

require_cmd bash go python3 date

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "${RUN_DIR}" ]]; then
  RUN_DIR="runtime/experiments/burst-${RUN_ID}"
fi

MASTER_ID="exp-master-burst"
WORKER_ID="exp-worker-burst"
MASTER_GRPC="127.0.0.1:28180"
WORKER_GRPC="127.0.0.1:29190"
MASTER_HTTP="127.0.0.1:38180"
WORKER_HTTP="127.0.0.1:39190"

CONFIG_DIR="${RUN_DIR}/configs"
STDOUT_DIR="${RUN_DIR}/stdout"
RESULT_DIR="${RUN_DIR}/results"
LOG_DIR="${RUN_DIR}/logs"
BIN_DIR="${RUN_DIR}/bin"
mkdir -p "${CONFIG_DIR}" "${STDOUT_DIR}" "${RESULT_DIR}" "${LOG_DIR}" "${BIN_DIR}"

MASTER_CONFIG="${CONFIG_DIR}/master.yaml"
WORKER_CONFIG="${CONFIG_DIR}/worker.yaml"
CONTROL_CONFIG="${CONFIG_DIR}/control.yaml"

render_config \
  --base "${BASE_CONFIG}" \
  --out "${MASTER_CONFIG}" \
  --set "app.id=${MASTER_ID}" \
  --set "app.role=master" \
  --set "grpc.master_listen=${MASTER_GRPC}" \
  --set "grpc.master_advertise=${MASTER_GRPC}" \
  --set "observability.master_http_listen=${MASTER_HTTP}" \
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
  --set "app.id=exp-control-burst" \
  --set "app.role=control" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false"

(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/master" ./cmd/master)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/worker" ./cmd/worker)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/control" ./cmd/control)

MASTER_PID=""
WORKER_PID=""
cleanup() {
  [[ -n "${MASTER_PID}" ]] && safe_kill "${MASTER_PID}" TERM
  [[ -n "${WORKER_PID}" ]] && safe_kill "${WORKER_PID}" TERM
  [[ -n "${MASTER_PID}" ]] && wait_for_process_exit "${MASTER_PID}"
  [[ -n "${WORKER_PID}" ]] && wait_for_process_exit "${WORKER_PID}"
}
trap cleanup EXIT

(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/master" -config "${MASTER_CONFIG}"
) >"${STDOUT_DIR}/master.stdout" 2>&1 &
MASTER_PID=$!

(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/worker" -config "${WORKER_CONFIG}"
) >"${STDOUT_DIR}/worker.stdout" 2>&1 &
WORKER_PID=$!

MASTER_LOG="${LOG_DIR}/master-${MASTER_ID}.log"
WORKER_LOG="${LOG_DIR}/worker-${WORKER_ID}.log"

wait_log_event --file "${WORKER_LOG}" --event startup --timeout-sec "${STARTUP_TIMEOUT_SEC}" >/dev/null
wait_log_event --file "${MASTER_LOG}" --event leader_acquired --timeout-sec "${STARTUP_TIMEOUT_SEC}" >/dev/null

slot_json="$(next_slot_plan "${TIMEZONE}" "${MIN_SLOT_LEAD_SEC}")"
slot_utc="$(printf '%s' "${slot_json}" | json_query slot_utc)"
slot_local="$(printf '%s' "${slot_json}" | json_query slot_local)"
cron_expr="$(printf '%s' "${slot_json}" | json_query cron)"
seconds_until_slot="$(printf '%s' "${slot_json}" | json_query seconds_until_slot)"

payload="{\"kind\":\"mock\",\"duration_ms\":${PAYLOAD_DURATION_MS},\"result_summary\":{\"run_id\":\"${RUN_ID}\"}}"
jobs_file="${RESULT_DIR}/jobs.jsonl"
: >"${jobs_file}"

create_started_ts="$(now_utc_iso)"
for idx in $(seq 1 "${JOB_COUNT}"); do
  job_name="burst-${RUN_ID}-${idx}"
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
  ) >"${RESULT_DIR}/job-${idx}.json" 2>>"${STDOUT_DIR}/control.stderr"
  python3 - "${RESULT_DIR}/job-${idx}.json" "${job_name}" "${idx}" >>"${jobs_file}" <<'PY'
import json
import sys

path, job_name, idx = sys.argv[1:4]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
payload["job_name"] = job_name
payload["ordinal"] = int(idx)
print(json.dumps(payload, ensure_ascii=False))
PY
done
create_finished_ts="$(now_utc_iso)"

sleep_total="$(python3 - "${seconds_until_slot}" "${SAMPLE_WINDOW_SEC}" <<'PY'
import sys
seconds_until_slot = float(sys.argv[1])
sample_window = float(sys.argv[2])
print(max(0.0, seconds_until_slot + sample_window + 1.0))
PY
)"
sleep "${sleep_total}"

window_end_ts="$(python3 - "${slot_utc}" "${SAMPLE_WINDOW_SEC}" <<'PY'
from datetime import datetime, timedelta, timezone
import sys

value = sys.argv[1]
if value.endswith("Z"):
    value = value[:-1] + "+00:00"
slot = datetime.fromisoformat(value)
end = slot + timedelta(seconds=float(sys.argv[2]))
print(end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
PY
)"

master_summary="$(summarize_log_window --file "${MASTER_LOG}" --event dispatch_attempted --event dispatch_rpc_failed --start-ts "${slot_utc}" --end-ts "${window_end_ts}")"
worker_summary="$(summarize_log_window --file "${WORKER_LOG}" --event task_started --event task_finished --start-ts "${slot_utc}" --end-ts "${window_end_ts}")"

dispatch_count="$(printf '%s' "${master_summary}" | json_query counts.dispatch_attempted)"
dispatch_failures="$(printf '%s' "${master_summary}" | json_query counts.dispatch_rpc_failed)"
task_started_count="$(printf '%s' "${worker_summary}" | json_query counts.task_started)"
task_finished_count="$(printf '%s' "${worker_summary}" | json_query counts.task_finished)"
first_dispatch_ts="$(printf '%s' "${master_summary}" | json_query first_ts.dispatch_attempted)"
first_started_ts="$(printf '%s' "${worker_summary}" | json_query first_ts.task_started)"
first_finished_ts="$(printf '%s' "${worker_summary}" | json_query first_ts.task_finished)"

if [[ -n "${first_dispatch_ts}" ]]; then
  first_dispatch_latency_ms="$(iso_diff_ms "${slot_utc}" "${first_dispatch_ts}")"
else
  first_dispatch_latency_ms=""
fi
if [[ -n "${first_started_ts}" ]]; then
  first_started_latency_ms="$(iso_diff_ms "${slot_utc}" "${first_started_ts}")"
else
  first_started_latency_ms=""
fi
if [[ -n "${first_finished_ts}" ]]; then
  first_finished_latency_ms="$(iso_diff_ms "${slot_utc}" "${first_finished_ts}")"
else
  first_finished_latency_ms=""
fi

export RUN_ID RUN_DIR BASE_CONFIG MASTER_ID WORKER_ID JOB_COUNT PAYLOAD_DURATION_MS SAMPLE_WINDOW_SEC SLOT_UTC="${slot_utc}" SLOT_LOCAL="${slot_local}" CRON_EXPR="${cron_expr}" CREATE_STARTED_TS="${create_started_ts}" CREATE_FINISHED_TS="${create_finished_ts}" DISPATCH_COUNT="${dispatch_count}" DISPATCH_FAILURES="${dispatch_failures}" TASK_STARTED_COUNT="${task_started_count}" TASK_FINISHED_COUNT="${task_finished_count}" FIRST_DISPATCH_TS="${first_dispatch_ts}" FIRST_STARTED_TS="${first_started_ts}" FIRST_FINISHED_TS="${first_finished_ts}" FIRST_DISPATCH_LATENCY_MS="${first_dispatch_latency_ms}" FIRST_STARTED_LATENCY_MS="${first_started_latency_ms}" FIRST_FINISHED_LATENCY_MS="${first_finished_latency_ms}" MASTER_LOG WORKER_LOG
python3 - <<'PY'
import json
import os

def to_int(value: str):
    return int(value) if value not in ("", None) else None

sample_window = float(os.environ["SAMPLE_WINDOW_SEC"])
dispatch_count = int(os.environ["DISPATCH_COUNT"])
finished_count = int(os.environ["TASK_FINISHED_COUNT"])

payload = {
    "run_id": os.environ["RUN_ID"],
    "run_dir": os.environ["RUN_DIR"],
    "mode": "minute_burst",
    "base_config": os.environ["BASE_CONFIG"],
    "master_id": os.environ["MASTER_ID"],
    "worker_id": os.environ["WORKER_ID"],
    "job_count": int(os.environ["JOB_COUNT"]),
    "payload_duration_ms": int(os.environ["PAYLOAD_DURATION_MS"]),
    "sample_window_sec": sample_window,
    "slot_utc": os.environ["SLOT_UTC"],
    "slot_local": os.environ["SLOT_LOCAL"],
    "cron_expr": os.environ["CRON_EXPR"],
    "create_started_ts": os.environ["CREATE_STARTED_TS"],
    "create_finished_ts": os.environ["CREATE_FINISHED_TS"],
    "dispatch_count": dispatch_count,
    "dispatch_rpc_failures": int(os.environ["DISPATCH_FAILURES"]),
    "task_started_count": int(os.environ["TASK_STARTED_COUNT"]),
    "task_finished_count": finished_count,
    "dispatch_qps": round(dispatch_count / sample_window, 3),
    "completion_qps": round(finished_count / sample_window, 3),
    "first_dispatch_ts": os.environ["FIRST_DISPATCH_TS"] or None,
    "first_task_started_ts": os.environ["FIRST_STARTED_TS"] or None,
    "first_task_finished_ts": os.environ["FIRST_FINISHED_TS"] or None,
    "first_dispatch_latency_ms": to_int(os.environ["FIRST_DISPATCH_LATENCY_MS"]),
    "first_task_started_latency_ms": to_int(os.environ["FIRST_STARTED_LATENCY_MS"]),
    "first_task_finished_latency_ms": to_int(os.environ["FIRST_FINISHED_LATENCY_MS"]),
    "logs": {
        "master": os.environ["MASTER_LOG"],
        "worker": os.environ["WORKER_LOG"],
    },
}
summary_path = os.path.join(os.environ["RUN_DIR"], "results", "summary.json")
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False, indent=2)
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
