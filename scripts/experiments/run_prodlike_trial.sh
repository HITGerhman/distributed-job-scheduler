#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF'
usage: scripts/experiments/run_prodlike_trial.sh [-config configs/local.yaml] [-masters 2] [-workers 2]

Start a production-like isolated experiment topology with N masters, M workers, one audit-consumer,
clear MySQL + Redis state, create jobs for one or more future slots, optionally kill the leader,
and emit a prodlike summary.json with per-slot and aggregate metrics.
EOF
}

BASE_CONFIG="configs/local.yaml"
RUN_DIR=""
TIMEZONE="Asia/Shanghai"
MASTERS=2
WORKERS=2
PHASE="burst"
PAYLOAD_PROFILE="mock-short"
JOBS_PER_SLOT=200
SLOTS=1
KILL_BEFORE_SEC=3
STARTUP_TIMEOUT_SEC=120
FAILOVER_TIMEOUT_SEC=90
MIN_SLOT_LEAD_SEC=""
OBSERVATION_SEC=""
CONTROL_PARALLELISM=12

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
    -masters)
      MASTERS="$2"
      shift 2
      ;;
    -workers)
      WORKERS="$2"
      shift 2
      ;;
    -phase)
      PHASE="$2"
      shift 2
      ;;
    -payload-profile)
      PAYLOAD_PROFILE="$2"
      shift 2
      ;;
    -jobs-per-slot)
      JOBS_PER_SLOT="$2"
      shift 2
      ;;
    -slots)
      SLOTS="$2"
      shift 2
      ;;
    -kill-before-sec)
      KILL_BEFORE_SEC="$2"
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
    -min-slot-lead-sec)
      MIN_SLOT_LEAD_SEC="$2"
      shift 2
      ;;
    -observation-sec)
      OBSERVATION_SEC="$2"
      shift 2
      ;;
    -control-parallelism)
      CONTROL_PARALLELISM="$2"
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

case "${PHASE}" in
  burst|steady|failover) ;;
  *)
    echo "unsupported phase: ${PHASE}" >&2
    exit 1
    ;;
esac

case "${PAYLOAD_PROFILE}" in
  mock-short)
    PAYLOAD_KIND="mock"
    PAYLOAD_DURATION_MS=50
    PAYLOAD_JSON='{"kind":"mock","duration_ms":50,"result_summary":{"profile":"mock-short"}}'
    ;;
  mock-medium)
    PAYLOAD_KIND="mock"
    PAYLOAD_DURATION_MS=100
    PAYLOAD_JSON='{"kind":"mock","duration_ms":100,"result_summary":{"profile":"mock-medium"}}'
    ;;
  mock-long)
    PAYLOAD_KIND="mock"
    PAYLOAD_DURATION_MS=5000
    PAYLOAD_JSON='{"kind":"mock","duration_ms":5000,"result_summary":{"profile":"mock-long"}}'
    ;;
  shell-short)
    PAYLOAD_KIND="shell"
    PAYLOAD_DURATION_MS=200
    PAYLOAD_JSON='{"kind":"shell","command":["/bin/sh","-lc","sleep 0.2"],"workdir":"","env":{},"result_summary":{"profile":"shell-short"}}'
    ;;
  *)
    echo "unsupported payload profile: ${PAYLOAD_PROFILE}" >&2
    exit 1
    ;;
esac

require_cmd bash go python3 date kill mysql docker.exe grep

EXPECTED_JOB_COUNT=$((JOBS_PER_SLOT * SLOTS))
if [[ -z "${MIN_SLOT_LEAD_SEC}" ]]; then
  MIN_SLOT_LEAD_SEC="$(python3 - "${EXPECTED_JOB_COUNT}" "${CONTROL_PARALLELISM}" <<'PY'
import math
import sys

expected = max(1, int(sys.argv[1]))
parallelism = max(1, int(sys.argv[2]))
per_worker_rps = 12.0
lead = math.ceil(expected / (parallelism * per_worker_rps)) + 30
print(max(45, int(lead)))
PY
)"
fi

if [[ -z "${OBSERVATION_SEC}" ]]; then
  OBSERVATION_SEC="$(python3 - "${PHASE}" "${JOBS_PER_SLOT}" "${PAYLOAD_DURATION_MS}" "${WORKERS}" <<'PY'
import math
import sys

phase = sys.argv[1]
jobs_per_slot = max(1, int(sys.argv[2]))
payload_ms = max(1, int(sys.argv[3]))
workers = max(1, int(sys.argv[4]))

if phase == "failover":
    print(45)
else:
    estimate = (jobs_per_slot * payload_ms) / (workers * 1000.0)
    extra = 15 if phase == "steady" else 10
    print(max(20, min(90, int(math.ceil(estimate * 3 + extra)))))
PY
)"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "${RUN_DIR}" ]]; then
  RUN_DIR="runtime/experiments/prodlike-${PHASE}-${RUN_ID}"
fi

TOPIC="djs.lifecycle.prodlike.${RUN_ID}"
CONSUMER_GROUP="djs-audit-consumer-${RUN_ID}"
BATCH_SIZE="$(python3 - "${EXPECTED_JOB_COUNT}" <<'PY'
import sys

expected = max(100, int(sys.argv[1]))
print(max(1000, expected * 2))
PY
)"

CONFIG_DIR="${RUN_DIR}/configs"
STDOUT_DIR="${RUN_DIR}/stdout"
RESULT_DIR="${RUN_DIR}/results"
LOG_DIR="${RUN_DIR}/logs"
BIN_DIR="${RUN_DIR}/bin"
mkdir -p "${CONFIG_DIR}" "${STDOUT_DIR}" "${RESULT_DIR}" "${LOG_DIR}" "${BIN_DIR}"

MASTER_IDS=()
MASTER_CONFIGS=()
MASTER_LOGS=()
MASTER_PIDS=()
WORKER_IDS=()
WORKER_CONFIGS=()
WORKER_LOGS=()
WORKER_PIDS=()
ALL_PIDS=()

AUDIT_ID="exp-prodlike-audit-${RUN_ID}"
AUDIT_CONFIG="${CONFIG_DIR}/audit-consumer.yaml"
AUDIT_LOG="${LOG_DIR}/audit-consumer-${AUDIT_ID}.log"
AUDIT_PID=""
CONTROL_CONFIG="${CONFIG_DIR}/control.yaml"
CONTROL_STDERR="${STDOUT_DIR}/control.stderr"
SLOTS_JSON="${RESULT_DIR}/slots.json"
SLOTS_TSV="${RESULT_DIR}/slots.tsv"
JOBS_JSONL="${RESULT_DIR}/jobs.jsonl"

cleanup() {
  local pid
  for pid in "${ALL_PIDS[@]:-}"; do
    safe_kill "${pid}" TERM
  done
  for pid in "${ALL_PIDS[@]:-}"; do
    wait_for_process_exit "${pid}"
  done
}
trap cleanup EXIT

for idx in $(seq 1 "${MASTERS}"); do
  id="exp-prodlike-master-${idx}"
  grpc_addr="127.0.0.1:$(port_for master grpc "${idx}")"
  http_addr="127.0.0.1:$(port_for master http "${idx}")"
  config_path="${CONFIG_DIR}/master-${idx}.yaml"
  log_path="${LOG_DIR}/master-${id}.log"
  render_config \
    --base "${BASE_CONFIG}" \
    --out "${config_path}" \
    --set "app.id=${id}" \
    --set "app.role=master" \
    --set "grpc.master_listen=${grpc_addr}" \
    --set "grpc.master_advertise=${grpc_addr}" \
    --set "observability.master_http_listen=${http_addr}" \
    --set "observability.log_dir=${LOG_DIR}" \
    --set "tracing.enabled=false" \
    --set "scheduling.create_interval=250ms" \
    --set "scheduling.dispatch_interval=250ms" \
    --set "scheduling.reconcile_interval=500ms" \
    --set "scheduling.lookahead=0s" \
    --set "scheduling.batch_size=${BATCH_SIZE}" \
    --set "messaging.topic_lifecycle=${TOPIC}" \
    --set "messaging.consumer_group=${CONSUMER_GROUP}" \
    --set "messaging.relay_batch_size=${BATCH_SIZE}" \
    --set "messaging.producer_batch_timeout=500ms"
  MASTER_IDS+=("${id}")
  MASTER_CONFIGS+=("${config_path}")
  MASTER_LOGS+=("${log_path}")
done

for idx in $(seq 1 "${WORKERS}"); do
  id="exp-prodlike-worker-${idx}"
  grpc_addr="127.0.0.1:$(port_for worker grpc "${idx}")"
  http_addr="127.0.0.1:$(port_for worker http "${idx}")"
  config_path="${CONFIG_DIR}/worker-${idx}.yaml"
  log_path="${LOG_DIR}/worker-${id}.log"
  render_config \
    --base "${BASE_CONFIG}" \
    --out "${config_path}" \
    --set "app.id=${id}" \
    --set "app.role=worker" \
    --set "grpc.worker_listen=${grpc_addr}" \
    --set "grpc.worker_advertise=${grpc_addr}" \
    --set "observability.worker_http_listen=${http_addr}" \
    --set "observability.log_dir=${LOG_DIR}" \
    --set "tracing.enabled=false"
  WORKER_IDS+=("${id}")
  WORKER_CONFIGS+=("${config_path}")
  WORKER_LOGS+=("${log_path}")
done

render_config \
  --base "${BASE_CONFIG}" \
  --out "${AUDIT_CONFIG}" \
  --set "app.id=${AUDIT_ID}" \
  --set "app.role=audit-consumer" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false" \
  --set "messaging.topic_lifecycle=${TOPIC}" \
  --set "messaging.consumer_group=${CONSUMER_GROUP}"

render_config \
  --base "${BASE_CONFIG}" \
  --out "${CONTROL_CONFIG}" \
  --set "app.id=exp-prodlike-control-${RUN_ID}" \
  --set "app.role=control" \
  --set "observability.log_dir=${LOG_DIR}" \
  --set "tracing.enabled=false"

MYSQL_DSN="$(awk '/^mysql:/{getline; sub(/^  dsn: /,""); print; exit}' "${MASTER_CONFIGS[0]}")"
if [[ -z "${MYSQL_DSN}" ]]; then
  echo "failed to extract mysql dsn from ${MASTER_CONFIGS[0]}" >&2
  exit 1
fi

mysql_exec_from_dsn "${MYSQL_DSN}" "SELECT 1;" >/dev/null
ETCDCTL_API=3 "${REPO_ROOT}/runtime/deps/etcd-v3.5.15-linux-amd64/etcdctl" --endpoints=127.0.0.1:2379 endpoint health >/dev/null
docker.exe inspect djs-redis >/dev/null
docker.exe inspect djs-redpanda >/dev/null
reset_mysql_tables "${MYSQL_DSN}"
flush_redis_container djs-redis

(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/master" ./cmd/master)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/worker" ./cmd/worker)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/control" ./cmd/control)
(cd "${REPO_ROOT}" && go build -o "${BIN_DIR}/audit-consumer" ./cmd/audit-consumer)

for idx in $(seq 1 "${MASTERS}"); do
  (
    cd "${REPO_ROOT}"
    "${BIN_DIR}/master" -config "${MASTER_CONFIGS[$((idx - 1))]}"
  ) >"${STDOUT_DIR}/master-${idx}.stdout" 2>&1 &
  pid=$!
  MASTER_PIDS+=("${pid}")
  ALL_PIDS+=("${pid}")
done

for idx in $(seq 1 "${WORKERS}"); do
  (
    cd "${REPO_ROOT}"
    "${BIN_DIR}/worker" -config "${WORKER_CONFIGS[$((idx - 1))]}"
  ) >"${STDOUT_DIR}/worker-${idx}.stdout" 2>&1 &
  pid=$!
  WORKER_PIDS+=("${pid}")
  ALL_PIDS+=("${pid}")
done

(
  cd "${REPO_ROOT}"
  "${BIN_DIR}/audit-consumer" -config "${AUDIT_CONFIG}" -id "${AUDIT_ID}"
) >"${STDOUT_DIR}/audit-consumer.stdout" 2>&1 &
AUDIT_PID=$!
ALL_PIDS+=("${AUDIT_PID}")

for log_path in "${WORKER_LOGS[@]}"; do
  wait_log_event --file "${log_path}" --event startup --timeout-sec "${STARTUP_TIMEOUT_SEC}" >/dev/null
done
wait_log_event --file "${AUDIT_LOG}" --event startup --timeout-sec "${STARTUP_TIMEOUT_SEC}" >/dev/null

leader_wait_cmd=(wait_log_event)
for log_path in "${MASTER_LOGS[@]}"; do
  leader_wait_cmd+=(--file "${log_path}")
done
leader_wait_cmd+=(--event leader_acquired --timeout-sec "${STARTUP_TIMEOUT_SEC}")
leader_json="$("${leader_wait_cmd[@]}")"
leader_file="$(printf '%s' "${leader_json}" | json_query file)"
leader_id="$(printf '%s' "${leader_json}" | json_query node_id)"
leader_acquired_ts="$(printf '%s' "${leader_json}" | json_query ts)"

leader_pid=""
for idx in "${!MASTER_IDS[@]}"; do
  if [[ "${MASTER_IDS[$idx]}" == "${leader_id}" ]]; then
    leader_pid="${MASTER_PIDS[$idx]}"
    break
  fi
done
if [[ -z "${leader_pid}" ]]; then
  echo "failed to map leader ${leader_id} to pid" >&2
  exit 1
fi

slots_raw_json="$(next_slot_series "${TIMEZONE}" "${MIN_SLOT_LEAD_SEC}" "${SLOTS}")"
slots_json="$(SLOTS_RAW_JSON="${slots_raw_json}" python3 - "${OBSERVATION_SEC}" <<'PY'
import json
import os
import sys
from datetime import datetime, timedelta, timezone

observation_sec = float(sys.argv[1])
data = json.loads(os.environ["SLOTS_RAW_JSON"])
slots = data["slots"]
for idx, item in enumerate(slots):
    slot_ts = datetime.fromisoformat(item["slot_utc"].replace("Z", "+00:00"))
    natural_end = slot_ts + timedelta(seconds=observation_sec)
    if idx + 1 < len(slots):
        next_ts = datetime.fromisoformat(slots[idx + 1]["slot_utc"].replace("Z", "+00:00")) - timedelta(milliseconds=1)
        window_end = min(natural_end, next_ts)
    else:
        window_end = natural_end
    item["window_end_ts"] = window_end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
print(json.dumps(data, ensure_ascii=False))
PY
)"
printf '%s\n' "${slots_json}" >"${SLOTS_JSON}"
python3 - "${SLOTS_JSON}" >"${SLOTS_TSV}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
for item in payload["slots"]:
    print(
        "\t".join([
            str(item["index"]),
            item["slot_utc"],
            item["slot_local"],
            item["cron"],
            str(item["seconds_until_slot"]),
            item["window_end_ts"],
        ])
    )
PY

create_started_ts="$(now_utc_iso)"
python3 - "${BIN_DIR}/control" "${CONTROL_CONFIG}" "${SLOTS_JSON}" "${JOBS_JSONL}" "${CONTROL_STDERR}" "${RUN_ID}" "${JOBS_PER_SLOT}" "${TIMEZONE}" "${PAYLOAD_JSON}" "${CONTROL_PARALLELISM}" <<'PY'
import concurrent.futures
import json
import pathlib
import subprocess
import sys

control_bin, control_config, slots_path, jobs_path, stderr_path, run_id, jobs_per_slot, timezone_name, payload_json, parallelism = sys.argv[1:11]
jobs_per_slot = int(jobs_per_slot)
parallelism = max(1, int(parallelism))

with open(slots_path, "r", encoding="utf-8") as handle:
    slots = json.load(handle)["slots"]

job_specs = []
for slot in slots:
    slot_index = int(slot["index"])
    cron_expr = slot["cron"]
    slot_utc = slot["slot_utc"]
    for ordinal in range(1, jobs_per_slot + 1):
        job_name = f"prodlike-{run_id}-s{slot_index}-j{ordinal}"
        job_specs.append({
            "slot_index": slot_index,
            "slot_utc": slot_utc,
            "ordinal": ordinal,
            "job_name": job_name,
            "cron": cron_expr,
        })

jobs_path_obj = pathlib.Path(jobs_path)
jobs_path_obj.write_text("", encoding="utf-8")
stderr_file = pathlib.Path(stderr_path).open("a", encoding="utf-8")


def create_job(spec: dict) -> dict:
    cmd = [
        control_bin,
        "-config", control_config,
        "-action", "create-job",
        "-name", spec["job_name"],
        "-cron", spec["cron"],
        "-timezone", timezone_name,
        "-payload", payload_json,
        "-timeout", "30",
        "-max-retries", "0",
        "-retry-backoff", "0",
        "-allow-concurrent=true",
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if completed.stderr:
        stderr_file.write(completed.stderr)
        stderr_file.flush()
    if completed.returncode != 0:
        raise RuntimeError(f"create job failed for {spec['job_name']}: {completed.stderr.strip()}")
    data = json.loads(completed.stdout)
    data.update(spec)
    return data


results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
    future_map = {executor.submit(create_job, spec): spec for spec in job_specs}
    for future in concurrent.futures.as_completed(future_map):
        results.append(future.result())

results.sort(key=lambda item: (item["slot_index"], item["ordinal"]))
with jobs_path_obj.open("w", encoding="utf-8") as handle:
    for item in results:
        handle.write(json.dumps(item, ensure_ascii=False) + "\n")

stderr_file.close()
print(json.dumps({"created": len(results)}, ensure_ascii=False))
PY
create_finished_ts="$(now_utc_iso)"

kill_ts=""
new_leader_ts=""
first_post_kill_dispatch_ts=""
first_post_kill_started_ts=""
failover_gap_ms=""

if [[ "${PHASE}" == "failover" ]]; then
  first_slot_utc="$(python3 - "${SLOTS_JSON}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
print(payload["slots"][0]["slot_utc"])
PY
)"
  sleep_before_kill="$(python3 - "${first_slot_utc}" "${KILL_BEFORE_SEC}" <<'PY'
from datetime import datetime, timezone
import sys

slot = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
kill_before = float(sys.argv[2])
now = datetime.now(timezone.utc)
value = (slot - now).total_seconds() - kill_before
print(max(0.0, value))
PY
)"
  sleep "${sleep_before_kill}"
  kill_ts="$(now_utc_iso)"
  safe_kill "${leader_pid}" KILL

  post_kill_leader_cmd=(wait_log_event)
  for log_path in "${MASTER_LOGS[@]}"; do
    post_kill_leader_cmd+=(--file "${log_path}")
  done
  post_kill_leader_cmd+=(--event leader_acquired --after-ts "${kill_ts}" --timeout-sec "${FAILOVER_TIMEOUT_SEC}")
  new_leader_json="$("${post_kill_leader_cmd[@]}")"
  new_leader_ts="$(printf '%s' "${new_leader_json}" | json_query ts)"

  post_kill_dispatch_cmd=(wait_log_event)
  for log_path in "${MASTER_LOGS[@]}"; do
    post_kill_dispatch_cmd+=(--file "${log_path}")
  done
  post_kill_dispatch_cmd+=(--event dispatch_attempted --after-ts "${kill_ts}" --timeout-sec "${FAILOVER_TIMEOUT_SEC}")
  first_post_kill_dispatch_json="$("${post_kill_dispatch_cmd[@]}")"
  first_post_kill_dispatch_ts="$(printf '%s' "${first_post_kill_dispatch_json}" | json_query ts)"

  post_kill_started_cmd=(wait_log_event)
  for log_path in "${WORKER_LOGS[@]}"; do
    post_kill_started_cmd+=(--file "${log_path}")
  done
  post_kill_started_cmd+=(--event task_started --after-ts "${kill_ts}" --timeout-sec "${FAILOVER_TIMEOUT_SEC}")
  first_post_kill_started_json="$("${post_kill_started_cmd[@]}")"
  first_post_kill_started_ts="$(printf '%s' "${first_post_kill_started_json}" | json_query ts)"
fi

last_window_end_ts="$(python3 - "${SLOTS_JSON}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
print(payload["slots"][-1]["window_end_ts"])
PY
)"
sleep_total="$(python3 - "${last_window_end_ts}" <<'PY'
from datetime import datetime, timezone
import sys

end_ts = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
now = datetime.now(timezone.utc)
print(max(0.0, (end_ts - now).total_seconds() + 1.0))
PY
)"
sleep "${sleep_total}"
run_finished_ts="$(now_utc_iso)"
db_status_json="$(python3 - "${MYSQL_DSN}" "${RUN_ID}" <<'PY'
import json
import re
import subprocess
import sys

dsn = sys.argv[1]
run_id = sys.argv[2]
match = re.match(r"(?P<user>[^:]+):(?P<password>[^@]*)@tcp\((?P<host>[^:]+):(?P<port>\d+)\)/(?P<db>[^?]+)", dsn)
if not match:
    raise SystemExit(f"unsupported mysql dsn: {dsn}")
parts = match.groupdict()
pattern = f"prodlike-{run_id}-%"
sql = f"""
SELECT
  DATE_FORMAT(i.scheduled_at, '%Y-%m-%dT%H:%i:%sZ') AS slot_utc,
  COUNT(*) AS expected_count,
  SUM(i.status = 'succeeded') AS succeeded_count,
  SUM(i.status = 'failed') AS failed_count,
  SUM(i.status = 'running') AS running_count,
  SUM(i.status = 'dispatched') AS dispatched_count,
  SUM(i.status = 'pending') AS pending_count
FROM job_instances i
JOIN jobs j ON j.id = i.job_id
WHERE j.name LIKE '{pattern}'
GROUP BY i.scheduled_at
ORDER BY i.scheduled_at ASC
"""
cmd = [
    "mysql",
    f"-h{parts['host']}",
    f"-P{parts['port']}",
    f"-u{parts['user']}",
    f"-p{parts['password']}",
    "-Nse",
    sql,
    parts["db"],
]
completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
if completed.returncode != 0:
    raise SystemExit(completed.stderr.strip())
items = []
for raw in completed.stdout.splitlines():
    line = raw.strip()
    if not line:
      continue
    slot_utc, expected_count, succeeded_count, failed_count, running_count, dispatched_count, pending_count = line.split("\t")
    items.append({
        "slot_utc": slot_utc,
        "expected_count": int(expected_count),
        "succeeded_count": int(succeeded_count),
        "failed_count": int(failed_count),
        "running_count": int(running_count),
        "dispatched_count": int(dispatched_count),
        "pending_count": int(pending_count),
    })
print(json.dumps(items, ensure_ascii=False))
PY
)"
printf '%s\n' "${db_status_json}" >"${RESULT_DIR}/db-status.json"

SUMMARY_PATH="${RESULT_DIR}/summary.json"
MASTER_LOGS_RAW="$(join_by '::' "${MASTER_LOGS[@]}")"
WORKER_LOGS_RAW="$(join_by '::' "${WORKER_LOGS[@]}")"
python3 - "${SUMMARY_PATH}" "${SLOTS_JSON}" "${RESULT_DIR}/db-status.json" "${MASTER_LOGS_RAW}" "${WORKER_LOGS_RAW}" "${AUDIT_LOG}" "${RUN_ID}" "${RUN_DIR}" "${BASE_CONFIG}" "${PHASE}" "${PAYLOAD_PROFILE}" "${PAYLOAD_KIND}" "${PAYLOAD_DURATION_MS}" "${MASTERS}" "${WORKERS}" "${JOBS_PER_SLOT}" "${SLOTS}" "${EXPECTED_JOB_COUNT}" "${OBSERVATION_SEC}" "${TOPIC}" "${CONSUMER_GROUP}" "${create_started_ts}" "${create_finished_ts}" "${leader_id}" "${leader_acquired_ts}" "${kill_ts}" "${new_leader_ts}" "${first_post_kill_dispatch_ts}" "${first_post_kill_started_ts}" "${last_window_end_ts}" "${run_finished_ts}" <<'PY'
import json
import pathlib
import sys
from datetime import datetime


def parse_ts(value: str | None):
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def load_entries(paths):
    items = []
    for raw in paths:
        path = pathlib.Path(raw)
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts_value = entry.get("ts")
                if not isinstance(ts_value, str):
                    continue
                try:
                    ts = parse_ts(ts_value)
                except ValueError:
                    continue
                entry["_ts"] = ts
                entry["_file"] = str(path)
                items.append(entry)
    items.sort(key=lambda item: item["_ts"])
    return items


def event_stats(entries, event, start_ts, end_ts):
    matches = []
    for entry in entries:
        if entry.get("event") != event:
            continue
        if start_ts and entry["_ts"] < start_ts:
            continue
        if end_ts and entry["_ts"] > end_ts:
            continue
        matches.append(entry)
    return {
        "count": len(matches),
        "first_ts": matches[0]["ts"] if matches else "",
        "last_ts": matches[-1]["ts"] if matches else "",
    }


def diff_ms(start, end):
    if not start or not end:
        return None
    return int((parse_ts(end) - parse_ts(start)).total_seconds() * 1000)


summary_path = pathlib.Path(sys.argv[1])
slots_path = pathlib.Path(sys.argv[2])
db_status_path = pathlib.Path(sys.argv[3])
master_logs = [item for item in sys.argv[4].split("::") if item]
worker_logs = [item for item in sys.argv[5].split("::") if item]
audit_logs = [sys.argv[6]] if sys.argv[6] else []
run_id = sys.argv[7]
run_dir = sys.argv[8]
base_config = sys.argv[9]
phase = sys.argv[10]
payload_profile = sys.argv[11]
payload_kind = sys.argv[12]
payload_duration_ms = int(sys.argv[13])
masters = int(sys.argv[14])
workers = int(sys.argv[15])
jobs_per_slot = int(sys.argv[16])
slots_count = int(sys.argv[17])
expected_job_count = int(sys.argv[18])
observation_sec = int(float(sys.argv[19]))
topic = sys.argv[20]
consumer_group = sys.argv[21]
create_started_ts = sys.argv[22]
create_finished_ts = sys.argv[23]
leader_id = sys.argv[24]
leader_acquired_ts = sys.argv[25]
kill_ts = sys.argv[26]
new_leader_ts = sys.argv[27]
first_post_kill_dispatch_ts = sys.argv[28]
first_post_kill_started_ts = sys.argv[29]
last_window_end_ts = sys.argv[30]
run_finished_ts = sys.argv[31]

with slots_path.open("r", encoding="utf-8") as handle:
    slot_plan = json.load(handle)
with db_status_path.open("r", encoding="utf-8") as handle:
    db_status = json.load(handle)

db_by_slot = {item["slot_utc"]: item for item in db_status}
master_entries = load_entries(master_logs)
worker_entries = load_entries(worker_logs)
audit_entries = load_entries(audit_logs)

per_slot = []
for slot in slot_plan["slots"]:
    slot_utc = slot["slot_utc"]
    window_end_ts = slot["window_end_ts"]
    start_dt = parse_ts(slot_utc)
    end_dt = parse_ts(window_end_ts)
    dispatch_stats = event_stats(master_entries, "dispatch_attempted", start_dt, end_dt)
    dispatch_fail_stats = event_stats(master_entries, "dispatch_rpc_failed", start_dt, end_dt)
    stale_stats = event_stats(master_entries, "stale_callback", start_dt, end_dt)
    started_stats = event_stats(worker_entries, "task_started", start_dt, end_dt)
    finished_stats = event_stats(worker_entries, "task_finished", start_dt, end_dt)
    audit_stats = event_stats(audit_entries, "audit_event_persisted", start_dt, end_dt)
    db_item = db_by_slot.get(slot_utc, {
        "expected_count": jobs_per_slot,
        "succeeded_count": 0,
        "failed_count": 0,
        "running_count": 0,
        "dispatched_count": 0,
        "pending_count": 0,
    })
    terminal_count = int(db_item["succeeded_count"]) + int(db_item["failed_count"])
    expected_count = int(db_item["expected_count"])
    per_slot.append({
        "index": int(slot["index"]),
        "slot_utc": slot_utc,
        "slot_local": slot["slot_local"],
        "window_end_ts": window_end_ts,
        "expected_count": expected_count,
        "dispatch_count": dispatch_stats["count"],
        "dispatch_rpc_failures": dispatch_fail_stats["count"],
        "task_started_count": started_stats["count"],
        "task_finished_count": finished_stats["count"],
        "audit_persisted_count": audit_stats["count"],
        "stale_callback_count": stale_stats["count"],
        "first_dispatch_latency_ms": diff_ms(slot_utc, dispatch_stats["first_ts"]),
        "first_task_started_latency_ms": diff_ms(slot_utc, started_stats["first_ts"]),
        "first_task_finished_latency_ms": diff_ms(slot_utc, finished_stats["first_ts"]),
        "dispatch_drain_ms": diff_ms(slot_utc, dispatch_stats["last_ts"]),
        "task_started_drain_ms": diff_ms(slot_utc, started_stats["last_ts"]),
        "task_finished_drain_ms": diff_ms(slot_utc, finished_stats["last_ts"]),
        "completion_ratio": round((terminal_count / expected_count) if expected_count else 0.0, 6),
        "db_status": {
            "succeeded_count": int(db_item["succeeded_count"]),
            "failed_count": int(db_item["failed_count"]),
            "running_count": int(db_item["running_count"]),
            "dispatched_count": int(db_item["dispatched_count"]),
            "pending_count": int(db_item["pending_count"]),
        },
    })

dispatch_count = sum(item["dispatch_count"] for item in per_slot)
dispatch_rpc_failures = sum(item["dispatch_rpc_failures"] for item in per_slot)
task_started_count = sum(item["task_started_count"] for item in per_slot)
task_finished_count = sum(item["task_finished_count"] for item in per_slot)
audit_persisted_count = sum(item["audit_persisted_count"] for item in per_slot)
total_terminal = sum(item["db_status"]["succeeded_count"] + item["db_status"]["failed_count"] for item in per_slot)
completion_ratio = round((total_terminal / expected_job_count) if expected_job_count else 0.0, 6)

dispatch_drain_ms_values = [item["dispatch_drain_ms"] for item in per_slot if item["dispatch_drain_ms"] is not None]
task_started_drain_ms_values = [item["task_started_drain_ms"] for item in per_slot if item["task_started_drain_ms"] is not None]
task_finished_drain_ms_values = [item["task_finished_drain_ms"] for item in per_slot if item["task_finished_drain_ms"] is not None]
dispatch_drain_ms = max(dispatch_drain_ms_values) if dispatch_drain_ms_values else None
task_started_drain_ms = max(task_started_drain_ms_values) if task_started_drain_ms_values else None
task_finished_drain_ms = max(task_finished_drain_ms_values) if task_finished_drain_ms_values else None
dispatch_burst_tps = None
completion_burst_tps = None
dispatch_drain_total_ms = sum(dispatch_drain_ms_values)
task_finished_drain_total_ms = sum(task_finished_drain_ms_values)
if dispatch_count > 0 and dispatch_drain_total_ms > 0:
    dispatch_burst_tps = round(dispatch_count / (dispatch_drain_total_ms / 1000.0), 3)
if task_finished_count > 0 and task_finished_drain_total_ms > 0:
    completion_burst_tps = round(task_finished_count / (task_finished_drain_total_ms / 1000.0), 3)

leader_acquired_stats = event_stats(master_entries, "leader_acquired", parse_ts(create_started_ts), parse_ts(last_window_end_ts))
leader_lost_stats = event_stats(master_entries, "leader_lost", parse_ts(create_started_ts), parse_ts(last_window_end_ts))
stale_callback_stats = event_stats(master_entries, "stale_callback", parse_ts(create_started_ts), parse_ts(last_window_end_ts))
leader_transitions = leader_acquired_stats["count"] + leader_lost_stats["count"]

first_slot = per_slot[0] if per_slot else {}
takeover_ms = diff_ms(kill_ts, new_leader_ts)
kill_to_first_dispatch_ms = diff_ms(kill_ts, first_post_kill_dispatch_ts)
pre_kill_dispatch = event_stats(master_entries, "dispatch_attempted", parse_ts(first_slot.get("slot_utc", "")), parse_ts(kill_ts) if kill_ts else None)
if pre_kill_dispatch["last_ts"] and first_post_kill_dispatch_ts:
    failover_gap_ms = diff_ms(pre_kill_dispatch["last_ts"], first_post_kill_dispatch_ts)
else:
    failover_gap_ms = kill_to_first_dispatch_ms

payload = {
    "mode": "prodlike",
    "run_id": run_id,
    "run_dir": run_dir,
    "base_config": base_config,
    "phase": phase,
    "payload_profile": payload_profile,
    "payload_kind": payload_kind,
    "payload_duration_ms": payload_duration_ms,
    "masters": masters,
    "workers": workers,
    "slots": slots_count,
    "jobs_per_slot": jobs_per_slot,
    "expected_job_count": expected_job_count,
    "observation_window_sec": observation_sec,
    "topic_lifecycle": topic,
    "consumer_group": consumer_group,
    "slot_utc": first_slot.get("slot_utc", ""),
    "create_started_ts": create_started_ts,
    "create_finished_ts": create_finished_ts,
    "run_finished_ts": run_finished_ts,
    "leader_id": leader_id,
    "leader_acquired_ts": leader_acquired_ts,
    "dispatch_count": dispatch_count,
    "dispatch_rpc_failures": dispatch_rpc_failures,
    "task_started_count": task_started_count,
    "task_finished_count": task_finished_count,
    "audit_persisted_count": audit_persisted_count,
    "leader_transitions": leader_transitions,
    "stale_callback_count": stale_callback_stats["count"],
    "completion_ratio": completion_ratio,
    "first_dispatch_latency_ms": first_slot.get("first_dispatch_latency_ms"),
    "first_task_started_latency_ms": first_slot.get("first_task_started_latency_ms"),
    "first_task_finished_latency_ms": first_slot.get("first_task_finished_latency_ms"),
    "dispatch_drain_ms": dispatch_drain_ms,
    "task_started_drain_ms": task_started_drain_ms,
    "task_finished_drain_ms": task_finished_drain_ms,
    "dispatch_burst_tps": dispatch_burst_tps,
    "completion_burst_tps": completion_burst_tps,
    "takeover_ms": takeover_ms,
    "kill_to_first_dispatch_ms": kill_to_first_dispatch_ms,
    "failover_gap_ms": failover_gap_ms,
    "post_failover_completion_ratio": completion_ratio if phase == "failover" else None,
    "kill_ts": kill_ts or "",
    "new_leader_ts": new_leader_ts or "",
    "first_post_kill_dispatch_ts": first_post_kill_dispatch_ts or "",
    "first_post_kill_started_ts": first_post_kill_started_ts or "",
    "logs": {
        "masters": master_logs,
        "workers": worker_logs,
        "audit_consumer": audit_logs,
    },
    "per_slot": per_slot,
}

summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

printf 'summary written to %s\n' "${SUMMARY_PATH}"
