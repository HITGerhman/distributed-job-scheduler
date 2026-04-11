#!/usr/bin/env bash

set -euo pipefail

EXPERIMENTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${EXPERIMENTS_DIR}/../.." && pwd)"

require_cmd() {
  local cmd
  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "missing required command: $cmd" >&2
      exit 1
    fi
  done
}

now_utc_iso() {
  date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"
}

join_by() {
  local delimiter="$1"
  shift
  local first=1
  local item
  for item in "$@"; do
    if [[ ${first} -eq 1 ]]; then
      printf '%s' "${item}"
      first=0
      continue
    fi
    printf '%s%s' "${delimiter}" "${item}"
  done
}

json_query() {
  local path="$1"
  local input
  input="$(cat)"
  python3 - "$path" "$input" <<'PY'
import json
import sys

path = sys.argv[1].split(".")
data = json.loads(sys.argv[2])
value = data
for part in path:
    if isinstance(value, dict):
        value = value[part]
    elif isinstance(value, list):
        value = value[int(part)]
    else:
        raise KeyError(part)
if isinstance(value, (dict, list)):
    print(json.dumps(value, ensure_ascii=False))
elif value is None:
    print("")
else:
    print(value)
PY
}

iso_diff_ms() {
  local start_ts="$1"
  local end_ts="$2"
  python3 - "$start_ts" "$end_ts" <<'PY'
from datetime import datetime
import sys

def parse(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)

start = parse(sys.argv[1])
end = parse(sys.argv[2])
delta = end - start
print(int(delta.total_seconds() * 1000))
PY
}

iso_add_seconds() {
  local base_ts="$1"
  local seconds="$2"
  python3 - "$base_ts" "$seconds" <<'PY'
from datetime import datetime, timedelta, timezone
import sys

value = sys.argv[1]
seconds = float(sys.argv[2])
if value.endswith("Z"):
    value = value[:-1] + "+00:00"
base = datetime.fromisoformat(value)
target = base + timedelta(seconds=seconds)
print(target.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
PY
}

next_slot_plan() {
  local timezone="$1"
  local minimum_lead_sec="$2"
  python3 - "$timezone" "$minimum_lead_sec" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

timezone_name = sys.argv[1]
minimum_lead_sec = float(sys.argv[2])

tz = ZoneInfo(timezone_name)
now_utc = datetime.now(timezone.utc)
now_local = now_utc.astimezone(tz)
slot_local = now_local.replace(second=0, microsecond=0) + timedelta(minutes=1)
while (slot_local - now_local).total_seconds() < minimum_lead_sec:
    slot_local += timedelta(minutes=1)
slot_utc = slot_local.astimezone(timezone.utc)

print(json.dumps({
    "timezone": timezone_name,
    "now_utc": now_utc.isoformat().replace("+00:00", "Z"),
    "now_local": now_local.isoformat(),
    "slot_utc": slot_utc.isoformat().replace("+00:00", "Z"),
    "slot_local": slot_local.isoformat(),
    "seconds_until_slot": max(0.0, (slot_utc - now_utc).total_seconds()),
    "cron": f"{slot_local.minute} {slot_local.hour} {slot_local.day} {slot_local.month} *",
}, ensure_ascii=False))
PY
}

next_slot_series() {
  local timezone="$1"
  local minimum_lead_sec="$2"
  local slots="$3"
  python3 - "$timezone" "$minimum_lead_sec" "$slots" <<'PY'
import json
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

timezone_name = sys.argv[1]
minimum_lead_sec = float(sys.argv[2])
slots = int(sys.argv[3])
tz = ZoneInfo(timezone_name)
now_utc = datetime.now(timezone.utc)
now_local = now_utc.astimezone(tz)
slot_local = now_local.replace(second=0, microsecond=0) + timedelta(minutes=1)
while (slot_local - now_local).total_seconds() < minimum_lead_sec:
    slot_local += timedelta(minutes=1)
items = []
for idx in range(slots):
    current_local = slot_local + timedelta(minutes=idx)
    current_utc = current_local.astimezone(timezone.utc)
    items.append({
        "index": idx + 1,
        "slot_utc": current_utc.isoformat().replace("+00:00", "Z"),
        "slot_local": current_local.isoformat(),
        "cron": f"{current_local.minute} {current_local.hour} {current_local.day} {current_local.month} *",
        "seconds_until_slot": max(0.0, (current_utc - now_utc).total_seconds()),
    })
print(json.dumps({
    "timezone": timezone_name,
    "now_utc": now_utc.isoformat().replace("+00:00", "Z"),
    "now_local": now_local.isoformat(),
    "slots": items,
}, ensure_ascii=False))
PY
}

round_down_to_hundred() {
  local value="$1"
  python3 - "$value" <<'PY'
import math
import sys

value = float(sys.argv[1])
if value < 100:
    print(max(1, int(math.floor(value))))
else:
    print(int(math.floor(value / 100.0) * 100))
PY
}

port_for() {
  local role="$1"
  local kind="$2"
  local index="$3"
  local base
  case "${role}:${kind}" in
    master:grpc) base=28080 ;;
    master:http) base=38080 ;;
    worker:grpc) base=29080 ;;
    worker:http) base=39080 ;;
    *)
      echo "unsupported port kind: ${role}:${kind}" >&2
      return 1
      ;;
  esac
  echo $((base + index))
}

render_config() {
  python3 "${EXPERIMENTS_DIR}/render_config.py" "$@"
}

wait_log_event() {
  python3 "${EXPERIMENTS_DIR}/log_tools.py" wait-event "$@"
}

summarize_log_window() {
  python3 "${EXPERIMENTS_DIR}/log_tools.py" summarize-window "$@"
}

safe_kill() {
  local pid="$1"
  local signal_name="${2:-TERM}"
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -"${signal_name}" "$pid" >/dev/null 2>&1 || true
  fi
}

wait_for_process_exit() {
  local pid="$1"
  wait "$pid" 2>/dev/null || true
}

mysql_exec_from_dsn() {
  local dsn="$1"
  local sql="$2"
  python3 - "$dsn" "$sql" <<'PY'
import re
import subprocess
import sys

dsn = sys.argv[1]
sql = sys.argv[2]
match = re.match(r"(?P<user>[^:]+):(?P<password>[^@]*)@tcp\((?P<host>[^:]+):(?P<port>\d+)\)/(?P<db>[^?]+)", dsn)
if not match:
    raise SystemExit(f"unsupported mysql dsn: {dsn}")
parts = match.groupdict()
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
completed = subprocess.run(cmd, check=False)
raise SystemExit(completed.returncode)
PY
}

reset_mysql_tables() {
  local dsn="$1"
  mysql_exec_from_dsn "${dsn}" "SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE audit_events; TRUNCATE TABLE outbox_events; TRUNCATE TABLE attempts; TRUNCATE TABLE job_instances; TRUNCATE TABLE jobs; SET FOREIGN_KEY_CHECKS=1;"
}

flush_redis_container() {
  local container_name="${1:-djs-redis}"
  docker.exe exec "${container_name}" redis-cli FLUSHDB >/dev/null
}
