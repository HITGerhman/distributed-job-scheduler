#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF'
usage: scripts/experiments/run_prodlike_matrix.sh [-config configs/local.yaml] [-run-root runtime/experiments/...]

Run the production-like experiment matrix:
- burst peak finding on 2M2W / 2M4W / 3M6W / 3M8W
- steady mock on top 2 confirmed topologies
- steady shell on top 2 confirmed topologies
- failover under load on the strongest confirmed topology
EOF
}

BASE_CONFIG="configs/local.yaml"
RUN_ROOT=""
CONTROL_PARALLELISM=12

while [[ $# -gt 0 ]]; do
  case "$1" in
    -config)
      BASE_CONFIG="$2"
      shift 2
      ;;
    -run-root)
      RUN_ROOT="$2"
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

require_cmd bash python3

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "${RUN_ROOT}" ]]; then
  RUN_ROOT="runtime/experiments/prodlike-matrix-${RUN_ID}"
fi
mkdir -p "${RUN_ROOT}" "runtime/experiments/aggregate"

TRIAL_SCRIPT="${SCRIPT_DIR}/run_prodlike_trial.sh"
AGG_SCRIPT="${SCRIPT_DIR}/aggregate_results.py"

TOPO_KEYS=(T1 T2 T3 T4)
declare -A TOPO_MASTERS=([T1]=2 [T2]=2 [T3]=3 [T4]=3)
declare -A TOPO_WORKERS=([T1]=2 [T2]=4 [T3]=6 [T4]=8)
declare -A TOPO_LOADS=(
  [T1]="200 400 800"
  [T2]="400 800 1600"
  [T3]="800 1600 2400"
  [T4]="1200 2400 3200"
)

declare -A CANDIDATE_BEST_LOAD=()
declare -A CONFIRMED_LOAD=()
declare -A STEADY_MOCK_LOAD=()
declare -A STEADY_SHELL_LOAD=()

BURST_RUN_DIRS=()
STEADY_MOCK_RUN_DIRS=()
STEADY_SHELL_RUN_DIRS=()
FAILOVER_RUN_DIRS=()

is_stable_summary() {
  local summary_path="$1"
  python3 - "${summary_path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    summary = json.load(handle)
expected = int(summary.get("expected_job_count", 0))
dispatch_count = int(summary.get("dispatch_count", 0))
finished_count = int(summary.get("task_finished_count", 0))
dispatch_failures = int(summary.get("dispatch_rpc_failures", 0))
stale_callbacks = int(summary.get("stale_callback_count", 0))
ok = (
    expected > 0
    and dispatch_failures == 0
    and dispatch_count == expected
    and finished_count == expected
    and stale_callbacks == 0
)
print("1" if ok else "0")
PY
}

calc_ratio_load() {
  local base="$1"
  local ratio="$2"
  python3 - "${base}" "${ratio}" <<'PY'
import math
import sys

base = float(sys.argv[1])
ratio = float(sys.argv[2])
print(int(math.floor(base * ratio)))
PY
}

sorted_topologies_by_load() {
  python3 - "$@" <<'PY'
import sys

items = []
for raw in sys.argv[1:]:
    topo, load = raw.split("=", 1)
    if not load:
        continue
    items.append((topo, int(load)))
for topo, _ in sorted(items, key=lambda item: item[1], reverse=True):
    print(topo)
PY
}

run_trial() {
  local run_dir="$1"
  local masters="$2"
  local workers="$3"
  local phase="$4"
  local payload_profile="$5"
  local jobs_per_slot="$6"
  local slots="$7"
  "${TRIAL_SCRIPT}" \
    -config "${BASE_CONFIG}" \
    -run-dir "${run_dir}" \
    -masters "${masters}" \
    -workers "${workers}" \
    -phase "${phase}" \
    -payload-profile "${payload_profile}" \
    -jobs-per-slot "${jobs_per_slot}" \
    -slots "${slots}" \
    -control-parallelism "${CONTROL_PARALLELISM}"
}

for topo in "${TOPO_KEYS[@]}"; do
  masters="${TOPO_MASTERS[$topo]}"
  workers="${TOPO_WORKERS[$topo]}"
  best_load=""
  for load in ${TOPO_LOADS[$topo]}; do
    run_dir="${RUN_ROOT}/burst-candidate-${topo}-${load}"
    run_trial "${run_dir}" "${masters}" "${workers}" "burst" "mock-short" "${load}" 1
    if [[ "$(is_stable_summary "${run_dir}/results/summary.json")" == "1" ]]; then
      best_load="${load}"
    fi
  done
  CANDIDATE_BEST_LOAD[$topo]="${best_load}"
  if [[ -z "${best_load}" ]]; then
    continue
  fi
  stable_rounds=0
  for round in 1 2 3; do
    run_dir="${RUN_ROOT}/burst-confirm-${topo}-r${round}"
    run_trial "${run_dir}" "${masters}" "${workers}" "burst" "mock-short" "${best_load}" 1
    if [[ "$(is_stable_summary "${run_dir}/results/summary.json")" == "1" ]]; then
      stable_rounds=$((stable_rounds + 1))
      BURST_RUN_DIRS+=("${run_dir}")
    fi
  done
  if [[ "${stable_rounds}" -eq 3 ]]; then
    CONFIRMED_LOAD[$topo]="${best_load}"
  else
    CONFIRMED_LOAD[$topo]=""
  fi
done

confirmed_args=()
for topo in "${TOPO_KEYS[@]}"; do
  confirmed_args+=("${topo}=${CONFIRMED_LOAD[$topo]:-}")
done
mapfile -t SORTED_CONFIRMED < <(sorted_topologies_by_load "${confirmed_args[@]}")

TOP2=()
for topo in "${SORTED_CONFIRMED[@]}"; do
  TOP2+=("${topo}")
  if [[ "${#TOP2[@]}" -eq 2 ]]; then
    break
  fi
done

for topo in "${TOP2[@]}"; do
  best_load="${CONFIRMED_LOAD[$topo]}"
  steady_mock_jobs="$(round_down_to_hundred "$(calc_ratio_load "${best_load}" 0.75)")"
  STEADY_MOCK_LOAD[$topo]="${steady_mock_jobs}"
  masters="${TOPO_MASTERS[$topo]}"
  workers="${TOPO_WORKERS[$topo]}"
  for round in 1 2 3; do
    run_dir="${RUN_ROOT}/steady-mock-${topo}-r${round}"
    run_trial "${run_dir}" "${masters}" "${workers}" "steady" "mock-medium" "${steady_mock_jobs}" 3
    STEADY_MOCK_RUN_DIRS+=("${run_dir}")
  done

  steady_shell_jobs="$(round_down_to_hundred "$(calc_ratio_load "${steady_mock_jobs}" 0.5)")"
  STEADY_SHELL_LOAD[$topo]="${steady_shell_jobs}"
  for round in 1 2 3; do
    run_dir="${RUN_ROOT}/steady-shell-${topo}-r${round}"
    run_trial "${run_dir}" "${masters}" "${workers}" "steady" "shell-short" "${steady_shell_jobs}" 2
    STEADY_SHELL_RUN_DIRS+=("${run_dir}")
  done
done

FAILOVER_TOPO=""
for topo in T4 T3 T2; do
  if [[ -n "${CONFIRMED_LOAD[$topo]:-}" ]]; then
    FAILOVER_TOPO="${topo}"
    break
  fi
done
if [[ -z "${FAILOVER_TOPO}" && -n "${CONFIRMED_LOAD[T1]:-}" ]]; then
  FAILOVER_TOPO="T1"
fi

if [[ -n "${FAILOVER_TOPO}" ]]; then
  failover_best="${CONFIRMED_LOAD[$FAILOVER_TOPO]}"
  failover_jobs="$(round_down_to_hundred "$(calc_ratio_load "${failover_best}" 0.6)")"
  masters="${TOPO_MASTERS[$FAILOVER_TOPO]}"
  workers="${TOPO_WORKERS[$FAILOVER_TOPO]}"
  for round in 1 2 3 4 5; do
    run_dir="${RUN_ROOT}/failover-${FAILOVER_TOPO}-r${round}"
    "${TRIAL_SCRIPT}" \
      -config "${BASE_CONFIG}" \
      -run-dir "${run_dir}" \
      -masters "${masters}" \
      -workers "${workers}" \
      -phase failover \
      -payload-profile mock-long \
      -jobs-per-slot "${failover_jobs}" \
      -slots 3 \
      -kill-before-sec 3 \
      -control-parallelism "${CONTROL_PARALLELISM}"
    FAILOVER_RUN_DIRS+=("${run_dir}")
  done
fi

if [[ "${#BURST_RUN_DIRS[@]}" -gt 0 ]]; then
  agg_cmd=(python3 "${AGG_SCRIPT}" prodlike --phase burst)
  for run_dir in "${BURST_RUN_DIRS[@]}"; do
    agg_cmd+=(--run-dir "${run_dir}")
  done
  "${agg_cmd[@]}" >"runtime/experiments/aggregate/prodlike-burst.json"
fi

if [[ "${#STEADY_MOCK_RUN_DIRS[@]}" -gt 0 ]]; then
  agg_cmd=(python3 "${AGG_SCRIPT}" prodlike --phase steady)
  for run_dir in "${STEADY_MOCK_RUN_DIRS[@]}"; do
    agg_cmd+=(--run-dir "${run_dir}")
  done
  "${agg_cmd[@]}" >"runtime/experiments/aggregate/prodlike-steady-mock.json"
fi

if [[ "${#STEADY_SHELL_RUN_DIRS[@]}" -gt 0 ]]; then
  agg_cmd=(python3 "${AGG_SCRIPT}" prodlike --phase steady)
  for run_dir in "${STEADY_SHELL_RUN_DIRS[@]}"; do
    agg_cmd+=(--run-dir "${run_dir}")
  done
  "${agg_cmd[@]}" >"runtime/experiments/aggregate/prodlike-steady-shell.json"
fi

if [[ "${#FAILOVER_RUN_DIRS[@]}" -gt 0 ]]; then
  agg_cmd=(python3 "${AGG_SCRIPT}" prodlike --phase failover)
  for run_dir in "${FAILOVER_RUN_DIRS[@]}"; do
    agg_cmd+=(--run-dir "${run_dir}")
  done
  "${agg_cmd[@]}" >"runtime/experiments/aggregate/prodlike-failover-under-load.json"
fi

MATRIX_SUMMARY="runtime/experiments/aggregate/prodlike-matrix-summary.json"
python3 - "${MATRIX_SUMMARY}" "${RUN_ROOT}" "${FAILOVER_TOPO}" <<'PY'
import json
import os
import sys

summary_path = sys.argv[1]
run_root = sys.argv[2]
failover_topo = sys.argv[3]

payload = {
    "run_root": run_root,
    "failover_topology": failover_topo,
    "generated_at": os.popen("date -u +%Y-%m-%dT%H:%M:%SZ").read().strip(),
}
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False, indent=2)
    handle.write("\n")
PY

printf 'matrix finished under %s\n' "${RUN_ROOT}"
