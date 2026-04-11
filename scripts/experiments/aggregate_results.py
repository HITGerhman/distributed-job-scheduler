#!/usr/bin/env python3

import argparse
import json
import math
import pathlib
import statistics
import sys
from datetime import datetime, timedelta
from typing import Any


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    items = sorted(values)
    if len(items) == 1:
        return float(items[0])
    rank = (len(items) - 1) * q
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(items[lower])
    weight = rank - lower
    return float(items[lower] + (items[upper] - items[lower]) * weight)


def summarize_numeric(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "avg": round(statistics.fmean(values), 3),
        "p50": round(percentile(values, 0.50), 3),
        "p95": round(percentile(values, 0.95), 3),
    }


def load_summary(run_dir: pathlib.Path) -> dict[str, Any]:
    summary_path = run_dir / "results" / "summary.json"
    with summary_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def collect_run_dirs(items: list[str], mode: str) -> list[pathlib.Path]:
    run_dirs: list[pathlib.Path] = []
    for raw in items:
        path = pathlib.Path(raw)
        if path.is_dir():
            run_dirs.append(path)
            continue
        matches = sorted(path.parent.glob(path.name))
        run_dirs.extend(item for item in matches if item.is_dir())
    valid_run_dirs = []
    skipped = []
    for run_dir in sorted(set(run_dirs)):
        summary_path = run_dir / "results" / "summary.json"
        if summary_path.is_file():
            valid_run_dirs.append(run_dir)
        else:
            skipped.append(run_dir)
    for run_dir in skipped:
        print(f"skip {mode} run without summary: {run_dir}", file=sys.stderr)
    if not valid_run_dirs:
        raise SystemExit(f"no {mode} run directories matched")
    return valid_run_dirs


def log_paths(raw: Any) -> list[pathlib.Path]:
    if isinstance(raw, str) and raw:
        return [pathlib.Path(raw)]
    if isinstance(raw, list):
        return [pathlib.Path(item) for item in raw if isinstance(item, str) and item]
    return []


def load_log_entries(paths: list[pathlib.Path], event: str, start_ts: datetime, end_ts: datetime) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in paths:
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
                if entry.get("event") != event:
                    continue
                ts_value = entry.get("ts")
                if not isinstance(ts_value, str):
                    continue
                try:
                    ts = parse_ts(ts_value)
                except ValueError:
                    continue
                if ts < start_ts or ts > end_ts:
                    continue
                entry["_ts"] = ts
                entry["_file"] = str(path)
                entries.append(entry)
    entries.sort(key=lambda item: item["_ts"])
    return entries


def compute_latencies(paths: list[pathlib.Path], event: str, slot_ts: datetime, end_ts: datetime) -> list[float]:
    values = []
    for entry in load_log_entries(paths, event, slot_ts, end_ts):
        values.append((entry["_ts"] - slot_ts).total_seconds() * 1000)
    return values


def cmd_failover(args: argparse.Namespace) -> int:
    run_dirs = collect_run_dirs(args.run_dir, "failover")
    runs = []
    metrics = {
        "takeover_ms": [],
        "kill_to_first_dispatch_ms": [],
        "slot_to_dispatch_ms": [],
        "slot_to_worker_started_ms": [],
    }

    for run_dir in run_dirs:
        summary = load_summary(run_dir)
        runs.append({
            "run_id": summary["run_id"],
            "run_dir": str(run_dir),
            "takeover_ms": summary["takeover_ms"],
            "kill_to_first_dispatch_ms": summary["kill_to_first_dispatch_ms"],
            "slot_to_dispatch_ms": summary["slot_to_dispatch_ms"],
            "slot_to_worker_started_ms": summary["slot_to_worker_started_ms"],
        })
        for key in metrics:
            metrics[key].append(float(summary[key]))

    payload = {
        "mode": "failover",
        "run_count": len(runs),
        "metrics": {key: summarize_numeric(values) for key, values in metrics.items()},
        "runs": runs,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_burst(args: argparse.Namespace) -> int:
    run_dirs = collect_run_dirs(args.run_dir, "burst")
    runs = []
    dispatch_counts: list[float] = []
    completion_counts: list[float] = []
    dispatch_qps: list[float] = []
    completion_qps: list[float] = []
    first_task_started_latency: list[float] = []
    dispatch_drain_ms_runs: list[float] = []
    task_started_drain_ms_runs: list[float] = []
    task_finished_drain_ms_runs: list[float] = []
    dispatch_burst_tps_runs: list[float] = []
    completion_burst_tps_runs: list[float] = []
    all_dispatch_latencies: list[float] = []
    all_task_started_latencies: list[float] = []
    all_task_finished_latencies: list[float] = []

    for run_dir in run_dirs:
        summary = load_summary(run_dir)
        slot_ts = parse_ts(summary["slot_utc"])
        window_end_ts = slot_ts + timedelta(seconds=float(summary["sample_window_sec"]))
        master_log = pathlib.Path(summary["logs"]["master"])
        worker_log = pathlib.Path(summary["logs"]["worker"])

        dispatch_latencies = compute_latencies([master_log], "dispatch_attempted", slot_ts, window_end_ts)
        task_started_latencies = compute_latencies([worker_log], "task_started", slot_ts, window_end_ts)
        task_finished_latencies = compute_latencies([worker_log], "task_finished", slot_ts, window_end_ts)

        dispatch_drain_ms = max(dispatch_latencies) if dispatch_latencies else None
        task_started_drain_ms = max(task_started_latencies) if task_started_latencies else None
        task_finished_drain_ms = max(task_finished_latencies) if task_finished_latencies else None
        dispatch_burst_tps = None
        completion_burst_tps = None
        if dispatch_drain_ms and dispatch_drain_ms > 0:
            dispatch_burst_tps = float(summary["dispatch_count"]) / (dispatch_drain_ms / 1000)
        if task_finished_drain_ms and task_finished_drain_ms > 0:
            completion_burst_tps = float(summary["task_finished_count"]) / (task_finished_drain_ms / 1000)

        all_dispatch_latencies.extend(dispatch_latencies)
        all_task_started_latencies.extend(task_started_latencies)
        all_task_finished_latencies.extend(task_finished_latencies)
        dispatch_counts.append(float(summary["dispatch_count"]))
        completion_counts.append(float(summary["task_finished_count"]))
        dispatch_qps.append(float(summary["dispatch_qps"]))
        completion_qps.append(float(summary["completion_qps"]))
        first_task_started_latency.append(float(summary["first_task_started_latency_ms"]))
        if dispatch_drain_ms is not None:
            dispatch_drain_ms_runs.append(dispatch_drain_ms)
        if task_started_drain_ms is not None:
            task_started_drain_ms_runs.append(task_started_drain_ms)
        if task_finished_drain_ms is not None:
            task_finished_drain_ms_runs.append(task_finished_drain_ms)
        if dispatch_burst_tps is not None:
            dispatch_burst_tps_runs.append(dispatch_burst_tps)
        if completion_burst_tps is not None:
            completion_burst_tps_runs.append(completion_burst_tps)

        runs.append({
            "run_id": summary["run_id"],
            "run_dir": str(run_dir),
            "job_count": summary["job_count"],
            "payload_duration_ms": summary["payload_duration_ms"],
            "dispatch_qps": summary["dispatch_qps"],
            "completion_qps": summary["completion_qps"],
            "dispatch_drain_ms": round(dispatch_drain_ms, 3) if dispatch_drain_ms is not None else None,
            "task_started_drain_ms": round(task_started_drain_ms, 3) if task_started_drain_ms is not None else None,
            "task_finished_drain_ms": round(task_finished_drain_ms, 3) if task_finished_drain_ms is not None else None,
            "dispatch_burst_tps": round(dispatch_burst_tps, 3) if dispatch_burst_tps is not None else None,
            "completion_burst_tps": round(completion_burst_tps, 3) if completion_burst_tps is not None else None,
            "task_started_latency_p95_ms": round(percentile(task_started_latencies, 0.95), 3) if task_started_latencies else None,
            "dispatch_latency_p95_ms": round(percentile(dispatch_latencies, 0.95), 3) if dispatch_latencies else None,
            "first_task_started_latency_ms": summary["first_task_started_latency_ms"],
        })

    payload = {
        "mode": "burst",
        "run_count": len(runs),
        "metrics": {
            "dispatch_count": summarize_numeric(dispatch_counts),
            "task_finished_count": summarize_numeric(completion_counts),
            "dispatch_qps": summarize_numeric(dispatch_qps),
            "completion_qps": summarize_numeric(completion_qps),
            "first_task_started_latency_ms": summarize_numeric(first_task_started_latency),
            "dispatch_drain_ms": summarize_numeric(dispatch_drain_ms_runs),
            "task_started_drain_ms": summarize_numeric(task_started_drain_ms_runs),
            "task_finished_drain_ms": summarize_numeric(task_finished_drain_ms_runs),
            "dispatch_burst_tps": summarize_numeric(dispatch_burst_tps_runs),
            "completion_burst_tps": summarize_numeric(completion_burst_tps_runs),
            "dispatch_latency_ms_all_tasks": summarize_numeric(all_dispatch_latencies),
            "task_started_latency_ms_all_tasks": summarize_numeric(all_task_started_latencies),
            "task_finished_latency_ms_all_tasks": summarize_numeric(all_task_finished_latencies),
        },
        "runs": runs,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_prodlike(args: argparse.Namespace) -> int:
    run_dirs = collect_run_dirs(args.run_dir, "prodlike")
    runs = []
    completion_ratio_runs: list[float] = []
    dispatch_burst_tps_runs: list[float] = []
    completion_burst_tps_runs: list[float] = []
    first_task_started_latency_runs: list[float] = []
    task_finished_drain_ms_runs: list[float] = []
    leader_transition_runs: list[float] = []
    stale_callback_runs: list[float] = []
    per_slot_completion_ratio: list[float] = []
    takeover_runs: list[float] = []
    kill_to_dispatch_runs: list[float] = []
    failover_gap_runs: list[float] = []
    post_failover_completion_ratio_runs: list[float] = []
    all_dispatch_latencies: list[float] = []
    all_task_started_latencies: list[float] = []
    all_task_finished_latencies: list[float] = []

    for run_dir in run_dirs:
        summary = load_summary(run_dir)
        phase = summary.get("phase", "")
        if args.phase and phase != args.phase:
            continue

        master_logs = log_paths(summary.get("logs", {}).get("masters"))
        worker_logs = log_paths(summary.get("logs", {}).get("workers"))
        slots = summary.get("per_slot", [])
        run_dispatch_latencies: list[float] = []
        run_started_latencies: list[float] = []
        run_finished_latencies: list[float] = []
        for slot in slots:
            if "slot_utc" not in slot or "window_end_ts" not in slot:
                continue
            slot_ts = parse_ts(slot["slot_utc"])
            end_ts = parse_ts(slot["window_end_ts"])
            run_dispatch_latencies.extend(compute_latencies(master_logs, "dispatch_attempted", slot_ts, end_ts))
            run_started_latencies.extend(compute_latencies(worker_logs, "task_started", slot_ts, end_ts))
            run_finished_latencies.extend(compute_latencies(worker_logs, "task_finished", slot_ts, end_ts))
            ratio = slot.get("completion_ratio")
            if ratio is not None:
                per_slot_completion_ratio.append(float(ratio))

        all_dispatch_latencies.extend(run_dispatch_latencies)
        all_task_started_latencies.extend(run_started_latencies)
        all_task_finished_latencies.extend(run_finished_latencies)

        expected = float(summary.get("expected_job_count", 0))
        finished = float(summary.get("task_finished_count", 0))
        completion_ratio = float(summary.get("completion_ratio", finished / expected if expected else 0.0))
        completion_ratio_runs.append(completion_ratio)
        if summary.get("dispatch_burst_tps") is not None:
            dispatch_burst_tps_runs.append(float(summary["dispatch_burst_tps"]))
        if summary.get("completion_burst_tps") is not None:
            completion_burst_tps_runs.append(float(summary["completion_burst_tps"]))
        if summary.get("first_task_started_latency_ms") is not None:
            first_task_started_latency_runs.append(float(summary["first_task_started_latency_ms"]))
        if summary.get("task_finished_drain_ms") is not None:
            task_finished_drain_ms_runs.append(float(summary["task_finished_drain_ms"]))
        leader_transition_runs.append(float(summary.get("leader_transitions", 0)))
        stale_callback_runs.append(float(summary.get("stale_callback_count", 0)))

        if phase == "failover":
            if summary.get("takeover_ms") is not None:
                takeover_runs.append(float(summary["takeover_ms"]))
            if summary.get("kill_to_first_dispatch_ms") is not None:
                kill_to_dispatch_runs.append(float(summary["kill_to_first_dispatch_ms"]))
            if summary.get("failover_gap_ms") is not None:
                failover_gap_runs.append(float(summary["failover_gap_ms"]))
            if summary.get("post_failover_completion_ratio") is not None:
                post_failover_completion_ratio_runs.append(float(summary["post_failover_completion_ratio"]))

        runs.append({
            "run_id": summary.get("run_id"),
            "run_dir": str(run_dir),
            "phase": phase,
            "masters": summary.get("masters"),
            "workers": summary.get("workers"),
            "payload_profile": summary.get("payload_profile"),
            "jobs_per_slot": summary.get("jobs_per_slot"),
            "slots": summary.get("slots"),
            "expected_job_count": summary.get("expected_job_count"),
            "dispatch_count": summary.get("dispatch_count"),
            "task_finished_count": summary.get("task_finished_count"),
            "completion_ratio": round(completion_ratio, 6),
            "dispatch_burst_tps": summary.get("dispatch_burst_tps"),
            "completion_burst_tps": summary.get("completion_burst_tps"),
            "first_task_started_latency_ms": summary.get("first_task_started_latency_ms"),
            "task_finished_drain_ms": summary.get("task_finished_drain_ms"),
            "leader_transitions": summary.get("leader_transitions"),
            "stale_callback_count": summary.get("stale_callback_count"),
            "takeover_ms": summary.get("takeover_ms"),
            "kill_to_first_dispatch_ms": summary.get("kill_to_first_dispatch_ms"),
            "failover_gap_ms": summary.get("failover_gap_ms"),
            "post_failover_completion_ratio": summary.get("post_failover_completion_ratio"),
        })

    if not runs:
        raise SystemExit("no prodlike runs matched the requested phase")

    payload = {
        "mode": "prodlike",
        "phase": args.phase or "all",
        "run_count": len(runs),
        "metrics": {
            "completion_ratio": summarize_numeric(completion_ratio_runs),
            "dispatch_burst_tps": summarize_numeric(dispatch_burst_tps_runs),
            "completion_burst_tps": summarize_numeric(completion_burst_tps_runs),
            "first_task_started_latency_ms": summarize_numeric(first_task_started_latency_runs),
            "task_finished_drain_ms": summarize_numeric(task_finished_drain_ms_runs),
            "leader_transitions": summarize_numeric(leader_transition_runs),
            "stale_callback_count": summarize_numeric(stale_callback_runs),
            "per_slot_completion_ratio": summarize_numeric(per_slot_completion_ratio),
            "takeover_ms": summarize_numeric(takeover_runs),
            "kill_to_first_dispatch_ms": summarize_numeric(kill_to_dispatch_runs),
            "failover_gap_ms": summarize_numeric(failover_gap_runs),
            "post_failover_completion_ratio": summarize_numeric(post_failover_completion_ratio_runs),
            "dispatch_latency_ms_all_tasks": summarize_numeric(all_dispatch_latencies),
            "task_started_latency_ms_all_tasks": summarize_numeric(all_task_started_latencies),
            "task_finished_latency_ms_all_tasks": summarize_numeric(all_task_finished_latencies),
        },
        "runs": runs,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate DJS experiment results.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    failover = subparsers.add_parser("failover", help="aggregate failover trial summaries")
    failover.add_argument("--run-dir", action="append", required=True, help="run dir or glob pattern")
    failover.set_defaults(func=cmd_failover)

    burst = subparsers.add_parser("burst", help="aggregate minute burst summaries")
    burst.add_argument("--run-dir", action="append", required=True, help="run dir or glob pattern")
    burst.set_defaults(func=cmd_burst)

    prodlike = subparsers.add_parser("prodlike", help="aggregate production-like experiment summaries")
    prodlike.add_argument("--run-dir", action="append", required=True, help="run dir or glob pattern")
    prodlike.add_argument("--phase", default="", choices=["", "burst", "steady", "failover"], help="optional phase filter")
    prodlike.set_defaults(func=cmd_prodlike)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
