#!/usr/bin/env python3

import argparse
import json
import pathlib
import sys
import time
from datetime import datetime


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def load_entries(files: list[str]) -> list[dict]:
    entries: list[dict] = []
    for file_name in files:
        path = pathlib.Path(file_name)
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
                    entry["_ts"] = parse_ts(ts_value)
                except ValueError:
                    continue
                entry["_file"] = str(path)
                entries.append(entry)
    entries.sort(key=lambda item: item["_ts"])
    return entries


def matches(entry: dict, event: str | None, where: dict[str, str], start_ts: datetime | None = None, end_ts: datetime | None = None) -> bool:
    if event and entry.get("event") != event:
        return False
    if start_ts and entry["_ts"] < start_ts:
        return False
    if end_ts and entry["_ts"] > end_ts:
        return False
    for key, expected in where.items():
        value = entry.get(key)
        if value is None:
            return False
        if str(value) != expected:
            return False
    return True


def parse_where(items: list[str]) -> dict[str, str]:
    where: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"where filter must look like key=value: {item}")
        key, value = item.split("=", 1)
        where[key] = value
    return where


def print_json(value: dict) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def cmd_wait_event(args: argparse.Namespace) -> int:
    where = parse_where(args.where)
    after_ts = parse_ts(args.after_ts) if args.after_ts else None
    deadline = time.time() + args.timeout_sec

    while time.time() <= deadline:
        for entry in load_entries(args.file):
            if matches(entry, args.event, where, start_ts=after_ts):
                entry.pop("_ts", None)
                entry["file"] = entry.pop("_file", "")
                print_json(entry)
                return 0
        time.sleep(args.poll_sec)

    return 1


def cmd_summarize_window(args: argparse.Namespace) -> int:
    where = parse_where(args.where)
    start_ts = parse_ts(args.start_ts)
    end_ts = parse_ts(args.end_ts)
    events = args.event or []

    summary = {
        "files": args.file,
        "start_ts": args.start_ts,
        "end_ts": args.end_ts,
        "counts": {},
        "first_ts": {},
        "last_ts": {},
        "total_matches": 0,
    }
    for event in events:
        summary["counts"][event] = 0
        summary["first_ts"][event] = None
        summary["last_ts"][event] = None

    for entry in load_entries(args.file):
        event = entry.get("event")
        if events and event not in events:
            continue
        if not matches(entry, None, where, start_ts=start_ts, end_ts=end_ts):
            continue
        if event not in summary["counts"]:
            summary["counts"][event] = 0
            summary["first_ts"][event] = None
            summary["last_ts"][event] = None
        summary["counts"][event] += 1
        summary["total_matches"] += 1
        ts_value = entry["ts"]
        if summary["first_ts"][event] is None:
            summary["first_ts"][event] = ts_value
        summary["last_ts"][event] = ts_value

    print_json(summary)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal JSON log helpers for DJS experiments.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    wait_event = subparsers.add_parser("wait-event", help="wait until a log event appears")
    wait_event.add_argument("--file", action="append", required=True, help="log file to watch")
    wait_event.add_argument("--event", required=True, help="event name to wait for")
    wait_event.add_argument("--after-ts", default="", help="ignore events before this RFC3339 timestamp")
    wait_event.add_argument("--timeout-sec", type=float, default=30.0, help="overall timeout in seconds")
    wait_event.add_argument("--poll-sec", type=float, default=0.2, help="poll interval in seconds")
    wait_event.add_argument("--where", action="append", default=[], help="extra filter like key=value")
    wait_event.set_defaults(func=cmd_wait_event)

    summarize = subparsers.add_parser("summarize-window", help="count events in a time window")
    summarize.add_argument("--file", action="append", required=True, help="log file to scan")
    summarize.add_argument("--event", action="append", default=[], help="event name to count; repeat to add more")
    summarize.add_argument("--start-ts", required=True, help="window start RFC3339 timestamp")
    summarize.add_argument("--end-ts", required=True, help="window end RFC3339 timestamp")
    summarize.add_argument("--where", action="append", default=[], help="extra filter like key=value")
    summarize.set_defaults(func=cmd_summarize_window)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
