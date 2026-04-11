#!/usr/bin/env python3

import argparse
import pathlib
import re
import sys


def parse_override(raw: str) -> tuple[str, str, str]:
    if "=" not in raw:
        raise ValueError(f"override must look like section.key=value: {raw}")
    path, value = raw.split("=", 1)
    if "." not in path:
        raise ValueError(f"override path must look like section.key: {raw}")
    section, key = path.split(".", 1)
    return section, key, value


def replace_value(lines: list[str], section: str, key: str, value: str) -> bool:
    section_pattern = re.compile(rf"^{re.escape(section)}:\s*$")
    key_pattern = re.compile(rf"^  {re.escape(key)}:\s*.*$")
    section_start = None
    insert_at = None

    for idx, line in enumerate(lines):
        if section_pattern.match(line):
            section_start = idx
            insert_at = idx + 1
            continue
        if section_start is None:
            continue
        if line and not line.startswith(" "):
            break
        if key_pattern.match(line):
            lines[idx] = f"  {key}: {value}\n"
            return True
        insert_at = idx + 1

    if section_start is None:
        return False

    lines.insert(insert_at, f"  {key}: {value}\n")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a temporary experiment config from a base YAML file.")
    parser.add_argument("--base", required=True, help="base config file")
    parser.add_argument("--out", required=True, help="output config file")
    parser.add_argument("--set", dest="overrides", action="append", default=[], help="override like section.key=value")
    args = parser.parse_args()

    base_path = pathlib.Path(args.base)
    out_path = pathlib.Path(args.out)
    lines = base_path.read_text(encoding="utf-8").splitlines(keepends=True)

    for raw in args.overrides:
        section, key, value = parse_override(raw)
        if not replace_value(lines, section, key, value):
            raise SystemExit(f"could not find {section}.{key} in {base_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
