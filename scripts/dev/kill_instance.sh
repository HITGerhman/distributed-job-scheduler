#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <instance_id> [extra control flags...]"
  exit 1
fi

INSTANCE_ID="$1"
shift

go run ./cmd/control \
  -config configs/local.yaml \
  -action kill-instance \
  -instance "${INSTANCE_ID}" \
  "$@"
