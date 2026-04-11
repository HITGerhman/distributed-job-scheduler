#!/usr/bin/env bash

set -euo pipefail

PAYLOAD="${PAYLOAD:-{\"kind\":\"shell\",\"command\":[\"/bin/sh\",\"-lc\",\"sleep 2\"],\"result_summary\":{\"message\":\"shell completed\"}}}"

go run ./cmd/control \
  -config configs/local.yaml \
  -action create-job \
  -name "${JOB_NAME:-demo-shell-job}" \
  -cron "${JOB_CRON:-* * * * *}" \
  -payload "${PAYLOAD}" \
  "$@"
