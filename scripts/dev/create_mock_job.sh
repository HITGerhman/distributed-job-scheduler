#!/usr/bin/env bash

set -euo pipefail

PAYLOAD="${PAYLOAD:-{\"kind\":\"mock\",\"duration_ms\":1000,\"result_summary\":{\"message\":\"ok\"}}}"

go run ./cmd/control \
  -config configs/local.yaml \
  -action create-job \
  -name "${JOB_NAME:-demo-mock-job}" \
  -cron "${JOB_CRON:-* * * * *}" \
  -payload "${PAYLOAD}" \
  "$@"
