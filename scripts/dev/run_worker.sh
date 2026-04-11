#!/usr/bin/env bash

set -euo pipefail

go run ./cmd/worker -config configs/local.yaml "$@"
