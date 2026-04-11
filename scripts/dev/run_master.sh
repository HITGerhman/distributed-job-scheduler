#!/usr/bin/env bash

set -euo pipefail

go run ./cmd/master -config configs/local.yaml "$@"
