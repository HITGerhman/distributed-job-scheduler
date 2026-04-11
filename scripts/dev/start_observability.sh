#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p "$ROOT_DIR/runtime/logs"
docker compose -f "$ROOT_DIR/deploy/observability/docker-compose.yaml" up -d
