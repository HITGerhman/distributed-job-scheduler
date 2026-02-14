#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROTO_DIR="$ROOT_DIR/api/proto"
PROTO_FILE="$PROTO_DIR/scheduler.proto"

if [[ ! -f "$PROTO_FILE" ]]; then
  echo "proto file not found: $PROTO_FILE" >&2
  exit 1
fi

export PATH="$(go env GOPATH)/bin:${PATH}"

if command -v buf >/dev/null 2>&1; then
  (cd "$ROOT_DIR" && buf generate)
  echo "proto generated via buf: $PROTO_FILE"
  exit 0
fi

if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc is required but not found in PATH" >&2
  exit 1
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  echo "protoc-gen-go is required but not found in PATH" >&2
  exit 1
fi

if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  echo "protoc-gen-go-grpc is required but not found in PATH" >&2
  exit 1
fi

protoc \
  --proto_path="$PROTO_DIR" \
  --go_out=paths=source_relative:"$PROTO_DIR" \
  --go-grpc_out=paths=source_relative:"$PROTO_DIR" \
  "$PROTO_FILE"

echo "proto generated via protoc: $PROTO_FILE"
