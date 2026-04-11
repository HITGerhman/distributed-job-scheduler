#!/usr/bin/env bash

set -euo pipefail

if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc is required"
  echo "install protoc, protoc-gen-go, and protoc-gen-go-grpc first"
  exit 1
fi

protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/worker.proto
