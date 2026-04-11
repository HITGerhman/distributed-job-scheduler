#!/usr/bin/env bash

set -euo pipefail

if [[ -z "${MYSQL_DSN:-}" ]]; then
  echo "MYSQL_DSN is required"
  exit 1
fi

echo "Apply migrations/001_init.sql with your preferred mysql client:"
echo "mysql \"${MYSQL_DSN}\" < migrations/001_init.sql"
