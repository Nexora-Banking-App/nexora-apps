#!/usr/bin/env bash
# Copies api-gateway/nexora_common.py into every other Python service.
# Each service is its own Docker build context, so the file is duplicated on
# purpose. Run with --check in CI to fail when the copies differ.
set -euo pipefail
cd "$(dirname "$0")/.."
SOURCE=api-gateway/nexora_common.py
status=0
for svc in auth-service account-service transaction-service fraud-service; do
  if [ "${1:-}" = "--check" ]; then
    if ! cmp -s "$SOURCE" "$svc/nexora_common.py"; then
      echo "$svc/nexora_common.py differs from $SOURCE (run scripts/sync-common.sh)"
      status=1
    fi
  else
    cp "$SOURCE" "$svc/nexora_common.py"
  fi
done
exit $status
