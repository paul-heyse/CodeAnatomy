#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"

ALLOW_GLOBS=(
  "!src/obs/manifest.py"
  "!src/obs/repro.py"
)

MATCHES=$(
  rg -n \
    --glob 'src/**/*.py' \
    --glob "${ALLOW_GLOBS[0]}" \
    --glob "${ALLOW_GLOBS[1]}" \
    '(^|[^A-Za-z0-9_])import json\\b|json\\.(dumps|dump|loads|load)\\b' \
    "$ROOT" || true
)

if [[ -n "$MATCHES" ]]; then
  echo "JSON usage detected outside allowlist:"
  echo "$MATCHES"
  exit 1
fi

echo "No disallowed JSON usage found."
