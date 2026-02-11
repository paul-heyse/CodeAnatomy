#!/usr/bin/env bash

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/check_no_legacy_planning_imports.sh from repository root (pyproject.toml not found)." >&2
  exit 1
fi

legacy_pattern='datafusion_engine\.planning\.(bundle|execution|pipeline)\b|relspec\.(execution_plan|planning)\b'
engine_import_pattern='^\s*(from|import)\s+engine(\.|$)'
strict_tests=false

if [ "${1:-}" = "--strict-tests" ]; then
  strict_tests=true
fi

if rg -n "${legacy_pattern}" src; then
  echo "Legacy planning/engine imports were found in src/." >&2
  exit 1
fi
if rg -n "${engine_import_pattern}" src; then
  echo "Legacy planning/engine imports were found in src/." >&2
  exit 1
fi

if [ "${strict_tests}" = true ]; then
  if rg -n "${legacy_pattern}" tests; then
    echo "Legacy planning/engine imports were found in tests/." >&2
    exit 1
  fi
  if rg -n "${engine_import_pattern}" tests; then
    echo "Legacy planning/engine imports were found in tests/." >&2
    exit 1
  fi
fi

echo "No legacy planning/engine imports detected."
