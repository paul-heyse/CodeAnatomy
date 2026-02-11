#!/usr/bin/env bash

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/check_no_legacy_planning_imports.sh from repository root (pyproject.toml not found)." >&2
  exit 1
fi

legacy_pattern='datafusion_engine\.planning\.(bundle|execution|pipeline)\b|relspec\.(execution_plan|planning)\b'
engine_import_pattern='^\s*(from|import)\s+engine(\.|$)'
legacy_planning_engine_pattern='^\s*(from|import)\s+planning_engine\.(build_orchestrator|spec_builder|runtime_profile|runtime|session|session_factory|materialize_pipeline|delta_tools|semantic_boundary|plan_product|diagnostics)\b'
strict_tests=false

if [ "${1:-}" = "--strict-tests" ]; then
  strict_tests=true
fi

# Transitional extract authority modules that should not be reintroduced once deleted.
extract_authority_modules=(
  "runtime_profile"
  "engine_runtime"
  "engine_session"
  "engine_session_factory"
  "materialize_pipeline"
  "delta_tools"
  "semantic_boundary"
  "plan_product"
  "diagnostics"
)

if rg -n "${legacy_pattern}" src; then
  echo "Legacy planning/engine imports were found in src/." >&2
  exit 1
fi
if rg -n "${engine_import_pattern}" src; then
  echo "Legacy planning/engine imports were found in src/." >&2
  exit 1
fi
if rg -n "${legacy_planning_engine_pattern}" src; then
  echo "Deleted planning_engine authority imports were found in src/." >&2
  exit 1
fi

for module in "${extract_authority_modules[@]}"; do
  module_path="src/extract/${module}.py"
  if [ ! -f "${module_path}" ]; then
    extract_pattern="^\s*(from|import)\s+extract\.${module}\b"
    if rg -n "${extract_pattern}" src; then
      echo "Deleted extract authority imports were found in src/ for module extract.${module}." >&2
      exit 1
    fi
  fi
done

if [ "${strict_tests}" = true ]; then
  if rg -n "${legacy_pattern}" tests; then
    echo "Legacy planning/engine imports were found in tests/." >&2
    exit 1
  fi
  if rg -n "${engine_import_pattern}" tests; then
    echo "Legacy planning/engine imports were found in tests/." >&2
    exit 1
  fi
  if rg -n "${legacy_planning_engine_pattern}" tests; then
    echo "Deleted planning_engine authority imports were found in tests/." >&2
    exit 1
  fi
  for module in "${extract_authority_modules[@]}"; do
    module_path="src/extract/${module}.py"
    if [ ! -f "${module_path}" ]; then
      extract_pattern="^\s*(from|import)\s+extract\.${module}\b"
      if rg -n "${extract_pattern}" tests; then
        echo "Deleted extract authority imports were found in tests/ for module extract.${module}." >&2
        exit 1
      fi
    fi
  done
fi

echo "No legacy planning/engine imports detected."
