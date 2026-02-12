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

# CPG runtime authority modules targeted for hard cutover deletion.
cpg_authority_modules=(
  "view_builders_df"
  "spec_registry"
  "specs"
  "emit_specs"
  "prop_catalog"
  "node_families"
  "kind_catalog"
)

# semantics.cpg runtime authority modules targeted for final hard cutover deletion.
semantics_cpg_authority_modules=(
  "emit_specs"
  "kind_catalog"
  "node_families"
  "prop_catalog"
  "prop_transforms"
  "spec_registry"
  "specs"
)

# schema_spec operational modules targeted for hard cutover deletion.
schema_authority_modules=(
  "dataset_spec_ops"
  "pandera_bridge"
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

for module in "${cpg_authority_modules[@]}"; do
  module_path="src/cpg/${module}.py"
  if [ ! -f "${module_path}" ]; then
    cpg_pattern="^\s*(from|import)\s+cpg\.${module}\b"
    if rg -n "${cpg_pattern}" src; then
      echo "Deleted CPG authority imports were found in src/ for module cpg.${module}." >&2
      exit 1
    fi
  fi
done

for module in "${schema_authority_modules[@]}"; do
  module_path="src/schema_spec/${module}.py"
  if [ ! -f "${module_path}" ]; then
    schema_pattern="^\s*(from|import)\s+schema_spec\.${module}\b"
    if rg -n "${schema_pattern}" src; then
      echo "Deleted schema authority imports were found in src/ for module schema_spec.${module}." >&2
      exit 1
    fi
  fi
done

for module in "${semantics_cpg_authority_modules[@]}"; do
  module_path="src/semantics/cpg/${module}.py"
  if [ ! -f "${module_path}" ]; then
    semantics_cpg_pattern="^\s*(from|import)\s+semantics\.cpg\.${module}\b"
    if rg -n "${semantics_cpg_pattern}" src; then
      echo "Deleted semantics.cpg authority imports were found in src/ for module semantics.cpg.${module}." >&2
      exit 1
    fi
  fi
done

# Once the legacy system facade is removed, block any reintroduction.
if [ ! -f "src/schema_spec/system.py" ]; then
  if rg -n "^\s*(from|import)\s+schema_spec\.system\b" src; then
    echo "Deleted schema_spec.system imports were found in src/." >&2
    exit 1
  fi
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
  for module in "${cpg_authority_modules[@]}"; do
    module_path="src/cpg/${module}.py"
    if [ ! -f "${module_path}" ]; then
      cpg_pattern="^\s*(from|import)\s+cpg\.${module}\b"
      if rg -n "${cpg_pattern}" tests; then
        echo "Deleted CPG authority imports were found in tests/ for module cpg.${module}." >&2
        exit 1
      fi
    fi
  done
  for module in "${schema_authority_modules[@]}"; do
    module_path="src/schema_spec/${module}.py"
    if [ ! -f "${module_path}" ]; then
      schema_pattern="^\s*(from|import)\s+schema_spec\.${module}\b"
      if rg -n "${schema_pattern}" tests; then
        echo "Deleted schema authority imports were found in tests/ for module schema_spec.${module}." >&2
        exit 1
      fi
    fi
  done
  for module in "${semantics_cpg_authority_modules[@]}"; do
    module_path="src/semantics/cpg/${module}.py"
    if [ ! -f "${module_path}" ]; then
      semantics_cpg_pattern="^\s*(from|import)\s+semantics\.cpg\.${module}\b"
      if rg -n "${semantics_cpg_pattern}" tests; then
        echo "Deleted semantics.cpg authority imports were found in tests/ for module semantics.cpg.${module}." >&2
        exit 1
      fi
    fi
  done
  if [ ! -f "src/schema_spec/system.py" ]; then
    if rg -n "^\s*(from|import)\s+schema_spec\.system\b" tests; then
      echo "Deleted schema_spec.system imports were found in tests/." >&2
      exit 1
    fi
  fi
fi

echo "No legacy planning/engine imports detected."
