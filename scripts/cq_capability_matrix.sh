#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-build/cq_capability_validation}"
LOG_DIR="${OUT_DIR}/logs"
SUMMARY_PATH="${OUT_DIR}/summary.tsv"

mkdir -p "${LOG_DIR}"
printf "case_id\tstatus\tlog_path\n" > "${SUMMARY_PATH}"

run_case() {
  local case_id="$1"
  shift
  local log_path="${LOG_DIR}/${case_id}.log"
  if "$@" >"${log_path}" 2>&1; then
    printf "%s\tok\t%s\n" "${case_id}" "${log_path}" >> "${SUMMARY_PATH}"
  else
    printf "%s\tfail\t%s\n" "${case_id}" "${log_path}" >> "${SUMMARY_PATH}"
  fi
}

run_case "search_python" ./cq search build_graph_product --in src --lang python --format summary --no-save-artifact
run_case "search_rust" ./cq search compile_target --in rust --lang rust --format summary --no-save-artifact
run_case "q_entity_auto" ./cq q "entity=function name=build_graph_product lang=auto" --format summary --no-save-artifact
run_case "q_pattern_auto" ./cq q "pattern='getattr(\$X, \$Y)' lang=auto" --format summary --no-save-artifact
run_case "calls_macro" ./cq calls build_graph_product --format summary --no-save-artifact
run_case "imports_macro" ./cq imports --include "tools/cq/**/*.py" --exclude "tests/**" --format summary --no-save-artifact
run_case "exceptions_macro" ./cq exceptions --include "tools/cq/**/*.py" --exclude "tests/**" --format summary --no-save-artifact
run_case "side_effects_macro" ./cq side-effects --include "tools/cq/**/*.py" --exclude "tests/**" --format summary --no-save-artifact
run_case "ldmd_index_roundtrip" ./cq ldmd index docs/architecture/cq/01_core_infrastructure.md
run_case "schema_result" ./cq schema --kind result --format json
run_case "schema_components" ./cq schema --kind components --format json

printf "Capability matrix complete: %s\n" "${SUMMARY_PATH}"
