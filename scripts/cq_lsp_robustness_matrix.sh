#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-build/cq_lsp_robustness_validation}"
LOG_DIR="${OUT_DIR}/logs"
SUMMARY_PATH="${OUT_DIR}/summary.tsv"

mkdir -p "${LOG_DIR}"
printf "case_id\tstatus\toutput_path\tnotes\n" > "${SUMMARY_PATH}"

assert_json_contract() {
  local json_path="$1"
  local assertion_id="$2"
  uv run python - "${json_path}" "${assertion_id}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
assertion_id = sys.argv[2]
payload = json.loads(path.read_text(encoding="utf-8"))
summary = payload.get("summary", {}) if isinstance(payload, dict) else {}

def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)

if assertion_id == "search_python":
    insight = summary.get("front_door_insight")
    ensure(isinstance(insight, dict), "front_door_insight missing")
    target = insight.get("target")
    ensure(isinstance(target, dict), "insight.target missing")
    ensure(target.get("kind") in {"function", "class", "type"}, "unexpected target.kind")
elif assertion_id == "search_rust":
    insight = summary.get("front_door_insight")
    ensure(isinstance(insight, dict), "front_door_insight missing")
    target = insight.get("target")
    ensure(isinstance(target, dict), "insight.target missing")
    ensure(target.get("kind") in {"function", "class", "type"}, "rust target not grounded")
    degradation = insight.get("degradation")
    ensure(isinstance(degradation, dict), "insight.degradation missing")
    ensure(
        degradation.get("lsp") in {"ok", "partial", "failed", "skipped", "unavailable"},
        "unexpected degradation.lsp",
    )
elif assertion_id == "q_entity_auto":
    insight = summary.get("front_door_insight")
    ensure(isinstance(insight, dict), "front_door_insight missing")
    degradation = insight.get("degradation")
    ensure(isinstance(degradation, dict), "insight.degradation missing")
    notes = degradation.get("notes")
    ensure(isinstance(notes, list), "degradation.notes missing")
elif assertion_id == "calls_python":
    ensure(summary.get("mode") == "macro:calls", "calls mode missing")
    ensure("front_door_insight" in summary, "calls front_door_insight missing")
elif assertion_id == "calls_rust":
    ensure(summary.get("mode") == "macro:calls", "calls mode missing")
    target_file = summary.get("target_file")
    ensure(isinstance(target_file, str) and target_file.endswith(".rs"), "rust target_file missing")
    ensure("front_door_insight" in summary, "calls front_door_insight missing")
elif assertion_id == "neighborhood_rust":
    ensure(summary.get("target_file"), "neighborhood target_file missing")
    ensure(isinstance(summary.get("total_slices"), int), "neighborhood total_slices missing")
elif assertion_id == "run_mixed":
    ensure(isinstance(summary.get("steps"), int), "run summary missing steps")
else:
    raise AssertionError(f"unknown assertion id: {assertion_id}")
PY
}

run_case() {
  local case_id="$1"
  local assertion_id="$2"
  shift 2
  local json_path="${LOG_DIR}/${case_id}.json"
  local notes="ok"
  if "$@" >"${json_path}" 2>"${LOG_DIR}/${case_id}.stderr"; then
    if assert_json_contract "${json_path}" "${assertion_id}"; then
      printf "%s\tok\t%s\t%s\n" "${case_id}" "${json_path}" "${notes}" >> "${SUMMARY_PATH}"
      return
    fi
    notes="contract_assertion_failed"
  else
    notes="command_failed"
  fi
  printf "%s\tfail\t%s\t%s\n" "${case_id}" "${json_path}" "${notes}" >> "${SUMMARY_PATH}"
}

run_case "search_python_asyncservice" "search_python" \
  ./cq search AsyncService \
  --in tests/e2e/cq/_golden_workspace/python_project \
  --lang python \
  --format json \
  --no-save-artifact

run_case "search_rust_compile_target" "search_rust" \
  ./cq search compile_target \
  --in tests/e2e/cq/_golden_workspace/rust_workspace \
  --lang rust \
  --format json \
  --no-save-artifact

run_case "q_entity_auto_compile_target" "q_entity_auto" \
  ./cq q "entity=function name=compile_target lang=auto in=tests/e2e/cq/_golden_workspace/rust_workspace" \
  --format json \
  --no-save-artifact

run_case "calls_python_resolve" "calls_python" \
  ./cq --root tests/e2e/cq/_golden_workspace/python_project calls resolve \
  --format json \
  --no-save-artifact

run_case "calls_rust_compile_target" "calls_rust" \
  ./cq --root tests/e2e/cq/_golden_workspace/rust_workspace calls compile_target \
  --format json \
  --no-save-artifact

run_case "neighborhood_rust_anchor" "neighborhood_rust" \
  ./cq --root tests/e2e/cq/_golden_workspace/rust_workspace neighborhood src/lib.rs:1 \
  --lang rust \
  --format json \
  --no-save-artifact

run_case "neighborhood_rust_symbol" "neighborhood_rust" \
  ./cq --root tests/e2e/cq/_golden_workspace/rust_workspace neighborhood compile_target \
  --lang rust \
  --format json \
  --no-save-artifact

run_case "run_mixed_search_q" "run_mixed" \
  ./cq --root tests/e2e/cq/_golden_workspace/rust_workspace run --steps '[{"type":"search","query":"compile_target","lang":"auto"},{"type":"q","query":"entity=function name=compile_target lang=auto"}]' \
  --format json \
  --no-save-artifact

printf "CQ LSP robustness matrix complete: %s\n" "${SUMMARY_PATH}"
