#!/usr/bin/env bash
# check_drift_surfaces.sh - CI guardrails for architectural drift detection.
#
# Checks drift surface metrics from the programmatic architecture assessment
# plan (Section 7) and reports current counts against targets.
#
# Usage:
#   scripts/check_drift_surfaces.sh          # Warning mode (non-blocking)
#   scripts/check_drift_surfaces.sh --strict # Blocking mode (exit 1 on violations)
#
# Drift surfaces are points where static declarations or runtime re-derivations
# could diverge from what compiled plans actually require. Tracking these counts
# prevents architectural regression.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$REPO_ROOT/src"

STRICT=false
if [ "${1:-}" = "--strict" ]; then
    STRICT=true
fi

WARNINGS=0
TOTAL_CHECKS=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

check_metric() {
    local name="$1"
    local count="$2"
    local target="$3"

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    if [ "$count" -gt "$target" ]; then
        printf "  WARNING  %s: %d (target: %d)\n" "$name" "$count" "$target"
        WARNINGS=$((WARNINGS + 1))
    else
        printf "  OK       %s: %d (target: %d)\n" "$name" "$count" "$target"
    fi
}

# Portable line-count helper that always returns a clean integer.
# Reads stdin; outputs "0" when stdin is empty.
count_lines() {
    local n
    n=$(wc -l | tr -d ' ')
    if [ -z "$n" ]; then echo 0; else echo "$n"; fi
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

echo "=== Drift Surface Audit ==="
echo "Source root: $SRC_DIR"
echo ""

# ---------------------------------------------------------------------------
# Disable pipefail for all grep pipelines below.  Re-enabled at the summary.
# grep returns exit 1 when no lines match, which kills the script under
# set -eo pipefail.  Metrics are advisory; a zero-match grep is not an error.
# ---------------------------------------------------------------------------
set +o pipefail

# ---------------------------------------------------------------------------
# Check 1: dataset_bindings_for_profile() consumer files
# ---------------------------------------------------------------------------

COUNT=$(grep -rl "dataset_bindings_for_profile" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "compile_context\.py" \
    | grep -v "__pycache__" \
    | count_lines)

echo "1. dataset_bindings_for_profile() consumer files"
echo "   (files outside compile_context.py that call this function)"
check_metric "Consumer files" "$COUNT" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 2: Direct CompileContext(runtime_profile=...) outside compile boundary
# ---------------------------------------------------------------------------

COUNT=$(grep -rn "CompileContext(runtime_profile" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "compile_context\.py" \
    | grep -v "__pycache__" \
    | count_lines)

echo "2. Direct CompileContext(runtime_profile=...) outside compile boundary"
echo "   (callsites outside compile_context.py)"
check_metric "Direct instantiation callsites" "$COUNT" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 3: Global mutable registries in pipeline modules
# ---------------------------------------------------------------------------

COUNT_EXECUTORS=$(grep -rl "_EXTRACT_ADAPTER_EXECUTORS" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | count_lines)

COUNT_STATE=$(grep -rl "_EXTRACT_EXECUTOR_REGISTRATION_STATE" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | count_lines)

GLOBAL_MUTABLE_TOTAL=$((COUNT_EXECUTORS + COUNT_STATE))

echo "3. Global mutable registry patterns"
echo "   _EXTRACT_ADAPTER_EXECUTORS files:            $COUNT_EXECUTORS"
echo "   _EXTRACT_EXECUTOR_REGISTRATION_STATE files:  $COUNT_STATE"
check_metric "Global mutable registry files" "$GLOBAL_MUTABLE_TOTAL" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 4: record_artifact() callsite typing
# ---------------------------------------------------------------------------

ARTIFACT_LINES=$(mktemp)
trap 'rm -f "$ARTIFACT_LINES"' EXIT

grep -rn "record_artifact(" "$SRC_DIR" --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | grep -v "def record_artifact" \
    | grep -v "def _record_artifact" \
    | grep -v "_resolve_artifact_name" \
    | grep -v '"""' \
    | grep -v "# " \
    | grep -v "suitable for" \
    > "$ARTIFACT_LINES" || true

TOTAL=$(wc -l < "$ARTIFACT_LINES" | tr -d ' ')
TYPED=$(grep -cE "_SPEC[,) ]|ArtifactSpec\(" "$ARTIFACT_LINES" || true)
STRING_COUNT=$((TOTAL - TYPED))

echo "4. record_artifact() callsite typing"
echo "   Total production callsites: $TOTAL"
echo "   Typed (ArtifactSpec):        $TYPED"
echo "   String-name (untyped):       $STRING_COUNT"
check_metric "String-name callsites" "$STRING_COUNT" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 5: Redundant compile_semantic_program() call sites
# ---------------------------------------------------------------------------

COMPILE_SITES=$(grep -rn "compile_semantic_program(" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | grep -v "def compile_semantic_program" \
    | grep -v '"""' \
    | grep -v "# " \
    | grep -v 'compile_semantic_program()' \
    | grep -v "NOT inside" \
    | grep -v "Call this from" \
    | count_lines)

echo "5. compile_semantic_program() callsites"
echo "   (excluding definition; target is 1 canonical entrypoint)"
check_metric "Compile callsites" "$COMPILE_SITES" "1"
echo ""

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
set -o pipefail

echo "=== Summary ==="
echo "Checks run: $TOTAL_CHECKS"
echo "Warnings:   $WARNINGS"
echo ""

if [ "$STRICT" = "true" ] && [ "$WARNINGS" -gt 0 ]; then
    echo "FAIL: $WARNINGS drift surface warning(s) detected in strict mode."
    exit 1
fi

if [ "$WARNINGS" -gt 0 ]; then
    echo "ADVISORY: $WARNINGS drift surface warning(s) detected (non-blocking)."
    exit 0
fi

echo "PASS: All drift surfaces within target."
exit 0
