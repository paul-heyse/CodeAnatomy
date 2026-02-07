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
    if [ -z "$n" ]; then
        echo 0
    else
        echo "$n"
    fi
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

echo "=== Drift Surface Audit ==="
echo "Source root: $SRC_DIR"
echo ""

# ---------------------------------------------------------------------------
# Check 1: dataset_bindings_for_profile() consumer files
#
# What: Files in src/ (excluding the definition in compile_context.py) that
#        call dataset_bindings_for_profile(). Each consumer re-derives dataset
#        locations from a runtime profile rather than using the compiled manifest.
# Why:  The target architecture routes all dataset resolution through a single
#        compiled manifest, eliminating redundant profile-to-binding lookups.
# ---------------------------------------------------------------------------

COUNT=$(grep -rl "dataset_bindings_for_profile" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "compile_context\.py" \
    | grep -v "__pycache__" \
    | count_lines || echo 0)

echo "1. dataset_bindings_for_profile() consumer files"
echo "   (files outside compile_context.py that call this function)"
check_metric "Consumer files" "$COUNT" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 2: Direct CompileContext(runtime_profile=...) outside compile boundary
#
# What: Callsites that instantiate CompileContext with a runtime profile
#        outside the canonical compile boundary (compile_context.py).
# Why:  Multiple instantiation points mean multiple compile operations per
#        pipeline run, violating the single-compilation invariant.
# ---------------------------------------------------------------------------

COUNT=$(grep -rn "CompileContext(runtime_profile" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "compile_context\.py" \
    | grep -v "__pycache__" \
    | count_lines || echo 0)

echo "2. Direct CompileContext(runtime_profile=...) outside compile boundary"
echo "   (callsites outside compile_context.py)"
check_metric "Direct instantiation callsites" "$COUNT" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 3: Global mutable registries in pipeline modules
#
# What: Module-level mutable dict/list state used for executor registration
#        or other cross-call coordination.
# Why:  Global mutable state prevents dependency injection, complicates
#        testing, and creates implicit coupling between pipeline phases.
#
# Tracked patterns:
#   _EXTRACT_ADAPTER_EXECUTORS  (extract_execution_registry.py)
#   _EXTRACT_EXECUTOR_REGISTRATION_STATE  (task_execution.py)
# ---------------------------------------------------------------------------

COUNT_EXECUTORS=$(grep -rl "_EXTRACT_ADAPTER_EXECUTORS" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | count_lines || echo 0)

COUNT_STATE=$(grep -rl "_EXTRACT_EXECUTOR_REGISTRATION_STATE" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | count_lines || echo 0)

GLOBAL_MUTABLE_TOTAL=$((COUNT_EXECUTORS + COUNT_STATE))

echo "3. Global mutable registry patterns"
echo "   _EXTRACT_ADAPTER_EXECUTORS files:            $COUNT_EXECUTORS"
echo "   _EXTRACT_EXECUTOR_REGISTRATION_STATE files:  $COUNT_STATE"
check_metric "Global mutable registry files" "$GLOBAL_MUTABLE_TOTAL" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 4: record_artifact() callsite typing
#
# What: Production callsites of record_artifact (method or free function)
#        that pass string literal names vs typed ArtifactSpec objects.
# Why:  Typed specs enable schema validation, fingerprinting, and registry
#        governance. String names bypass all of these safeguards.
#
# Counting methodology:
#   - Total: all record_artifact( lines in src/ that are not definition,
#     resolver, or comment lines.
#   - Typed: callsites referencing _SPEC constants or ArtifactSpec.
#   - String: Total minus Typed.
# ---------------------------------------------------------------------------

# Total production callsites (exclude definitions and internal plumbing)
TOTAL=$(grep -rn "record_artifact(" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | grep -v "def record_artifact" \
    | grep -v "def _record_artifact" \
    | grep -v "_resolve_artifact_name" \
    | grep -v '"""' \
    | grep -v "# " \
    | grep -v "suitable for" \
    | count_lines || echo 0)

# Typed callsites: those passing ArtifactSpec objects or _SPEC constants
TYPED=$(grep -rn "record_artifact(" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | grep -v "def record_artifact" \
    | grep -v "def _record_artifact" \
    | grep -v "_resolve_artifact_name" \
    | grep -E "_SPEC[,) ]|ArtifactSpec\(" \
    | count_lines || echo 0)

STRING_COUNT=$((TOTAL - TYPED))

echo "4. record_artifact() callsite typing"
echo "   Total production callsites: $TOTAL"
echo "   Typed (ArtifactSpec):        $TYPED"
echo "   String-name (untyped):       $STRING_COUNT"
check_metric "String-name callsites" "$STRING_COUNT" "0"
echo ""

# ---------------------------------------------------------------------------
# Check 5: Redundant compile_semantic_program() call sites
#
# What: Production callsites of compile_semantic_program() in src/.
#        The target is exactly 1 canonical entrypoint.
# Why:  Multiple compile sites risk inconsistent manifests across subsystems
#        within a single pipeline run.
# ---------------------------------------------------------------------------

# Filter out definitions, triple-quoted docstrings, comments, and
# backtick-quoted references that appear in docstrings but are not
# actual callsites. The closing-paren form compile_semantic_program()
# appears only in docstring references; actual callsites continue with args.
COMPILE_SITES=$(grep -rn "compile_semantic_program(" "$SRC_DIR" \
    --include="*.py" 2>/dev/null \
    | grep -v "__pycache__" \
    | grep -v "def compile_semantic_program" \
    | grep -v '"""' \
    | grep -v "# " \
    | grep -v 'compile_semantic_program()' \
    | grep -v "NOT inside" \
    | grep -v "Call this from" \
    | count_lines || echo 0)

echo "5. compile_semantic_program() callsites"
echo "   (excluding definition; target is 1 canonical entrypoint)"
check_metric "Compile callsites" "$COMPILE_SITES" "1"
echo ""

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

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
