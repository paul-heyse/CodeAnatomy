#!/usr/bin/env bash
set -euo pipefail

BEFORE="${1:-artifacts/semantic_plan_fingerprints.before.json}"
AFTER="${2:-artifacts/semantic_plan_fingerprints.after.json}"

uv run python tools/plan_fingerprint_gate.py --before "${BEFORE}" --after "${AFTER}"
