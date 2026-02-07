#!/usr/bin/env bash
# Thin wrapper to preserve existing entrypoint while delegating to AST audit.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

exec uv run python scripts/check_drift_surfaces.py "$@"
