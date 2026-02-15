#!/usr/bin/env bash
set -euo pipefail

uv run python scripts/generate_cq_completion.py "$@"
