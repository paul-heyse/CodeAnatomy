#!/usr/bin/env bash
set -euo pipefail

uv run scripts/generate_cq_completion.py "$@"
