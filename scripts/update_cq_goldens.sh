#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[cq-goldens] Updating CQ e2e command snapshots..."
uv run pytest -q \
  tests/e2e/cq/test_search_command_e2e.py \
  tests/e2e/cq/test_q_command_e2e.py \
  tests/e2e/cq/test_neighborhood_command_e2e.py \
  tests/e2e/cq/test_run_command_e2e.py \
  tests/e2e/cq/test_chain_command_e2e.py \
  tests/e2e/cq/test_ldmd_command_e2e.py \
  --update-golden

echo "[cq-goldens] Verifying snapshots are stable without update flag..."
uv run pytest -q \
  tests/e2e/cq/test_search_command_e2e.py \
  tests/e2e/cq/test_q_command_e2e.py \
  tests/e2e/cq/test_neighborhood_command_e2e.py \
  tests/e2e/cq/test_run_command_e2e.py \
  tests/e2e/cq/test_chain_command_e2e.py \
  tests/e2e/cq/test_ldmd_command_e2e.py

echo "[cq-goldens] Changed fixture files:"
git status --short tests/e2e/cq/fixtures || true
