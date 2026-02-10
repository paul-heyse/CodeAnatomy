#!/usr/bin/env bash

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/check_rust_quality.sh from the repository root (pyproject.toml not found)." >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required. Install Rust and re-run this script." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install uv and re-run this script." >&2
  exit 1
fi

rust_dir="rust"

bash scripts/check_no_legacy_planning_imports.sh --strict-tests

(cd "${rust_dir}" && cargo test -p codeanatomy-engine)
(cd "${rust_dir}" && cargo clippy -p codeanatomy-engine --all-targets --all-features --no-deps -- -D warnings)

bash scripts/check_bindings_lane.sh

(cd "${rust_dir}" && cargo clippy -p codeanatomy_engine_py --all-targets --all-features -- -D warnings)

(cd "${rust_dir}" && cargo test -p datafusion_ext)
(cd "${rust_dir}" && cargo clippy -p datafusion_ext --all-targets --all-features -- -D warnings)
