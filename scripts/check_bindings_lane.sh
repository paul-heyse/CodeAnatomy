#!/usr/bin/env bash

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/check_bindings_lane.sh from repository root (pyproject.toml not found)." >&2
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

python_exec="${PYO3_PYTHON:-$(uv run python -c 'import sys; print(sys.executable)')}"
export PYO3_PYTHON="${python_exec}"
export PYTHON_SYS_EXECUTABLE="${PYTHON_SYS_EXECUTABLE:-${python_exec}}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(pwd)/rust/target/bindings_lane}"

(cd rust && cargo test -p codeanatomy_engine_py --no-run)
uv run maturin develop -m rust/codeanatomy_engine_py/Cargo.toml --release
uv run python -c "import codeanatomy_engine; print(codeanatomy_engine.__doc__ or 'ok')"
uv run pytest tests/integration/test_rust_engine_e2e.py -q
