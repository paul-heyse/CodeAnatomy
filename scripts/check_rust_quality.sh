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

export PYO3_PYTHON="${PYO3_PYTHON:-$(uv run python -c 'import sys; print(sys.executable)')}"

rust_dir="rust"

(cd "${rust_dir}" && cargo test -p codeanatomy-engine)
(cd "${rust_dir}" && cargo clippy -p codeanatomy-engine --all-targets --all-features --no-deps -- -D warnings)

uv run maturin develop -m rust/codeanatomy_engine_py/Cargo.toml --release
uv run python -c "import codeanatomy_engine; print(codeanatomy_engine.__doc__ or 'ok')"
uv run pytest tests/integration/test_rust_engine_e2e.py -q

(cd "${rust_dir}" && cargo clippy -p codeanatomy_engine_py --all-targets --all-features -- -D warnings)

(cd "${rust_dir}" && cargo test -p datafusion_ext)
(cd "${rust_dir}" && cargo clippy -p datafusion_ext --all-targets --all-features -- -D warnings)
