#!/usr/bin/env bash
# scripts/rebuild_rust_artifacts.sh
# Purpose: Rebuild Rust-backed Python extensions and stage the DataFusion plugin.

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/rebuild_rust_artifacts.sh from the repository root (pyproject.toml not found)." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install uv and re-run this script." >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required. Install Rust and re-run this script." >&2
  exit 1
fi

profile="${CODEANATOMY_RUST_PROFILE:-release}"
if [ "${profile}" != "release" ] && [ "${profile}" != "debug" ]; then
  echo "Invalid CODEANATOMY_RUST_PROFILE: ${profile}. Use 'release' or 'debug'." >&2
  exit 1
fi

if [ "${CODEANATOMY_SKIP_UV_SYNC:-}" != "1" ]; then
  uv sync
fi

uv run maturin develop -m rust/datafusion_python/Cargo.toml --${profile}
uv run maturin develop -m rust/datafusion_ext_py/Cargo.toml --${profile}

wheel_dir="dist/wheels"
mkdir -p "${wheel_dir}"
manylinux_args=()
if [ "$(uname -s)" = "Linux" ]; then
  manylinux_args=(--manylinux 2_39)
fi
uv run maturin build -m rust/datafusion_python/Cargo.toml --${profile} "${manylinux_args[@]}" -o "${wheel_dir}"
uv lock --refresh-package datafusion
uv run maturin build -m rust/datafusion_ext_py/Cargo.toml --${profile} "${manylinux_args[@]}" -o "${wheel_dir}"
uv lock --refresh-package datafusion-ext

(
  cd rust/df_plugin_codeanatomy
  cargo build --${profile}
)

plugin_lib="rust/target/${profile}/libdf_plugin_codeanatomy.so"
if [ "$(uname -s)" = "Darwin" ]; then
  plugin_lib="rust/target/${profile}/libdf_plugin_codeanatomy.dylib"
elif [[ "$(uname -s)" =~ MINGW|MSYS|CYGWIN ]]; then
  plugin_lib="rust/target/${profile}/df_plugin_codeanatomy.dll"
fi

if [ ! -f "${plugin_lib}" ]; then
  echo "Plugin library not found at ${plugin_lib}. Ensure cargo build succeeded." >&2
  exit 1
fi

mkdir -p rust/datafusion_ext_py/plugin
cp -f "${plugin_lib}" rust/datafusion_ext_py/plugin/

mkdir -p build
plugin_abs_path=\"$(pwd)/rust/datafusion_ext_py/plugin/$(basename \"${plugin_lib}\")\"
cat <<MANIFEST > build/datafusion_plugin_manifest.json
{
  \"plugin_path\": \"${plugin_abs_path}\",
  \"build_profile\": \"${profile}\"
}
MANIFEST

echo "Rust rebuild complete."
echo "  profile: ${profile}"
echo "  plugin : rust/datafusion_ext_py/plugin/$(basename \"${plugin_lib}\")"
