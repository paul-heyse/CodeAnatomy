#!/usr/bin/env bash
# scripts/bootstrap.sh
# Purpose: Provision uv env, build DataFusion extensions, and stage plugin artifacts.

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/bootstrap.sh from the repository root (pyproject.toml not found)." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install uv and re-run this script." >&2
  exit 1
fi

uv sync

# Build and install the datafusion_ext module.
(
  cd rust/datafusion_ext_py
  uv run maturin develop --release
)

# Build the CodeAnatomy DataFusion plugin.
(
  cd rust/df_plugin_codeanatomy
  cargo build --release
)

plugin_lib="rust/target/release/libdf_plugin_codeanatomy.so"
if [ "$(uname -s)" = "Darwin" ]; then
  plugin_lib="rust/target/release/libdf_plugin_codeanatomy.dylib"
elif [[ "$(uname -s)" =~ MINGW|MSYS|CYGWIN ]]; then
  plugin_lib="rust/target/release/df_plugin_codeanatomy.dll"
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
  \"build_profile\": \"release\"
}
MANIFEST

echo "Bootstrap complete. Plugin staged at rust/datafusion_ext_py/plugin." 
