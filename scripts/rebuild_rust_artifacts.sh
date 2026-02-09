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

datafusion_python_features=("substrait" "async-udf")
if [ "${CODEANATOMY_SUBSTRAIT_PROTOC:-}" = "1" ]; then
  datafusion_python_features+=("protoc")
fi
feature_csv="$(IFS=,; echo "${datafusion_python_features[*]}")"
datafusion_feature_flags=(--features "${feature_csv}")
maturin_common_flags=(--locked)
maturin_wheel_flags=("${maturin_common_flags[@]}")
if [ "${profile}" = "release" ] && [ "${CODEANATOMY_WHEEL_BUILD_SDIST:-1}" = "1" ]; then
  maturin_wheel_flags+=(--sdist)
fi

uv run maturin develop -m rust/datafusion_python/Cargo.toml --${profile} "${datafusion_feature_flags[@]}"
uv run maturin develop -m rust/datafusion_ext_py/Cargo.toml --${profile} "${datafusion_feature_flags[@]}"
(
  cd rust/codeanatomy_engine
  uv run maturin develop --${profile} --features codeanatomy-engine/pyo3
)

wheel_dir="dist/wheels"
mkdir -p "${wheel_dir}"
manylinux_args=()
compatibility="${CODEANATOMY_WHEEL_COMPATIBILITY:-linux}"
case "${compatibility}" in
  linux|off|pypi)
    ;;
  *)
    echo "Invalid CODEANATOMY_WHEEL_COMPATIBILITY: ${compatibility}. Use 'linux', 'off', or 'pypi'." >&2
    exit 1
    ;;
esac
compatibility_args=(--compatibility "${compatibility}")
if [ "$(uname -s)" = "Linux" ] && [ "${compatibility}" = "pypi" ]; then
  manylinux_policy="${CODEANATOMY_WHEEL_MANYLINUX:-2014}"
  if [ "${manylinux_policy}" != "off" ]; then
    manylinux_args=(--manylinux "${manylinux_policy}")
  fi
fi
uv run maturin build -m rust/datafusion_python/Cargo.toml --${profile} "${datafusion_feature_flags[@]}" "${maturin_wheel_flags[@]}" "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${wheel_dir}"
uv lock --refresh-package datafusion
uv run maturin build -m rust/datafusion_ext_py/Cargo.toml --${profile} "${datafusion_feature_flags[@]}" "${maturin_wheel_flags[@]}" "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${wheel_dir}"
uv lock --refresh-package datafusion-ext
(
  cd rust/codeanatomy_engine
  uv run maturin build --${profile} --features codeanatomy-engine/pyo3 "${maturin_common_flags[@]}" "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${wheel_dir}"
)

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
