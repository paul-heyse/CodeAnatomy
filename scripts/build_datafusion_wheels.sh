#!/usr/bin/env bash
# scripts/build_datafusion_wheels.sh
# Purpose: Build release wheels for datafusion + datafusion_ext and stage plugin artifacts.
# Note: Release builds are CPU-intensive and can take 10+ minutes on typical hardware.

set -Eeuo pipefail

if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/build_datafusion_wheels.sh from the repository root (pyproject.toml not found)." >&2
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

profile="${CODEANATOMY_WHEEL_PROFILE:-release}"
if [ "${profile}" != "release" ] && [ "${profile}" != "debug" ]; then
  echo "Invalid CODEANATOMY_WHEEL_PROFILE: ${profile}. Use 'release' or 'debug'." >&2
  exit 1
fi

wheel_dir="${CODEANATOMY_WHEEL_DIR:-dist/wheels}"
mkdir -p "${wheel_dir}"

if [ "${CODEANATOMY_SKIP_UV_SYNC:-}" != "1" ]; then
  if ls -1 "${wheel_dir}"/datafusion-*.whl >/dev/null 2>&1; then
    uv lock --refresh-package datafusion
  fi
  if ls -1 "${wheel_dir}"/datafusion_ext-*.whl >/dev/null 2>&1; then
    uv lock --refresh-package datafusion-ext
  fi
  if ! uv sync; then
    echo "uv sync failed; refreshing datafusion wheel hashes and retrying..." >&2
    uv lock --refresh-package datafusion
    uv lock --refresh-package datafusion-ext
    uv sync
  fi
fi

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

datafusion_python_features=("substrait" "async-udf")
if [ "${CODEANATOMY_SUBSTRAIT_PROTOC:-}" = "1" ]; then
  datafusion_python_features+=("protoc")
fi
datafusion_feature_csv=""
datafusion_feature_flags=()
if [ "${#datafusion_python_features[@]}" -gt 0 ]; then
  datafusion_feature_csv="$(IFS=,; echo "${datafusion_python_features[*]}")"
  datafusion_feature_flags=(--features "${datafusion_feature_csv}")
fi

uv run maturin build -m rust/datafusion_python/Cargo.toml --${profile} "${datafusion_feature_flags[@]}" "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${wheel_dir}"
uv lock --refresh-package datafusion
uv run maturin build -m rust/datafusion_ext_py/Cargo.toml --${profile} "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${wheel_dir}"
uv lock --refresh-package datafusion-ext

datafusion_wheel="$(ls -1 "${wheel_dir}"/datafusion-*.whl | sort | tail -n 1)"
datafusion_ext_wheel="$(ls -1 "${wheel_dir}"/datafusion_ext-*.whl | sort | tail -n 1)"

if [ -z "${datafusion_wheel}" ] || [ -z "${datafusion_ext_wheel}" ]; then
  echo "Wheel build did not produce expected artifacts in ${wheel_dir}." >&2
  exit 1
fi

uv run python - <<PY
from __future__ import annotations

import importlib
import sys
import tempfile
import zipfile
from pathlib import Path

wheel_path = Path("${datafusion_wheel}")
if not wheel_path.exists():
    raise SystemExit(f"DataFusion wheel not found: {wheel_path}")

ext_path = Path("${datafusion_ext_wheel}")
if not ext_path.exists():
    raise SystemExit(f"DataFusion-ext wheel not found: {ext_path}")

with tempfile.TemporaryDirectory() as tmpdir:
    with zipfile.ZipFile(wheel_path) as archive:
        archive.extractall(tmpdir)
    with zipfile.ZipFile(ext_path) as archive:
        archive.extractall(tmpdir)
    sys.path.insert(0, tmpdir)
    # Ensure the extension module is importable under the expected package.
    importlib.import_module("datafusion._internal")
    module = importlib.import_module("datafusion.substrait")
    producer = getattr(module, "Producer", None)
    to_substrait = getattr(producer, "to_substrait_plan", None) if producer else None
    if not callable(to_substrait):
        raise SystemExit("Substrait Producer API missing from built wheel.")
    ext = importlib.import_module("datafusion_ext")
    capabilities = getattr(ext, "capabilities_snapshot", None)
    if callable(capabilities):
        capabilities()
    else:
        snapshot = getattr(ext, "registry_snapshot", None)
        if not callable(snapshot):
            raise SystemExit(
                "Neither capabilities_snapshot() nor registry_snapshot() is available in datafusion_ext."
            )
        ctx_builder = getattr(ext, "delta_session_context", None)
        if not callable(ctx_builder):
            raise SystemExit("delta_session_context() missing from datafusion_ext wheel.")
        snapshot(ctx_builder())
PY

uv run python - <<PY
from __future__ import annotations

from pathlib import Path

pyproject = Path("pyproject.toml")
text = pyproject.read_text(encoding="utf-8")
lines = text.splitlines()

header = "[tool.uv.sources]"
datafusion_wheel = Path("${datafusion_wheel}").as_posix()
datafusion_ext_wheel = Path("${datafusion_ext_wheel}").as_posix()

def update_section(section_lines: list[str]) -> list[str]:
    updated = []
    seen_datafusion = False
    seen_ext = False
    for line in section_lines:
        stripped = line.strip()
        if stripped.startswith("datafusion "):
            updated.append(f'datafusion = {{ path = "{datafusion_wheel}" }}')
            seen_datafusion = True
            continue
        if stripped.startswith("datafusion_ext "):
            updated.append(f'datafusion_ext = {{ path = "{datafusion_ext_wheel}" }}')
            seen_ext = True
            continue
        updated.append(line)
    if not seen_datafusion:
        updated.append(f'datafusion = {{ path = "{datafusion_wheel}" }}')
    if not seen_ext:
        updated.append(f'datafusion_ext = {{ path = "{datafusion_ext_wheel}" }}')
    return updated

try:
    start = next(index for index, line in enumerate(lines) if line.strip() == header)
except StopIteration:
    lines.append("")
    lines.append(header)
    lines.extend(
        [
            f'datafusion = {{ path = "{datafusion_wheel}" }}',
            f'datafusion_ext = {{ path = "{datafusion_ext_wheel}" }}',
        ]
    )
else:
    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index].strip().startswith("[")),
        len(lines),
    )
    section = update_section(lines[start + 1 : end])
    lines = lines[: start + 1] + section + lines[end:]

pyproject.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

mkdir -p build
plugin_abs_path="$(pwd)/rust/datafusion_ext_py/plugin/$(basename "${plugin_lib}")"
uv run python - <<PY
from __future__ import annotations

import json
from pathlib import Path
import tomllib

datafusion_wheel = Path("${datafusion_wheel}")
datafusion_ext_wheel = Path("${datafusion_ext_wheel}")
feature_csv = "${datafusion_feature_csv}"
features = [feature for feature in feature_csv.split(",") if feature]

def wheel_version(path: Path) -> str:
    parts = path.name.split("-")
    if len(parts) < 2:
        return ""
    return parts[1]

cargo_text = Path("rust/datafusion_python/Cargo.toml").read_text(encoding="utf-8")
cargo = tomllib.loads(cargo_text)
datafusion_dep = cargo.get("dependencies", {}).get("datafusion", {})
datafusion_features = list(datafusion_dep.get("features", []))

manifest = {
    "build_profile": "${profile}",
    "datafusion_ext_version": wheel_version(datafusion_ext_wheel),
    "datafusion_features": datafusion_features,
    "datafusion_python_features": features,
    "datafusion_version": wheel_version(datafusion_wheel),
    "plugin_path": "${plugin_abs_path}",
    "wheel_paths": {
        "datafusion": str(datafusion_wheel),
        "datafusion_ext": str(datafusion_ext_wheel),
    },
}

Path("build").mkdir(parents=True, exist_ok=True)
Path("build/datafusion_plugin_manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "Wheel build complete."
echo "  profile: ${profile}"
echo "  wheels : ${wheel_dir}"
echo "  plugin : rust/datafusion_ext_py/plugin/$(basename \"${plugin_lib}\")"
echo "  uv src : pyproject.toml updated to ${datafusion_wheel} and ${datafusion_ext_wheel}"
