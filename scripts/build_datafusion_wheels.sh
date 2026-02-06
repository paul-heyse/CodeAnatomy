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
run_wheel_dir="$(mktemp -d "${wheel_dir%/}/.build_datafusion_wheels.XXXXXX")"
run_wheel_dir="$(cd "${run_wheel_dir}" && pwd)"
cleanup_run_wheel_dir() {
  rm -rf "${run_wheel_dir}"
}
trap cleanup_run_wheel_dir EXIT

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

plugin_filename="$(basename "${plugin_lib}")"

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
maturin_common_flags=(--locked)
if [ "${profile}" = "release" ] && [ "${CODEANATOMY_WHEEL_BUILD_SDIST:-1}" = "1" ]; then
  maturin_common_flags+=(--sdist)
fi

uv run maturin build -m rust/datafusion_python/Cargo.toml --${profile} "${datafusion_feature_flags[@]}" "${maturin_common_flags[@]}" "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${run_wheel_dir}"
uv lock --refresh-package datafusion
(
  cd rust/datafusion_ext_py
  uv run maturin build --${profile} "${datafusion_feature_flags[@]}" "${maturin_common_flags[@]}" "${compatibility_args[@]}" "${manylinux_args[@]}" -o "${run_wheel_dir}"
)
uv lock --refresh-package datafusion-ext

shopt -s nullglob
datafusion_wheels=("${run_wheel_dir}"/datafusion-*.whl)
datafusion_ext_wheels=("${run_wheel_dir}"/datafusion_ext-*.whl)
shopt -u nullglob

if [ "${#datafusion_wheels[@]}" -ne 1 ] || [ "${#datafusion_ext_wheels[@]}" -ne 1 ]; then
  echo "Wheel build did not produce exactly one datafusion and one datafusion_ext wheel in ${run_wheel_dir}." >&2
  echo "Found datafusion wheels: ${#datafusion_wheels[@]}, datafusion_ext wheels: ${#datafusion_ext_wheels[@]}." >&2
  exit 1
fi

datafusion_wheel="${datafusion_wheels[0]}"
datafusion_ext_wheel="${datafusion_ext_wheels[0]}"

if [ "$(uname -s)" = "Linux" ]; then
  case "${compatibility}" in
    linux)
      if [[ "$(basename "${datafusion_wheel}")" == *manylinux* || "$(basename "${datafusion_wheel}")" == *musllinux* ]]; then
        echo "Expected a linux-tagged datafusion wheel for compatibility=linux, got: ${datafusion_wheel}" >&2
        exit 1
      fi
      if [[ "$(basename "${datafusion_ext_wheel}")" == *manylinux* || "$(basename "${datafusion_ext_wheel}")" == *musllinux* ]]; then
        echo "Expected a linux-tagged datafusion_ext wheel for compatibility=linux, got: ${datafusion_ext_wheel}" >&2
        exit 1
      fi
      ;;
    pypi)
      if [[ "$(basename "${datafusion_wheel}")" != *manylinux* && "$(basename "${datafusion_wheel}")" != *musllinux* ]]; then
        echo "Expected a manylinux/musllinux-tagged datafusion wheel for compatibility=pypi, got: ${datafusion_wheel}" >&2
        exit 1
      fi
      if [[ "$(basename "${datafusion_ext_wheel}")" != *manylinux* && "$(basename "${datafusion_ext_wheel}")" != *musllinux* ]]; then
        echo "Expected a manylinux/musllinux-tagged datafusion_ext wheel for compatibility=pypi, got: ${datafusion_ext_wheel}" >&2
        exit 1
      fi
      ;;
  esac
fi

# Ensure the plugin shared library is embedded in the ext wheel payload.
# We include both `datafusion_ext/<lib>` (preferred lookup path) and
# `datafusion_ext/plugin/<lib>` (legacy compatibility path), then rewrite RECORD.
uv run python - <<PY
from __future__ import annotations

import base64
import csv
import hashlib
import tempfile
import zipfile
from pathlib import Path

wheel_path = Path("${datafusion_ext_wheel}")
plugin_src = Path("${plugin_lib}").resolve()
plugin_name = "${plugin_filename}"

if not wheel_path.exists():
    raise SystemExit(f"DataFusion-ext wheel not found: {wheel_path}")
if not plugin_src.exists():
    raise SystemExit(f"Plugin library not found: {plugin_src}")

def digest_sha256(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return f"sha256={encoded}"

plugin_bytes = plugin_src.read_bytes()
with tempfile.TemporaryDirectory() as tmpdir:
    root = Path(tmpdir)
    with zipfile.ZipFile(wheel_path) as archive:
        archive.extractall(root)

    package_dir = root / "datafusion_ext"
    if not package_dir.exists():
        raise SystemExit(
            "Expected a datafusion_ext package directory in the built wheel payload."
        )

    direct_target = package_dir / plugin_name
    direct_target.write_bytes(plugin_bytes)

    compat_dir = package_dir / "plugin"
    compat_dir.mkdir(parents=True, exist_ok=True)
    compat_target = compat_dir / plugin_name
    compat_target.write_bytes(plugin_bytes)

    dist_infos = sorted(root.glob("*.dist-info"))
    if len(dist_infos) != 1:
        raise SystemExit(f"Expected exactly one .dist-info directory, found {len(dist_infos)}")
    record_path = dist_infos[0] / "RECORD"
    if not record_path.exists():
        raise SystemExit("Wheel RECORD file is missing.")

    rows: list[tuple[str, str, str]] = []
    for file_path in sorted(path for path in root.rglob("*") if path.is_file()):
        rel_path = file_path.relative_to(root).as_posix()
        if file_path == record_path:
            continue
        payload = file_path.read_bytes()
        rows.append((rel_path, digest_sha256(payload), str(len(payload))))
    rows.append((record_path.relative_to(root).as_posix(), "", ""))

    with record_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(rows)

    rebuilt_path = wheel_path.with_suffix(".rebuilt.whl")
    if rebuilt_path.exists():
        rebuilt_path.unlink()
    with zipfile.ZipFile(rebuilt_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(path for path in root.rglob("*") if path.is_file()):
            archive.write(file_path, file_path.relative_to(root).as_posix())
    rebuilt_path.replace(wheel_path)
PY

echo "Selected wheel artifacts:"
echo "  datafusion    : ${datafusion_wheel}"
echo "  datafusion_ext: ${datafusion_ext_wheel}"

cp -f "${run_wheel_dir}"/* "${wheel_dir}/"
datafusion_wheel="${wheel_dir}/$(basename "${datafusion_wheel}")"
datafusion_ext_wheel="${wheel_dir}/$(basename "${datafusion_ext_wheel}")"

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
        ext_entries = archive.namelist()
    plugin_entries = [
        entry
        for entry in ext_entries
        if "df_plugin_codeanatomy" in entry and entry.endswith((".so", ".dylib", ".dll"))
    ]
    if not plugin_entries:
        raise SystemExit("datafusion_ext wheel is missing embedded plugin shared library.")
    sys.path.insert(0, tmpdir)
    # Ensure the extension module is importable under the expected package.
    importlib.import_module("datafusion._internal")
    module = importlib.import_module("datafusion.substrait")
    producer = getattr(module, "Producer", None)
    to_substrait = getattr(producer, "to_substrait_plan", None) if producer else None
    if not callable(to_substrait):
        raise SystemExit("Substrait Producer API missing from built wheel.")
    ext = importlib.import_module("datafusion_ext")
    ctx_builder = getattr(ext, "delta_session_context", None)
    if not callable(ctx_builder):
        raise SystemExit("delta_session_context() missing from datafusion_ext wheel.")
    capabilities = getattr(ext, "capabilities_snapshot", None)
    if not callable(capabilities):
        raise SystemExit("capabilities_snapshot() is missing from datafusion_ext wheel.")
    caps = capabilities()
    if not isinstance(caps, dict):
        raise SystemExit("capabilities_snapshot() must return a dict payload.")
    required_caps = ("delta_control_plane", "substrait", "async_udf")
    missing_caps = [name for name in required_caps if name not in caps]
    if missing_caps:
        raise SystemExit(f"Missing required capabilities: {missing_caps}")
    plugin_manifest = getattr(ext, "plugin_manifest", None)
    if not callable(plugin_manifest):
        raise SystemExit("plugin_manifest() is missing from datafusion_ext wheel.")
    manifest = plugin_manifest()
    plugin_path = manifest.get("plugin_path") if isinstance(manifest, dict) else None
    if not plugin_path or not Path(str(plugin_path)).exists():
        raise SystemExit(f"plugin_manifest() returned invalid plugin_path: {plugin_path!r}")
    register_udfs = getattr(ext, "register_codeanatomy_udfs", None)
    if not callable(register_udfs):
        raise SystemExit("register_codeanatomy_udfs() missing from datafusion_ext wheel.")
    try:
        register_udfs(ctx_builder(), True, 1000, 64)
    except Exception as exc:
        raise SystemExit(f"Async UDF feature validation failed: {exc}") from exc
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
    "wheel_plugin_paths": [
        f"datafusion_ext/${plugin_filename}",
        f"datafusion_ext/plugin/${plugin_filename}",
    ],
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
