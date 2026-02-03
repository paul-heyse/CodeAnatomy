#!/usr/bin/env bash
# scripts/bootstrap_codex.sh
# Purpose: Make sure Codex CLI can *use* the existing uv-managed env
#          without trying to install uv or Python or write outside the repo.

set -Eeuo pipefail

# Must be run from repo root
if [ ! -f "pyproject.toml" ]; then
  echo "Run scripts/bootstrap_codex.sh from the repository root (pyproject.toml not found)." >&2
  exit 1
fi

project_root="$(pwd)"
venv_path="${project_root}/.venv"

# Optional: constrain uv to project-local storage if you ever *do* call uv here.
# This keeps all uv writes in the workspace, which is sandbox-safe.
UV_ROOT="${UV_ROOT:-"$project_root/.uv"}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-"$UV_ROOT/cache"}"
export UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR:-"$UV_ROOT/python"}"
export UV_PYTHON_BIN_DIR="${UV_PYTHON_BIN_DIR:-"$UV_ROOT/python/bin"}"
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-"$venv_path"}"

mkdir -p "$UV_CACHE_DIR" "$UV_PYTHON_INSTALL_DIR" "$UV_PYTHON_BIN_DIR"

# Sanity: env must have been created already by scripts/bootstrap.sh (outside Codex)
if [ ! -d "${venv_path}" ]; then
  cat >&2 <<EOF
Error: Expected project environment at ${venv_path}, but it does not exist.

For Codex CLI, do NOT try to create the env from inside the sandbox.

Instead, from a normal shell on this machine run:

  bash scripts/bootstrap.sh

That will install uv (if needed), Python ${PY_VER_DEFAULT:-3.14.2}, create .venv,
and install all dependencies. Then re-run this script from Codex.
EOF
  exit 1
fi

python_bin="${venv_path}/bin/python"

if [ ! -x "${python_bin}" ]; then
  echo "Error: ${python_bin} is missing or not executable. Re-run scripts/bootstrap.sh outside Codex." >&2
  exit 1
fi

plugin_lib_linux="rust/datafusion_ext_py/plugin/libdf_plugin_codeanatomy.so"
plugin_lib_macos="rust/datafusion_ext_py/plugin/libdf_plugin_codeanatomy.dylib"
plugin_lib_windows="rust/datafusion_ext_py/plugin/df_plugin_codeanatomy.dll"
plugin_lib="${plugin_lib_linux}"
case "$(uname -s)" in
  Darwin) plugin_lib="${plugin_lib_macos}" ;;
  MINGW*|MSYS*|CYGWIN*) plugin_lib="${plugin_lib_windows}" ;;
esac

if [ ! -f "${plugin_lib}" ]; then
  echo "DataFusion plugin library missing; attempting rebuild..." >&2
  if ! bash scripts/rebuild_rust_artifacts.sh; then
    cat >&2 <<EOF
Error: Expected DataFusion plugin library at ${plugin_lib}, but rebuild failed.

From a normal shell on this machine run:

  bash scripts/rebuild_rust_artifacts.sh

Or, for a full environment bootstrap:

  bash scripts/bootstrap.sh

That will build and stage the plugin library into rust/datafusion_ext_py/plugin.
EOF
    exit 1
  fi
fi

if ! "${python_bin}" - <<'PY'
import importlib
import sys

try:
    mod = importlib.import_module("datafusion_ext")
except ImportError as exc:
    print(f"Failed to import datafusion_ext: {exc}", file=sys.stderr)
    raise SystemExit(1) from exc

required = ("register_codeanatomy_udfs", "delta_table_provider_from_session")
missing = [name for name in required if not hasattr(mod, name)]
if missing:
    print(
        "datafusion_ext missing required hooks: " + ", ".join(missing),
        file=sys.stderr,
    )
    raise SystemExit(1)
PY
then
  cat >&2 <<EOF
Error: datafusion_ext is unavailable or missing required hooks.

From a normal shell on this machine run:

  bash scripts/rebuild_rust_artifacts.sh

Or, for a full environment bootstrap:

  bash scripts/bootstrap.sh
EOF
  exit 1
fi

# Optionally mimic "activation" for the *current process* (useful if this script is
# chained with other commands in a single Codex shell tool call).
export VIRTUAL_ENV="${venv_path}"
case ":${PATH}:" in
  *":${venv_path}/bin:"*) ;;
  *) export PATH="${venv_path}/bin:${PATH}" ;;
esac
hash -r 2>/dev/null || true

echo "Environment looks good:"
echo "  python: $(${python_bin} -V 2>&1)"
echo "  where : ${python_bin}"
echo "  plugin : ${plugin_lib}"
