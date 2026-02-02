"""Pytest diagnostics and crash context helpers."""

from __future__ import annotations

import faulthandler
import json
import os
import platform
import resource
import signal
import sys
from importlib import metadata
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

try:
    import psutil
except ImportError:  # pragma: no cover - optional dependency
    psutil = None

try:
    from hamilton.execution import graph_functions as hamilton_graph_functions
except ImportError:  # pragma: no cover - optional dependency
    hamilton_graph_functions = None

os.environ.setdefault("HAMILTON_TELEMETRY_ENABLED", "true")

_DIAG_DIR = Path("build/test-results")
_ENV_PATH = _DIAG_DIR / "diagnostics_env.json"
_VERSIONS_PATH = _DIAG_DIR / "diagnostics_versions.json"
_RESOURCES_PATH = _DIAG_DIR / "diagnostics_resources.json"
_EXTENSIONS_PATH = _DIAG_DIR / "diagnostics_extensions.json"
_TRACE_PATH = _DIAG_DIR / "diagnostics_tracebacks.log"
_PLAN_SNAPSHOT_PATH = _DIAG_DIR / "plan_snapshots.jsonl"
_PLAN_SCHEMA_LOG_PATH = _DIAG_DIR / "plan_schema_log.jsonl"

_STATE: dict[str, Any] = {"faulthandler_file": None}


def _env_subset(prefixes: tuple[str, ...]) -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if key.startswith(prefixes)}


def _collect_env() -> dict[str, Any]:
    return {
        "python": sys.version,
        "executable": sys.executable,
        "platform": platform.platform(),
        "uname": platform.uname()._asdict(),
        "env": _env_subset(
            (
                "PYTHON",
                "LD_",
                "ARROW",
                "DATAFUSION",
                "HAMILTON",
                "CODEANATOMY",
            )
        ),
        "pyarrow_version": pa.__version__,
        "sys_path": list(sys.path),
    }


def _collect_versions() -> dict[str, str]:
    packages = (
        "pyarrow",
        "datafusion",
        "libcst",
        "hamilton",
        "numpy",
        "pandas",
        "pytest",
        "ruff",
        "pyrefly",
    )
    versions: dict[str, str] = {}
    for name in packages:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
    return versions


def _collect_resources() -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    resources: dict[str, Any] = {
        "rusage": {
            "user_time": usage.ru_utime,
            "system_time": usage.ru_stime,
            "max_rss_kb": usage.ru_maxrss,
        },
        "arrow_memory_pool": {
            "bytes_allocated": pa.default_memory_pool().bytes_allocated(),
        },
    }
    if psutil is None:
        return resources
    process = psutil.Process()
    resources["psutil"] = {
        "memory_info": process.memory_info()._asdict(),
        "num_threads": process.num_threads(),
    }
    return resources


def _collect_extensions() -> dict[str, str]:
    extensions: dict[str, str] = {}
    for name, module in sys.modules.items():
        path = getattr(module, "__file__", None)
        if not path:
            continue
        if path.endswith((".so", ".pyd")) or ".so." in path:
            extensions[name] = path
    return extensions


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        return


def _patch_hamilton_input_string() -> None:
    graph_functions = hamilton_graph_functions
    if graph_functions is None:
        return

    def _safe_input_string(kwargs: dict[str, object]) -> str:
        _ = kwargs
        return "<node inputs elided>"

    graph_functions.create_input_string = _safe_input_string


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register shared command-line options for test suites."""
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update golden snapshot files with current output",
    )


def pytest_sessionstart(session: object) -> None:
    """Initialize diagnostic capture for the pytest session."""
    _DIAG_DIR.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("HAMILTON_TELEMETRY_ENABLED", "true")
    if os.environ.get("CODEANATOMY_PLUGIN_STUB", "").lower() in {"1", "true", "yes"}:
        from test_support import datafusion_ext_stub

        sys.modules.setdefault("datafusion_ext", datafusion_ext_stub)
    _write_json(_ENV_PATH, _collect_env())
    _write_json(_VERSIONS_PATH, _collect_versions())
    _write_json(_RESOURCES_PATH, _collect_resources())
    if "ARROWDSL_PLAN_SNAPSHOT_PATH" not in os.environ:
        os.environ["ARROWDSL_PLAN_SNAPSHOT_PATH"] = str(_PLAN_SNAPSHOT_PATH)
    if "ARROWDSL_PLAN_SCHEMA_LOG_PATH" not in os.environ:
        os.environ["ARROWDSL_PLAN_SCHEMA_LOG_PATH"] = str(_PLAN_SCHEMA_LOG_PATH)
    _setup_faulthandler()
    _patch_hamilton_input_string()
    _ = session


def pytest_sessionfinish(session: object, exitstatus: int) -> None:
    """Persist diagnostics at pytest session completion."""
    _write_json(_RESOURCES_PATH, _collect_resources())
    _write_json(_EXTENSIONS_PATH, _collect_extensions())
    _ = (session, exitstatus)


def _setup_faulthandler() -> None:
    if _STATE["faulthandler_file"] is not None:
        return
    try:
        _STATE["faulthandler_file"] = _TRACE_PATH.open("a", encoding="utf-8")
    except OSError:
        return
    faulthandler.enable(_STATE["faulthandler_file"], all_threads=True)
    sigabrt = getattr(signal, "SIGABRT", None)
    if sigabrt is None:
        return
    try:
        faulthandler.register(sigabrt, file=_STATE["faulthandler_file"], all_threads=True)
    except RuntimeError:
        return


@pytest.fixture
def df_profile() -> object:
    """Provide a default DataFusion runtime profile for tests.

    Returns
    -------
    object
        Runtime profile instance for tests.
    """
    from tests.test_helpers.datafusion_runtime import df_profile as _df_profile

    return _df_profile()


@pytest.fixture
def df_ctx() -> object:
    """Provide a default DataFusion session context for tests.

    Returns
    -------
    object
        Session context instance for tests.
    """
    from tests.test_helpers.datafusion_runtime import df_ctx as _df_ctx

    return _df_ctx()


@pytest.fixture
def diagnostic_profile() -> tuple[object, object]:
    """Provide a runtime profile wired to a diagnostics collector.

    Returns
    -------
    tuple[object, object]
        Runtime profile and diagnostics collector tuple.
    """
    from tests.test_helpers.diagnostics import diagnostic_profile as _diagnostic_profile

    return _diagnostic_profile()
