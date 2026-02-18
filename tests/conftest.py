"""Pytest diagnostics and crash context helpers."""

from __future__ import annotations

import contextlib
import faulthandler
import importlib
import json
import os
import platform
import resource
import signal
import sys
from collections.abc import Mapping
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest

if TYPE_CHECKING:
    from datafusion import SessionContext

try:
    import psutil
except ImportError:  # pragma: no cover - optional dependency
    psutil = None

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
        "numpy",
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
    _ = session


def pytest_sessionfinish(session: object, exitstatus: int) -> None:
    """Persist diagnostics at pytest session completion."""
    _write_json(_RESOURCES_PATH, _collect_resources())
    _write_json(_EXTENSIONS_PATH, _collect_extensions())
    _teardown_faulthandler()
    try:
        from cache.diskcache_factory import close_cache_pool
    except (ImportError, ModuleNotFoundError):
        close_cache_pool = None
    if close_cache_pool is not None:
        close_cache_pool()
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


def _teardown_faulthandler() -> None:
    stream = _STATE.get("faulthandler_file")
    if stream is None:
        return
    with contextlib.suppress(RuntimeError):
        faulthandler.disable()
    sigabrt = getattr(signal, "SIGABRT", None)
    if sigabrt is not None:
        with contextlib.suppress(RuntimeError):
            faulthandler.unregister(sigabrt)
    with contextlib.suppress(OSError):
        stream.close()
    _STATE["faulthandler_file"] = None


@pytest.fixture
def df_profile() -> object:
    """Provide a default DataFusion runtime profile for tests.

    Returns:
    -------
    object
        Runtime profile instance for tests.
    """
    from tests.test_helpers.datafusion_runtime import df_profile as _df_profile

    return _df_profile()


@pytest.fixture
def df_ctx() -> object:
    """Provide a default DataFusion session context for tests.

    Returns:
    -------
    object
        Session context instance for tests.
    """
    from tests.test_helpers.datafusion_runtime import df_ctx as _df_ctx

    return _df_ctx()


@pytest.fixture
def diagnostic_profile() -> tuple[object, object]:
    """Provide a runtime profile wired to a diagnostics collector.

    Returns:
    -------
    tuple[object, object]
        Runtime profile and diagnostics collector tuple.
    """
    from tests.test_helpers.diagnostics import diagnostic_profile as _diagnostic_profile

    return _diagnostic_profile()


def _validate_native_runtime_contract(ctx: SessionContext) -> None:
    from datafusion_engine.udf.extension_core import (
        rust_runtime_install_payload,
        validate_required_udfs,
    )

    runtime_payload = rust_runtime_install_payload(ctx)
    runtime_install_mode = str(runtime_payload.get("runtime_install_mode") or "")
    if runtime_install_mode == "internal_compat":
        msg = (
            "native runtime contract unavailable: runtime_install_mode=internal_compat "
            "(rebuild/install matching datafusion/datafusion_ext wheels)"
        )
        raise RuntimeError(msg)
    snapshot = runtime_payload.get("snapshot")
    if not isinstance(snapshot, Mapping):
        msg = "native runtime contract unavailable: missing Rust runtime snapshot payload"
        raise TypeError(msg)
    validate_required_udfs(
        snapshot,
        required=(
            "stable_hash64",
            "stable_id",
            "stable_id_parts",
            "span_make",
        ),
    )


@pytest.fixture(scope="session")
def require_native_runtime() -> None:
    """Require the Rust-first DataFusion runtime contract for native-path tests."""
    from datafusion_engine.extensions.schema_runtime import load_schema_runtime
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.extension_validation import validate_extension_capabilities

    try:
        profile = DataFusionRuntimeProfile()
        ctx = profile.session_context()
        validate_extension_capabilities(strict=True, ctx=ctx)
        _validate_native_runtime_contract(ctx)
        _ = load_schema_runtime()
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        pytest.skip(f"native runtime contract unavailable: {exc}")


_WHEEL_GATED_TEST_FILES: tuple[str, ...] = (
    "tests/e2e/test_extraction_to_cpg.py",
    "tests/integration/extraction/test_byte_span_canonicalization.py",
    "tests/integration/extraction/test_file_identity_coordination.py",
    "tests/integration/extraction/test_materialize_extract_plan.py",
    "tests/integration/runtime/test_zero_row_internal_table_bootstrap.py",
    "tests/integration/storage/test_cdf_cursor_lifecycle.py",
    "tests/integration/storage/test_resolve_dataset_provider.py",
    "tests/integration/test_cleanup_metadata.py",
    "tests/integration/test_concurrent_writes_localstack.py",
    "tests/integration/test_df_delta_smoke.py",
    "tests/integration/test_engine_session_semantic_config.py",
    "tests/integration/test_engine_session_smoke.py",
    "tests/integration/test_delta_protocol_and_schema_mode.py",
    "tests/integration/test_resolver_identity_integration.py",
    "tests/integration/test_snapshot_identity.py",
    "tests/integration/test_substrait_cross_validation.py",
    "tests/integration/test_vacuum_retention.py",
    "tests/integration/test_zero_row_bootstrap_e2e.py",
    "tests/plan_golden/test_plan_artifacts.py",
    "tests/unit/test_ast_extract.py",
    "tests/unit/test_datafusion_projection_pushdown.py",
    "tests/unit/test_extraction_orchestrator.py",
    "tests/unit/test_inferred_deps.py",
    "tests/unit/test_lineage_plan_variants.py",
    "tests/unit/test_prepared_statements.py",
    "tests/unit/test_repo_blobs_git.py",
    "tests/unit/test_sql_param_binding.py",
    "tests/unit/test_substrait_validation.py",
    "tests/unit/test_view_registry_snapshot.py",
    "tests/incremental/test_view_artifacts.py",
)


def _probe_wheel_runtime_gaps() -> tuple[str, ...]:
    gaps: list[str] = []
    extension = None
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            extension = importlib.import_module(module_name)
            break
        except ImportError:
            continue
    if extension is None or not callable(getattr(extension, "delta_write_ipc", None)):
        gaps.append("delta_write_ipc unavailable")
    try:
        from extraction.rust_session_bridge import (
            build_extraction_session,
            extraction_session_payload,
        )

        _ = build_extraction_session(extraction_session_payload())
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        gaps.append(f"extraction session bridge unavailable ({exc})")
    return tuple(gaps)


_WHEEL_RUNTIME_GAPS = _probe_wheel_runtime_gaps()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Apply wheel-runtime skip markers during test collection."""
    _ = config
    if not _WHEEL_RUNTIME_GAPS:
        return
    reason = "wheel-gated runtime unavailable: " + "; ".join(_WHEEL_RUNTIME_GAPS)
    skip_mark = pytest.mark.skip(reason=reason)
    gated_files = set(_WHEEL_GATED_TEST_FILES)
    for item in items:
        file_path = item.nodeid.split("::", maxsplit=1)[0]
        if file_path in gated_files:
            item.add_marker(skip_mark)
