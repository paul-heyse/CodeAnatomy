"""Optional dependency helpers for tests."""

from __future__ import annotations

import importlib
from types import ModuleType


def _resolve_datafusion_extension(required: tuple[str, ...]) -> ModuleType | None:
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if all(hasattr(module, attr) for attr in required):
            return module
    return None


def require_datafusion() -> ModuleType:
    """Require DataFusion and extension hooks for tests.

    Returns
    -------
    ModuleType
        Imported datafusion module.

    Raises
    ------
    RuntimeError
        Raised when DataFusion or required extension hooks are unavailable.
    """
    try:
        datafusion = importlib.import_module("datafusion")
    except ImportError as exc:
        msg = "DataFusion is required for this test profile."
        raise RuntimeError(msg) from exc
    internal = _resolve_datafusion_extension(
        ("install_codeanatomy_policy_config", "install_codeanatomy_physical_config")
    )
    if internal is None:
        msg = (
            "DataFusion extension hooks are unavailable. Rebuild rust artifacts via "
            "`bash scripts/rebuild_rust_artifacts.sh`."
        )
        raise RuntimeError(msg)
    return datafusion


def _fallback_udfs_available() -> bool:
    try:
        from datafusion_engine.udf.fallback import fallback_udf_specs
    except ImportError:
        return False
    return bool(fallback_udf_specs())


def require_datafusion_udfs() -> ModuleType:
    """Require DataFusion UDF extensions for tests.

    Returns
    -------
    ModuleType
        Imported datafusion module.

    Raises
    ------
    RuntimeError
        Raised when UDF support is unavailable and no fallback registry exists.
    """
    datafusion = require_datafusion()
    internal = _resolve_datafusion_extension(("register_codeanatomy_udfs",))
    if internal is None:
        if _fallback_udfs_available():
            return datafusion
        msg = (
            "DataFusion build is missing codeanatomy UDF support. Rebuild rust artifacts via "
            "`bash scripts/rebuild_rust_artifacts.sh`."
        )
        raise RuntimeError(msg)
    return datafusion


def require_deltalake() -> ModuleType:
    """Require Delta Lake for tests.

    Returns
    -------
    ModuleType
        Imported deltalake module.

    Raises
    ------
    RuntimeError
        Raised when the ``deltalake`` package is unavailable.
    """
    try:
        return importlib.import_module("deltalake")
    except ImportError as exc:
        msg = "Delta Lake is required for this test profile."
        raise RuntimeError(msg) from exc


def require_delta_extension() -> ModuleType:
    """Require Delta DataFusion extension support for tests.

    Returns
    -------
    ModuleType
        Imported datafusion module.

    Raises
    ------
    RuntimeError
        Raised when Delta extension support is unavailable or incompatible.
    """
    datafusion = require_datafusion()
    from datafusion import SessionContext

    from datafusion_engine.delta.capabilities import is_delta_extension_compatible

    ctx = SessionContext()
    compatibility = is_delta_extension_compatible(ctx)
    if not compatibility.available:
        msg = (
            "Delta extension module unavailable. Rebuild rust artifacts via "
            "`bash scripts/rebuild_rust_artifacts.sh`."
        )
        raise RuntimeError(msg)
    if not compatibility.compatible:
        details: list[str] = [
            "Delta extension incompatible for this test profile.",
            f"entrypoint={compatibility.entrypoint}",
        ]
        if compatibility.module is not None:
            details.append(f"module={compatibility.module}")
        if compatibility.ctx_kind is not None:
            details.append(f"ctx_kind={compatibility.ctx_kind}")
        if compatibility.probe_result is not None:
            details.append(f"probe_result={compatibility.probe_result}")
        if compatibility.error:
            details.append(f"error={compatibility.error}")
        details.append("rebuild=`bash scripts/rebuild_rust_artifacts.sh`")
        raise RuntimeError(" ".join(details))
    return datafusion
