"""Optional dependency helpers for tests."""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest


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
    """Skip tests when datafusion is unavailable.

    Returns
    -------
    ModuleType
        Imported datafusion module.
    """
    datafusion = pytest.importorskip("datafusion")
    internal = _resolve_datafusion_extension(
        ("install_codeanatomy_policy_config", "install_codeanatomy_physical_config")
    )
    if internal is None:
        pytest.skip(
            "DataFusion extension hooks are unavailable; skipping DataFusion tests.",
            allow_module_level=True,
        )
    return datafusion


def _fallback_udfs_available() -> bool:
    try:
        from datafusion_engine.udf.fallback import fallback_udf_specs
    except ImportError:
        return False
    return bool(fallback_udf_specs())


def require_datafusion_udfs() -> ModuleType:
    """Skip tests when DataFusion UDF extensions are unavailable.

    Returns
    -------
    ModuleType
        Imported datafusion module.
    """
    datafusion = require_datafusion()
    internal = _resolve_datafusion_extension(("register_codeanatomy_udfs",))
    if internal is None:
        if _fallback_udfs_available():
            return datafusion
        pytest.skip(
            "DataFusion build missing codeanatomy UDFs; skipping tests.",
            allow_module_level=True,
        )
    return datafusion


def require_deltalake() -> ModuleType:
    """Skip tests when deltalake is unavailable.

    Returns
    -------
    ModuleType
        Imported deltalake module.
    """
    return pytest.importorskip("deltalake")
