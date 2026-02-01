"""Optional dependency helpers for tests."""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest


def require_datafusion() -> ModuleType:
    """Skip tests when datafusion is unavailable.

    Returns
    -------
    ModuleType
        Imported datafusion module.
    """
    datafusion = pytest.importorskip("datafusion")
    try:
        internal = importlib.import_module("datafusion._internal")
    except ImportError:
        pytest.skip(
            "datafusion._internal is unavailable; skipping DataFusion tests.",
            allow_module_level=True,
        )
    if not hasattr(internal, "install_codeanatomy_policy_config"):
        pytest.skip(
            "DataFusion build missing codeanatomy_policy config; skipping tests.",
            allow_module_level=True,
        )
    if not hasattr(internal, "install_codeanatomy_physical_config"):
        pytest.skip(
            "DataFusion build missing codeanatomy_physical config; skipping tests.",
            allow_module_level=True,
        )
    if not hasattr(internal, "register_codeanatomy_udfs"):
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
