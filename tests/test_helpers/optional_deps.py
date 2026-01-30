"""Optional dependency helpers for tests."""

from __future__ import annotations

from types import ModuleType

import pytest


def require_datafusion() -> ModuleType:
    """Skip tests when datafusion is unavailable.

    Returns
    -------
    ModuleType
        Imported datafusion module.
    """
    return pytest.importorskip("datafusion")


def require_deltalake() -> ModuleType:
    """Skip tests when deltalake is unavailable.

    Returns
    -------
    ModuleType
        Imported deltalake module.
    """
    return pytest.importorskip("deltalake")
