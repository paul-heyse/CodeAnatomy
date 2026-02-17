"""Tests for UDF extension module loader."""

from __future__ import annotations

from types import ModuleType

import pytest

from datafusion_engine.udf import extension_loader


def test_load_extension_module_uses_importlib(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loader delegates module import to importlib.import_module."""
    module = ModuleType("mock_ext")
    monkeypatch.setattr(extension_loader.importlib, "import_module", lambda _path: module)

    assert extension_loader.load_extension_module() is module
