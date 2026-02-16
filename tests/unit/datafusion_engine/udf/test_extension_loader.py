# ruff: noqa: D100, D103, ANN001, INP001
from __future__ import annotations

from types import ModuleType

from datafusion_engine.udf import extension_loader


def test_load_extension_module_uses_importlib(monkeypatch) -> None:
    module = ModuleType("mock_ext")
    monkeypatch.setattr(extension_loader.importlib, "import_module", lambda _path: module)

    assert extension_loader.load_extension_module() is module
