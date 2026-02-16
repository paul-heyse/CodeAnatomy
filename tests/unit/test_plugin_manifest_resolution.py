"""Unit tests for plugin manifest resolution helpers."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from datafusion_engine.extensions import plugin_manifest as plugin_manifest_mod


class _ManifestModule(ModuleType):
    def __init__(self, *, plugin_path: Path) -> None:
        super().__init__("datafusion_ext")
        self._plugin_path = plugin_path

    def plugin_library_path(self) -> str:
        return str(self._plugin_path.with_name("missing_plugin.so"))

    def plugin_manifest(self, path: str | None = None) -> dict[str, object]:
        if path is None:
            msg = "default plugin location is unavailable"
            raise RuntimeError(msg)
        if Path(path) != self._plugin_path:
            msg = f"unexpected path: {path}"
            raise RuntimeError(msg)
        return {"plugin_path": path, "plugin_name": "codeanatomy"}


class _FailingModule(ModuleType):
    def __init__(self) -> None:
        super().__init__("datafusion_ext")

    def plugin_manifest(self, path: str | None = None) -> dict[str, object]:
        _ = path
        msg = "manifest unavailable"
        raise RuntimeError(msg)


def test_resolve_plugin_manifest_uses_candidate_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Test resolve plugin manifest uses candidate path."""
    plugin_path = tmp_path / "libdf_plugin_codeanatomy.so"
    plugin_path.write_bytes(b"plugin")
    module = _ManifestModule(plugin_path=plugin_path)

    monkeypatch.setattr(
        plugin_manifest_mod.importlib,
        "import_module",
        lambda name: module if name == "datafusion_ext" else (_raise_import(name)),
    )
    monkeypatch.setattr(
        plugin_manifest_mod,
        "_plugin_path_candidates",
        lambda _module: (plugin_path,),
    )

    resolved = plugin_manifest_mod.resolve_plugin_manifest("datafusion_ext")
    assert resolved.error is None
    assert resolved.manifest is not None
    assert resolved.manifest.get("plugin_path") == str(plugin_path)


def test_resolve_plugin_manifest_reports_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test resolve plugin manifest reports error."""
    module = _FailingModule()

    monkeypatch.setattr(
        plugin_manifest_mod.importlib,
        "import_module",
        lambda name: module if name == "datafusion_ext" else (_raise_import(name)),
    )
    monkeypatch.setattr(
        plugin_manifest_mod,
        "_plugin_path_candidates",
        lambda _module: (),
    )

    resolved = plugin_manifest_mod.resolve_plugin_manifest("datafusion_ext")
    assert resolved.manifest is None
    assert resolved.error is not None
    assert "manifest unavailable" in resolved.error


def _raise_import(name: str) -> ModuleType:
    msg = f"no module named {name}"
    raise ImportError(msg)
