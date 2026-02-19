"""Unit tests for Delta extension capability probing."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from types import ModuleType

import pytest
from datafusion import SessionContext

from datafusion_engine.delta import capabilities


class _FallbackOnlyModule(ModuleType):
    def __init__(self) -> None:
        super().__init__("datafusion_ext")
        self._ctx = object()

    def delta_session_context(self) -> object:
        return self._ctx

    def delta_scan_config_from_session(self, ctx: object, *args: object) -> dict[str, object]:
        if ctx is not self._ctx:
            msg = "argument 'ctx': incompatible context"
            raise TypeError(msg)
        _ = args
        return {"ok": True}


class _MissingEntrypointModule(ModuleType):
    def __init__(self) -> None:
        super().__init__("datafusion_ext")


def _import_override(module: ModuleType, *, match: str) -> Callable[[str], ModuleType]:
    def _load(name: str) -> ModuleType:
        if name == match:
            return module
        msg = f"no module named {name}"
        raise ImportError(msg)

    return _load


def test_delta_extension_compatibility_uses_fallback_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Required Delta entrypoints reject fallback-only context compatibility."""
    module = _FallbackOnlyModule()
    monkeypatch.setattr(capabilities, "_DELTA_EXTENSION_MODULES", ("datafusion_ext",))
    monkeypatch.setattr(
        importlib, "import_module", _import_override(module, match="datafusion_ext")
    )

    compatibility = capabilities.is_delta_extension_compatible(SessionContext())

    assert compatibility.available is True
    assert compatibility.compatible is False
    assert compatibility.entrypoint == "delta_scan_config_from_session"
    assert compatibility.module == "datafusion_ext"
    assert compatibility.ctx_kind is None
    assert compatibility.probe_result == "error"
    assert compatibility.error is not None


def test_delta_extension_compatibility_strict_non_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict and non-strict probes both reject fallback-only required entrypoints."""
    module = _FallbackOnlyModule()
    monkeypatch.setattr(capabilities, "_DELTA_EXTENSION_MODULES", ("datafusion_ext",))
    monkeypatch.setattr(
        importlib, "import_module", _import_override(module, match="datafusion_ext")
    )

    compatibility = capabilities.is_delta_extension_compatible(
        SessionContext(),
        require_non_fallback=True,
    )

    assert compatibility.available is True
    assert compatibility.compatible is False
    assert compatibility.entrypoint == "delta_scan_config_from_session"
    assert compatibility.module == "datafusion_ext"
    assert compatibility.ctx_kind is None
    assert compatibility.probe_result == "error"
    assert compatibility.error is not None


def test_delta_extension_compatibility_reports_missing_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return actionable diagnostics when the probe entrypoint is unavailable."""
    module = _MissingEntrypointModule()
    monkeypatch.setattr(capabilities, "_DELTA_EXTENSION_MODULES", ("datafusion_ext",))
    monkeypatch.setattr(
        importlib, "import_module", _import_override(module, match="datafusion_ext")
    )

    compatibility = capabilities.is_delta_extension_compatible(SessionContext())

    assert compatibility.available is True
    assert compatibility.compatible is False
    assert compatibility.entrypoint == "delta_scan_config_from_session"
    assert compatibility.module == "datafusion_ext"
    assert compatibility.ctx_kind is None
    assert compatibility.probe_result == "entrypoint_unavailable"
