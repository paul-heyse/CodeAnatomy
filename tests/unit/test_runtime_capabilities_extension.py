"""Unit tests for runtime execution metrics collection helpers."""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.extensions import runtime_capabilities as runtime_capabilities_mod

if TYPE_CHECKING:
    from datafusion import SessionContext


class _Ctx:
    def __init__(self) -> None:
        self.ctx = object()


class _MetricsModule(ModuleType):
    def __init__(self, name: str, payload: dict[str, object] | None = None) -> None:
        super().__init__(name)
        self._payload = payload
        self.calls = 0

    def runtime_execution_metrics_snapshot(self, _ctx: object) -> dict[str, object]:
        self.calls += 1
        if self._payload is None:
            msg = f"{self.__name__} metrics unavailable"
            raise TypeError(msg)
        return dict(self._payload)


def _session_ctx_for_metrics() -> SessionContext:
    """Return a typed context shim for extension metric probes."""
    return cast("SessionContext", _Ctx())


def test_collect_runtime_execution_metrics_prefers_internal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal = _MetricsModule(
        "datafusion._internal",
        payload={"summary": {"memory_reserved_bytes": 42}, "rows": []},
    )
    ext = _MetricsModule("datafusion_ext", payload={"summary": {"memory_reserved_bytes": 1}})

    def _import_module(name: str) -> ModuleType:
        if name == "datafusion._internal":
            return internal
        if name == "datafusion_ext":
            return ext
        msg = f"no module named {name}"
        raise ImportError(msg)

    monkeypatch.setattr(runtime_capabilities_mod.importlib, "import_module", _import_module)

    payload = runtime_capabilities_mod.collect_runtime_execution_metrics(_session_ctx_for_metrics())
    assert payload is not None
    assert payload.get("summary") == {"memory_reserved_bytes": 42}
    assert internal.calls == 1
    assert ext.calls == 0


def test_collect_runtime_execution_metrics_falls_back_to_datafusion_ext(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ext = _MetricsModule(
        "datafusion_ext",
        payload={"summary": {"metadata_cache_entries": 11}, "rows": []},
    )

    def _import_module(name: str) -> ModuleType:
        if name == "datafusion._internal":
            msg = "module not available"
            raise ImportError(msg)
        if name == "datafusion_ext":
            return ext
        msg = f"no module named {name}"
        raise ImportError(msg)

    monkeypatch.setattr(runtime_capabilities_mod.importlib, "import_module", _import_module)

    payload = runtime_capabilities_mod.collect_runtime_execution_metrics(_session_ctx_for_metrics())
    assert payload is not None
    assert payload.get("summary") == {"metadata_cache_entries": 11}
    assert ext.calls == 1


def test_collect_runtime_execution_metrics_returns_error_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal = _MetricsModule("datafusion._internal", payload=None)
    ext = _MetricsModule("datafusion_ext", payload=None)

    def _import_module(name: str) -> ModuleType:
        if name == "datafusion._internal":
            return internal
        if name == "datafusion_ext":
            return ext
        msg = f"no module named {name}"
        raise ImportError(msg)

    monkeypatch.setattr(runtime_capabilities_mod.importlib, "import_module", _import_module)

    payload = runtime_capabilities_mod.collect_runtime_execution_metrics(_session_ctx_for_metrics())
    assert payload is not None
    error = payload.get("error")
    assert isinstance(error, str)
    assert "datafusion._internal:" in error
    assert "datafusion_ext:" in error
