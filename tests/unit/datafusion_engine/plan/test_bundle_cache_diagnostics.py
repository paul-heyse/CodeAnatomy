"""Tests for cache-diagnostics capture in plan bundle assembly."""

from __future__ import annotations

from typing import cast

import pytest
from datafusion import SessionContext

from datafusion_engine.plan import bundle_assembly


class _DummyContext:
    pass


def test_capture_cache_diagnostics_returns_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache-diagnostic capture should preserve mapping payloads."""

    def _capture(_ctx: SessionContext) -> dict[str, object]:
        return {"cache_snapshots": [{"cache_name": "metadata", "entry_count": 1}]}

    monkeypatch.setattr(bundle_assembly, "capture_cache_diagnostics", _capture)
    payload = bundle_assembly._capture_cache_diagnostics(  # noqa: SLF001
        cast("SessionContext", _DummyContext())
    )
    assert isinstance(payload, dict)
    assert "cache_snapshots" in payload


def test_capture_cache_diagnostics_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache-diagnostic capture should fail open and return None."""

    def _capture(_ctx: SessionContext) -> dict[str, object]:
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(bundle_assembly, "capture_cache_diagnostics", _capture)
    payload = bundle_assembly._capture_cache_diagnostics(  # noqa: SLF001
        cast("SessionContext", _DummyContext())
    )
    assert payload is None
