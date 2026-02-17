"""Tests for Rust plan bundle bridge helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.plan import rust_bundle_bridge


def test_rust_bundle_bridge_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bridge helpers should delegate to extension entrypoints."""
    monkeypatch.setattr(
        rust_bundle_bridge.datafusion_ext,
        "capture_plan_bundle_runtime",
        lambda _ctx, payload, *, _df: {"captured": payload},
    )
    monkeypatch.setattr(
        rust_bundle_bridge.datafusion_ext,
        "build_plan_bundle_artifact_with_warnings",
        lambda _ctx, payload, *, _df: {"built": payload},
    )

    assert rust_bundle_bridge.capture_plan_bundle_runtime(object(), {"a": 1}, df=object()) == {
        "captured": {"a": 1}
    }
    assert rust_bundle_bridge.build_plan_bundle_artifact_with_warnings(
        object(),
        {"b": 2},
        df=object(),
    ) == {"built": {"b": 2}}
