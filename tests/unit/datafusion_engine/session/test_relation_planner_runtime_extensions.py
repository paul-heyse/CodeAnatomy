"""Tests for relation planner runtime extension helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.expr import relation_planner


def test_install_relation_planner_delegates_to_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime installer should delegate directly to native extension hook."""
    called: dict[str, object] = {}

    def _install(ctx: object) -> None:
        called["ctx"] = ctx

    monkeypatch.setattr(
        "datafusion_engine.extensions.datafusion_ext.install_relation_planner", _install
    )
    ctx = object()
    relation_planner.install_relation_planner(ctx)
    assert called["ctx"] is ctx


def test_relation_planner_extension_available_reflects_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Availability probe should reflect entrypoint presence on extension module."""
    monkeypatch.setattr(
        "datafusion_engine.extensions.datafusion_ext.install_relation_planner",
        lambda _ctx: None,
    )
    assert relation_planner.relation_planner_extension_available()
