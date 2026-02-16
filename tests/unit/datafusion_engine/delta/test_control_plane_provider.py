# ruff: noqa: D103
"""Tests for Delta control-plane provider split module."""

from __future__ import annotations

import inspect

from datafusion_engine.delta import control_plane_provider


def test_control_plane_provider_has_local_function_definition() -> None:
    source = inspect.getsource(control_plane_provider)
    assert "def delta_provider_from_session" in source
    assert "control_plane_core as _core" not in source
    assert "resolve_storage_profile(" in source
