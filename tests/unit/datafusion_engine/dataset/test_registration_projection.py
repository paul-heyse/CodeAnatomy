"""Tests for projection-specific dataset registration helpers."""

from __future__ import annotations

import inspect

from datafusion_engine.dataset import registration_projection


def test_registration_projection_exports_projection_helpers() -> None:
    """Expose public projection helper entrypoints from registration module."""
    assert callable(registration_projection.apply_projection_overrides)
    assert callable(registration_projection.apply_projection_scan_overrides)


def test_registration_projection_contains_owned_projection_logic() -> None:
    """Keep projection internals in this module during architecture cutover."""
    source = inspect.getsource(registration_projection)
    assert "def _projection_exprs_for_schema" in source
    assert "def _sql_literal_for_field" in source
