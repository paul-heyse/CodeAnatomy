"""Tests for delta control configuration defaults."""

from __future__ import annotations

from datafusion_engine.delta.control_config import DeltaControlConfig


def test_delta_control_config_defaults() -> None:
    """Control config defaults preserve required runtime gate values."""
    config = DeltaControlConfig()
    assert config.require_native_extension is True
    assert config.emit_observability is True
