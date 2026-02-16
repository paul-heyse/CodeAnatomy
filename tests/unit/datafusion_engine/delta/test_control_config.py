# ruff: noqa: D100, D103
from __future__ import annotations

from datafusion_engine.delta.control_config import DeltaControlConfig


def test_delta_control_config_defaults() -> None:
    config = DeltaControlConfig()
    assert config.require_native_extension is True
    assert config.emit_observability is True
