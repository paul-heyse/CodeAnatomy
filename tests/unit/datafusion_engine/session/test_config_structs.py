"""Tests for runtime profile config struct defaults."""

from __future__ import annotations

from datafusion_engine.session.config_structs import (
    ExecutionConfig,
    RuntimeProfileConfig,
)


def test_runtime_profile_config_defaults() -> None:
    """Runtime profile config initializes expected nested defaults."""
    config = RuntimeProfileConfig()

    assert isinstance(config.execution, ExecutionConfig)
    assert config.features.enable_udfs is True
