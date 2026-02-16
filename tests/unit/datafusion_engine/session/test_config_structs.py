# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from datafusion_engine.session.config_structs import (
    ExecutionConfig,
    RuntimeProfileConfig,
)


def test_runtime_profile_config_defaults() -> None:
    config = RuntimeProfileConfig()

    assert isinstance(config.execution, ExecutionConfig)
    assert config.features.enable_udfs is True
