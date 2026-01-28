"""Unit tests for async UDF policy validation."""

from __future__ import annotations

import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.udf_runtime import register_rust_udfs


def test_async_udf_policy_requires_enable_flag() -> None:
    """Reject async policy settings when async UDFs are disabled."""
    ctx = DataFusionRuntimeProfile().session_context()
    with pytest.raises(ValueError, match="enable_async is False"):
        register_rust_udfs(
            ctx,
            enable_async=False,
            async_udf_timeout_ms=100,
            async_udf_batch_size=64,
        )


def test_async_udf_policy_requires_timeout_and_batch() -> None:
    """Reject incomplete async UDF policy settings."""
    ctx = DataFusionRuntimeProfile().session_context()
    with pytest.raises(ValueError, match="async_udf_timeout_ms"):
        register_rust_udfs(
            ctx,
            enable_async=True,
            async_udf_timeout_ms=None,
            async_udf_batch_size=64,
        )
    ctx = DataFusionRuntimeProfile().session_context()
    with pytest.raises(ValueError, match="async_udf_batch_size"):
        register_rust_udfs(
            ctx,
            enable_async=True,
            async_udf_timeout_ms=100,
            async_udf_batch_size=None,
        )
