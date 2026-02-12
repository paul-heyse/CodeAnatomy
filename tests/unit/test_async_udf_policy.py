"""Unit tests for async UDF policy validation."""

from __future__ import annotations

import pytest

from datafusion_engine.udf.platform import ensure_rust_udfs
from tests.test_helpers.datafusion_runtime import df_ctx
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


def test_async_udf_policy_requires_enable_flag(require_native_runtime: None) -> None:
    """Reject async policy settings when async UDFs are disabled."""
    _ = require_native_runtime
    ctx = df_ctx()
    with pytest.raises(ValueError, match="enable_async is False"):
        ensure_rust_udfs(
            ctx,
            enable_async=False,
            async_udf_timeout_ms=100,
            async_udf_batch_size=64,
        )


def test_async_udf_policy_requires_timeout_and_batch(require_native_runtime: None) -> None:
    """Reject incomplete async UDF policy settings."""
    _ = require_native_runtime
    ctx = df_ctx()
    with pytest.raises(ValueError, match="async_udf_timeout_ms"):
        ensure_rust_udfs(
            ctx,
            enable_async=True,
            async_udf_timeout_ms=None,
            async_udf_batch_size=64,
        )
    ctx = df_ctx()
    with pytest.raises(ValueError, match="async_udf_batch_size"):
        ensure_rust_udfs(
            ctx,
            enable_async=True,
            async_udf_timeout_ms=100,
            async_udf_batch_size=None,
        )
