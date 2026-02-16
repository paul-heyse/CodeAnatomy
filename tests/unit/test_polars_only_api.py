"""Polars-only API surface tests."""

from __future__ import annotations

import polars as pl

from datafusion_engine.session.streaming import StreamingExecutionResult
from tests.test_helpers.datafusion_runtime import df_profile


def test_streaming_execution_to_polars() -> None:
    """Test streaming execution to polars."""
    profile = df_profile()
    ctx = profile.session_context()
    df = ctx.from_pylist([{"id": 1}])

    result = StreamingExecutionResult(df=df)
    out = result.to_polars()

    assert isinstance(out, pl.DataFrame)
    assert out.shape == (1, 1)


def test_datafusion_api_has_no_pandas_methods() -> None:
    """Test datafusion api has no pandas methods."""
    profile = df_profile()
    ctx = profile.session_context()
    df = ctx.from_pylist([{"id": 1}])

    assert not hasattr(ctx, "from_pandas")
    assert not hasattr(df, "to_pandas")
