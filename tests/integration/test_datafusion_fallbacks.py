"""Regression coverage for DataFusion SQL execution policies."""

from __future__ import annotations

import pytest
from sqlglot import parse_one

from datafusion_engine.bridge import df_from_sql
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    DataFusionRuntimeProfile,
    apply_execution_policy,
)


@pytest.mark.integration
def test_datafusion_sql_executes() -> None:
    """Execute SQL through DataFusion without fallback hooks."""
    ctx = DataFusionRuntimeProfile().session_context()
    expr = parse_one("select 1 as value")
    _ = df_from_sql(ctx, expr, options=DataFusionCompileOptions())


@pytest.mark.integration
def test_execution_policy_forces_sql() -> None:
    """Apply execution policy without altering options."""
    options = DataFusionCompileOptions()
    policy = AdapterExecutionPolicy()
    guarded = apply_execution_policy(options, execution_policy=policy)
    assert guarded == options
