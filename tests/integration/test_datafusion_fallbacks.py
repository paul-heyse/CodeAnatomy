"""Regression coverage for DataFusion SQL fallback diagnostics."""

from __future__ import annotations

import pytest
from datafusion import SessionContext
from sqlglot import parse_one

from datafusion_engine.bridge import SqlFallbackContext, df_from_sql
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.runtime import AdapterExecutionPolicy, apply_execution_policy


@pytest.mark.integration
def test_datafusion_fallback_event_emitted() -> None:
    """Emit a fallback event when SQL execution is chosen."""
    ctx = SessionContext()
    expr = parse_one("select 1 as value")
    events = []

    def _hook(event: object) -> None:
        events.append(event)

    options = DataFusionCompileOptions(fallback_hook=_hook)
    _ = df_from_sql(ctx, expr, options=options, fallback=SqlFallbackContext(reason="test_sql"))
    assert len(events) == 1


@pytest.mark.integration
def test_datafusion_fallback_policy_blocks() -> None:
    """Raise when fallback execution is blocked."""
    ctx = SessionContext()
    expr = parse_one("select 1 as value")
    events = []

    def _hook(event: object) -> None:
        events.append(event)

    options = DataFusionCompileOptions(fallback_hook=_hook)
    policy = AdapterExecutionPolicy(allow_fallback=False, fail_on_fallback=True)
    guarded = apply_execution_policy(options, execution_policy=policy)
    with pytest.raises(ValueError, match="DataFusion fallback blocked"):
        _ = df_from_sql(ctx, expr, options=guarded, fallback=SqlFallbackContext(reason="test_sql"))
    assert len(events) == 1
