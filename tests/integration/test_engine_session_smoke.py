"""Smoke tests for engine session construction."""

from __future__ import annotations

import pyarrow as pa
import pytest

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from engine.session_factory import build_engine_session

EXPECTED_ROWS = 2


@pytest.mark.integration
def test_engine_session_runs_plan() -> None:
    """Build an EngineSession and run a trivial DataFusion query."""
    runtime = runtime_profile_factory("default")
    ctx = ExecutionContext(runtime=runtime)
    session = build_engine_session(ctx=ctx)
    df_ctx = session.df_ctx()
    assert df_ctx is not None
    df_ctx.register_record_batches(
        "values",
        [pa.table({"value": [1, 2]}).to_batches()],
    )
    table = df_ctx.sql("SELECT * FROM values").to_arrow_table()
    assert table.num_rows == EXPECTED_ROWS
