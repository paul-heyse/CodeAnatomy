"""Smoke tests for engine session construction."""

from __future__ import annotations

import pyarrow as pa
import pytest

from extraction.engine_session_factory import build_engine_session
from extraction.runtime_profile import resolve_runtime_profile
from tests.test_helpers.arrow_seed import register_arrow_table

EXPECTED_ROWS = 2


@pytest.mark.integration
def test_engine_session_runs_plan() -> None:
    """Build an EngineSession and run a trivial DataFusion query."""
    runtime_spec = resolve_runtime_profile("default")
    session = build_engine_session(runtime_spec=runtime_spec)
    df_ctx = session.df_ctx()
    assert df_ctx is not None
    register_arrow_table(df_ctx, name="values", value=pa.table({"value": [1, 2]}))
    table = df_ctx.sql("SELECT * FROM values").to_arrow_table()
    assert table.num_rows == EXPECTED_ROWS
