"""Tests for DataFusion parameterized execution."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.build import rows_from_table
from datafusion_engine.session.facade import DataFusionExecutionFacade
from tests.test_helpers.datafusion_runtime import df_profile


def test_parameterized_execution_matches_unprepared() -> None:
    """Match DataFusion builder output to direct SQL."""
    profile = df_profile()
    ctx = profile.session_context()
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    ctx.from_arrow(table, name="t")

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    bundle = facade.compile_to_bundle(
        lambda session: session.sql("SELECT id, name FROM t WHERE id = 2")
    )
    result = facade.execute_plan_bundle(bundle)
    assert result.dataframe is not None
    prepared = result.dataframe.to_arrow_table()
    direct = ctx.sql("SELECT id, name FROM t WHERE id = 2").to_arrow_table()

    assert rows_from_table(prepared) == rows_from_table(direct)
