"""Tests for DataFusion parameterized execution."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.schema.build import rows_from_table
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.execution_facade import DataFusionExecutionFacade
from datafusion_engine.runtime import DataFusionRuntimeProfile


def test_parameterized_execution_matches_unprepared() -> None:
    """Match parameterized execution output to direct SQL."""
    ctx = DataFusionRuntimeProfile().session_context()
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    ctx.from_arrow(table, name="t")

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    options = DataFusionCompileOptions(params={"id": 2})
    plan = facade.compile("SELECT id, name FROM t WHERE id = :id", options=options)
    result = facade.execute(plan)
    assert result.dataframe is not None
    prepared = result.dataframe.to_arrow_table()
    direct = ctx.sql("SELECT id, name FROM t WHERE id = 2").to_arrow_table()

    assert rows_from_table(prepared) == rows_from_table(direct)
