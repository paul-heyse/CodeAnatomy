"""Async streaming checks for DataFusion outputs."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pyarrow as pa
import pytest
from datafusion.dataframe import DataFrame

from datafusion_engine.plan.result_types import datafusion_to_async_batches
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.datafusion_runtime import df_ctx


async def _collect_batches(df: DataFrame) -> Sequence[pa.RecordBatch]:
    return [batch async for batch in datafusion_to_async_batches(df)]


@pytest.mark.integration
def test_datafusion_async_streaming_batches() -> None:
    """Yield RecordBatches asynchronously from DataFusion results."""
    ctx = df_ctx()
    table = pa.table({"entity_id": [1, 2, 3], "name": ["a", "b", "c"]})

    register_arrow_table(ctx, name="input_table", value=table)
    df = ctx.sql("select * from input_table")
    batches = asyncio.run(_collect_batches(df))
    assert batches
    assert sum(batch.num_rows for batch in batches) == table.num_rows
