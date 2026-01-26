"""Async streaming checks for DataFusion outputs."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pyarrow as pa
import pytest
from datafusion.dataframe import DataFrame

from datafusion_engine.execution_helpers import datafusion_to_async_batches
from datafusion_engine.runtime import DataFusionRuntimeProfile


async def _collect_batches(df: DataFrame) -> Sequence[pa.RecordBatch]:
    return [batch async for batch in datafusion_to_async_batches(df)]


@pytest.mark.integration
def test_datafusion_async_streaming_batches() -> None:
    """Yield RecordBatches asynchronously from DataFusion results."""
    ctx = DataFusionRuntimeProfile().session_context()
    table = pa.table({"entity_id": [1, 2, 3], "name": ["a", "b", "c"]})
    ctx.register_record_batches("input_table", [table.to_batches()])
    df = ctx.sql("select * from input_table")
    batches = asyncio.run(_collect_batches(df))
    assert batches
    assert sum(batch.num_rows for batch in batches) == table.num_rows
