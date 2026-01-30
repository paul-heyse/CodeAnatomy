"""Tests for Delta QueryBuilder fallback path."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

pytest.importorskip("deltalake")


def test_delta_querybuilder_path(tmp_path: Path) -> None:
    """Ensure delta_query can execute via QueryBuilder when enabled."""
    from datafusion_engine.compile_options import DataFusionSqlPolicy
    from datafusion_engine.ingest import datafusion_from_arrow
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest
    from engine.delta_tools import DeltaQueryRequest, delta_query

    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    table_path = tmp_path / "delta_table"
    seed_profile = DataFusionRuntimeProfile()
    ctx = seed_profile.session_context()
    df = datafusion_from_arrow(ctx, name="delta_seed", value=table)
    pipeline = WritePipeline(ctx, runtime_profile=seed_profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(table_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )

    profile = DataFusionRuntimeProfile(
        enable_delta_querybuilder=True,
        sql_policy=DataFusionSqlPolicy(allow_statements=True),
        sql_policy_name=None,
    )
    request = DeltaQueryRequest(
        path=str(table_path),
        sql="SELECT id FROM t WHERE id > 1",
        table_name="t",
        runtime_profile=profile,
    )
    reader = delta_query(request)
    result = reader.read_all()
    table = cast("pa.Table", result)
    rows = table.to_pylist()
    assert [row["id"] for row in rows] == [2, 3]
