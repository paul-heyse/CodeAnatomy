"""Tests for Delta query execution via runtime session registration."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa
from datafusion import DataFrame, SessionContext, col, lit

from tests.test_helpers.delta_seed import DeltaSeedOptions, write_delta_table
from tests.test_helpers.optional_deps import require_delta_extension, require_deltalake

require_deltalake()
require_delta_extension()


def test_delta_query_runtime_path(tmp_path: Path) -> None:
    """Ensure delta_query executes through SessionContext table registration."""
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extraction.delta_tools import DeltaQueryRequest, delta_query

    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    _, _, table_path = write_delta_table(
        tmp_path,
        table=table,
        options=DeltaSeedOptions(
            profile=DataFusionRuntimeProfile(),
            table_name="delta_table",
        ),
    )

    profile = DataFusionRuntimeProfile()

    def _builder(ctx: SessionContext, table_name: str) -> DataFrame:
        return ctx.table(table_name).filter(col("id") > lit(1))

    request = DeltaQueryRequest(
        path=str(table_path),
        table_name="t",
        runtime_profile=profile,
        builder=_builder,
        query_label="id_gt_1",
    )
    reader = delta_query(request)
    result = reader.read_all()
    table = cast("pa.Table", result)
    rows = table.to_pylist()
    assert [row["id"] for row in rows] == [2, 3]
