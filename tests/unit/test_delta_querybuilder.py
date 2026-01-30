"""Tests for Delta QueryBuilder fallback path."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa

from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.delta_seed import write_delta_table
from tests.test_helpers.optional_deps import require_deltalake

require_deltalake()


def test_delta_querybuilder_path(tmp_path: Path) -> None:
    """Ensure delta_query can execute via QueryBuilder when enabled."""
    from datafusion_engine.compile.options import DataFusionSqlPolicy
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from engine.delta_tools import DeltaQueryRequest, delta_query

    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    _, _, table_path = write_delta_table(
        tmp_path,
        table=table,
        profile=df_profile(),
        table_name="delta_table",
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
