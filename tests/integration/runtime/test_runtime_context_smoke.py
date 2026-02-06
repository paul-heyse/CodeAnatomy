"""Integration smoke tests for runtime context pooling and cleanup."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import require_datafusion_udfs

if TYPE_CHECKING:
    from datafusion import SessionContext

require_datafusion_udfs()


def _table_names(ctx: SessionContext) -> set[str]:
    rows = ctx.sql("SHOW TABLES").to_arrow_table().to_pylist()
    names: set[str] = set()
    for row in rows:
        candidate = row.get("table_name") or row.get("name") or row.get("table")
        if isinstance(candidate, str):
            names.add(candidate)
    return names


@pytest.mark.integration
def test_runtime_context_pool_cleans_run_scoped_tables() -> None:
    """Ensure context pool checkout cleanup removes run-scoped tables."""
    profile = DataFusionRuntimeProfile()
    pool = profile.context_pool(size=1, run_name_prefix="runtime_smoke")
    run_prefix = "runtime_smoke_case"
    table_name = f"{run_prefix}_events"

    with pool.checkout(run_prefix=run_prefix) as ctx:
        register_arrow_table(ctx, name=table_name, value=pa.table({"id": [1, 2, 3]}))
        assert table_name in _table_names(ctx)

    with pool.checkout(run_prefix=run_prefix) as ctx:
        assert table_name not in _table_names(ctx)
