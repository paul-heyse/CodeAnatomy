"""Integration smoke tests for runtime context pooling and cleanup."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    SchemaRegistryValidationResult,
    ZeroRowBootstrapConfig,
)
from datafusion_engine.session.runtime_schema_registry import (
    _schema_registry_issues as schema_registry_issues,
)
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


@pytest.mark.integration
def test_schema_registry_view_errors_are_advisory_in_non_strict_bootstrap_mode() -> None:
    """Bootstrap mode should downgrade schema view errors when strict mode is disabled."""
    issues, advisory = schema_registry_issues(
        SchemaRegistryValidationResult(view_errors={"semantic_types": "missing metadata"}),
        zero_row_bootstrap=ZeroRowBootstrapConfig(
            validation_mode="bootstrap",
            strict=False,
        ),
    )
    assert "view_errors" not in issues
    assert advisory == {"view_errors": {"semantic_types": "missing metadata"}}
