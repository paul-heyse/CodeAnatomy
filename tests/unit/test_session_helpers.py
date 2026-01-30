"""Tests for DataFusion session helper utilities."""

from __future__ import annotations

from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.session.helpers import deregister_table, register_temp_table, temp_table


def _supports_deregister(ctx: SessionContext) -> bool:
    return callable(getattr(ctx, "deregister_table", None))


def test_register_and_deregister_temp_table() -> None:
    """Ensure temp table registration and cleanup behaves as expected."""
    ctx = SessionContext()
    table = pa.table({"x": [1, 2]})
    name = register_temp_table(ctx, table, prefix="__test_")
    assert name.startswith("__test_")
    assert ctx.table_exist(name)
    deregister_table(ctx, name)
    if _supports_deregister(ctx):
        assert not ctx.table_exist(name)


def test_temp_table_context_manager() -> None:
    """Ensure temp_table context manager deregisters tables."""
    ctx = SessionContext()
    table = pa.table({"x": [1]})
    with temp_table(ctx, table, prefix="__test_ctx_") as name:
        assert name.startswith("__test_ctx_")
        assert ctx.table_exist(name)
    if _supports_deregister(ctx):
        assert not ctx.table_exist(name)
