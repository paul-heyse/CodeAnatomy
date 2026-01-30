"""Unit tests for parameter binding resolver behavior."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.tables.param import register_table_params, resolve_param_bindings
from tests.test_helpers.datafusion_runtime import df_ctx
from tests.test_helpers.optional_deps import require_datafusion

datafusion = require_datafusion()


def test_resolve_param_bindings_splits_scalar_and_table() -> None:
    """Split scalar params from table-like params."""
    ctx = df_ctx()
    df = ctx.sql("SELECT 1 AS id")
    table = pa.table({"id": [1]})
    bindings = resolve_param_bindings(
        {
            "scalar": 5,
            "df_param": df,
            "table_param": table,
        }
    )
    assert bindings.param_values == {"scalar": 5}
    assert set(bindings.named_tables) == {"df_param", "table_param"}


def test_register_table_params_unregisters_tables() -> None:
    """Register and clean up table-like params via context manager."""
    ctx = df_ctx()
    table = pa.table({"id": [1]})
    bindings = resolve_param_bindings({"temp_table": table})
    with register_table_params(ctx, bindings):
        assert ctx.table("temp_table") is not None
    exceptions: tuple[type[Exception], ...] = (KeyError, RuntimeError, TypeError, ValueError)
    with pytest.raises(exceptions):
        ctx.table("temp_table")
