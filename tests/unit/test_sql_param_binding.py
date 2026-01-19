"""Unit tests for DataFusion SQL parameter bindings."""

from __future__ import annotations

import pytest

from datafusion_engine.bridge import execute_sql
from datafusion_engine.compile_options import DataFusionCompileOptions

datafusion = pytest.importorskip("datafusion")


def test_param_allowlist_blocks_unknown_names() -> None:
    """Reject parameter bindings that are not allowlisted."""
    ctx = datafusion.SessionContext()
    options = DataFusionCompileOptions(
        params={"val": 1},
        param_identifier_allowlist=("other",),
    )
    with pytest.raises(ValueError, match="allowlisted"):
        execute_sql(ctx, sql="SELECT :val", options=options)


def test_param_allowlist_allows_named_params() -> None:
    """Execute SQL when parameters are allowlisted."""
    ctx = datafusion.SessionContext()
    options = DataFusionCompileOptions(
        params={"val": 1},
        param_identifier_allowlist=("val",),
    )
    reader = execute_sql(ctx, sql="SELECT :val", options=options)
    result = reader.read_all()
    assert result.num_rows == 1
    assert result.column(0)[0].as_py() == 1
