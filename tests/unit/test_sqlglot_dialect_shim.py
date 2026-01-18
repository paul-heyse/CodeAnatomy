"""Tests for SQLGlot dialect surface policies."""

from __future__ import annotations

from sqlglot import exp

from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionDmlOptions
from sqlglot_tools.optimizer import (
    SqlGlotSurface,
    default_sqlglot_policy,
    register_datafusion_dialect,
    sqlglot_sql,
    sqlglot_surface_policy,
)


def test_sqlglot_surface_policy_defaults() -> None:
    """Expose surface-specific dialect and lane settings."""
    compile_policy = sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_COMPILE)
    dml_policy = sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_DML)
    assert compile_policy.dialect == "datafusion_ext"
    assert compile_policy.lane == "dialect_shim"
    assert dml_policy.dialect == "datafusion"
    assert dml_policy.lane == "transforms"


def test_datafusion_options_use_surface_dialects() -> None:
    """Default DataFusion options should honor surface policies."""
    compile_options = DataFusionCompileOptions()
    dml_options = DataFusionDmlOptions()
    assert (
        compile_options.dialect == sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_COMPILE).dialect
    )
    assert dml_options.dialect == sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_DML).dialect


def test_datafusion_shim_array_sql_generation() -> None:
    """Ensure the shim generator emits ARRAY[...] syntax."""
    register_datafusion_dialect()
    expr = exp.select(
        exp.Array(expressions=[exp.Literal.number(1), exp.Literal.number(2)]).as_("items")
    )
    sql = sqlglot_sql(expr, policy=default_sqlglot_policy())
    assert "ARRAY[" in sql
