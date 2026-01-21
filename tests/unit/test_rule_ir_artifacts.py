"""Unit tests for rule IR metadata."""

from __future__ import annotations

import ibis

from arrowdsl.core.execution_context import execution_context_factory
from ibis_engine.param_tables import ParamTablePolicy
from relspec.rules.definitions import RuleDefinition
from relspec.rules.validation import (
    RuleSqlGlotSource,
    SqlGlotDiagnosticsContext,
    SqlGlotRuleContext,
    rule_ir_metadata,
)
from schema_spec.catalog_registry import schema_registry


def test_rule_ir_metadata_contains_decompile_and_sql() -> None:
    """Capture decompile and SQL artifacts for rule IR."""
    backend = ibis.duckdb.connect()
    ctx = execution_context_factory("default")
    table = ibis.memtable({"id": [1]})
    rule = RuleDefinition(
        name="rule_ir_test",
        domain="normalize",
        kind="filter_project",
        inputs=("events",),
        output="output_table",
    )
    source = RuleSqlGlotSource(expr=table, input_names=("events",), plan_signature=None)
    rule_ctx = SqlGlotRuleContext(
        rule=rule,
        source=source,
        schema_map=None,
        union_schema=None,
        schema_ddl=None,
        plan_signature=None,
    )
    diagnostics_ctx = SqlGlotDiagnosticsContext(
        backend=backend,
        registry=schema_registry(),
        ctx=ctx,
        param_specs={},
        param_policy=ParamTablePolicy(),
        list_filter_gate_policy=None,
        kernel_lane_policy=None,
    )
    metadata = rule_ir_metadata(rule_ctx, context=diagnostics_ctx)
    assert "ibis_decompile" in metadata
    assert "ibis_sql" in metadata
    assert "ibis_sql_pretty" in metadata
    assert "sqlglot_plan_hash" in metadata
    assert "sqlglot_policy_hash" in metadata


def test_rule_ir_metadata_is_stable() -> None:
    """Ensure rule IR metadata is stable across runs."""
    backend = ibis.duckdb.connect()
    ctx = execution_context_factory("default")
    table = ibis.memtable({"id": [1]})
    rule = RuleDefinition(
        name="rule_ir_stable",
        domain="normalize",
        kind="filter_project",
        inputs=("events",),
        output="output_table",
    )
    source = RuleSqlGlotSource(expr=table, input_names=("events",), plan_signature=None)
    rule_ctx = SqlGlotRuleContext(
        rule=rule,
        source=source,
        schema_map=None,
        union_schema=None,
        schema_ddl=None,
        plan_signature=None,
    )
    diagnostics_ctx = SqlGlotDiagnosticsContext(
        backend=backend,
        registry=schema_registry(),
        ctx=ctx,
        param_specs={},
        param_policy=ParamTablePolicy(),
        list_filter_gate_policy=None,
        kernel_lane_policy=None,
    )
    first = rule_ir_metadata(rule_ctx, context=diagnostics_ctx)
    second = rule_ir_metadata(rule_ctx, context=diagnostics_ctx)
    assert first == second
