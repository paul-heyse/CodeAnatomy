"""Unit tests for rule IR metadata."""

from __future__ import annotations

import ibis

from arrowdsl.core.execution_context import execution_context_factory
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.param_tables import ParamTablePolicy
from relspec.rules.definitions import RuleDefinition
from relspec.rules.validation import (
    RuleSqlGlotSource,
    SqlGlotDiagnosticsConfig,
    SqlGlotRuleContext,
    build_sqlglot_context,
    rule_ir_metadata,
)
from relspec.schema_context import RelspecSchemaContext


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
    source = RuleSqlGlotSource(expr=table, input_names=("events",))
    rule_ctx = SqlGlotRuleContext(
        rule=rule,
        source=source,
        plan_signature=None,
    )
    schema_context = RelspecSchemaContext.from_session(DataFusionRuntimeProfile().session_context())
    diagnostics_ctx = build_sqlglot_context(
        SqlGlotDiagnosticsConfig(
            backend=backend,
            schema_context=schema_context,
            ctx=ctx,
            param_table_policy=ParamTablePolicy(),
        )
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
    source = RuleSqlGlotSource(expr=table, input_names=("events",))
    rule_ctx = SqlGlotRuleContext(
        rule=rule,
        source=source,
        plan_signature=None,
    )
    schema_context = RelspecSchemaContext.from_session(DataFusionRuntimeProfile().session_context())
    diagnostics_ctx = build_sqlglot_context(
        SqlGlotDiagnosticsConfig(
            backend=backend,
            schema_context=schema_context,
            ctx=ctx,
            param_table_policy=ParamTablePolicy(),
        )
    )
    first = rule_ir_metadata(rule_ctx, context=diagnostics_ctx)
    second = rule_ir_metadata(rule_ctx, context=diagnostics_ctx)
    assert first == second
