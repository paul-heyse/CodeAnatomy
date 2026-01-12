"""Tests for plan-lane CPG property transforms."""

from __future__ import annotations

import arrowdsl.core.interop as pa
from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.plan.plan import Plan, PlanSpec, union_all_plans
from cpg.emit_props import emit_props_plans
from cpg.kinds import EntityKind, NodeKind
from cpg.prop_transforms import expr_context_value
from cpg.specs import PropFieldSpec, PropTableSpec


def _ctx() -> ExecutionContext:
    return ExecutionContext(runtime=RuntimeProfile(name="TEST"))


def test_expr_context_transform_plan() -> None:
    """Normalize expr context via the plan-lane transform."""
    table = pa.Table.from_arrays(
        [
            pa.array(["node"], type=pa.string()),
            pa.array(["pkg.attr"], type=pa.string()),
        ],
        names=["node_id", "expr_ctx"],
    )
    ctx = _ctx()
    plan = Plan.table_source(table, label="expr_ctx")
    spec = PropTableSpec(
        name="expr_ctx_props",
        option_flag="include_props",
        table_getter=lambda tables: tables.get("expr_ctx"),
        entity_kind=EntityKind.NODE,
        id_cols=("node_id",),
        node_kind=NodeKind.CST_NAME_REF,
        fields=(
            PropFieldSpec(
                prop_key="expr_context",
                source_col="expr_ctx",
                transform=expr_context_value,
                value_type="string",
            ),
        ),
    )
    plans = emit_props_plans(plan, spec=spec, schema_version=None, ctx=ctx)
    out = PlanSpec.from_plan(union_all_plans(plans, label="expr_ctx")).to_table(ctx=ctx)
    assert out["value_str"].to_pylist()[0] == "ATTR"


def test_json_transform_plan() -> None:
    """Serialize JSON values via the registered UDF."""
    table = pa.Table.from_arrays(
        [
            pa.array(["node"], type=pa.string()),
            pa.array([["a", "b"]], type=pa.list_(pa.string())),
        ],
        names=["node_id", "qnames"],
    )
    ctx = _ctx()
    plan = Plan.table_source(table, label="json_props")
    spec = PropTableSpec(
        name="json_props",
        option_flag="include_props",
        table_getter=lambda tables: tables.get("json_props"),
        entity_kind=EntityKind.NODE,
        id_cols=("node_id",),
        node_kind=NodeKind.CST_DEF,
        fields=(
            PropFieldSpec(
                prop_key="qnames",
                source_col="qnames",
                value_type="json",
            ),
        ),
    )
    plans = emit_props_plans(plan, spec=spec, schema_version=None, ctx=ctx)
    out = PlanSpec.from_plan(union_all_plans(plans, label="json_props")).to_table(ctx=ctx)
    assert out["value_json"].to_pylist()[0] == '["a", "b"]'
