"""Tests for schema specs, adapters, and builder helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace

import arrowdsl.core.interop as pa
from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.plan.plan import Plan, PlanSpec, union_all_plans
from cpg.emit_edges import emit_edges_plan
from cpg.emit_nodes import emit_node_plan
from cpg.emit_props import emit_props_plans, filter_fields
from cpg.kinds import EdgeKind, EntityKind, NodeKind
from cpg.specs import INCLUDE_HEAVY_JSON, EdgeEmitSpec, NodeEmitSpec, PropFieldSpec, PropTableSpec
from schema_spec import (
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
    ArrowFieldSpec,
    ArrowValidationOptions,
    TableSchemaSpec,
    validate_arrow_table,
)

EXPECTED_ROWS = 2


def test_table_schema_spec_to_arrow() -> None:
    """Build an Arrow schema from a TableSchemaSpec."""
    spec = TableSchemaSpec(
        name="test_spec",
        fields=[
            ArrowFieldSpec(name="id", dtype=pa.int64(), nullable=False),
            ArrowFieldSpec(name="name", dtype=pa.string()),
        ],
        required_non_null=("id",),
        key_fields=("id",),
    )
    schema = spec.to_arrow_schema()
    assert schema.names == ["id", "name"]
    assert schema.field("id").type == pa.int64()


def test_table_schema_spec_attaches_metadata() -> None:
    """Attach schema name/version metadata when version is set."""
    spec = TableSchemaSpec(
        name="meta_spec",
        version=2,
        fields=[ArrowFieldSpec(name="id", dtype=pa.int64(), nullable=False)],
        required_non_null=("id",),
    )
    schema = spec.to_arrow_schema()
    assert schema.metadata is not None
    assert schema.metadata[SCHEMA_META_NAME] == b"meta_spec"
    assert schema.metadata[SCHEMA_META_VERSION] == b"2"


def test_validate_arrow_table_filters_extra_columns() -> None:
    """Validate Arrow tables with Arrow validation and filter extra columns."""
    spec = TableSchemaSpec(
        name="test_table",
        fields=[
            ArrowFieldSpec(name="id", dtype=pa.int64(), nullable=False),
            ArrowFieldSpec(name="name", dtype=pa.string()),
        ],
        required_non_null=("id",),
    )
    table = pa.Table.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.array(["a", "b"], type=pa.string()),
            pa.array([True, False], type=pa.bool_()),
        ],
        names=["id", "name", "extra"],
    )
    validated = validate_arrow_table(
        table,
        spec=spec,
        options=ArrowValidationOptions(strict="filter"),
    )
    assert validated.schema.names == ["id", "name"]
    assert validated.num_rows == EXPECTED_ROWS


@dataclass(frozen=True)
class _EdgeOptions:
    emit_edges: bool = True


def test_edge_builder_emits_edges() -> None:
    """Emit edge rows from a relation plan."""
    rel = pa.Table.from_arrays(
        [
            pa.array(["src"], type=pa.string()),
            pa.array(["dst"], type=pa.string()),
            pa.array(["path.py"], type=pa.string()),
            pa.array([1], type=pa.int64()),
            pa.array([2], type=pa.int64()),
        ],
        names=["src", "dst", "path", "bstart", "bend"],
    )
    ctx = ExecutionContext(runtime=RuntimeProfile(name="TEST"))
    plan = Plan.table_source(rel, label="rel")
    out_plan = emit_edges_plan(
        plan,
        spec=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_DEFINES_SYMBOL,
            src_cols=("src",),
            dst_cols=("dst",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        ctx=ctx,
    )
    out = PlanSpec.from_plan(out_plan).to_table(ctx=ctx)
    assert out.num_rows == 1
    assert out["edge_kind"].to_pylist()[0] == EdgeKind.PY_DEFINES_SYMBOL.value


@dataclass(frozen=True)
class _NodeOptions:
    include_nodes: bool = True


def test_node_builder_emits_nodes() -> None:
    """Emit node rows from a source plan."""
    table = pa.Table.from_arrays(
        [
            pa.array(["node"], type=pa.string()),
            pa.array(["path.py"], type=pa.string()),
            pa.array([3], type=pa.int64()),
            pa.array([4], type=pa.int64()),
        ],
        names=["node_id", "path", "bstart", "bend"],
    )
    ctx = ExecutionContext(runtime=RuntimeProfile(name="TEST"))
    plan = Plan.table_source(table, label="nodes")
    out_plan = emit_node_plan(
        plan,
        spec=NodeEmitSpec(
            node_kind=NodeKind.CST_DEF,
            id_cols=("node_id",),
            path_cols=("path",),
            bstart_cols=("bstart",),
            bend_cols=("bend",),
            file_id_cols=(),
        ),
        ctx=ctx,
    )
    out = PlanSpec.from_plan(out_plan).to_table(ctx=ctx)
    assert out.num_rows == 1
    assert out["node_kind"].to_pylist()[0] == NodeKind.CST_DEF.value


@dataclass(frozen=True)
class _PropOptions:
    include_props: bool = True
    include_heavy_json_props: bool = False


def test_prop_builder_emits_props() -> None:
    """Emit property rows with option-gated fields."""
    table = pa.Table.from_arrays(
        [
            pa.array(["node"], type=pa.string()),
            pa.array(["value"], type=pa.string()),
            pa.array(["skip"], type=pa.string()),
        ],
        names=["node_id", "name", "heavy"],
    )
    ctx = ExecutionContext(runtime=RuntimeProfile(name="TEST"))
    plan = Plan.table_source(table, label="props")
    spec = PropTableSpec(
        name="basic",
        option_flag="include_props",
        table_ref="props",
        entity_kind=EntityKind.NODE,
        id_cols=("node_id",),
        node_kind=NodeKind.CST_NAME_REF,
        fields=(
            PropFieldSpec(prop_key="name", source_col="name", value_type="string"),
            PropFieldSpec(
                prop_key="heavy",
                source_col="heavy",
                include_if_id=INCLUDE_HEAVY_JSON,
                value_type="string",
            ),
        ),
    )
    options = _PropOptions()
    filtered_fields = filter_fields(spec.fields, options=options)
    spec = replace(spec, fields=tuple(filtered_fields))
    plans = emit_props_plans(plan, spec=spec, schema_version=None, ctx=ctx)
    out_plan = union_all_plans(plans, label="props")
    out = PlanSpec.from_plan(out_plan).to_table(ctx=ctx)
    prop_keys = out["prop_key"].to_pylist()
    assert "name" in prop_keys
    assert "heavy" not in prop_keys
