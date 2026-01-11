"""Tests for schema specs, adapters, and builder helpers."""

from __future__ import annotations

from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from cpg.builders import EdgeBuilder, NodeBuilder, PropBuilder
from cpg.kinds import EdgeKind, EntityKind, NodeKind
from cpg.schemas import CPG_EDGES_SCHEMA, CPG_NODES_SCHEMA, CPG_PROPS_SCHEMA, SCHEMA_VERSION
from cpg.specs import (
    EdgeEmitSpec,
    EdgePlanSpec,
    NodeEmitSpec,
    NodePlanSpec,
    PropFieldSpec,
    PropTableSpec,
)
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec
from schema_spec.pandera_adapter import validate_arrow_table

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


def test_validate_arrow_table_filters_extra_columns() -> None:
    """Validate Arrow tables with Pandera and filter extra columns."""
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
    validated = validate_arrow_table(table, spec=spec, lazy=False)
    assert validated.schema.names == ["id", "name"]
    assert validated.num_rows == EXPECTED_ROWS


@dataclass(frozen=True)
class _EdgeOptions:
    emit_edges: bool = True


def test_edge_builder_emits_edges() -> None:
    """Emit edge rows from a relation table."""
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
    builder = EdgeBuilder(
        emitters=(
            EdgePlanSpec(
                name="basic",
                option_flag="emit_edges",
                relation_getter=lambda tables: tables.get("rel"),
                emit=EdgeEmitSpec(
                    edge_kind=EdgeKind.PY_DEFINES_SYMBOL,
                    src_cols=("src",),
                    dst_cols=("dst",),
                    origin="scip",
                    default_resolution_method="SPAN_EXACT",
                ),
            ),
        ),
        schema_version=SCHEMA_VERSION,
        edge_schema=CPG_EDGES_SCHEMA,
    )
    parts = builder.build(tables={"rel": rel}, options=_EdgeOptions())
    assert len(parts) == 1
    assert parts[0].num_rows == 1
    assert parts[0]["edge_kind"].to_pylist()[0] == EdgeKind.PY_DEFINES_SYMBOL.value


@dataclass(frozen=True)
class _NodeOptions:
    include_nodes: bool = True


def test_node_builder_emits_nodes() -> None:
    """Emit node rows from a source table."""
    table = pa.Table.from_arrays(
        [
            pa.array(["node"], type=pa.string()),
            pa.array(["path.py"], type=pa.string()),
            pa.array([3], type=pa.int64()),
            pa.array([4], type=pa.int64()),
        ],
        names=["node_id", "path", "bstart", "bend"],
    )
    builder = NodeBuilder(
        emitters=(
            NodePlanSpec(
                name="basic",
                option_flag="include_nodes",
                table_getter=lambda tables: tables.get("nodes"),
                emit=NodeEmitSpec(
                    node_kind=NodeKind.CST_DEF,
                    id_cols=("node_id",),
                    path_cols=("path",),
                    bstart_cols=("bstart",),
                    bend_cols=("bend",),
                    file_id_cols=(),
                ),
            ),
        ),
        schema_version=SCHEMA_VERSION,
        node_schema=CPG_NODES_SCHEMA,
    )
    parts = builder.build(tables={"nodes": table}, options=_NodeOptions())
    assert len(parts) == 1
    assert parts[0].num_rows == 1
    assert parts[0]["node_kind"].to_pylist()[0] == NodeKind.CST_DEF.value


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
    builder = PropBuilder(
        table_specs=(
            PropTableSpec(
                name="basic",
                option_flag="include_props",
                table_getter=lambda tables: tables.get("props"),
                entity_kind=EntityKind.NODE,
                id_cols=("node_id",),
                node_kind=NodeKind.CST_NAME_REF,
                fields=(
                    PropFieldSpec(prop_key="name", source_col="name"),
                    PropFieldSpec(
                        prop_key="heavy",
                        source_col="heavy",
                        include_if=lambda options: options.include_heavy_json_props,
                    ),
                ),
            ),
        ),
        schema_version=SCHEMA_VERSION,
        prop_schema=CPG_PROPS_SCHEMA,
    )
    out = builder.build(tables={"props": table}, options=_PropOptions())
    prop_keys = out["prop_key"].to_pylist()
    assert "name" in prop_keys
    assert "heavy" not in prop_keys
