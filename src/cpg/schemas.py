"""CPG Arrow schemas and contracts."""

from __future__ import annotations

import pyarrow as pa

from schema_spec.contracts import ContractSpec, DedupeSpecSpec, SortKeySpec
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec

SCHEMA_VERSION = 1

CPG_NODES_SPEC = TableSchemaSpec(
    name="cpg_nodes_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="node_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="node_kind", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="bstart", dtype=pa.int64()),
        ArrowFieldSpec(name="bend", dtype=pa.int64()),
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
    ],
    required_non_null=("node_id", "node_kind"),
    key_fields=("node_id", "node_kind"),
)

CPG_EDGES_SPEC = TableSchemaSpec(
    name="cpg_edges_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="edge_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="edge_kind", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="src_node_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="dst_node_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="bstart", dtype=pa.int64()),
        ArrowFieldSpec(name="bend", dtype=pa.int64()),
        ArrowFieldSpec(name="origin", dtype=pa.string()),
        ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
        ArrowFieldSpec(name="confidence", dtype=pa.float32()),
        ArrowFieldSpec(name="score", dtype=pa.float32()),
        ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
        ArrowFieldSpec(name="qname_source", dtype=pa.string()),
        ArrowFieldSpec(name="ambiguity_group_id", dtype=pa.string()),
        ArrowFieldSpec(name="rule_name", dtype=pa.string()),
        ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
    ],
    required_non_null=("edge_id", "edge_kind", "src_node_id", "dst_node_id"),
    key_fields=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
)

CPG_PROPS_SPEC = TableSchemaSpec(
    name="cpg_props_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="entity_kind", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="entity_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="prop_key", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="value_str", dtype=pa.string()),
        ArrowFieldSpec(name="value_int", dtype=pa.int64()),
        ArrowFieldSpec(name="value_float", dtype=pa.float64()),
        ArrowFieldSpec(name="value_bool", dtype=pa.bool_()),
        ArrowFieldSpec(name="value_json", dtype=pa.string()),
    ],
    required_non_null=("entity_kind", "entity_id", "prop_key"),
)

CPG_NODES_CONTRACT_SPEC = ContractSpec(
    name="cpg_nodes_v1",
    table_schema=CPG_NODES_SPEC,
    dedupe=DedupeSpecSpec(
        keys=("node_kind", "node_id"),
        tie_breakers=(
            SortKeySpec(column="path", order="ascending"),
            SortKeySpec(column="bstart", order="ascending"),
            SortKeySpec(column="bend", order="ascending"),
        ),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(
        SortKeySpec(column="node_kind", order="ascending"),
        SortKeySpec(column="path", order="ascending"),
        SortKeySpec(column="bstart", order="ascending"),
        SortKeySpec(column="bend", order="ascending"),
        SortKeySpec(column="node_id", order="ascending"),
    ),
    version=SCHEMA_VERSION,
)

CPG_EDGES_CONTRACT_SPEC = ContractSpec(
    name="cpg_edges_v1",
    table_schema=CPG_EDGES_SPEC,
    dedupe=DedupeSpecSpec(
        keys=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
        tie_breakers=(
            SortKeySpec(column="score", order="descending"),
            SortKeySpec(column="confidence", order="descending"),
            SortKeySpec(column="rule_priority", order="ascending"),
            SortKeySpec(column="edge_id", order="ascending"),
        ),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(
        SortKeySpec(column="path", order="ascending"),
        SortKeySpec(column="bstart", order="ascending"),
        SortKeySpec(column="bend", order="ascending"),
        SortKeySpec(column="edge_kind", order="ascending"),
        SortKeySpec(column="src_node_id", order="ascending"),
        SortKeySpec(column="dst_node_id", order="ascending"),
        SortKeySpec(column="edge_id", order="ascending"),
    ),
    version=SCHEMA_VERSION,
)

CPG_PROPS_CONTRACT_SPEC = ContractSpec(
    name="cpg_props_v1",
    table_schema=CPG_PROPS_SPEC,
    dedupe=DedupeSpecSpec(
        keys=(
            "entity_kind",
            "entity_id",
            "prop_key",
            "value_str",
            "value_int",
            "value_float",
            "value_bool",
            "value_json",
        ),
        tie_breakers=(SortKeySpec(column="prop_key", order="ascending"),),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(
        SortKeySpec(column="entity_kind", order="ascending"),
        SortKeySpec(column="entity_id", order="ascending"),
        SortKeySpec(column="prop_key", order="ascending"),
    ),
    version=SCHEMA_VERSION,
)

CPG_NODES_SCHEMA = CPG_NODES_SPEC.to_arrow_schema()
CPG_EDGES_SCHEMA = CPG_EDGES_SPEC.to_arrow_schema()
CPG_PROPS_SCHEMA = CPG_PROPS_SPEC.to_arrow_schema()

CPG_NODES_CONTRACT = CPG_NODES_CONTRACT_SPEC.to_contract()
CPG_EDGES_CONTRACT = CPG_EDGES_CONTRACT_SPEC.to_contract()
CPG_PROPS_CONTRACT = CPG_PROPS_CONTRACT_SPEC.to_contract()


def empty_nodes() -> pa.Table:
    """Return an empty nodes table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty nodes table.
    """
    return pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in CPG_NODES_SCHEMA],
        schema=CPG_NODES_SCHEMA,
    )


def empty_edges() -> pa.Table:
    """Return an empty edges table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty edges table.
    """
    return pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in CPG_EDGES_SCHEMA],
        schema=CPG_EDGES_SCHEMA,
    )


def empty_props() -> pa.Table:
    """Return an empty props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty props table.
    """
    return pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in CPG_PROPS_SCHEMA],
        schema=CPG_PROPS_SCHEMA,
    )
