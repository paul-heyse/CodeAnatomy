"""CPG Arrow schemas and contracts."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_schema
from arrowdsl.schema.schema import SchemaMetadataSpec
from schema_spec.specs import DICT_STRING, ArrowFieldSpec, file_identity_bundle, span_bundle
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    DedupeSpecSpec,
    SchemaRegistry,
    SortKeySpec,
    TableSpecConstraints,
    make_contract_spec,
    make_dataset_spec,
    make_table_spec,
)

SCHEMA_VERSION = 1
ENCODING_METADATA_KEY = "encoding"
ENCODING_DICTIONARY = "dictionary"


def _dict_metadata() -> dict[str, str]:
    return {ENCODING_METADATA_KEY: ENCODING_DICTIONARY}


def _cpg_metadata(dataset_name: str, *, contract_name: str) -> SchemaMetadataSpec:
    return SchemaMetadataSpec(
        schema_metadata={
            b"cpg_stage": b"cpg",
            b"cpg_dataset": dataset_name.encode("utf-8"),
            b"contract_name": contract_name.encode("utf-8"),
            b"determinism_tier": b"best_effort",
        }
    )


CPG_NODES_TABLE_SPEC = make_table_spec(
    name="cpg_nodes_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
    fields=[
        ArrowFieldSpec(name="node_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(
            name="node_kind",
            dtype=DICT_STRING,
            nullable=False,
            metadata=_dict_metadata(),
        ),
    ],
    constraints=TableSpecConstraints(
        required_non_null=("node_id", "node_kind"),
        key_fields=("node_id", "node_kind"),
    ),
)

CPG_NODES_CONTRACT_SPEC = make_contract_spec(
    table_spec=CPG_NODES_TABLE_SPEC,
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

CPG_NODES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=CPG_NODES_TABLE_SPEC,
        contract_spec=CPG_NODES_CONTRACT_SPEC,
        metadata_spec=_cpg_metadata("cpg_nodes_v1", contract_name="cpg_nodes_v1"),
    )
)

CPG_EDGES_TABLE_SPEC = make_table_spec(
    name="cpg_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(span_bundle(),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(
            name="edge_kind",
            dtype=DICT_STRING,
            nullable=False,
            metadata=_dict_metadata(),
        ),
        ArrowFieldSpec(name="src_node_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="dst_node_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="origin", dtype=DICT_STRING, metadata=_dict_metadata()),
        ArrowFieldSpec(
            name="resolution_method",
            dtype=DICT_STRING,
            metadata=_dict_metadata(),
        ),
        ArrowFieldSpec(name="confidence", dtype=pa.float32()),
        ArrowFieldSpec(name="score", dtype=pa.float32()),
        ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
        ArrowFieldSpec(name="qname_source", dtype=DICT_STRING, metadata=_dict_metadata()),
        ArrowFieldSpec(name="ambiguity_group_id", dtype=pa.string()),
        ArrowFieldSpec(name="rule_name", dtype=DICT_STRING, metadata=_dict_metadata()),
        ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
    ],
    constraints=TableSpecConstraints(
        required_non_null=("edge_id", "edge_kind", "src_node_id", "dst_node_id"),
        key_fields=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
    ),
)

CPG_EDGES_CONTRACT_SPEC = make_contract_spec(
    table_spec=CPG_EDGES_TABLE_SPEC,
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

CPG_EDGES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=CPG_EDGES_TABLE_SPEC,
        contract_spec=CPG_EDGES_CONTRACT_SPEC,
        metadata_spec=_cpg_metadata("cpg_edges_v1", contract_name="cpg_edges_v1"),
    )
)

CPG_PROPS_TABLE_SPEC = make_table_spec(
    name="cpg_props_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="entity_kind", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="entity_id", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="prop_key", dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name="value_str", dtype=pa.string()),
        ArrowFieldSpec(name="value_int", dtype=pa.int64()),
        ArrowFieldSpec(name="value_float", dtype=pa.float64()),
        ArrowFieldSpec(name="value_bool", dtype=pa.bool_()),
        ArrowFieldSpec(name="value_json", dtype=pa.string()),
    ],
    constraints=TableSpecConstraints(required_non_null=("entity_kind", "entity_id", "prop_key")),
)

CPG_PROPS_CONTRACT_SPEC = make_contract_spec(
    table_spec=CPG_PROPS_TABLE_SPEC,
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

CPG_PROPS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=CPG_PROPS_TABLE_SPEC,
        contract_spec=CPG_PROPS_CONTRACT_SPEC,
        metadata_spec=_cpg_metadata("cpg_props_v1", contract_name="cpg_props_v1"),
    )
)

CPG_NODES_SCHEMA = CPG_NODES_SPEC.schema()
CPG_EDGES_SCHEMA = CPG_EDGES_SPEC.schema()
CPG_PROPS_SCHEMA = CPG_PROPS_SPEC.schema()

CPG_NODES_CONTRACT = CPG_NODES_CONTRACT_SPEC.to_contract()
CPG_EDGES_CONTRACT = CPG_EDGES_CONTRACT_SPEC.to_contract()
CPG_PROPS_CONTRACT = CPG_PROPS_CONTRACT_SPEC.to_contract()


def register_cpg_specs(registry: SchemaRegistry) -> SchemaRegistry:
    """Register CPG table and contract specs into the registry.

    Returns
    -------
    SchemaRegistry
        Registry with CPG specs added.
    """
    registry.register_dataset(CPG_NODES_SPEC)
    registry.register_dataset(CPG_EDGES_SPEC)
    registry.register_dataset(CPG_PROPS_SPEC)
    return registry


CPG_SCHEMA_REGISTRY = register_cpg_specs(GLOBAL_SCHEMA_REGISTRY)


def empty_nodes() -> TableLike:
    """Return an empty nodes table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty nodes table.
    """
    return table_from_schema(CPG_NODES_SCHEMA, columns={}, num_rows=0)


def empty_edges() -> TableLike:
    """Return an empty edges table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty edges table.
    """
    return table_from_schema(CPG_EDGES_SCHEMA, columns={}, num_rows=0)


def empty_props() -> TableLike:
    """Return an empty props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty props table.
    """
    return table_from_schema(CPG_PROPS_SCHEMA, columns={}, num_rows=0)
