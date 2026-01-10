from __future__ import annotations

import pyarrow as pa

from ..arrowdsl.contracts import Contract, DedupeSpec, SortKey


SCHEMA_VERSION = 1


CPG_NODES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("node_id", pa.string()),
        ("node_kind", pa.string()),     # NodeKind
        ("path", pa.string()),          # nullable for non-anchored nodes
        ("bstart", pa.int64()),         # nullable
        ("bend", pa.int64()),           # nullable
        ("file_id", pa.string()),       # nullable (for file-scoped nodes)
    ]
)

CPG_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("edge_kind", pa.string()),     # EdgeKind
        ("src_node_id", pa.string()),
        ("dst_node_id", pa.string()),
        ("path", pa.string()),          # evidence location (nullable for non-anchored edges)
        ("bstart", pa.int64()),         # evidence span start (nullable)
        ("bend", pa.int64()),           # evidence span end (nullable)

        # provenance / resolution metadata
        ("origin", pa.string()),            # "scip" | "qnp" | ...
        ("resolution_method", pa.string()),
        ("confidence", pa.float32()),
        ("score", pa.float32()),

        # optional but useful “payload columns”
        ("symbol_roles", pa.int32()),
        ("qname_source", pa.string()),
        ("ambiguity_group_id", pa.string()),

        # rule meta (critical for deterministic winner selection across multiple producers)
        ("rule_name", pa.string()),
        ("rule_priority", pa.int32()),
    ]
)

CPG_PROPS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("entity_kind", pa.string()),   # EntityKind
        ("entity_id", pa.string()),
        ("prop_key", pa.string()),
        ("value_str", pa.string()),
        ("value_int", pa.int64()),
        ("value_float", pa.float64()),
        ("value_bool", pa.bool_()),
        ("value_json", pa.string()),    # for lists/structs/maps
    ]
)


def empty_nodes() -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in CPG_NODES_SCHEMA], schema=CPG_NODES_SCHEMA)


def empty_edges() -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in CPG_EDGES_SCHEMA], schema=CPG_EDGES_SCHEMA)


def empty_props() -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in CPG_PROPS_SCHEMA], schema=CPG_PROPS_SCHEMA)


# ----------------------
# Contracts
# ----------------------

CPG_NODES_CONTRACT = Contract(
    name="cpg_nodes_v1",
    schema=CPG_NODES_SCHEMA,
    key_fields=("node_id", "node_kind"),
    required_non_null=("node_id", "node_kind"),
    dedupe=DedupeSpec(
        keys=("node_kind", "node_id"),
        tie_breakers=(
            SortKey("path", "ascending"),
            SortKey("bstart", "ascending"),
            SortKey("bend", "ascending"),
        ),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(
        SortKey("node_kind", "ascending"),
        SortKey("path", "ascending"),
        SortKey("bstart", "ascending"),
        SortKey("bend", "ascending"),
        SortKey("node_id", "ascending"),
    ),
    version=SCHEMA_VERSION,
)

CPG_EDGES_CONTRACT = Contract(
    name="cpg_edges_v1",
    schema=CPG_EDGES_SCHEMA,
    key_fields=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
    required_non_null=("edge_id", "edge_kind", "src_node_id", "dst_node_id"),
    dedupe=DedupeSpec(
        keys=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
        tie_breakers=(
            SortKey("score", "descending"),
            SortKey("confidence", "descending"),
            SortKey("rule_priority", "ascending"),
            SortKey("edge_id", "ascending"),
        ),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(
        SortKey("path", "ascending"),
        SortKey("bstart", "ascending"),
        SortKey("bend", "ascending"),
        SortKey("edge_kind", "ascending"),
        SortKey("src_node_id", "ascending"),
        SortKey("dst_node_id", "ascending"),
        SortKey("edge_id", "ascending"),
    ),
    version=SCHEMA_VERSION,
)

# cpg_props are “multi-valued” by design; we dedupe exact duplicates only
CPG_PROPS_CONTRACT = Contract(
    name="cpg_props_v1",
    schema=CPG_PROPS_SCHEMA,
    required_non_null=("entity_kind", "entity_id", "prop_key"),
    dedupe=DedupeSpec(
        keys=("entity_kind", "entity_id", "prop_key", "value_str", "value_int", "value_float", "value_bool", "value_json"),
        tie_breakers=(SortKey("prop_key", "ascending"),),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(
        SortKey("entity_kind", "ascending"),
        SortKey("entity_id", "ascending"),
        SortKey("prop_key", "ascending"),
    ),
    version=SCHEMA_VERSION,
)
