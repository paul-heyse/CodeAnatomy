"""Shared Arrow schema definitions for normalization."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.schema.schema import EncodingSpec
from schema_spec.specs import (
    DICT_STRING,
    ArrowFieldSpec,
    file_identity_bundle,
    span_bundle,
)
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, make_dataset_spec, make_table_spec

SCHEMA_VERSION = 1


TYPE_EXPRS_NORM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="type_exprs_norm_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
            fields=[
                ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
                ArrowFieldSpec(name="owner_def_id", dtype=pa.string()),
                ArrowFieldSpec(name="param_name", dtype=pa.string()),
                ArrowFieldSpec(name="expr_kind", dtype=pa.string()),
                ArrowFieldSpec(name="expr_role", dtype=pa.string()),
                ArrowFieldSpec(name="expr_text", dtype=pa.string()),
                ArrowFieldSpec(name="type_repr", dtype=pa.string()),
                ArrowFieldSpec(name="type_id", dtype=pa.string()),
            ],
        )
    )
)

TYPE_NODES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="type_nodes_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="type_id", dtype=pa.string()),
                ArrowFieldSpec(name="type_repr", dtype=pa.string()),
                ArrowFieldSpec(name="type_form", dtype=pa.string()),
                ArrowFieldSpec(name="origin", dtype=pa.string()),
            ],
        )
    )
)

TYPE_EXPRS_NORM_SCHEMA = TYPE_EXPRS_NORM_SPEC.table_spec.to_arrow_schema()
TYPE_NODES_SCHEMA = TYPE_NODES_SPEC.table_spec.to_arrow_schema()


CFG_BLOCKS_NORM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="py_bc_blocks_norm_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False),),
            fields=[
                ArrowFieldSpec(name="block_id", dtype=pa.string()),
                ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
                ArrowFieldSpec(name="start_offset", dtype=pa.int32()),
                ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
                ArrowFieldSpec(name="kind", dtype=pa.string()),
            ],
        )
    )
)

CFG_EDGES_NORM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="py_bc_cfg_edges_norm_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False),),
            fields=[
                ArrowFieldSpec(name="edge_id", dtype=pa.string()),
                ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
                ArrowFieldSpec(name="src_block_id", dtype=pa.string()),
                ArrowFieldSpec(name="dst_block_id", dtype=pa.string()),
                ArrowFieldSpec(name="kind", dtype=pa.string()),
                ArrowFieldSpec(name="cond_instr_id", dtype=pa.string()),
                ArrowFieldSpec(name="exc_index", dtype=pa.int32()),
            ],
        )
    )
)

CFG_BLOCKS_NORM_SCHEMA = CFG_BLOCKS_NORM_SPEC.table_spec.to_arrow_schema()
CFG_EDGES_NORM_SCHEMA = CFG_EDGES_NORM_SPEC.table_spec.to_arrow_schema()


DEF_USE_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="py_bc_def_use_events_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False),),
            fields=[
                ArrowFieldSpec(name="event_id", dtype=pa.string()),
                ArrowFieldSpec(name="instr_id", dtype=pa.string()),
                ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
                ArrowFieldSpec(name="kind", dtype=pa.string()),
                ArrowFieldSpec(name="symbol", dtype=pa.string()),
                ArrowFieldSpec(name="opname", dtype=pa.string()),
                ArrowFieldSpec(name="offset", dtype=pa.int32()),
            ],
        )
    )
)

REACHES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="py_bc_reaches_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False),),
            fields=[
                ArrowFieldSpec(name="edge_id", dtype=pa.string()),
                ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
                ArrowFieldSpec(name="def_event_id", dtype=pa.string()),
                ArrowFieldSpec(name="use_event_id", dtype=pa.string()),
                ArrowFieldSpec(name="symbol", dtype=pa.string()),
            ],
        )
    )
)

DEF_USE_SCHEMA = DEF_USE_SPEC.table_spec.to_arrow_schema()
REACHES_SCHEMA = REACHES_SPEC.table_spec.to_arrow_schema()


SPAN_ERROR_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="span_errors_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="document_id", dtype=pa.string()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                ArrowFieldSpec(name="reason", dtype=pa.string()),
            ],
        )
    )
)

SPAN_ERROR_SCHEMA = SPAN_ERROR_SPEC.table_spec.to_arrow_schema()


DIAG_DETAIL_STRUCT = pa.struct(
    [
        ("detail_kind", pa.string()),
        ("error_type", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.string())),
    ]
)

DIAG_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="diagnostics_norm_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
            fields=[
                ArrowFieldSpec(name="diag_id", dtype=pa.string()),
                ArrowFieldSpec(name="severity", dtype=DICT_STRING),
                ArrowFieldSpec(name="message", dtype=pa.string()),
                ArrowFieldSpec(name="diag_source", dtype=DICT_STRING),
                ArrowFieldSpec(name="code", dtype=pa.string()),
                ArrowFieldSpec(name="details", dtype=pa.list_(DIAG_DETAIL_STRUCT)),
            ],
        )
    )
)

DIAG_ENCODING_SPECS: tuple[EncodingSpec, ...] = (
    EncodingSpec(column="severity"),
    EncodingSpec(column="diag_source"),
)

DIAG_SCHEMA = DIAG_SPEC.table_spec.to_arrow_schema()


__all__ = [
    "CFG_BLOCKS_NORM_SCHEMA",
    "CFG_BLOCKS_NORM_SPEC",
    "CFG_EDGES_NORM_SCHEMA",
    "CFG_EDGES_NORM_SPEC",
    "DEF_USE_SCHEMA",
    "DEF_USE_SPEC",
    "DIAG_DETAIL_STRUCT",
    "DIAG_ENCODING_SPECS",
    "DIAG_SCHEMA",
    "DIAG_SPEC",
    "REACHES_SCHEMA",
    "REACHES_SPEC",
    "SCHEMA_VERSION",
    "SPAN_ERROR_SCHEMA",
    "SPAN_ERROR_SPEC",
    "TYPE_EXPRS_NORM_SCHEMA",
    "TYPE_EXPRS_NORM_SPEC",
    "TYPE_NODES_SCHEMA",
    "TYPE_NODES_SPEC",
]
