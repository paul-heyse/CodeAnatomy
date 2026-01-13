"""Shared Arrow schema definitions for normalization."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.expr_specs import (
    CoalesceStringExprSpec,
    DefUseKindExprSpec,
    HashExprSpec,
    HashFromExprsSpec,
    MaskedHashExprSpec,
    TrimExprSpec,
)
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.schema.arrays import FieldExpr, struct_type
from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.factories import DatasetRegistration, register_dataset
from normalize.hash_specs import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from schema_spec.specs import (
    ArrowFieldSpec,
    dict_field,
    file_identity_bundle,
    list_view_type,
    span_bundle,
)
from schema_spec.system import (
    DedupeSpecSpec,
    SortKeySpec,
    make_contract_spec,
    make_table_spec,
)

SCHEMA_VERSION = 1
_DEF_USE_PREFIXES = ("STORE_", "DELETE_")
_USE_PREFIXES = ("LOAD_",)
_DEF_USE_OPS = ("IMPORT_NAME", "IMPORT_FROM")


DIAG_TAGS_TYPE = list_view_type(pa.string(), large=True)
DIAG_DETAIL_STRUCT = struct_type(
    {
        "detail_kind": pa.string(),
        "error_type": pa.string(),
        "source": pa.string(),
        "tags": DIAG_TAGS_TYPE,
    }
)
DIAG_DETAILS_TYPE = list_view_type(DIAG_DETAIL_STRUCT, large=True)


def _normalize_metadata(dataset_name: str) -> SchemaMetadataSpec:
    return SchemaMetadataSpec(
        schema_metadata={
            b"normalize_stage": b"normalize",
            b"normalize_dataset": dataset_name.encode("utf-8"),
        }
    )


TYPE_EXPRS_TABLE_SPEC = make_table_spec(
    name="type_exprs_norm_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
    fields=[
        ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
        ArrowFieldSpec(name="owner_def_id", dtype=pa.string()),
        ArrowFieldSpec(name="param_name", dtype=pa.string()),
        dict_field("expr_kind"),
        dict_field("expr_role"),
        ArrowFieldSpec(name="expr_text", dtype=pa.string()),
        ArrowFieldSpec(name="type_repr", dtype=pa.string()),
        ArrowFieldSpec(name="type_id", dtype=pa.string()),
    ],
)

TYPE_NODES_TABLE_SPEC = make_table_spec(
    name="type_nodes_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="type_id", dtype=pa.string()),
        ArrowFieldSpec(name="type_repr", dtype=pa.string()),
        dict_field("type_form"),
        dict_field("origin"),
    ],
)

CFG_BLOCKS_TABLE_SPEC = make_table_spec(
    name="py_bc_blocks_norm_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False),),
    fields=[
        ArrowFieldSpec(name="block_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        ArrowFieldSpec(name="start_offset", dtype=pa.int32()),
        ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
        dict_field("kind"),
    ],
)

CFG_EDGES_TABLE_SPEC = make_table_spec(
    name="py_bc_cfg_edges_norm_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        ArrowFieldSpec(name="src_block_id", dtype=pa.string()),
        ArrowFieldSpec(name="dst_block_id", dtype=pa.string()),
        dict_field("kind"),
        ArrowFieldSpec(name="cond_instr_id", dtype=pa.string()),
        ArrowFieldSpec(name="exc_index", dtype=pa.int32()),
    ],
)

DEF_USE_TABLE_SPEC = make_table_spec(
    name="py_bc_def_use_events_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False),),
    fields=[
        ArrowFieldSpec(name="event_id", dtype=pa.string()),
        ArrowFieldSpec(name="instr_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        dict_field("kind"),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
        dict_field("opname"),
        ArrowFieldSpec(name="offset", dtype=pa.int32()),
    ],
)

REACHES_TABLE_SPEC = make_table_spec(
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

SPAN_ERROR_TABLE_SPEC = make_table_spec(
    name="span_errors_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="document_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        dict_field("reason"),
    ],
)

DIAG_TABLE_SPEC = make_table_spec(
    name="diagnostics_norm_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
    fields=[
        ArrowFieldSpec(name="diag_id", dtype=pa.string()),
        dict_field("severity"),
        ArrowFieldSpec(name="message", dtype=pa.string()),
        dict_field("diag_source"),
        ArrowFieldSpec(name="code", dtype=pa.string()),
        ArrowFieldSpec(name="details", dtype=DIAG_DETAILS_TYPE),
    ],
)

TYPE_EXPRS_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=(
            "file_id",
            "path",
            "bstart",
            "bend",
            "owner_def_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "expr_text",
        ),
        derived={
            "type_repr": TrimExprSpec("expr_text"),
            "type_expr_id": MaskedHashExprSpec(
                spec=TYPE_EXPR_ID_SPEC,
                required=("path", "bstart", "bend"),
            ),
            "type_id": HashFromExprsSpec(
                prefix=TYPE_ID_SPEC.prefix,
                as_string=TYPE_ID_SPEC.as_string,
                null_sentinel=TYPE_ID_SPEC.null_sentinel,
                parts=(TrimExprSpec("expr_text"),),
            ),
        },
    )
)

TYPE_NODES_QUERY = QuerySpec.simple(*[field.name for field in TYPE_NODES_TABLE_SPEC.fields])

DEF_USE_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=(
            "file_id",
            "path",
            "instr_id",
            "code_unit_id",
            "opname",
            "offset",
        ),
        derived={
            "symbol": CoalesceStringExprSpec(columns=("argval_str", "argrepr")),
            "kind": DefUseKindExprSpec(
                column="opname",
                def_ops=_DEF_USE_OPS,
                def_prefixes=_DEF_USE_PREFIXES,
                use_prefixes=_USE_PREFIXES,
            ),
            "event_id": HashFromExprsSpec(
                prefix=DEF_USE_EVENT_ID_SPEC.prefix,
                as_string=DEF_USE_EVENT_ID_SPEC.as_string,
                null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
                parts=(
                    FieldExpr("code_unit_id"),
                    FieldExpr("instr_id"),
                    DefUseKindExprSpec(
                        column="opname",
                        def_ops=_DEF_USE_OPS,
                        def_prefixes=_DEF_USE_PREFIXES,
                        use_prefixes=_USE_PREFIXES,
                    ),
                    CoalesceStringExprSpec(columns=("argval_str", "argrepr")),
                ),
            ),
        },
    )
)

REACHES_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=(
            "file_id",
            "path",
            "code_unit_id",
            "def_event_id",
            "use_event_id",
            "symbol",
        ),
        derived={
            "edge_id": HashExprSpec(spec=REACH_EDGE_ID_SPEC),
        },
    )
)

CFG_BLOCKS_QUERY = QuerySpec.simple(*[field.name for field in CFG_BLOCKS_TABLE_SPEC.fields])
CFG_EDGES_QUERY = QuerySpec.simple(*[field.name for field in CFG_EDGES_TABLE_SPEC.fields])

DIAG_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=(
            "file_id",
            "path",
            "bstart",
            "bend",
            "severity",
            "message",
            "diag_source",
            "code",
            "details",
        ),
        derived={
            "diag_id": HashExprSpec(spec=DIAG_ID_SPEC),
        },
    )
)

TYPE_EXPRS_CONTRACT = make_contract_spec(
    table_spec=TYPE_EXPRS_TABLE_SPEC,
    canonical_sort=(SortKeySpec(column="type_expr_id", order="ascending"),),
)

TYPE_NODES_CONTRACT = make_contract_spec(
    table_spec=TYPE_NODES_TABLE_SPEC,
    dedupe=DedupeSpecSpec(
        keys=("type_id",),
        tie_breakers=(
            SortKeySpec(column="type_repr", order="ascending"),
            SortKeySpec(column="type_form", order="ascending"),
            SortKeySpec(column="origin", order="ascending"),
        ),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(SortKeySpec(column="type_id", order="ascending"),),
)

DEF_USE_CONTRACT = make_contract_spec(
    table_spec=DEF_USE_TABLE_SPEC,
    canonical_sort=(
        SortKeySpec(column="code_unit_id", order="ascending"),
        SortKeySpec(column="event_id", order="ascending"),
    ),
)

REACHES_CONTRACT = make_contract_spec(
    table_spec=REACHES_TABLE_SPEC,
    canonical_sort=(
        SortKeySpec(column="code_unit_id", order="ascending"),
        SortKeySpec(column="symbol", order="ascending"),
        SortKeySpec(column="def_event_id", order="ascending"),
        SortKeySpec(column="use_event_id", order="ascending"),
    ),
)

CFG_BLOCKS_CONTRACT = make_contract_spec(
    table_spec=CFG_BLOCKS_TABLE_SPEC,
    canonical_sort=(
        SortKeySpec(column="code_unit_id", order="ascending"),
        SortKeySpec(column="block_id", order="ascending"),
    ),
)

CFG_EDGES_CONTRACT = make_contract_spec(
    table_spec=CFG_EDGES_TABLE_SPEC,
    canonical_sort=(
        SortKeySpec(column="code_unit_id", order="ascending"),
        SortKeySpec(column="edge_id", order="ascending"),
    ),
)

DIAG_CONTRACT = make_contract_spec(
    table_spec=DIAG_TABLE_SPEC,
    canonical_sort=(SortKeySpec(column="diag_id", order="ascending"),),
)

TYPE_EXPRS_NORM_SPEC = register_dataset(
    table_spec=TYPE_EXPRS_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=TYPE_EXPRS_QUERY,
        contract_spec=TYPE_EXPRS_CONTRACT,
        metadata_spec=_normalize_metadata("type_exprs_norm"),
    ),
)

TYPE_NODES_SPEC = register_dataset(
    table_spec=TYPE_NODES_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=TYPE_NODES_QUERY,
        contract_spec=TYPE_NODES_CONTRACT,
        metadata_spec=_normalize_metadata("type_nodes"),
    ),
)

CFG_BLOCKS_NORM_SPEC = register_dataset(
    table_spec=CFG_BLOCKS_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=CFG_BLOCKS_QUERY,
        contract_spec=CFG_BLOCKS_CONTRACT,
        metadata_spec=_normalize_metadata("cfg_blocks"),
    ),
)

CFG_EDGES_NORM_SPEC = register_dataset(
    table_spec=CFG_EDGES_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=CFG_EDGES_QUERY,
        contract_spec=CFG_EDGES_CONTRACT,
        metadata_spec=_normalize_metadata("cfg_edges"),
    ),
)

DEF_USE_SPEC = register_dataset(
    table_spec=DEF_USE_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=DEF_USE_QUERY,
        contract_spec=DEF_USE_CONTRACT,
        metadata_spec=_normalize_metadata("def_use_events"),
    ),
)

REACHES_SPEC = register_dataset(
    table_spec=REACHES_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=REACHES_QUERY,
        contract_spec=REACHES_CONTRACT,
        metadata_spec=_normalize_metadata("reaches"),
    ),
)

SPAN_ERROR_SPEC = register_dataset(
    table_spec=SPAN_ERROR_TABLE_SPEC,
    registration=DatasetRegistration(metadata_spec=_normalize_metadata("span_errors")),
)

DIAG_SPEC = register_dataset(
    table_spec=DIAG_TABLE_SPEC,
    registration=DatasetRegistration(
        query_spec=DIAG_QUERY,
        contract_spec=DIAG_CONTRACT,
        metadata_spec=_normalize_metadata("diagnostics"),
    ),
)

TYPE_EXPRS_NORM_SCHEMA = TYPE_EXPRS_NORM_SPEC.schema()
TYPE_NODES_SCHEMA = TYPE_NODES_SPEC.schema()
CFG_BLOCKS_NORM_SCHEMA = CFG_BLOCKS_NORM_SPEC.schema()
CFG_EDGES_NORM_SCHEMA = CFG_EDGES_NORM_SPEC.schema()
DEF_USE_SCHEMA = DEF_USE_SPEC.schema()
REACHES_SCHEMA = REACHES_SPEC.schema()
SPAN_ERROR_SCHEMA = SPAN_ERROR_SPEC.schema()
DIAG_SCHEMA = DIAG_SPEC.schema()

__all__ = [
    "CFG_BLOCKS_CONTRACT",
    "CFG_BLOCKS_NORM_SCHEMA",
    "CFG_BLOCKS_NORM_SPEC",
    "CFG_BLOCKS_QUERY",
    "CFG_EDGES_CONTRACT",
    "CFG_EDGES_NORM_SCHEMA",
    "CFG_EDGES_NORM_SPEC",
    "CFG_EDGES_QUERY",
    "DEF_USE_CONTRACT",
    "DEF_USE_QUERY",
    "DEF_USE_SCHEMA",
    "DEF_USE_SPEC",
    "DIAG_CONTRACT",
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_QUERY",
    "DIAG_SCHEMA",
    "DIAG_SPEC",
    "REACHES_CONTRACT",
    "REACHES_QUERY",
    "REACHES_SCHEMA",
    "REACHES_SPEC",
    "SCHEMA_VERSION",
    "SPAN_ERROR_SCHEMA",
    "SPAN_ERROR_SPEC",
    "TYPE_EXPRS_CONTRACT",
    "TYPE_EXPRS_NORM_SCHEMA",
    "TYPE_EXPRS_NORM_SPEC",
    "TYPE_EXPRS_QUERY",
    "TYPE_NODES_CONTRACT",
    "TYPE_NODES_QUERY",
    "TYPE_NODES_SCHEMA",
    "TYPE_NODES_SPEC",
]
