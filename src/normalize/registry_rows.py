"""Dataset rows describing normalize schemas and derivations."""

from __future__ import annotations

from dataclasses import dataclass, field

from arrowdsl.compute.expr_core import (
    CoalesceStringExprSpec,
    DefUseKindExprSpec,
    HashExprSpec,
    HashFromExprsSpec,
    MaskedHashExprSpec,
    TrimExprSpec,
)
from arrowdsl.schema.build import FieldExpr
from arrowdsl.schema.validation import ArrowValidationOptions
from normalize.registry_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from schema_spec.specs import DerivedFieldSpec
from schema_spec.system import DedupeSpecSpec, SortKeySpec

SCHEMA_VERSION = 1
_DEF_USE_PREFIXES = ("STORE_", "DELETE_")
_USE_PREFIXES = ("LOAD_",)
_DEF_USE_OPS = ("IMPORT_NAME", "IMPORT_FROM")


@dataclass(frozen=True)
class ContractRow:
    """Contract specification for a dataset row."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None


@dataclass(frozen=True)
class DatasetRow:
    """Row spec describing a normalize dataset schema and derivations."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    derived: tuple[DerivedFieldSpec, ...] = ()
    input_fields: tuple[str, ...] = ()
    join_keys: tuple[str, ...] = ()
    contract: ContractRow | None = None
    template: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)


def _def_use_kind_expr() -> DefUseKindExprSpec:
    return DefUseKindExprSpec(
        column="opname",
        def_ops=_DEF_USE_OPS,
        def_prefixes=_DEF_USE_PREFIXES,
        use_prefixes=_USE_PREFIXES,
    )


DATASET_ROWS: tuple[DatasetRow, ...] = (
    DatasetRow(
        name="normalize_evidence_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "span_id",
            "evidence_family",
            "source",
            "role",
            "confidence",
            "ambiguity_group_id",
            "rule_name",
        ),
        join_keys=("span_id", "rule_name"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="span_id", order="ascending"),
                SortKeySpec(column="rule_name", order="ascending"),
            ),
        ),
        template="normalize_evidence",
    ),
    DatasetRow(
        name="type_exprs_norm_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "type_expr_id",
            "owner_def_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "expr_text",
            "type_repr",
            "type_id",
        ),
        derived=(
            DerivedFieldSpec(name="type_repr", expr=TrimExprSpec("expr_text")),
            DerivedFieldSpec(
                name="type_expr_id",
                expr=MaskedHashExprSpec(
                    spec=TYPE_EXPR_ID_SPEC,
                    required=("path", "bstart", "bend"),
                ),
            ),
            DerivedFieldSpec(
                name="type_id",
                expr=HashFromExprsSpec(
                    prefix=TYPE_ID_SPEC.prefix,
                    as_string=TYPE_ID_SPEC.as_string,
                    null_sentinel=TYPE_ID_SPEC.null_sentinel,
                    parts=(TrimExprSpec("expr_text"),),
                ),
            ),
        ),
        join_keys=("type_expr_id",),
        contract=ContractRow(
            canonical_sort=(SortKeySpec(column="type_expr_id", order="ascending"),),
        ),
        template="normalize_cst",
    ),
    DatasetRow(
        name="type_nodes_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=("type_id", "type_repr", "type_form", "origin"),
        join_keys=("type_id",),
        contract=ContractRow(
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
        ),
        template="normalize_type",
    ),
    DatasetRow(
        name="py_bc_blocks_norm_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=("block_id", "code_unit_id", "start_offset", "end_offset", "kind"),
        join_keys=("code_unit_id", "block_id"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="code_unit_id", order="ascending"),
                SortKeySpec(column="block_id", order="ascending"),
            ),
        ),
        template="normalize_bytecode",
    ),
    DatasetRow(
        name="py_bc_cfg_edges_norm_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=(
            "edge_id",
            "code_unit_id",
            "src_block_id",
            "dst_block_id",
            "kind",
            "cond_instr_id",
            "exc_index",
        ),
        join_keys=("code_unit_id", "edge_id"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="code_unit_id", order="ascending"),
                SortKeySpec(column="edge_id", order="ascending"),
            ),
        ),
        template="normalize_bytecode",
    ),
    DatasetRow(
        name="py_bc_def_use_events_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=("event_id", "instr_id", "code_unit_id", "kind", "symbol", "opname", "offset"),
        derived=(
            DerivedFieldSpec(
                name="symbol",
                expr=CoalesceStringExprSpec(columns=("argval_str", "argrepr")),
            ),
            DerivedFieldSpec(name="kind", expr=_def_use_kind_expr()),
            DerivedFieldSpec(
                name="event_id",
                expr=HashFromExprsSpec(
                    prefix=DEF_USE_EVENT_ID_SPEC.prefix,
                    as_string=DEF_USE_EVENT_ID_SPEC.as_string,
                    null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
                    parts=(
                        FieldExpr("code_unit_id"),
                        FieldExpr("instr_id"),
                        _def_use_kind_expr(),
                        CoalesceStringExprSpec(columns=("argval_str", "argrepr")),
                    ),
                ),
            ),
        ),
        input_fields=("argval_str", "argrepr"),
        join_keys=("code_unit_id", "event_id"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="code_unit_id", order="ascending"),
                SortKeySpec(column="event_id", order="ascending"),
            ),
        ),
        template="normalize_bytecode",
    ),
    DatasetRow(
        name="py_bc_reaches_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=("edge_id", "code_unit_id", "def_event_id", "use_event_id", "symbol"),
        derived=(DerivedFieldSpec(name="edge_id", expr=HashExprSpec(spec=REACH_EDGE_ID_SPEC)),),
        join_keys=("code_unit_id", "symbol", "def_event_id", "use_event_id"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="code_unit_id", order="ascending"),
                SortKeySpec(column="symbol", order="ascending"),
                SortKeySpec(column="def_event_id", order="ascending"),
                SortKeySpec(column="use_event_id", order="ascending"),
            ),
        ),
        template="normalize_bytecode",
    ),
    DatasetRow(
        name="span_errors_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=("document_id", "path", "reason"),
        template="normalize_span",
    ),
    DatasetRow(
        name="diagnostics_norm_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("diag_id", "severity", "message", "diag_source", "code", "details"),
        derived=(DerivedFieldSpec(name="diag_id", expr=HashExprSpec(spec=DIAG_ID_SPEC)),),
        join_keys=("diag_id",),
        contract=ContractRow(
            canonical_sort=(SortKeySpec(column="diag_id", order="ascending"),),
        ),
        template="normalize_diagnostics",
    ),
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
