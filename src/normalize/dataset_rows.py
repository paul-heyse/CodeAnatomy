"""Dataset rows describing normalize schemas and derivations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.schema.metadata import metadata_map_bytes, metadata_scalar_map_bytes
from arrowdsl.schema.validation import ArrowValidationOptions
from datafusion_engine.normalize_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from ibis_engine.hashing import (
    masked_stable_id_expr_ir,
    stable_id_expr_ir,
    stable_id_expr_ir_from_parts,
)
from normalize.evidence_specs import EVIDENCE_OUTPUT_LITERALS_META, EVIDENCE_OUTPUT_MAP_META
from schema_spec.specs import DerivedFieldSpec
from schema_spec.system import DedupeSpecSpec, SortKeySpec
from sqlglot_tools.expr_spec import ExprIR, SqlExprSpec

SCHEMA_VERSION = 1
_DEF_USE_PREFIXES = ("STORE_", "DELETE_")
_USE_PREFIXES = ("LOAD_",)
_DEF_USE_OPS = ("IMPORT_NAME", "IMPORT_FROM")


def _field_expr(name: str) -> ExprIR:
    return ExprIR(op="field", name=name)


def _literal_expr(value: ScalarValue) -> ExprIR:
    return ExprIR(op="literal", value=value)


def _call_expr(name: str, *args: ExprIR) -> ExprIR:
    return ExprIR(op="call", name=name, args=tuple(args))


def _stringify_expr(expr: ExprIR) -> ExprIR:
    return _call_expr("stringify", expr)


def _trim_expr(column: str) -> ExprIR:
    return _call_expr("utf8_trim_whitespace", _field_expr(column))


def _coalesce_expr(exprs: Sequence[ExprIR]) -> ExprIR:
    if not exprs:
        return _literal_expr(None)
    if len(exprs) == 1:
        return exprs[0]
    return _call_expr("coalesce", *exprs)


def _coalesce_string_expr(columns: Sequence[str]) -> ExprIR:
    return _coalesce_expr([_field_expr(name) for name in columns])


def _or_exprs(exprs: Sequence[ExprIR]) -> ExprIR:
    if not exprs:
        return _literal_expr(value=False)
    out = exprs[0]
    for expr in exprs[1:]:
        out = _call_expr("bit_wise_or", out, expr)
    return out


def _sql_spec(expr: ExprIR) -> SqlExprSpec:
    return SqlExprSpec(expr_ir=expr)


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
    view_builder: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    register_view: bool = True


def _def_use_kind_expr() -> ExprIR:
    opname = _stringify_expr(_field_expr("opname"))
    def_ops = tuple(_call_expr("equal", opname, _literal_expr(value)) for value in _DEF_USE_OPS)
    def_prefixes = tuple(
        _call_expr("starts_with", opname, _literal_expr(prefix)) for prefix in _DEF_USE_PREFIXES
    )
    is_def = _or_exprs((*def_ops, *def_prefixes))
    is_use = _call_expr("starts_with", opname, _literal_expr(_USE_PREFIXES[0]))
    return _call_expr(
        "if_else",
        is_def,
        _literal_expr("def"),
        _call_expr("if_else", is_use, _literal_expr("use"), _literal_expr(None)),
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
            "task_name",
        ),
        join_keys=("span_id", "task_name"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="span_id", order="ascending"),
                SortKeySpec(column="task_name", order="ascending"),
            ),
        ),
        template="normalize_evidence",
        register_view=False,
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
        input_fields=("bstart", "bend", "line_base", "col_unit", "end_exclusive"),
        derived=(
            DerivedFieldSpec(name="type_repr", expr=_sql_spec(_trim_expr("expr_text"))),
            DerivedFieldSpec(
                name="type_expr_id",
                expr=masked_stable_id_expr_ir(
                    spec=TYPE_EXPR_ID_SPEC,
                    required=("path", "bstart", "bend"),
                ),
            ),
            DerivedFieldSpec(
                name="type_id",
                expr=stable_id_expr_ir_from_parts(
                    prefix=TYPE_ID_SPEC.prefix,
                    null_sentinel=TYPE_ID_SPEC.null_sentinel,
                    as_string=TYPE_ID_SPEC.as_string,
                    parts=(_sql_spec(_trim_expr("expr_text")),),
                ),
            ),
        ),
        join_keys=("type_expr_id",),
        contract=ContractRow(
            canonical_sort=(SortKeySpec(column="type_expr_id", order="ascending"),),
        ),
        template="normalize_cst",
        view_builder="type_exprs_plan_ibis",
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
        view_builder="type_nodes_plan_ibis",
    ),
    DatasetRow(
        name="py_bc_blocks_norm_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("block_id", "code_unit_id", "start_offset", "end_offset", "kind"),
        join_keys=("code_unit_id", "block_id"),
        contract=ContractRow(
            canonical_sort=(
                SortKeySpec(column="code_unit_id", order="ascending"),
                SortKeySpec(column="block_id", order="ascending"),
            ),
        ),
        template="normalize_bytecode",
        view_builder="cfg_blocks_plan_ibis",
        metadata_extra={
            EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes({"span": "span", "role": "kind"}),
            EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes({"source": "py_bc_blocks"}),
        },
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
        view_builder="cfg_edges_plan_ibis",
        metadata_extra={
            EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes({"role": "kind"}),
            EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes({"source": "py_bc_cfg_edges"}),
        },
    ),
    DatasetRow(
        name="py_bc_def_use_events_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("event_id", "instr_id", "code_unit_id", "kind", "symbol", "opname", "offset"),
        derived=(
            DerivedFieldSpec(
                name="symbol",
                expr=_sql_spec(_coalesce_string_expr(("argval_str", "argrepr"))),
            ),
            DerivedFieldSpec(name="kind", expr=_sql_spec(_def_use_kind_expr())),
            DerivedFieldSpec(
                name="event_id",
                expr=stable_id_expr_ir_from_parts(
                    prefix=DEF_USE_EVENT_ID_SPEC.prefix,
                    null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
                    as_string=DEF_USE_EVENT_ID_SPEC.as_string,
                    parts=(
                        _sql_spec(_field_expr("code_unit_id")),
                        _sql_spec(_field_expr("instr_id")),
                        _sql_spec(_def_use_kind_expr()),
                        _sql_spec(_coalesce_string_expr(("argval_str", "argrepr"))),
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
        view_builder="def_use_events_plan_ibis",
        metadata_extra={
            EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes({"span": "span", "role": "kind"}),
            EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes(
                {"source": "py_bc_instructions"}
            ),
        },
    ),
    DatasetRow(
        name="py_bc_reaches_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=("edge_id", "code_unit_id", "def_event_id", "use_event_id", "symbol"),
        derived=(
            DerivedFieldSpec(name="edge_id", expr=stable_id_expr_ir(spec=REACH_EDGE_ID_SPEC)),
        ),
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
        view_builder="reaching_defs_plan_ibis",
        metadata_extra={
            EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes({"source": "py_bc_reaches"}),
        },
    ),
    DatasetRow(
        name="span_errors_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=("document_id", "path", "reason"),
        template="normalize_span",
        view_builder="span_errors_plan_ibis",
    ),
    DatasetRow(
        name="diagnostics_norm_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "diag_id",
            "severity",
            "message",
            "diag_source",
            "code",
            "details",
        ),
        derived=(DerivedFieldSpec(name="diag_id", expr=stable_id_expr_ir(spec=DIAG_ID_SPEC)),),
        join_keys=("diag_id",),
        contract=ContractRow(
            canonical_sort=(SortKeySpec(column="diag_id", order="ascending"),),
        ),
        template="normalize_diagnostics",
        view_builder="diagnostics_plan_ibis",
    ),
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
