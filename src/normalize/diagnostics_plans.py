"""Plan builders for normalized diagnostics tables."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.compute.filters import position_encoding_array
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_arrays
from arrowdsl.core.interop import ArrayLike, TableLike, pc
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan_helpers import column_or_null_expr
from arrowdsl.schema.build import column_or_null, empty_table, table_from_arrays
from arrowdsl.schema.metadata import normalize_dictionaries
from arrowdsl.schema.nested_builders import StructLargeListViewAccumulator
from normalize.registry_fields import DIAG_DETAILS_TYPE, DIAG_TAGS_TYPE
from normalize.registry_specs import (
    dataset_contract,
    dataset_query,
    dataset_schema,
    dataset_spec,
)
from normalize.spans import line_char_to_byte_offset
from normalize.text_index import (
    DEFAULT_POSITION_ENCODING,
    RepoTextIndex,
    file_index,
)
from schema_spec.specs import NestedFieldSpec

SCIP_SEVERITY_ERROR = 1
SCIP_SEVERITY_WARNING = 2
SCIP_SEVERITY_INFO = 3
SCIP_SEVERITY_HINT = 4

DIAG_NAME = "diagnostics_norm_v1"
DIAG_QUERY = dataset_query(DIAG_NAME)
DIAG_SCHEMA = dataset_schema(DIAG_NAME)
DIAG_SPEC = dataset_spec(DIAG_NAME)
DIAG_CONTRACT = dataset_contract(DIAG_NAME)

_DIAG_BASE_COLUMNS: tuple[str, ...] = DIAG_QUERY.projection.base


def _diag_base_field(field: pa.Field) -> pa.Field:
    if pa.types.is_dictionary(field.type):
        return pa.field(field.name, field.type.value_type, nullable=field.nullable)
    return pa.field(field.name, field.type, nullable=field.nullable)


_DIAG_BASE_SCHEMA = pa.schema(
    [_diag_base_field(DIAG_SCHEMA.field(name)) for name in _DIAG_BASE_COLUMNS]
)


def _empty_diag_base_table() -> TableLike:
    return empty_table(_DIAG_BASE_SCHEMA)


@dataclass(frozen=True)
class DiagnosticsSources:
    """Source tables for diagnostics aggregation."""

    cst_parse_errors: TableLike | None
    ts_errors: TableLike | None
    ts_missing: TableLike | None
    scip_diagnostics: TableLike | None
    scip_documents: TableLike | None = None


@dataclass(frozen=True)
class ScipDiagContext:
    """Context for SCIP diagnostics conversion."""

    repo_text_index: RepoTextIndex
    doc_enc: dict[str, int]


@dataclass(frozen=True)
class _DiagRow:
    path: object | None
    file_id: object | None
    bstart: int | None
    bend: int | None
    severity: str
    message: str
    source: str
    code: object | None
    detail: _DiagDetail | None


@dataclass(frozen=True)
class _DiagDetail:
    kind: str
    error_type: str | None
    source: str | None
    tags: list[str]


_DETAIL_FIELDS = ("detail_kind", "error_type", "source", "tags")


def _detail_accumulator() -> StructLargeListViewAccumulator:
    return StructLargeListViewAccumulator.with_fields(_DETAIL_FIELDS)


@dataclass
class _DiagBuffers:
    paths: list[object | None] = field(default_factory=list)
    file_ids: list[object | None] = field(default_factory=list)
    bstarts: list[int | None] = field(default_factory=list)
    bends: list[int | None] = field(default_factory=list)
    severities: list[str] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    codes: list[object | None] = field(default_factory=list)
    details_acc: StructLargeListViewAccumulator = field(default_factory=_detail_accumulator)

    def append(self, row: _DiagRow) -> None:
        self.paths.append(row.path)
        self.file_ids.append(row.file_id)
        self.bstarts.append(row.bstart)
        self.bends.append(row.bend)
        self.severities.append(row.severity)
        self.messages.append(row.message)
        self.sources.append(row.source)
        self.codes.append(row.code)
        self._append_detail(row.detail)

    def _append_detail(self, detail: _DiagDetail | None) -> None:
        if detail is None:
            self.details_acc.append_rows([])
            return
        self.details_acc.append_rows(
            [
                {
                    "detail_kind": detail.kind,
                    "error_type": detail.error_type,
                    "source": detail.source,
                    "tags": detail.tags,
                }
            ]
        )

    def details_array(self) -> ArrayLike:
        return DIAG_DETAIL_SPEC.builder(self)


@dataclass(frozen=True)
class _ScipDiagInput:
    doc_id: object | None
    path: object | None
    file_id: object | None
    start_line: object | None
    start_char: object | None
    end_line: object | None
    end_char: object | None
    message: object | None
    severity: object | None
    code: object | None
    source: object | None
    tags: object | None


def _build_diag_details(buffers: _DiagBuffers) -> ArrayLike:
    field_types = {
        "detail_kind": pa.string(),
        "error_type": pa.string(),
        "source": pa.string(),
        "tags": DIAG_TAGS_TYPE,
    }
    return buffers.details_acc.build(field_types=field_types)


DIAG_DETAIL_SPEC = NestedFieldSpec(
    name="details",
    dtype=DIAG_DETAILS_TYPE,
    builder=_build_diag_details,
)


def _detail_tags(value: object | None) -> list[str]:
    if isinstance(value, list):
        return [str(tag) for tag in value if tag]
    return []


def _cst_parse_error_row(
    repo_text_index: RepoTextIndex,
    values: tuple[object | None, ...],
) -> _DiagRow | None:
    path, file_id, raw_line, raw_column, message, error_type = values
    fidx = file_index(repo_text_index, file_id=file_id, path=path)
    if fidx is None:
        return None
    line_int = raw_line if isinstance(raw_line, int) else None
    col_int = raw_column if isinstance(raw_column, int) else None
    if line_int is None or col_int is None:
        return None
    line0 = line_int - 1 if line_int > 0 else 0
    bstart = line_char_to_byte_offset(
        fidx,
        line0,
        col_int,
        DEFAULT_POSITION_ENCODING,
    )
    msg = message if isinstance(message, str) else "LibCST parse error"
    detail = _DiagDetail(
        kind="libcst_parse_error",
        error_type=str(error_type) if error_type is not None else None,
        source=None,
        tags=[],
    )
    return _DiagRow(
        path=path,
        file_id=file_id,
        bstart=bstart,
        bend=bstart,
        severity="ERROR",
        message=msg,
        source="libcst",
        code=None,
        detail=detail,
    )


def _cst_parse_error_table(
    repo_text_index: RepoTextIndex,
    cst_parse_errors: TableLike,
) -> TableLike:
    buffers = _DiagBuffers()
    arrays = [
        column_or_null(cst_parse_errors, "path", pa.string()),
        column_or_null(cst_parse_errors, "file_id", pa.string()),
        column_or_null(cst_parse_errors, "raw_line", pa.int64()),
        column_or_null(cst_parse_errors, "raw_column", pa.int64()),
        column_or_null(cst_parse_errors, "message", pa.string()),
        column_or_null(cst_parse_errors, "error_type", pa.string()),
    ]
    for values in iter_arrays(arrays):
        row = _cst_parse_error_row(repo_text_index, values)
        if row is None:
            continue
        buffers.append(row)

    if not buffers.paths:
        return _empty_diag_base_table()

    path_arr = pa.array(buffers.paths, type=pa.string())
    bstart_arr = pa.array(buffers.bstarts, type=pa.int64())
    bend_arr = pa.array(buffers.bends, type=pa.int64())
    source_arr = pa.array(buffers.sources, type=pa.string())
    message_arr = pa.array(buffers.messages, type=pa.string())
    severity_arr = pa.array(buffers.severities, type=pa.string())
    details = buffers.details_array()
    columns = {
        "file_id": pa.array(buffers.file_ids, type=pa.string()),
        "path": path_arr,
        "bstart": bstart_arr,
        "bend": bend_arr,
        "severity": severity_arr,
        "message": message_arr,
        "source": source_arr,
        "code": pa.array(buffers.codes, type=pa.string()),
        "details": details,
    }
    table = table_from_arrays(_DIAG_BASE_SCHEMA, columns=columns, num_rows=len(buffers.paths))
    return normalize_dictionaries(table)


def _scip_severity(value: object | None) -> str:
    if isinstance(value, int):
        return _scip_severity_value(value)
    if isinstance(value, str):
        raw = value.strip().upper()
        if raw.isdigit():
            return _scip_severity_value(int(raw))
        if raw in {"ERROR", "WARNING", "INFO", "HINT"}:
            return raw
    return "ERROR"


def _scip_severity_value(value: int) -> str:
    mapping = {
        SCIP_SEVERITY_ERROR: "ERROR",
        SCIP_SEVERITY_WARNING: "WARNING",
        SCIP_SEVERITY_INFO: "INFO",
        SCIP_SEVERITY_HINT: "HINT",
    }
    return mapping.get(value, "INFO")


def _scip_diag_table(
    repo_text_index: RepoTextIndex,
    scip_diagnostics: TableLike,
    scip_documents: TableLike | None,
) -> TableLike:
    ctx = ScipDiagContext(
        repo_text_index=repo_text_index,
        doc_enc=_scip_doc_encodings(scip_documents),
    )

    buffers = _DiagBuffers()
    arrays = [
        column_or_null(scip_diagnostics, "document_id", pa.string()),
        column_or_null(scip_diagnostics, "path", pa.string()),
        column_or_null(scip_diagnostics, "file_id", pa.string()),
        column_or_null(scip_diagnostics, "start_line", pa.int64()),
        column_or_null(scip_diagnostics, "start_char", pa.int64()),
        column_or_null(scip_diagnostics, "end_line", pa.int64()),
        column_or_null(scip_diagnostics, "end_char", pa.int64()),
        column_or_null(scip_diagnostics, "message", pa.string()),
        column_or_null(scip_diagnostics, "severity", pa.string()),
        column_or_null(scip_diagnostics, "code", pa.string()),
        column_or_null(scip_diagnostics, "source", pa.string()),
        column_or_null(scip_diagnostics, "tags", pa.list_(pa.string())),
    ]
    for values in iter_arrays(arrays):
        diag_row = _scip_diag_row(ctx, _ScipDiagInput(*values))
        if diag_row is None:
            continue
        buffers.append(diag_row)

    if not buffers.paths:
        return _empty_diag_base_table()

    path_arr = pa.array(buffers.paths, type=pa.string())
    bstart_arr = pa.array(buffers.bstarts, type=pa.int64())
    bend_arr = pa.array(buffers.bends, type=pa.int64())
    source_arr = pa.array(buffers.sources, type=pa.string())
    message_arr = pa.array(buffers.messages, type=pa.string())
    severity_arr = pa.array(buffers.severities, type=pa.string())
    details = buffers.details_array()
    columns = {
        "file_id": pa.array(buffers.file_ids, type=pa.string()),
        "path": path_arr,
        "bstart": bstart_arr,
        "bend": bend_arr,
        "severity": severity_arr,
        "message": message_arr,
        "source": source_arr,
        "code": pa.array(buffers.codes, type=pa.string()),
        "details": details,
    }
    table = table_from_arrays(_DIAG_BASE_SCHEMA, columns=columns, num_rows=len(buffers.paths))
    return normalize_dictionaries(table)


def _scip_doc_encodings(scip_documents: TableLike | None) -> dict[str, int]:
    doc_enc: dict[str, int] = {}
    if scip_documents is None or scip_documents.num_rows == 0:
        return doc_enc
    doc_ids = column_or_null(scip_documents, "document_id", pa.string())
    enc_values = position_encoding_array(
        column_or_null(scip_documents, "position_encoding", pa.string())
    )
    for doc_id, enc in iter_arrays([doc_ids, enc_values]):
        if isinstance(doc_id, str):
            doc_enc[doc_id] = enc if isinstance(enc, int) else DEFAULT_POSITION_ENCODING
    return doc_enc


def _scip_diag_row(ctx: ScipDiagContext, values: _ScipDiagInput) -> _DiagRow | None:
    fidx = file_index(ctx.repo_text_index, file_id=values.file_id, path=values.path)
    if fidx is None:
        return None
    if not isinstance(values.start_line, int) or not isinstance(values.start_char, int):
        return None
    if not isinstance(values.end_line, int) or not isinstance(values.end_char, int):
        return None
    enc = (
        ctx.doc_enc.get(values.doc_id, DEFAULT_POSITION_ENCODING)
        if isinstance(values.doc_id, str)
        else DEFAULT_POSITION_ENCODING
    )
    bstart = line_char_to_byte_offset(fidx, values.start_line, values.start_char, enc)
    bend = line_char_to_byte_offset(fidx, values.end_line, values.end_char, enc)
    msg = values.message if isinstance(values.message, str) else "SCIP diagnostic"
    sev = _scip_severity(values.severity)
    detail = _DiagDetail(
        kind="scip_diag",
        error_type=None,
        source=str(values.source) if values.source is not None else None,
        tags=_detail_tags(values.tags),
    )
    return _DiagRow(
        path=values.path,
        file_id=values.file_id,
        bstart=bstart,
        bend=bend,
        severity=sev,
        message=msg,
        source="scip",
        code=values.code,
        detail=detail,
    )


def _ts_diag_plan(
    ts_table: TableLike,
    *,
    severity: str,
    message: str,
    ctx: ExecutionContext,
) -> Plan:
    plan = Plan.table_source(ts_table)
    available = set(plan.schema(ctx=ctx).names)
    details_scalar = pa.scalar([], type=DIAG_DETAILS_TYPE)
    exprs = [
        column_or_null_expr("file_id", pa.string(), available=available),
        column_or_null_expr("path", pa.string(), available=available),
        column_or_null_expr("start_byte", pa.int64(), available=available),
        column_or_null_expr("end_byte", pa.int64(), available=available),
        pc.scalar(severity),
        pc.scalar(message),
        pc.scalar("treesitter"),
        pc.scalar(pa.scalar(None, type=pa.string())),
        pc.scalar(details_scalar),
    ]
    return plan.project(exprs, list(_DIAG_BASE_COLUMNS), ctx=ctx)


def _diagnostics_plan(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> Plan:
    plans: list[Plan] = []
    if sources.cst_parse_errors is not None and sources.cst_parse_errors.num_rows:
        table = _cst_parse_error_table(repo_text_index, sources.cst_parse_errors)
        if table.num_rows:
            plans.append(Plan.table_source(table))
    if sources.ts_errors is not None and sources.ts_errors.num_rows:
        plans.append(
            _ts_diag_plan(
                sources.ts_errors,
                severity="ERROR",
                message="tree-sitter error node",
                ctx=ctx,
            )
        )
    if sources.ts_missing is not None and sources.ts_missing.num_rows:
        plans.append(
            _ts_diag_plan(
                sources.ts_missing,
                severity="WARNING",
                message="tree-sitter missing node",
                ctx=ctx,
            )
        )
    if sources.scip_diagnostics is not None and sources.scip_diagnostics.num_rows:
        table = _scip_diag_table(
            repo_text_index,
            sources.scip_diagnostics,
            sources.scip_documents,
        )
        if table.num_rows:
            plans.append(Plan.table_source(table))

    if not plans:
        plans.append(Plan.table_source(_empty_diag_base_table()))

    unioned = union_all_plans(plans, label="diagnostics")
    return DIAG_QUERY.apply_to_plan(unioned, ctx=ctx)


def diagnostics_plan(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane normalized diagnostics table.

    Returns
    -------
    Plan
        Plan producing normalized diagnostics rows.
    """
    return _diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)


__all__ = [
    "DIAG_CONTRACT",
    "DIAG_NAME",
    "DIAG_QUERY",
    "DIAG_SCHEMA",
    "DIAG_SPEC",
    "DiagnosticsSources",
    "diagnostics_plan",
]
