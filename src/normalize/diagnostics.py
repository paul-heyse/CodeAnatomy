"""Normalize diagnostics from extraction layers."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_arrays
from arrowdsl.core.interop import ArrayLike, TableLike, pc
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.schema.arrays import build_struct_array
from normalize.arrow_utils import column_or_null
from normalize.plan_exprs import column_or_null_expr
from normalize.plan_helpers import apply_query_spec
from normalize.runner import PostFn, ensure_canonical, ensure_execution_context, run_normalize
from normalize.schemas import (
    DIAG_CONTRACT,
    DIAG_DETAILS_TYPE,
    DIAG_QUERY,
    DIAG_SCHEMA,
    DIAG_SPEC,
)
from normalize.spans import line_char_to_byte_offset
from normalize.text_index import (
    DEFAULT_POSITION_ENCODING,
    RepoTextIndex,
    file_index,
    normalize_position_encoding,
)
from schema_spec.specs import NestedFieldSpec

SCIP_SEVERITY_ERROR = 1
SCIP_SEVERITY_WARNING = 2
SCIP_SEVERITY_INFO = 3
SCIP_SEVERITY_HINT = 4

_DIAG_BASE_COLUMNS: tuple[str, ...] = DIAG_QUERY.projection.base


def _diag_base_field(field: pa.Field) -> pa.Field:
    if pa.types.is_dictionary(field.type):
        return pa.field(field.name, field.type.value_type, nullable=field.nullable)
    return pa.field(field.name, field.type, nullable=field.nullable)


_DIAG_BASE_SCHEMA = pa.schema(
    [_diag_base_field(DIAG_SCHEMA.field(name)) for name in _DIAG_BASE_COLUMNS]
)


def _empty_diag_base_table() -> TableLike:
    arrays = [pa.array([], type=field.type) for field in _DIAG_BASE_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=_DIAG_BASE_SCHEMA)


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


def _offsets_start() -> list[int]:
    return [0]


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
    detail_offsets: list[int] = field(default_factory=_offsets_start)
    detail_kinds: list[str] = field(default_factory=list)
    detail_error_types: list[str | None] = field(default_factory=list)
    detail_sources: list[str | None] = field(default_factory=list)
    detail_tag_offsets: list[int] = field(default_factory=_offsets_start)
    detail_tag_values: list[str | None] = field(default_factory=list)

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
            self.detail_offsets.append(len(self.detail_kinds))
            return
        self.detail_kinds.append(detail.kind)
        self.detail_error_types.append(detail.error_type)
        self.detail_sources.append(detail.source)
        for tag in detail.tags:
            self.detail_tag_values.append(tag)
        self.detail_tag_offsets.append(len(self.detail_tag_values))
        self.detail_offsets.append(len(self.detail_kinds))

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
    tags = pa.LargeListArray.from_arrays(
        pa.array(buffers.detail_tag_offsets, type=pa.int64()),
        pa.array(buffers.detail_tag_values, type=pa.string()),
    )
    detail_struct = build_struct_array(
        {
            "detail_kind": pa.array(buffers.detail_kinds, type=pa.string()),
            "error_type": pa.array(buffers.detail_error_types, type=pa.string()),
            "source": pa.array(buffers.detail_sources, type=pa.string()),
            "tags": tags,
        }
    )
    return pa.LargeListArray.from_arrays(
        pa.array(buffers.detail_offsets, type=pa.int64()),
        detail_struct,
    )


DIAG_DETAIL_SPEC = NestedFieldSpec(
    name="details",
    dtype=DIAG_DETAILS_TYPE,
    builder=_build_diag_details,
)


def _detail_tags(value: object | None) -> list[str]:
    if isinstance(value, list):
        return [str(tag) for tag in value if tag]
    return []


def _empty_details_list(num_rows: int) -> ArrayLike:
    offsets = pa.array([0] * (num_rows + 1), type=pa.int64())
    tags = pa.LargeListArray.from_arrays(
        pa.array([0], type=pa.int64()),
        pa.array([], type=pa.string()),
    )
    detail_struct = build_struct_array(
        {
            "detail_kind": pa.array([], type=pa.string()),
            "error_type": pa.array([], type=pa.string()),
            "source": pa.array([], type=pa.string()),
            "tags": tags,
        }
    )
    return pa.LargeListArray.from_arrays(offsets, detail_struct)


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
    return pa.Table.from_arrays(
        [
            pa.array(buffers.file_ids, type=pa.string()),
            path_arr,
            bstart_arr,
            bend_arr,
            severity_arr,
            message_arr,
            source_arr,
            pa.array(buffers.codes, type=pa.string()),
            details,
        ],
        schema=_DIAG_BASE_SCHEMA,
    )


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
    return pa.Table.from_arrays(
        [
            pa.array(buffers.file_ids, type=pa.string()),
            path_arr,
            bstart_arr,
            bend_arr,
            severity_arr,
            message_arr,
            source_arr,
            pa.array(buffers.codes, type=pa.string()),
            details,
        ],
        schema=_DIAG_BASE_SCHEMA,
    )


def _scip_doc_encodings(scip_documents: TableLike | None) -> dict[str, int]:
    doc_enc: dict[str, int] = {}
    if scip_documents is None or scip_documents.num_rows == 0:
        return doc_enc
    arrays = [
        column_or_null(scip_documents, "document_id", pa.string()),
        column_or_null(scip_documents, "position_encoding", pa.string()),
    ]
    for doc_id, enc in iter_arrays(arrays):
        if isinstance(doc_id, str):
            doc_enc[doc_id] = normalize_position_encoding(enc)
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
    return apply_query_spec(unioned, spec=DIAG_QUERY, ctx=ctx)


def diagnostics_post_step(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
) -> PostFn:
    """Return a post step that builds diagnostics in the plan lane.

    Returns
    -------
    PostFn
        Post step that emits a diagnostics table.
    """

    def _apply(table: TableLike, ctx: ExecutionContext) -> TableLike:
        _ = table
        plan = _diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)
        return plan.to_table(ctx=ctx)

    return _apply


def collect_diags_result(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Aggregate diagnostics into a single normalized table.

    Parameters
    ----------
    repo_text_index:
        Repo text index for line/column to byte offsets.
    sources:
        Diagnostics source tables.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with normalized diagnostics.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _diagnostics_plan(repo_text_index, sources=sources, ctx=exec_ctx)
    return run_normalize(
        plan=plan,
        post=(),
        contract=DIAG_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=DIAG_SPEC.metadata_spec,
    )


def collect_diags(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Aggregate diagnostics into a single normalized table.

    Parameters
    ----------
    repo_text_index:
        Repo text index for line/column to byte offsets.
    sources:
        Diagnostics source tables.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    return collect_diags_result(repo_text_index, sources=sources, ctx=ctx).good


def collect_diags_canonical(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Aggregate diagnostics under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized diagnostics table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return collect_diags_result(repo_text_index, sources=sources, ctx=exec_ctx).good
