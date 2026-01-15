"""Plan builders for normalized diagnostics tables."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.compute.expr_core import ENC_UTF8, ENC_UTF16
from arrowdsl.compute.filters import position_encoding_array
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_arrays
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike, pc
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
    """Return a normalized base field for diagnostics tables.

    Parameters
    ----------
    field
        Arrow field to normalize.

    Returns
    -------
    pyarrow.Field
        Field with dictionaries normalized to value types.
    """
    if pa.types.is_dictionary(field.type):
        return pa.field(field.name, field.type.value_type, nullable=field.nullable)
    return pa.field(field.name, field.type, nullable=field.nullable)


_DIAG_BASE_SCHEMA = pa.schema(
    [_diag_base_field(DIAG_SCHEMA.field(name)) for name in _DIAG_BASE_COLUMNS]
)


def _empty_diag_base_table() -> TableLike:
    """Return an empty diagnostics base table.

    Returns
    -------
    TableLike
        Empty diagnostics base table.
    """
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
    """Structured diagnostics row payload."""

    path: object | None
    file_id: object | None
    bstart: int | None
    bend: int | None
    severity: str
    message: str
    diag_source: str
    code: object | None
    detail: _DiagDetail | None
    line_base: int | None
    col_unit: str | None
    end_exclusive: bool | None


@dataclass(frozen=True)
class _DiagDetail:
    """Structured diagnostics detail payload."""

    kind: str
    error_type: str | None
    source: str | None
    tags: list[str]


_DETAIL_FIELDS = ("detail_kind", "error_type", "source", "tags")


def _detail_accumulator() -> StructLargeListViewAccumulator:
    """Return a diagnostics detail accumulator.

    Returns
    -------
    StructLargeListViewAccumulator
        Accumulator configured for diagnostics details.
    """
    return StructLargeListViewAccumulator.with_fields(_DETAIL_FIELDS)


@dataclass
class _DiagBuffers:
    """Buffer accumulator for diagnostics rows."""

    paths: list[object | None] = field(default_factory=list)
    file_ids: list[object | None] = field(default_factory=list)
    bstarts: list[int | None] = field(default_factory=list)
    bends: list[int | None] = field(default_factory=list)
    severities: list[str] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    diag_sources: list[str] = field(default_factory=list)
    codes: list[object | None] = field(default_factory=list)
    line_bases: list[int | None] = field(default_factory=list)
    col_units: list[str | None] = field(default_factory=list)
    end_exclusives: list[bool | None] = field(default_factory=list)
    details_acc: StructLargeListViewAccumulator = field(default_factory=_detail_accumulator)

    def append(self, row: _DiagRow) -> None:
        """Append a diagnostics row into buffers.

        Parameters
        ----------
        row
            Diagnostics row to append.
        """
        self.paths.append(row.path)
        self.file_ids.append(row.file_id)
        self.bstarts.append(row.bstart)
        self.bends.append(row.bend)
        self.severities.append(row.severity)
        self.messages.append(row.message)
        self.diag_sources.append(row.diag_source)
        self.codes.append(row.code)
        self.line_bases.append(row.line_base)
        self.col_units.append(row.col_unit)
        self.end_exclusives.append(row.end_exclusive)
        self._append_detail(row.detail)

    def _append_detail(self, detail: _DiagDetail | None) -> None:
        """Append detail rows into the detail accumulator.

        Parameters
        ----------
        detail
            Detail payload to append.
        """
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
        """Build the details array from accumulated buffers.

        Returns
        -------
        ArrayLike
            Diagnostics detail array.
        """
        return DIAG_DETAIL_SPEC.builder(self)


@dataclass(frozen=True)
class _ScipDiagInput:
    """Row payload for SCIP diagnostics input."""

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
    """Build a details array for diagnostics buffers.

    Parameters
    ----------
    buffers
        Diagnostics buffers to convert.

    Returns
    -------
    ArrayLike
        Diagnostics detail array.
    """
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
    """Normalize tag values for diagnostics details.

    Parameters
    ----------
    value
        Tag value payload.

    Returns
    -------
    list[str]
        Normalized tag strings.
    """
    if isinstance(value, list):
        return [str(tag) for tag in value if tag]
    return []


def _col_unit_from_posenc(value: int) -> str:
    """Return the column unit label for a position encoding.

    Returns
    -------
    str
        Column unit label for the encoding.
    """
    if value == ENC_UTF8:
        return "utf8"
    if value == ENC_UTF16:
        return "utf16"
    return "utf32"


def _cst_parse_error_row(
    repo_text_index: RepoTextIndex,
    values: tuple[object | None, ...],
) -> _DiagRow | None:
    """Build a diagnostics row from a LibCST parse error.

    Parameters
    ----------
    repo_text_index
        Repository text index for path/offset resolution.
    values
        Raw parse error values.

    Returns
    -------
    _DiagRow | None
        Diagnostics row when offsets are resolvable.
    """
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
        diag_source="libcst",
        code=None,
        detail=detail,
        line_base=1,
        col_unit=_col_unit_from_posenc(DEFAULT_POSITION_ENCODING),
        end_exclusive=True,
    )


def _cst_parse_error_table(
    repo_text_index: RepoTextIndex,
    cst_parse_errors: TableLike,
) -> TableLike:
    """Build a diagnostics table from LibCST parse errors.

    Parameters
    ----------
    repo_text_index
        Repository text index for path/offset resolution.
    cst_parse_errors
        Table of LibCST parse errors.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
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
    diag_source_arr = pa.array(buffers.diag_sources, type=pa.string())
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
        "diag_source": diag_source_arr,
        "code": pa.array(buffers.codes, type=pa.string()),
        "details": details,
        "line_base": pa.array(buffers.line_bases, type=pa.int32()),
        "col_unit": pa.array(buffers.col_units, type=pa.string()),
        "end_exclusive": pa.array(buffers.end_exclusives, type=pa.bool_()),
    }
    table = table_from_arrays(_DIAG_BASE_SCHEMA, columns=columns, num_rows=len(buffers.paths))
    return normalize_dictionaries(table)


def _scip_severity(value: object | None) -> str:
    """Normalize SCIP severity values to standardized labels.

    Parameters
    ----------
    value
        Raw severity value.

    Returns
    -------
    str
        Normalized severity label.
    """
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
    """Map numeric SCIP severity to a label.

    Parameters
    ----------
    value
        Numeric SCIP severity value.

    Returns
    -------
    str
        Severity label.
    """
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
    """Build a diagnostics table from SCIP diagnostics.

    Parameters
    ----------
    repo_text_index
        Repository text index for path/offset resolution.
    scip_diagnostics
        Table of SCIP diagnostics.
    scip_documents
        Optional table of SCIP documents for encodings.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
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
    diag_source_arr = pa.array(buffers.diag_sources, type=pa.string())
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
        "diag_source": diag_source_arr,
        "code": pa.array(buffers.codes, type=pa.string()),
        "details": details,
        "line_base": pa.array(buffers.line_bases, type=pa.int32()),
        "col_unit": pa.array(buffers.col_units, type=pa.string()),
        "end_exclusive": pa.array(buffers.end_exclusives, type=pa.bool_()),
    }
    table = table_from_arrays(_DIAG_BASE_SCHEMA, columns=columns, num_rows=len(buffers.paths))
    return normalize_dictionaries(table)


def _scip_doc_encodings(scip_documents: TableLike | None) -> dict[str, int]:
    """Collect position encodings for SCIP documents.

    Parameters
    ----------
    scip_documents
        Optional table of SCIP documents.

    Returns
    -------
    dict[str, int]
        Mapping of document ID to position encoding.
    """
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
    """Build a diagnostics row from SCIP diagnostics input.

    Parameters
    ----------
    ctx
        SCIP diagnostics context.
    values
        SCIP diagnostics input payload.

    Returns
    -------
    _DiagRow | None
        Diagnostics row when offsets are resolvable.
    """
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
        diag_source="scip",
        code=values.code,
        detail=detail,
        line_base=0,
        col_unit=_col_unit_from_posenc(enc),
        end_exclusive=True,
    )


def _ts_diag_plan(
    ts_table: TableLike,
    *,
    severity: str,
    message: str,
    ctx: ExecutionContext,
) -> Plan:
    """Build a diagnostics plan for tree-sitter diagnostics.

    Parameters
    ----------
    ts_table
        Tree-sitter diagnostics table.
    severity
        Severity label to apply.
    message
        Message to apply.
    ctx
        Execution context.

    Returns
    -------
    Plan
        Plan emitting diagnostics rows.
    """
    plan = Plan.table_source(ts_table)
    plan = _alias_ts_span_columns(plan, ctx=ctx)
    available = set(plan.schema(ctx=ctx).names)
    details_scalar = pa.scalar([], type=DIAG_DETAILS_TYPE)
    defaults: dict[str, ComputeExpression] = {
        "severity": pc.scalar(severity),
        "message": pc.scalar(message),
        "diag_source": pc.scalar("treesitter"),
        "code": pc.scalar(pa.scalar(None, type=pa.string())),
        "details": pc.scalar(details_scalar),
        "line_base": pc.scalar(pa.scalar(None, type=pa.int32())),
        "col_unit": pc.scalar(pa.scalar(None, type=pa.string())),
        "end_exclusive": pc.scalar(pa.scalar(None, type=pa.bool_())),
    }
    exprs: list[ComputeExpression] = []
    names = list(_DIAG_BASE_COLUMNS)
    for name in names:
        if name in defaults:
            exprs.append(defaults[name])
            continue
        dtype = DIAG_SCHEMA.field(name).type
        exprs.append(column_or_null_expr(name, dtype, available=available))
    return plan.project(exprs, names, ctx=ctx)


def _alias_ts_span_columns(plan: Plan, *, ctx: ExecutionContext) -> Plan:
    """Alias tree-sitter span columns to standard names.

    Parameters
    ----------
    plan
        Plan to rename columns in.
    ctx
        Execution context.

    Returns
    -------
    Plan
        Updated plan with aliased columns.
    """
    available = set(plan.schema(ctx=ctx).names)
    mapping: dict[str, str] = {}
    if "bstart" not in available and "start_byte" in available:
        mapping["start_byte"] = "bstart"
    if "bend" not in available and "end_byte" in available:
        mapping["end_byte"] = "bend"
    if not mapping:
        return plan
    return plan.rename_columns(mapping, ctx=ctx)


def _diagnostics_plan(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> Plan:
    """Build the diagnostics plan from available sources.

    Parameters
    ----------
    repo_text_index
        Repository text index for path/offset resolution.
    sources
        Diagnostics input sources.
    ctx
        Execution context.

    Returns
    -------
    Plan
        Plan producing diagnostics rows.
    """
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
