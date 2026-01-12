"""Normalize diagnostics from extraction layers."""

from __future__ import annotations

from dataclasses import dataclass, field

import arrowdsl.pyarrow_core as pa
from arrowdsl.column_ops import const_array
from arrowdsl.empty import empty_table
from arrowdsl.encoding import EncodingSpec, encode_columns
from arrowdsl.ids import prefixed_hash_id
from arrowdsl.iter import iter_arrays
from arrowdsl.nested import build_list_array, build_struct_array
from arrowdsl.pyarrow_protocols import ArrayLike, ChunkedArrayLike, DataTypeLike, TableLike
from normalize.spans import (
    DEFAULT_POSITION_ENCODING,
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    FileTextIndex,
    RepoTextIndex,
    line_char_to_byte_offset,
)
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import DICT_STRING, file_identity_bundle, span_bundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY

SCHEMA_VERSION = 1

SCIP_SEVERITY_ERROR = 1
SCIP_SEVERITY_WARNING = 2
SCIP_SEVERITY_INFO = 3
SCIP_SEVERITY_HINT = 4


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
        tags = build_list_array(
            pa.array(self.detail_tag_offsets, type=pa.int32()),
            pa.array(self.detail_tag_values, type=pa.string()),
        )
        detail_struct = build_struct_array(
            {
                "detail_kind": pa.array(self.detail_kinds, type=pa.string()),
                "error_type": pa.array(self.detail_error_types, type=pa.string()),
                "source": pa.array(self.detail_sources, type=pa.string()),
                "tags": tags,
            }
        )
        return build_list_array(
            pa.array(self.detail_offsets, type=pa.int32()),
            detail_struct,
        )


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


DIAG_DETAIL_STRUCT = pa.struct(
    [
        ("detail_kind", pa.string()),
        ("error_type", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.string())),
    ]
)

DIAG_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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

DIAG_ENCODING_SPECS: tuple[EncodingSpec, ...] = (
    EncodingSpec(column="severity"),
    EncodingSpec(column="diag_source"),
)

DIAG_SCHEMA = DIAG_SPEC.to_arrow_schema()


def _column_or_null(
    table: TableLike,
    col: str,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


def _prefixed_hash64(
    prefix: str,
    arrays: list[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    return prefixed_hash_id(arrays, prefix=prefix)


def _detail_tags(value: object | None) -> list[str]:
    if isinstance(value, list):
        return [str(tag) for tag in value if tag]
    return []


def _empty_details_list(num_rows: int) -> ArrayLike:
    offsets = const_array(num_rows + 1, 0, dtype=pa.int32())
    tags = build_list_array(
        const_array(1, 0, dtype=pa.int32()),
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
    return build_list_array(offsets, detail_struct)


def _cst_parse_error_row(
    repo_text_index: RepoTextIndex,
    values: tuple[object | None, ...],
) -> _DiagRow | None:
    path, file_id, raw_line, raw_column, message, error_type = values
    fidx = _file_index(repo_text_index, file_id=file_id, path=path)
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


def _file_index(
    repo_text_index: RepoTextIndex,
    *,
    file_id: object | None,
    path: object | None,
) -> FileTextIndex | None:
    if isinstance(file_id, str):
        return repo_text_index.by_file_id.get(file_id)
    if isinstance(path, str):
        return repo_text_index.by_path.get(path)
    return None


def _cst_parse_error_table(
    repo_text_index: RepoTextIndex,
    cst_parse_errors: TableLike,
) -> TableLike:
    buffers = _DiagBuffers()
    arrays = [
        _column_or_null(cst_parse_errors, "path", pa.string()),
        _column_or_null(cst_parse_errors, "file_id", pa.string()),
        _column_or_null(cst_parse_errors, "raw_line", pa.int64()),
        _column_or_null(cst_parse_errors, "raw_column", pa.int64()),
        _column_or_null(cst_parse_errors, "message", pa.string()),
        _column_or_null(cst_parse_errors, "error_type", pa.string()),
    ]
    for values in iter_arrays(arrays):
        row = _cst_parse_error_row(repo_text_index, values)
        if row is None:
            continue
        buffers.append(row)

    if not buffers.paths:
        return empty_table(DIAG_SCHEMA)

    path_arr = pa.array(buffers.paths, type=pa.string())
    bstart_arr = pa.array(buffers.bstarts, type=pa.int64())
    bend_arr = pa.array(buffers.bends, type=pa.int64())
    source_arr = pa.array(buffers.sources, type=pa.string())
    message_arr = pa.array(buffers.messages, type=pa.string())
    severity_arr = pa.array(buffers.severities, type=pa.string())
    diag_id = _prefixed_hash64("diag", [path_arr, bstart_arr, bend_arr, source_arr, message_arr])
    details = buffers.details_array()
    table = pa.Table.from_arrays(
        [
            pa.array(buffers.file_ids, type=pa.string()),
            path_arr,
            bstart_arr,
            bend_arr,
            diag_id,
            severity_arr,
            message_arr,
            source_arr,
            pa.array(buffers.codes, type=pa.string()),
            details,
        ],
        names=DIAG_SCHEMA.names,
    )
    table = encode_columns(table, specs=DIAG_ENCODING_SPECS)
    return table.cast(DIAG_SCHEMA)


def _ts_diag_table(ts_table: TableLike, *, severity: str, message: str) -> TableLike:
    n = ts_table.num_rows
    if n == 0:
        return empty_table(DIAG_SCHEMA)
    path = _column_or_null(ts_table, "path", pa.string())
    bstart = _column_or_null(ts_table, "start_byte", pa.int64())
    bend = _column_or_null(ts_table, "end_byte", pa.int64())
    source_arr = const_array(n, "treesitter", dtype=pa.string())
    message_arr = const_array(n, message, dtype=pa.string())
    diag_id = _prefixed_hash64("diag", [path, bstart, bend, source_arr, message_arr])
    details = _empty_details_list(n)
    severity_arr = const_array(n, severity, dtype=pa.string())
    table = pa.Table.from_arrays(
        [
            _column_or_null(ts_table, "file_id", pa.string()),
            path,
            bstart,
            bend,
            diag_id,
            severity_arr,
            message_arr,
            source_arr,
            const_array(n, None, dtype=pa.string()),
            details,
        ],
        names=DIAG_SCHEMA.names,
    )
    table = encode_columns(table, specs=DIAG_ENCODING_SPECS)
    return table.cast(DIAG_SCHEMA)


def _scip_encoding(value: object | None) -> int:
    if isinstance(value, str):
        raw = value.strip().upper()
        if raw == "UTF8":
            return ENC_UTF8
        if raw == "UTF16":
            return ENC_UTF16
        if raw == "UTF32":
            return ENC_UTF32
    return DEFAULT_POSITION_ENCODING


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
        _column_or_null(scip_diagnostics, "document_id", pa.string()),
        _column_or_null(scip_diagnostics, "path", pa.string()),
        _column_or_null(scip_diagnostics, "file_id", pa.string()),
        _column_or_null(scip_diagnostics, "start_line", pa.int64()),
        _column_or_null(scip_diagnostics, "start_char", pa.int64()),
        _column_or_null(scip_diagnostics, "end_line", pa.int64()),
        _column_or_null(scip_diagnostics, "end_char", pa.int64()),
        _column_or_null(scip_diagnostics, "message", pa.string()),
        _column_or_null(scip_diagnostics, "severity", pa.string()),
        _column_or_null(scip_diagnostics, "code", pa.string()),
        _column_or_null(scip_diagnostics, "source", pa.string()),
        _column_or_null(scip_diagnostics, "tags", pa.list_(pa.string())),
    ]
    for values in iter_arrays(arrays):
        diag_row = _scip_diag_row(ctx, _ScipDiagInput(*values))
        if diag_row is None:
            continue
        buffers.append(diag_row)

    if not buffers.paths:
        return empty_table(DIAG_SCHEMA)

    path_arr = pa.array(buffers.paths, type=pa.string())
    bstart_arr = pa.array(buffers.bstarts, type=pa.int64())
    bend_arr = pa.array(buffers.bends, type=pa.int64())
    source_arr = pa.array(buffers.sources, type=pa.string())
    message_arr = pa.array(buffers.messages, type=pa.string())
    severity_arr = pa.array(buffers.severities, type=pa.string())
    diag_id = _prefixed_hash64("diag", [path_arr, bstart_arr, bend_arr, source_arr, message_arr])
    details = buffers.details_array()
    table = pa.Table.from_arrays(
        [
            pa.array(buffers.file_ids, type=pa.string()),
            path_arr,
            bstart_arr,
            bend_arr,
            diag_id,
            severity_arr,
            message_arr,
            source_arr,
            pa.array(buffers.codes, type=pa.string()),
            details,
        ],
        names=DIAG_SCHEMA.names,
    )
    table = encode_columns(table, specs=DIAG_ENCODING_SPECS)
    return table.cast(DIAG_SCHEMA)


def _scip_doc_encodings(scip_documents: TableLike | None) -> dict[str, int]:
    doc_enc: dict[str, int] = {}
    if scip_documents is None or scip_documents.num_rows == 0:
        return doc_enc
    arrays = [
        _column_or_null(scip_documents, "document_id", pa.string()),
        _column_or_null(scip_documents, "position_encoding", pa.string()),
    ]
    for doc_id, enc in iter_arrays(arrays):
        if isinstance(doc_id, str):
            doc_enc[doc_id] = _scip_encoding(enc)
    return doc_enc


def _scip_diag_row(ctx: ScipDiagContext, values: _ScipDiagInput) -> _DiagRow | None:
    fidx = _file_index(ctx.repo_text_index, file_id=values.file_id, path=values.path)
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


def collect_diags(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
) -> TableLike:
    """Aggregate diagnostics into a single normalized table.

    Parameters
    ----------
    repo_text_index:
        Repo text index for line/column to byte offsets.
    sources:
        Diagnostics source tables.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    parts: list[TableLike] = []
    if sources.cst_parse_errors is not None and sources.cst_parse_errors.num_rows:
        table = _cst_parse_error_table(repo_text_index, sources.cst_parse_errors)
        if table.num_rows:
            parts.append(table)
    if sources.ts_errors is not None and sources.ts_errors.num_rows:
        parts.append(
            _ts_diag_table(
                sources.ts_errors,
                severity="ERROR",
                message="tree-sitter error node",
            )
        )
    if sources.ts_missing is not None and sources.ts_missing.num_rows:
        parts.append(
            _ts_diag_table(
                sources.ts_missing,
                severity="WARNING",
                message="tree-sitter missing node",
            )
        )
    if sources.scip_diagnostics is not None and sources.scip_diagnostics.num_rows:
        table = _scip_diag_table(repo_text_index, sources.scip_diagnostics, sources.scip_documents)
        if table.num_rows:
            parts.append(table)

    if not parts:
        return empty_table(DIAG_SCHEMA)
    return pa.concat_tables(parts, promote=True)
