"""Normalize diagnostics from extraction layers."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.empty import empty_table
from arrowdsl.ids import hash64_from_arrays
from normalize.ids import stable_id
from normalize.spans import (
    DEFAULT_POSITION_ENCODING,
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    FileTextIndex,
    RepoTextIndex,
    line_char_to_byte_offset,
)

SCHEMA_VERSION = 1

SCIP_SEVERITY_ERROR = 1
SCIP_SEVERITY_WARNING = 2
SCIP_SEVERITY_INFO = 3


@dataclass(frozen=True)
class DiagnosticsSources:
    """Source tables for diagnostics aggregation."""

    cst_parse_errors: pa.Table | None
    ts_errors: pa.Table | None
    ts_missing: pa.Table | None
    scip_diagnostics: pa.Table | None
    scip_documents: pa.Table | None = None


@dataclass(frozen=True)
class ScipDiagContext:
    """Context for SCIP diagnostics conversion."""

    repo_text_index: RepoTextIndex
    doc_enc: dict[str, int]


DIAG_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("diag_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        ("severity", pa.string()),
        ("message", pa.string()),
        ("diag_source", pa.string()),
        ("code", pa.string()),
        ("details_json", pa.string()),
    ]
)


def _column_or_null(
    table: pa.Table,
    col: str,
    dtype: pa.DataType,
) -> pa.Array | pa.ChunkedArray:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


def _prefixed_hash64(
    prefix: str,
    arrays: list[pa.Array | pa.ChunkedArray],
) -> pa.Array | pa.ChunkedArray:
    hashed = hash64_from_arrays(arrays, prefix=prefix)
    hashed_str = pc.cast(hashed, pa.string())
    return pc.binary_join_element_wise(pa.scalar(prefix), hashed_str, ":")


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


def _diag_id(
    *,
    path: object | None,
    bstart: object | None,
    bend: object | None,
    source: str,
    message: str | None,
) -> str:
    return stable_id("diag", str(path), str(bstart), str(bend), source, str(message))


def _cst_parse_error_rows(
    repo_text_index: RepoTextIndex,
    cst_parse_errors: pa.Table,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in cst_parse_errors.to_pylist():
        path = row.get("path")
        file_id = row.get("file_id")
        fidx = _file_index(repo_text_index, file_id=file_id, path=path)
        raw_line = row.get("raw_line")
        raw_column = row.get("raw_column")
        if not isinstance(raw_line, int) or not isinstance(raw_column, int) or fidx is None:
            continue
        line0 = raw_line - 1 if raw_line > 0 else 0
        bstart = line_char_to_byte_offset(
            fidx,
            line0,
            raw_column,
            DEFAULT_POSITION_ENCODING,
        )
        bend = bstart
        message = row.get("message")
        msg = message if isinstance(message, str) else "LibCST parse error"
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "diag_id": _diag_id(
                    path=path,
                    bstart=bstart,
                    bend=bend,
                    source="libcst",
                    message=msg,
                ),
                "file_id": file_id,
                "path": path,
                "bstart": bstart,
                "bend": bend,
                "severity": "ERROR",
                "message": msg,
                "diag_source": "libcst",
                "code": None,
                "details_json": json.dumps(
                    {"error_type": row.get("error_type")},
                    ensure_ascii=False,
                ),
            }
        )
    return rows


def _ts_diag_table(ts_table: pa.Table, *, severity: str, message: str) -> pa.Table:
    n = ts_table.num_rows
    if n == 0:
        return empty_table(DIAG_SCHEMA)
    path = _column_or_null(ts_table, "path", pa.string())
    bstart = _column_or_null(ts_table, "start_byte", pa.int64())
    bend = _column_or_null(ts_table, "end_byte", pa.int64())
    source_arr = pa.array(["treesitter"] * n, type=pa.string())
    message_arr = pa.array([message] * n, type=pa.string())
    diag_id = _prefixed_hash64("diag", [path, bstart, bend, source_arr, message_arr])
    return pa.Table.from_arrays(
        [
            pa.array([SCHEMA_VERSION] * n, type=pa.int32()),
            diag_id,
            _column_or_null(ts_table, "file_id", pa.string()),
            path,
            bstart,
            bend,
            pa.array([severity] * n, type=pa.string()),
            message_arr,
            source_arr,
            pa.array([None] * n, type=pa.string()),
            pa.array([None] * n, type=pa.string()),
        ],
        schema=DIAG_SCHEMA,
    )


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
        if raw in {"ERROR", "WARNING", "INFO"}:
            return raw
    return "ERROR"


def _scip_severity_value(value: int) -> str:
    mapping = {
        SCIP_SEVERITY_ERROR: "ERROR",
        SCIP_SEVERITY_WARNING: "WARNING",
        SCIP_SEVERITY_INFO: "INFO",
    }
    return mapping.get(value, "INFO")


def _scip_diag_rows(
    repo_text_index: RepoTextIndex,
    scip_diagnostics: pa.Table,
    scip_documents: pa.Table | None,
) -> list[dict[str, object]]:
    ctx = ScipDiagContext(
        repo_text_index=repo_text_index,
        doc_enc=_scip_doc_encodings(scip_documents),
    )
    rows: list[dict[str, object]] = []
    for row in scip_diagnostics.to_pylist():
        diag_row = _scip_diag_row(row, ctx)
        if diag_row is not None:
            rows.append(diag_row)
    return rows


def _scip_doc_encodings(scip_documents: pa.Table | None) -> dict[str, int]:
    doc_enc: dict[str, int] = {}
    if scip_documents is None or scip_documents.num_rows == 0:
        return doc_enc
    for row in scip_documents.to_pylist():
        doc_id = row.get("document_id")
        enc = row.get("position_encoding")
        if isinstance(doc_id, str):
            doc_enc[doc_id] = _scip_encoding(enc)
    return doc_enc


def _scip_diag_row(row: dict[str, object], ctx: ScipDiagContext) -> dict[str, object] | None:
    path = row.get("path")
    file_id = row.get("file_id")
    fidx = _file_index(ctx.repo_text_index, file_id=file_id, path=path)
    if fidx is None:
        return None
    start_line = row.get("start_line")
    start_char = row.get("start_char")
    end_line = row.get("end_line")
    end_char = row.get("end_char")
    if not isinstance(start_line, int) or not isinstance(start_char, int):
        return None
    if not isinstance(end_line, int) or not isinstance(end_char, int):
        return None
    doc_id = row.get("document_id")
    enc = (
        ctx.doc_enc.get(doc_id, DEFAULT_POSITION_ENCODING)
        if isinstance(doc_id, str)
        else DEFAULT_POSITION_ENCODING
    )
    bstart = line_char_to_byte_offset(fidx, start_line, start_char, enc)
    bend = line_char_to_byte_offset(fidx, end_line, end_char, enc)
    message = row.get("message")
    msg = message if isinstance(message, str) else "SCIP diagnostic"
    sev = _scip_severity(row.get("severity"))
    return {
        "schema_version": SCHEMA_VERSION,
        "diag_id": _diag_id(
            path=path,
            bstart=bstart,
            bend=bend,
            source="scip",
            message=msg,
        ),
        "file_id": file_id,
        "path": path,
        "bstart": bstart,
        "bend": bend,
        "severity": sev,
        "message": msg,
        "diag_source": "scip",
        "code": row.get("code"),
        "details_json": json.dumps(
            {"source": row.get("source"), "tags": row.get("tags")},
            ensure_ascii=False,
        ),
    }


def collect_diags(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
) -> pa.Table:
    """Aggregate diagnostics into a single normalized table.

    Parameters
    ----------
    repo_text_index:
        Repo text index for line/column to byte offsets.
    sources:
        Diagnostics source tables.

    Returns
    -------
    pa.Table
        Normalized diagnostics table.
    """
    parts: list[pa.Table] = []
    if sources.cst_parse_errors is not None and sources.cst_parse_errors.num_rows:
        rows = _cst_parse_error_rows(repo_text_index, sources.cst_parse_errors)
        if rows:
            parts.append(pa.Table.from_pylist(rows, schema=DIAG_SCHEMA))
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
        rows = _scip_diag_rows(repo_text_index, sources.scip_diagnostics, sources.scip_documents)
        if rows:
            parts.append(pa.Table.from_pylist(rows, schema=DIAG_SCHEMA))

    if not parts:
        return empty_table(DIAG_SCHEMA)
    return pa.concat_tables(parts, promote=True)
