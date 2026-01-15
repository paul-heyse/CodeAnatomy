"""Normalize span coordinates to UTF-8 byte offsets."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

from arrowdsl.compute.filters import position_encoding_array
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_arrays
from arrowdsl.core.interop import ArrayLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.build import column_or_null, set_or_append_column
from normalize.registry_specs import dataset_query
from normalize.runner import PostFn
from normalize.span_pipeline import (
    SpanOutputColumns,
    append_alias_cols,
    append_span_columns,
    span_error_table,
)
from normalize.text_index import (
    DEFAULT_POSITION_ENCODING,
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    FileTextIndex,
    RepoTextIndex,
    row_value_int,
)
from normalize.utils import PlanSource, add_span_id_column, plan_source
from schema_spec.specs import scip_range_bundle

type RowValue = object | None

SCIP_RANGE_FIELDS = tuple(field.name for field in scip_range_bundle(include_len=True).fields)
SCIP_ENC_RANGE_FIELDS = tuple(
    field.name for field in scip_range_bundle(prefix="enc_", include_len=True).fields
)

SPAN_ERRORS_NAME = "span_errors_v1"


@dataclass(frozen=True)
class AstSpanColumns:
    """Column names used for AST span normalization."""

    file_id: str = "file_id"
    lineno: str = "lineno"
    col: str = "col_offset"
    end_lineno: str = "end_lineno"
    end_col: str = "end_col_offset"
    line_base: str = "line_base"
    col_unit: str = "col_unit"
    end_exclusive: str = "end_exclusive"
    out_bstart: str = "bstart"
    out_bend: str = "bend"
    out_ok: str = "span_ok"


@dataclass(frozen=True)
class ScipOccurrenceColumns:
    """Column names used for SCIP occurrence span normalization."""

    document_id: str = "document_id"
    doc_posenc: str = "position_encoding"
    path: str = "path"
    start_line: str = SCIP_RANGE_FIELDS[0]
    start_char: str = SCIP_RANGE_FIELDS[1]
    end_line: str = SCIP_RANGE_FIELDS[2]
    end_char: str = SCIP_RANGE_FIELDS[3]
    range_len: str = SCIP_RANGE_FIELDS[4]
    enc_start_line: str = SCIP_ENC_RANGE_FIELDS[0]
    enc_start_char: str = SCIP_ENC_RANGE_FIELDS[1]
    enc_end_line: str = SCIP_ENC_RANGE_FIELDS[2]
    enc_end_char: str = SCIP_ENC_RANGE_FIELDS[3]
    enc_range_len: str = SCIP_ENC_RANGE_FIELDS[4]
    line_base: str = "line_base"
    col_unit: str = "col_unit"
    end_exclusive: str = "end_exclusive"
    out_bstart: str = "bstart"
    out_bend: str = "bend"
    out_enc_bstart: str = "enc_bstart"
    out_enc_bend: str = "enc_bend"
    out_ok: str = "span_ok"


@dataclass(frozen=True)
class OccurrenceRow:
    """Raw occurrence row values for span conversion."""

    document_id: RowValue
    path: RowValue
    start_line: RowValue
    start_char: RowValue
    end_line: RowValue
    end_char: RowValue
    range_len: RowValue
    enc_start_line: RowValue
    enc_start_char: RowValue
    enc_end_line: RowValue
    enc_end_char: RowValue
    enc_range_len: RowValue
    line_base: RowValue
    col_unit: RowValue
    end_exclusive: RowValue


@dataclass(frozen=True)
class OccurrenceSpanResult:
    """Computed byte-span results for a single occurrence."""

    bstart: int | None
    bend: int | None
    enc_bstart: int | None
    enc_bend: int | None
    ok: bool
    error: dict[str, str] | None


@dataclass(frozen=True)
class SpanCoordPolicy:
    """Normalization policy for span coordinate inputs."""

    line_base: int
    end_exclusive: bool


@dataclass(frozen=True)
class AstSpanInput:
    """AST span input payload."""

    lineno: int | None
    col_offset: int | None
    end_lineno: int | None
    end_col: int | None
    line_base: RowValue = None
    col_unit: RowValue = None
    end_exclusive: RowValue = None


@dataclass(frozen=True)
class _OccurrenceContext:
    """Derived context for occurrence span conversion."""

    doc_id: str
    path: str
    fidx: FileTextIndex
    policy: SpanCoordPolicy
    unit_value: RowValue


def _decode_repo_row_text(
    text_value: RowValue,
    bytes_value: RowValue,
    encoding_value: RowValue,
) -> str | None:
    if isinstance(text_value, str) and text_value:
        return text_value
    if isinstance(bytes_value, (bytes, bytearray, memoryview)):
        enc = encoding_value if isinstance(encoding_value, str) else "utf-8"
        try:
            return bytes(bytes_value).decode(enc, errors="replace")
        except (LookupError, UnicodeError):
            return bytes(bytes_value).decode("utf-8", errors="replace")
    return None


def _normalized_range(
    start_line: RowValue,
    start_char: RowValue,
    end_line: RowValue,
    end_char: RowValue,
    *,
    policy: SpanCoordPolicy,
) -> tuple[int, int, int, int] | None:
    sl = row_value_int(start_line)
    sc = row_value_int(start_char)
    el = row_value_int(end_line)
    ec = row_value_int(end_char)
    if sl is None or sc is None or el is None or ec is None:
        return None
    return (
        sl - policy.line_base,
        sc,
        el - policy.line_base,
        _normalize_end_col(ec, end_exclusive=policy.end_exclusive),
    )


def _build_line_index_utf8(text: str) -> tuple[list[str], list[int]]:
    lines = text.splitlines(keepends=True)
    starts = [0]
    acc = 0
    for ln in lines:
        acc += len(ln.encode("utf-8"))
        starts.append(acc)
    return lines, starts


def build_repo_text_index(
    repo_files: TableLike, *, repo_root: str | None = None, ctx: object | None = None
) -> RepoTextIndex:
    """Build lookup structures from repo_files output.

    Expects columns:
      file_id, path, file_sha256, encoding, text/bytes

    Parameters
    ----------
    repo_files:
        Repo files table.
    repo_root:
        Optional repository root (unused).
    ctx:
        Execution context (unused).

    Returns
    -------
    RepoTextIndex
        Repository-wide text index keyed by file_id and path.
    """
    by_file_id: dict[str, FileTextIndex] = {}
    by_path: dict[str, FileTextIndex] = {}

    _ = repo_root
    _ = ctx

    arrays = [
        column_or_null(repo_files, "file_id", pa.string()),
        column_or_null(repo_files, "path", pa.string()),
        column_or_null(repo_files, "file_sha256", pa.string()),
        column_or_null(repo_files, "encoding", pa.string()),
        column_or_null(repo_files, "text", pa.string()),
        column_or_null(repo_files, "bytes", pa.binary()),
    ]
    for (
        file_id_value,
        path_value,
        file_sha256_value,
        encoding_value,
        text_value,
        bytes_value,
    ) in iter_arrays(arrays):
        if not isinstance(file_id_value, str) or not isinstance(path_value, str):
            continue

        file_id = file_id_value
        path = path_value
        file_sha256 = str(file_sha256_value) if file_sha256_value is not None else None
        encoding = encoding_value if isinstance(encoding_value, str) else None
        text = _decode_repo_row_text(text_value, bytes_value, encoding_value)
        if text is None:
            continue

        lines, starts = _build_line_index_utf8(text)
        idx = FileTextIndex(
            file_id=file_id,
            path=path,
            file_sha256=file_sha256,
            encoding=encoding,
            text=text,
            lines=lines,
            line_start_utf8=starts,
        )
        by_file_id[file_id] = idx
        by_path[path] = idx

    return RepoTextIndex(by_file_id=by_file_id, by_path=by_path)


# -----------------------------
# SCIP position encoding helpers
# -----------------------------


def code_unit_offset_to_py_index(line: str, offset: int, position_encoding: int) -> int:
    """Convert a SCIP character offset to a Python string index for a single line.

    position_encoding:
      1 UTF8 byte offsets
      2 UTF16 code unit offsets
      3 UTF32 code unit offsets (Pythonic codepoints)

    Returns
    -------
    int
        Python string index for the offset.
    """
    if offset <= 0:
        return 0

    if position_encoding == ENC_UTF32:
        # UTF32 code units ~= python str indexing by codepoint
        return min(offset, len(line))

    if position_encoding == ENC_UTF8:
        # offset is UTF-8 bytes; decode prefix to count python codepoints
        b = line.encode("utf-8")
        off = min(offset, len(b))
        return len(b[:off].decode("utf-8", errors="strict"))

    if position_encoding == ENC_UTF16:
        # offset is UTF-16 code units; 2 bytes each in LE representation
        b = line.encode("utf-16-le")
        byte_off = min(offset * 2, len(b))
        return len(b[:byte_off].decode("utf-16-le", errors="strict"))

    # unknown -> best effort
    return min(offset, len(line))


def _normalize_col_unit(value: object | None) -> str:
    if isinstance(value, int):
        return _col_unit_from_int(value)
    if not isinstance(value, str):
        return "utf32"
    text = value.strip().lower()
    if text.isdigit():
        return _col_unit_from_int(int(text))
    if "byte" in text:
        return "byte"
    for unit in ("utf8", "utf16", "utf32"):
        if unit in text:
            return unit
    return "utf32"


def _col_unit_from_int(value: int) -> str:
    if value == ENC_UTF8:
        return "utf8"
    if value == ENC_UTF16:
        return "utf16"
    if value == ENC_UTF32:
        return "utf32"
    return "utf32"


def _encoding_from_unit(unit: str) -> int:
    if unit == "utf8":
        return ENC_UTF8
    if unit == "utf16":
        return ENC_UTF16
    return ENC_UTF32


def _normalize_line_base(value: RowValue, *, default_base: int) -> int:
    base = row_value_int(value)
    if base is None:
        return default_base
    return base


def _normalize_end_exclusive(value: RowValue, *, default_exclusive: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    return default_exclusive


def _normalize_end_col(end_col: int, *, end_exclusive: bool) -> int:
    return end_col if end_exclusive else end_col + 1


def _clamp_offset(offset: int, limit: int) -> int:
    return max(0, min(offset, limit))


def _col_to_byte_offset(line: str, offset: int, col_unit: object | None) -> int | None:
    unit = _normalize_col_unit(col_unit)
    if unit == "byte":
        byte_len = len(line.encode("utf-8"))
        return _clamp_offset(offset, byte_len)
    enc = _encoding_from_unit(unit)
    py_i = code_unit_offset_to_py_index(line, int(offset), int(enc))
    py_i = _clamp_offset(py_i, len(line))
    return len(line[:py_i].encode("utf-8"))


def line_char_to_byte_offset(
    fidx: FileTextIndex,
    line0: int,
    char_off: int,
    col_unit: object | None,
) -> int | None:
    """Convert a line/char offset to a UTF-8 byte offset.

    Returns
    -------
    int | None
        Byte offset or None when out of range.
    """
    if line0 < 0 or line0 >= len(fidx.lines):
        return None
    line = fidx.lines[line0]
    byte_in_line = _col_to_byte_offset(line, int(char_off), col_unit)
    if byte_in_line is None:
        return None
    return fidx.line_start_utf8[line0] + byte_in_line


def scip_range_to_byte_span(
    fidx: FileTextIndex,
    start: tuple[int, int],
    end: tuple[int, int],
    col_unit: object | None,
) -> tuple[int | None, int | None]:
    """Convert SCIP normalized ranges into UTF-8 byte spans.

    Returns
    -------
    tuple[int | None, int | None]
        Byte-span start and end offsets.
    """
    start_line, start_char = start
    end_line, end_char = end
    bstart = line_char_to_byte_offset(fidx, start_line, start_char, col_unit)
    bend = line_char_to_byte_offset(fidx, end_line, end_char, col_unit)
    return bstart, bend


# -----------------------------
# AST span conversion
# -----------------------------


def ast_range_to_byte_span(
    fidx: FileTextIndex,
    span: AstSpanInput,
) -> tuple[int | None, int | None]:
    """Convert CPython AST coordinates to UTF-8 byte spans.

    If end coordinates are missing, we fall back to (bstart,bstart).

    Returns
    -------
    tuple[int | None, int | None]
        Byte-span start and end offsets.
    """
    if span.lineno is None or span.col_offset is None:
        return None, None

    line_base_value = _normalize_line_base(span.line_base, default_base=1)
    start_line0 = int(span.lineno) - line_base_value
    if start_line0 < 0 or start_line0 >= len(fidx.line_start_utf8) - 1:
        return None, None

    bstart = line_char_to_byte_offset(
        fidx,
        start_line0,
        int(span.col_offset),
        span.col_unit,
    )
    if bstart is None:
        return None, None

    bend = bstart
    if span.end_lineno is not None and span.end_col is not None:
        end_line0 = int(span.end_lineno) - line_base_value
        if 0 <= end_line0 < len(fidx.line_start_utf8) - 1:
            end_exclusive_value = _normalize_end_exclusive(
                span.end_exclusive, default_exclusive=True
            )
            end_col_norm = _normalize_end_col(int(span.end_col), end_exclusive=end_exclusive_value)
            candidate = line_char_to_byte_offset(
                fidx,
                end_line0,
                end_col_norm,
                span.col_unit,
            )
            if candidate is not None:
                bend = candidate
    return bstart, bend


@dataclass(frozen=True)
class AstSpanInputs:
    """Column values required for AST span conversion."""

    file_ids: ArrayLike
    linenos: ArrayLike
    col_offsets: ArrayLike
    end_linenos: ArrayLike
    end_cols: ArrayLike
    line_base: ArrayLike
    col_unit: ArrayLike
    end_exclusive: ArrayLike


def _ast_span_inputs(py_ast_nodes: TableLike, cols: AstSpanColumns) -> AstSpanInputs:
    end_linenos = column_or_null(py_ast_nodes, cols.end_lineno, pa.int64())
    end_cols = column_or_null(py_ast_nodes, cols.end_col, pa.int64())
    return AstSpanInputs(
        file_ids=column_or_null(py_ast_nodes, cols.file_id, pa.string()),
        linenos=column_or_null(py_ast_nodes, cols.lineno, pa.int64()),
        col_offsets=column_or_null(py_ast_nodes, cols.col, pa.int64()),
        end_linenos=end_linenos,
        end_cols=end_cols,
        line_base=column_or_null(py_ast_nodes, cols.line_base, pa.int32()),
        col_unit=column_or_null(py_ast_nodes, cols.col_unit, pa.string()),
        end_exclusive=column_or_null(py_ast_nodes, cols.end_exclusive, pa.bool_()),
    )


def _compute_ast_spans(
    repo_index: RepoTextIndex, inputs: AstSpanInputs
) -> tuple[list[int | None], list[int | None], list[bool]]:
    bstarts: list[int | None] = []
    bends: list[int | None] = []
    oks: list[bool] = []
    for fid, ln, co, eln, eco, line_base, col_unit, end_exclusive in iter_arrays(
        [
            inputs.file_ids,
            inputs.linenos,
            inputs.col_offsets,
            inputs.end_linenos,
            inputs.end_cols,
            inputs.line_base,
            inputs.col_unit,
            inputs.end_exclusive,
        ]
    ):
        fid_key = str(fid) if fid is not None else None
        if fid_key is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        fidx = repo_index.by_file_id.get(fid_key)
        if fidx is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        ln_int = row_value_int(ln)
        co_int = row_value_int(co)
        eln_int = row_value_int(eln)
        eco_int = row_value_int(eco)
        if ln_int is None or co_int is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        bs, be = ast_range_to_byte_span(
            fidx,
            AstSpanInput(
                lineno=ln_int,
                col_offset=co_int,
                end_lineno=eln_int,
                end_col=eco_int,
                line_base=line_base,
                col_unit=col_unit,
                end_exclusive=end_exclusive,
            ),
        )
        bstarts.append(bs)
        bends.append(be)
        oks.append(bs is not None and be is not None)
    return bstarts, bends, oks


# -----------------------------
# Table-level normalizers
# -----------------------------


def add_ast_byte_spans(
    repo_index: RepoTextIndex,
    py_ast_nodes: TableLike,
    *,
    columns: AstSpanColumns | None = None,
) -> TableLike:
    """Add (bstart, bend, span_ok) columns to AST nodes.

    This makes AST nodes joinable on byte spans (when applicable).

    Returns
    -------
    TableLike
        Table with appended span columns.
    """
    cols = columns or AstSpanColumns()
    if cols.out_bstart in py_ast_nodes.column_names and cols.out_bend in py_ast_nodes.column_names:
        return py_ast_nodes

    inputs = _ast_span_inputs(py_ast_nodes, cols)
    bstarts, bends, oks = _compute_ast_spans(repo_index, inputs)

    out = append_span_columns(
        py_ast_nodes,
        bstarts=bstarts,
        bends=bends,
        oks=oks,
        columns=SpanOutputColumns(
            bstart=cols.out_bstart,
            bend=cols.out_bend,
            ok=cols.out_ok,
        ),
    )
    return add_span_id_column(out)


def ast_span_post_step(
    repo_index: RepoTextIndex,
    *,
    columns: AstSpanColumns | None = None,
) -> PostFn:
    """Return a post step that adds AST byte spans in the kernel lane.

    Returns
    -------
    PostFn
        Post step that emits AST nodes with byte spans.
    """

    def _apply(table: TableLike, ctx: ExecutionContext) -> TableLike:
        _ = ctx
        return add_ast_byte_spans(repo_index, table, columns=columns)

    return _apply


def _span_error(document_id: str, path: str, reason: str) -> dict[str, str]:
    """Build a span error record.

    Returns
    -------
    dict[str, str]
        Span error record.
    """
    return {"document_id": document_id, "path": path, "reason": reason}


def _build_doc_posenc_map(
    scip_documents: TableLike,
    columns: ScipOccurrenceColumns,
) -> dict[str, int]:
    """Build document_id -> position encoding map.

    Returns
    -------
    dict[str, int]
        Mapping of document IDs to position encodings.
    """
    doc_posenc: dict[str, int] = {}
    doc_ids = column_or_null(scip_documents, columns.document_id, pa.string())
    enc_values = position_encoding_array(
        column_or_null(scip_documents, columns.doc_posenc, pa.string())
    )
    for did, posenc in iter_arrays([doc_ids, enc_values]):
        if did is None:
            continue
        doc_posenc[str(did)] = posenc if isinstance(posenc, int) else DEFAULT_POSITION_ENCODING
    return doc_posenc


def _build_occurrence_rows(
    scip_occurrences: TableLike,
    columns: ScipOccurrenceColumns,
) -> list[OccurrenceRow]:
    """Materialize occurrence rows for span conversion.

    Returns
    -------
    list[OccurrenceRow]
        Occurrence rows in order.
    """
    base_arrays = [
        column_or_null(scip_occurrences, columns.document_id, pa.string()),
        column_or_null(scip_occurrences, columns.path, pa.string()),
        column_or_null(scip_occurrences, columns.start_line, pa.int64()),
        column_or_null(scip_occurrences, columns.start_char, pa.int64()),
        column_or_null(scip_occurrences, columns.end_line, pa.int64()),
        column_or_null(scip_occurrences, columns.end_char, pa.int64()),
        column_or_null(scip_occurrences, columns.range_len, pa.int64()),
        column_or_null(scip_occurrences, columns.enc_start_line, pa.int64()),
        column_or_null(scip_occurrences, columns.enc_start_char, pa.int64()),
        column_or_null(scip_occurrences, columns.enc_end_line, pa.int64()),
        column_or_null(scip_occurrences, columns.enc_end_char, pa.int64()),
        column_or_null(scip_occurrences, columns.enc_range_len, pa.int64()),
        column_or_null(scip_occurrences, columns.line_base, pa.int32()),
        column_or_null(scip_occurrences, columns.col_unit, pa.string()),
        column_or_null(scip_occurrences, columns.end_exclusive, pa.bool_()),
    ]
    rows: list[OccurrenceRow] = []
    for (
        did,
        path,
        sl,
        sc,
        el,
        ec,
        rl,
        esl,
        esc,
        eel,
        eec,
        erl,
        line_base,
        col_unit,
        end_exclusive,
    ) in iter_arrays(base_arrays):
        rows.append(
            OccurrenceRow(
                document_id=did,
                path=path,
                start_line=sl,
                start_char=sc,
                end_line=el,
                end_char=ec,
                range_len=rl,
                enc_start_line=esl,
                enc_start_char=esc,
                enc_end_line=eel,
                enc_end_char=eec,
                enc_range_len=erl,
                line_base=line_base,
                col_unit=col_unit,
                end_exclusive=end_exclusive,
            )
        )
    return rows


def _compute_occurrence_span(
    repo_index: RepoTextIndex,
    doc_posenc: Mapping[str, int],
    row: OccurrenceRow,
) -> OccurrenceSpanResult:
    """Compute byte spans for a single occurrence row.

    Returns
    -------
    OccurrenceSpanResult
        Computed span results and optional error.

    Raises
    ------
    ValueError
        Raised when occurrence context resolution fails.
    """
    context, error = _resolve_occurrence_context(repo_index, doc_posenc, row)
    if error is not None:
        return error
    if context is None:
        msg = "Occurrence context resolution failed."
        raise ValueError(msg)

    range_len = row_value_int(row.range_len)
    if range_len is None or range_len <= 0:
        return OccurrenceSpanResult(
            bstart=None,
            bend=None,
            enc_bstart=None,
            enc_bend=None,
            ok=False,
            error=_span_error(context.doc_id, context.fidx.path, "invalid_range_len"),
        )

    main_range = _normalized_range(
        row.start_line,
        row.start_char,
        row.end_line,
        row.end_char,
        policy=context.policy,
    )
    if main_range is None:
        return OccurrenceSpanResult(
            bstart=None,
            bend=None,
            enc_bstart=None,
            enc_bend=None,
            ok=False,
            error=_span_error(context.doc_id, context.fidx.path, "missing_range_fields"),
        )

    bstart, bend = scip_range_to_byte_span(
        context.fidx,
        (main_range[0], main_range[1]),
        (main_range[2], main_range[3]),
        context.unit_value,
    )
    enc_bstart: int | None = None
    enc_bend: int | None = None
    enc_range_len = row_value_int(row.enc_range_len)
    if enc_range_len is not None and enc_range_len > 0:
        enc_range = _normalized_range(
            row.enc_start_line,
            row.enc_start_char,
            row.enc_end_line,
            row.enc_end_char,
            policy=context.policy,
        )
        if enc_range is not None:
            enc_bstart, enc_bend = scip_range_to_byte_span(
                context.fidx,
                (enc_range[0], enc_range[1]),
                (enc_range[2], enc_range[3]),
                context.unit_value,
            )

    ok = bstart is not None and bend is not None
    error = (
        _span_error(context.doc_id, context.fidx.path, "range_to_byte_span_failed")
        if not ok
        else None
    )

    return OccurrenceSpanResult(
        bstart=bstart,
        bend=bend,
        enc_bstart=enc_bstart,
        enc_bend=enc_bend,
        ok=ok,
        error=error,
    )


def _resolve_occurrence_context(
    repo_index: RepoTextIndex,
    doc_posenc: Mapping[str, int],
    row: OccurrenceRow,
) -> tuple[_OccurrenceContext | None, OccurrenceSpanResult | None]:
    did = str(row.document_id) if row.document_id is not None else ""
    path_value = str(row.path) if row.path is not None else ""
    posenc = doc_posenc.get(did, DEFAULT_POSITION_ENCODING)
    line_base = _normalize_line_base(row.line_base, default_base=0)
    end_exclusive = _normalize_end_exclusive(row.end_exclusive, default_exclusive=True)
    unit_value = row.col_unit if row.col_unit is not None else posenc
    fidx = repo_index.by_path.get(path_value) if path_value else None
    if fidx is None:
        return (
            None,
            OccurrenceSpanResult(
                bstart=None,
                bend=None,
                enc_bstart=None,
                enc_bend=None,
                ok=False,
                error=_span_error(did, path_value, "missing_repo_text_for_path"),
            ),
        )
    context = _OccurrenceContext(
        doc_id=did,
        path=path_value,
        fidx=fidx,
        policy=SpanCoordPolicy(line_base=line_base, end_exclusive=end_exclusive),
        unit_value=unit_value,
    )
    return context, None


def add_scip_occurrence_byte_spans(
    repo_text_index: RepoTextIndex,
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    *,
    columns: ScipOccurrenceColumns | None = None,
    ctx: object | None = None,
) -> tuple[TableLike, TableLike]:
    """Add byte spans to scip_occurrences using document position encodings.

    span_errors_table is intended for observability/debugging (invalid line/char, missing file text,
    etc.).

    Returns
    -------
    tuple[TableLike, TableLike]
        Occurrences with spans and a span errors table.
    """
    _ = ctx
    cols = columns or ScipOccurrenceColumns()
    doc_posenc = _build_doc_posenc_map(scip_documents, cols)
    rows = _build_occurrence_rows(scip_occurrences, cols)

    bstarts: list[int | None] = []
    bends: list[int | None] = []
    ebstarts: list[int | None] = []
    ebends: list[int | None] = []
    oks: list[bool] = []
    err_rows: list[dict[str, str]] = []

    for row in rows:
        res = _compute_occurrence_span(repo_text_index, doc_posenc, row)
        bstarts.append(res.bstart)
        bends.append(res.bend)
        ebstarts.append(res.enc_bstart)
        ebends.append(res.enc_bend)
        oks.append(res.ok)
        if res.error is not None:
            err_rows.append(res.error)

    out = append_span_columns(
        scip_occurrences,
        bstarts=bstarts,
        bends=bends,
        oks=oks,
        columns=SpanOutputColumns(
            bstart=cols.out_bstart,
            bend=cols.out_bend,
            ok=cols.out_ok,
        ),
    )
    out = set_or_append_column(out, cols.out_enc_bstart, pa.array(ebstarts, type=pa.int64()))
    out = set_or_append_column(out, cols.out_enc_bend, pa.array(ebends, type=pa.int64()))
    out = add_span_id_column(out)
    return out, span_error_table(err_rows)


# -----------------------------
# CST span canonicalization
# -----------------------------


def normalize_cst_callsites_spans(
    py_cst_callsites: TableLike,
    *,
    primary: Literal["callee", "call"] = "callee",
) -> TableLike:
    """Ensure callsites have canonical (bstart, bend) columns for joins.

    primary="callee" means:
      bstart/bend == callee_bstart/callee_bend (recommended for SCIP occurrence joins)

    Returns
    -------
    TableLike
        Callsites table with canonical span aliases.
    """
    if primary == "call":
        return append_alias_cols(py_cst_callsites, {"bstart": "call_bstart", "bend": "call_bend"})
    return append_alias_cols(py_cst_callsites, {"bstart": "callee_bstart", "bend": "callee_bend"})


def normalize_cst_callsites_spans_plan(
    py_cst_callsites: PlanSource,
    *,
    ctx: ExecutionContext,
    primary: Literal["callee", "call"] = "callee",
) -> Plan:
    """Plan-lane span canonicalization for callsites.

    Returns
    -------
    Plan
        Plan with canonical bstart/bend columns.
    """
    plan = plan_source(py_cst_callsites, ctx=ctx)
    if primary == "call":
        return _rename_span_columns(
            plan,
            {"call_bstart": "bstart", "call_bend": "bend"},
            ctx=ctx,
        )
    return _rename_span_columns(
        plan,
        {"callee_bstart": "bstart", "callee_bend": "bend"},
        ctx=ctx,
    )


def normalize_cst_imports_spans(
    py_cst_imports: TableLike,
    *,
    primary: Literal["alias", "stmt"] = "alias",
) -> TableLike:
    """Ensure imports have canonical (bstart, bend) columns.

    Returns
    -------
    TableLike
        Imports table with canonical span aliases.
    """
    if primary == "stmt":
        return append_alias_cols(py_cst_imports, {"bstart": "stmt_bstart", "bend": "stmt_bend"})
    return append_alias_cols(py_cst_imports, {"bstart": "alias_bstart", "bend": "alias_bend"})


def normalize_cst_imports_spans_plan(
    py_cst_imports: PlanSource,
    *,
    ctx: ExecutionContext,
    primary: Literal["alias", "stmt"] = "alias",
) -> Plan:
    """Plan-lane span canonicalization for imports.

    Returns
    -------
    Plan
        Plan with canonical bstart/bend columns.
    """
    plan = plan_source(py_cst_imports, ctx=ctx)
    if primary == "stmt":
        return _rename_span_columns(
            plan,
            {"stmt_bstart": "bstart", "stmt_bend": "bend"},
            ctx=ctx,
        )
    return _rename_span_columns(
        plan,
        {"alias_bstart": "bstart", "alias_bend": "bend"},
        ctx=ctx,
    )


def span_errors_plan(span_errors: PlanSource, *, ctx: ExecutionContext) -> Plan:
    """Build a plan-lane span error table.

    Returns
    -------
    Plan
        Plan producing span error rows.
    """
    plan = plan_source(span_errors, ctx=ctx)
    return dataset_query(SPAN_ERRORS_NAME).apply_to_plan(plan, ctx=ctx)


def normalize_cst_defs_spans(
    py_cst_defs: TableLike,
    *,
    primary: Literal["name", "def"] = "name",
) -> TableLike:
    """Ensure defs have canonical (bstart, bend) columns.

    primary="name" makes bstart/bend match the identifier token span (recommended for SCIP
    definition joins).

    Returns
    -------
    TableLike
        Definitions table with canonical span aliases.
    """
    if primary == "def":
        return append_alias_cols(py_cst_defs, {"bstart": "def_bstart", "bend": "def_bend"})
    return append_alias_cols(py_cst_defs, {"bstart": "name_bstart", "bend": "name_bend"})


def normalize_cst_defs_spans_plan(
    py_cst_defs: PlanSource,
    *,
    ctx: ExecutionContext,
    primary: Literal["name", "def"] = "name",
) -> Plan:
    """Plan-lane span canonicalization for definitions.

    Returns
    -------
    Plan
        Plan with canonical bstart/bend columns.
    """
    plan = plan_source(py_cst_defs, ctx=ctx)
    if primary == "def":
        return _rename_span_columns(
            plan,
            {"def_bstart": "bstart", "def_bend": "bend"},
            ctx=ctx,
        )
    return _rename_span_columns(
        plan,
        {"name_bstart": "bstart", "name_bend": "bend"},
        ctx=ctx,
    )


def _rename_span_columns(
    plan: Plan,
    mapping: Mapping[str, str],
    *,
    ctx: ExecutionContext,
) -> Plan:
    available = set(plan.schema(ctx=ctx).names)
    rename: dict[str, str] = {}
    for source, target in mapping.items():
        if target in available:
            continue
        if source in available:
            rename[source] = target
    if not rename:
        return plan
    return plan.rename_columns(rename, ctx=ctx)
