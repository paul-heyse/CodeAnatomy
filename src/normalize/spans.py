"""Normalize span coordinates to UTF-8 byte offsets."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa

type RowValue = object | None

ENC_UTF8 = 1
ENC_UTF16 = 2
ENC_UTF32 = 3
DEFAULT_POSITION_ENCODING = ENC_UTF32
VALID_POSITION_ENCODINGS = {ENC_UTF8, ENC_UTF16, ENC_UTF32}


@dataclass(frozen=True)
class FileTextIndex:
    """
    Per-file text index for byte-span conversion.

    Used to convert:
      - (line, char) -> byte offsets (bstart/bend)
      - AST lineno/col -> byte offsets

    All byte offsets are in UTF-8 bytes of the decoded document text (canonical coordinate system).

    Important: lines are split with keepends=True so that cumulative byte lengths reproduce
    file-length coordinates (including newlines).
    """

    file_id: str
    path: str
    file_sha256: str | None
    encoding: str | None
    text: str
    lines: list[str]  # splitlines(keepends=True)
    line_start_utf8: list[int]  # length = len(lines)+1; line_start_utf8[i] = byte start of line i


@dataclass(frozen=True)
class RepoTextIndex:
    """Repo-wide indices for fast lookup."""

    by_file_id: dict[str, FileTextIndex]
    by_path: dict[str, FileTextIndex]


@dataclass(frozen=True)
class AstSpanColumns:
    """Column names used for AST span normalization."""

    file_id: str = "file_id"
    lineno: str = "lineno"
    col: str = "col_offset"
    end_lineno: str = "end_lineno"
    end_col: str = "end_col_offset"
    out_bstart: str = "bstart"
    out_bend: str = "bend"
    out_ok: str = "span_ok"


@dataclass(frozen=True)
class ScipOccurrenceColumns:
    """Column names used for SCIP occurrence span normalization."""

    document_id: str = "document_id"
    doc_posenc: str = "position_encoding"
    path: str = "path"
    start_line: str = "start_line"
    start_char: str = "start_char"
    end_line: str = "end_line"
    end_char: str = "end_char"
    enc_start_line: str = "enc_start_line"
    enc_start_char: str = "enc_start_char"
    enc_end_line: str = "enc_end_line"
    enc_end_char: str = "enc_end_char"
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
    enc_start_line: RowValue
    enc_start_char: RowValue
    enc_end_line: RowValue
    enc_end_char: RowValue


@dataclass(frozen=True)
class OccurrenceSpanResult:
    """Computed byte-span results for a single occurrence."""

    bstart: int | None
    bend: int | None
    enc_bstart: int | None
    enc_bend: int | None
    ok: bool
    error: dict[str, str] | None


SPAN_ERROR_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("reason", pa.string()),
    ]
)


def _decode_repo_row_text(rf: Mapping[str, RowValue]) -> str | None:
    t = rf.get("text")
    if isinstance(t, str) and t:
        return t
    b = rf.get("bytes")
    if isinstance(b, (bytes, bytearray, memoryview)):
        enc_value = rf.get("encoding")
        enc = enc_value if isinstance(enc_value, str) else "utf-8"
        try:
            return bytes(b).decode(enc, errors="replace")
        except (LookupError, UnicodeError):
            return bytes(b).decode("utf-8", errors="replace")
    return None


def _row_value_int(value: RowValue) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        stripped = value.strip()
        return int(stripped) if stripped.isdigit() else None
    return None


def _normalized_range(
    start_line: RowValue,
    start_char: RowValue,
    end_line: RowValue,
    end_char: RowValue,
) -> tuple[int, int, int, int] | None:
    sl = _row_value_int(start_line)
    sc = _row_value_int(start_char)
    el = _row_value_int(end_line)
    ec = _row_value_int(end_char)
    if sl is None or sc is None or el is None or ec is None:
        return None
    return sl, sc, el, ec


def _none_list(n: int) -> list[RowValue]:
    return [None] * n


def _build_line_index_utf8(text: str) -> tuple[list[str], list[int]]:
    lines = text.splitlines(keepends=True)
    starts = [0]
    acc = 0
    for ln in lines:
        acc += len(ln.encode("utf-8"))
        starts.append(acc)
    return lines, starts


def build_repo_text_index(
    repo_files: pa.Table, *, repo_root: str | None = None, ctx: object | None = None
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

    for rf in repo_files.to_pylist():
        file_id_value = rf.get("file_id")
        path_value = rf.get("path")
        if not isinstance(file_id_value, str) or not isinstance(path_value, str):
            continue

        file_id = file_id_value
        path = path_value

        file_sha256_value = rf.get("file_sha256")
        file_sha256 = str(file_sha256_value) if file_sha256_value is not None else None
        encoding_value = rf.get("encoding")
        encoding = encoding_value if isinstance(encoding_value, str) else None

        text = _decode_repo_row_text(rf)
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


def normalize_position_encoding(value: RowValue) -> int:
    """Normalize position encoding values to SCIP enum integers.

    Accepts:
      - int enums (1 UTF8 bytes, 2 UTF16 code units, 3 UTF32 codepoints)
      - strings like "POSITION_ENCODING_UTF16", "UTF16", "2", etc.

    Returns
    -------
    int
        Normalized encoding enum (defaults to 3 for Python/scip-python).
    """
    encoding = DEFAULT_POSITION_ENCODING
    if value is None:
        return encoding
    if isinstance(value, int):
        return value if value in VALID_POSITION_ENCODINGS else encoding
    if isinstance(value, str):
        s = value.strip().upper()
        if s.isdigit():
            v = int(s)
            return v if v in VALID_POSITION_ENCODINGS else encoding
        if "UTF8" in s:
            encoding = ENC_UTF8
        elif "UTF16" in s:
            encoding = ENC_UTF16
        elif "UTF32" in s:
            encoding = ENC_UTF32
    return encoding


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


def line_char_to_byte_offset(
    fidx: FileTextIndex,
    line0: int,
    char_off: int,
    position_encoding: int,
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
    py_i = code_unit_offset_to_py_index(line, int(char_off), int(position_encoding))
    # Clamp to line length to avoid slicing errors
    py_i = max(0, min(py_i, len(line)))
    byte_in_line = len(line[:py_i].encode("utf-8"))
    return fidx.line_start_utf8[line0] + byte_in_line


def scip_range_to_byte_span(
    fidx: FileTextIndex,
    start: tuple[int, int],
    end: tuple[int, int],
    position_encoding: int,
) -> tuple[int | None, int | None]:
    """Convert SCIP normalized ranges into UTF-8 byte spans.

    Returns
    -------
    tuple[int | None, int | None]
        Byte-span start and end offsets.
    """
    start_line, start_char = start
    end_line, end_char = end
    bstart = line_char_to_byte_offset(fidx, start_line, start_char, position_encoding)
    bend = line_char_to_byte_offset(fidx, end_line, end_char, position_encoding)
    return bstart, bend


# -----------------------------
# AST span conversion
# -----------------------------


def ast_range_to_byte_span(
    fidx: FileTextIndex,
    lineno_1: int | None,
    col_utf8_bytes: int | None,
    end_lineno_1: int | None,
    end_col_utf8_bytes: int | None,
) -> tuple[int | None, int | None]:
    """Convert CPython AST coordinates to UTF-8 byte spans.

    If end coordinates are missing, we fall back to (bstart,bstart).

    Returns
    -------
    tuple[int | None, int | None]
        Byte-span start and end offsets.
    """
    if lineno_1 is None or col_utf8_bytes is None:
        return None, None

    li = int(lineno_1) - 1
    if li < 0 or li >= len(fidx.line_start_utf8) - 1:
        return None, None

    bstart = fidx.line_start_utf8[li] + int(col_utf8_bytes)

    if end_lineno_1 is None or end_col_utf8_bytes is None:
        return bstart, bstart

    eli = int(end_lineno_1) - 1
    if eli < 0 or eli >= len(fidx.line_start_utf8) - 1:
        return bstart, bstart

    bend = fidx.line_start_utf8[eli] + int(end_col_utf8_bytes)
    return bstart, bend


@dataclass(frozen=True)
class AstSpanInputs:
    """Column values required for AST span conversion."""

    file_ids: list[RowValue]
    linenos: list[RowValue]
    col_offsets: list[RowValue]
    end_linenos: list[RowValue]
    end_cols: list[RowValue]


def _ast_span_inputs(py_ast_nodes: pa.Table, cols: AstSpanColumns) -> AstSpanInputs:
    end_linenos = (
        cast("list[RowValue]", py_ast_nodes[cols.end_lineno].to_pylist())
        if cols.end_lineno in py_ast_nodes.column_names
        else _none_list(py_ast_nodes.num_rows)
    )
    end_cols = (
        cast("list[RowValue]", py_ast_nodes[cols.end_col].to_pylist())
        if cols.end_col in py_ast_nodes.column_names
        else _none_list(py_ast_nodes.num_rows)
    )
    return AstSpanInputs(
        file_ids=py_ast_nodes[cols.file_id].to_pylist(),
        linenos=py_ast_nodes[cols.lineno].to_pylist(),
        col_offsets=py_ast_nodes[cols.col].to_pylist(),
        end_linenos=end_linenos,
        end_cols=end_cols,
    )


def _compute_ast_spans(
    repo_index: RepoTextIndex, inputs: AstSpanInputs
) -> tuple[list[int | None], list[int | None], list[bool]]:
    bstarts: list[int | None] = []
    bends: list[int | None] = []
    oks: list[bool] = []
    for fid, ln, co, eln, eco in zip(
        inputs.file_ids,
        inputs.linenos,
        inputs.col_offsets,
        inputs.end_linenos,
        inputs.end_cols,
        strict=True,
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
        ln_int = _row_value_int(ln)
        co_int = _row_value_int(co)
        eln_int = _row_value_int(eln)
        eco_int = _row_value_int(eco)
        if ln_int is None or co_int is None or eln_int is None or eco_int is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        bs, be = ast_range_to_byte_span(fidx, ln_int, co_int, eln_int, eco_int)
        bstarts.append(bs)
        bends.append(be)
        oks.append(bs is not None and be is not None)
    return bstarts, bends, oks


# -----------------------------
# Table-level normalizers
# -----------------------------


def add_ast_byte_spans(
    repo_index: RepoTextIndex,
    py_ast_nodes: pa.Table,
    *,
    columns: AstSpanColumns | None = None,
) -> pa.Table:
    """Add (bstart, bend, span_ok) columns to AST nodes.

    This makes AST nodes joinable on byte spans (when applicable).

    Returns
    -------
    pa.Table
        Table with appended span columns.
    """
    cols = columns or AstSpanColumns()
    if cols.out_bstart in py_ast_nodes.column_names and cols.out_bend in py_ast_nodes.column_names:
        return py_ast_nodes

    inputs = _ast_span_inputs(py_ast_nodes, cols)
    bstarts, bends, oks = _compute_ast_spans(repo_index, inputs)

    return (
        py_ast_nodes.append_column(cols.out_bstart, pa.array(bstarts, type=pa.int64()))
        .append_column(cols.out_bend, pa.array(bends, type=pa.int64()))
        .append_column(cols.out_ok, pa.array(oks, type=pa.bool_()))
    )


def _span_error(document_id: str, path: str, reason: str) -> dict[str, str]:
    """Build a span error record.

    Returns
    -------
    dict[str, str]
        Span error record.
    """
    return {"document_id": document_id, "path": path, "reason": reason}


def _build_doc_posenc_map(
    scip_documents: pa.Table,
    columns: ScipOccurrenceColumns,
) -> dict[str, int]:
    """Build document_id -> position encoding map.

    Returns
    -------
    dict[str, int]
        Mapping of document IDs to position encodings.
    """
    doc_posenc: dict[str, int] = {}
    for row in scip_documents.to_pylist():
        did = row.get(columns.document_id)
        if did is None:
            continue
        doc_posenc[str(did)] = normalize_position_encoding(row.get(columns.doc_posenc))
    return doc_posenc


def _build_occurrence_rows(
    scip_occurrences: pa.Table,
    columns: ScipOccurrenceColumns,
) -> list[OccurrenceRow]:
    """Materialize occurrence rows for span conversion.

    Returns
    -------
    list[OccurrenceRow]
        Occurrence rows in order.
    """
    dids = scip_occurrences[columns.document_id].to_pylist()
    paths = scip_occurrences[columns.path].to_pylist()

    sls = scip_occurrences[columns.start_line].to_pylist()
    scs = scip_occurrences[columns.start_char].to_pylist()
    els = scip_occurrences[columns.end_line].to_pylist()
    ecs = scip_occurrences[columns.end_char].to_pylist()

    has_enc = all(
        c in scip_occurrences.column_names
        for c in (
            columns.enc_start_line,
            columns.enc_start_char,
            columns.enc_end_line,
            columns.enc_end_char,
        )
    )
    if has_enc:
        esls = scip_occurrences[columns.enc_start_line].to_pylist()
        escs = scip_occurrences[columns.enc_start_char].to_pylist()
        eels = scip_occurrences[columns.enc_end_line].to_pylist()
        eecs = scip_occurrences[columns.enc_end_char].to_pylist()
    else:
        none_list = [None] * scip_occurrences.num_rows
        esls = list(none_list)
        escs = list(none_list)
        eels = list(none_list)
        eecs = list(none_list)

    rows: list[OccurrenceRow] = []
    for did, path, sl, sc, el, ec, esl, esc, eel, eec in zip(
        dids, paths, sls, scs, els, ecs, esls, escs, eels, eecs, strict=True
    ):
        rows.append(
            OccurrenceRow(
                document_id=did,
                path=path,
                start_line=sl,
                start_char=sc,
                end_line=el,
                end_char=ec,
                enc_start_line=esl,
                enc_start_char=esc,
                enc_end_line=eel,
                enc_end_char=eec,
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
    """
    did = str(row.document_id) if row.document_id is not None else ""
    path_value = str(row.path) if row.path is not None else ""
    posenc = doc_posenc.get(did, DEFAULT_POSITION_ENCODING)

    fidx = repo_index.by_path.get(path_value) if path_value else None
    if fidx is None:
        return OccurrenceSpanResult(
            bstart=None,
            bend=None,
            enc_bstart=None,
            enc_bend=None,
            ok=False,
            error=_span_error(did, path_value, "missing_repo_text_for_path"),
        )

    main_range = _normalized_range(
        row.start_line, row.start_char, row.end_line, row.end_char
    )
    if main_range is None:
        return OccurrenceSpanResult(
            bstart=None,
            bend=None,
            enc_bstart=None,
            enc_bend=None,
            ok=False,
            error=_span_error(did, fidx.path, "missing_range_fields"),
        )

    bstart, bend = scip_range_to_byte_span(
        fidx,
        (main_range[0], main_range[1]),
        (main_range[2], main_range[3]),
        posenc,
    )
    enc_bstart: int | None = None
    enc_bend: int | None = None
    enc_range = _normalized_range(
        row.enc_start_line,
        row.enc_start_char,
        row.enc_end_line,
        row.enc_end_char,
    )
    if enc_range is not None:
        enc_bstart, enc_bend = scip_range_to_byte_span(
            fidx,
            (enc_range[0], enc_range[1]),
            (enc_range[2], enc_range[3]),
            posenc,
        )

    ok = bstart is not None and bend is not None
    error = _span_error(did, fidx.path, "range_to_byte_span_failed") if not ok else None

    return OccurrenceSpanResult(
        bstart=bstart,
        bend=bend,
        enc_bstart=enc_bstart,
        enc_bend=enc_bend,
        ok=ok,
        error=error,
    )


def _append_span_column(
    table: pa.Table,
    name: str,
    values: list[int | None] | list[bool],
    data_type: pa.DataType,
) -> pa.Table:
    """Append a column if missing.

    Returns
    -------
    pa.Table
        Updated table.
    """
    if name in table.column_names:
        return table
    return table.append_column(name, pa.array(values, type=data_type))


def _span_error_table(rows: list[dict[str, str]]) -> pa.Table:
    """Build the span error table.

    Returns
    -------
    pa.Table
        Span error table.
    """
    if rows:
        return pa.Table.from_pylist(rows, schema=SPAN_ERROR_SCHEMA)
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=["document_id", "path", "reason"],
    )


def add_scip_occurrence_byte_spans(
    repo_text_index: RepoTextIndex,
    scip_documents: pa.Table,
    scip_occurrences: pa.Table,
    *,
    columns: ScipOccurrenceColumns | None = None,
    ctx: object | None = None,
) -> tuple[pa.Table, pa.Table]:
    """Add byte spans to scip_occurrences using document position encodings.

    span_errors_table is intended for observability/debugging (invalid line/char, missing file text,
    etc.).

    Returns
    -------
    tuple[pa.Table, pa.Table]
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

    out = _append_span_column(scip_occurrences, cols.out_bstart, bstarts, pa.int64())
    out = _append_span_column(out, cols.out_bend, bends, pa.int64())
    out = _append_span_column(out, cols.out_enc_bstart, ebstarts, pa.int64())
    out = _append_span_column(out, cols.out_enc_bend, ebends, pa.int64())
    out = _append_span_column(out, cols.out_ok, oks, pa.bool_())
    return out, _span_error_table(err_rows)


# -----------------------------
# CST span canonicalization
# -----------------------------


def _append_alias_cols(table: pa.Table, aliases: dict[str, str]) -> pa.Table:
    """Append alias columns mapped to existing columns.

    Returns
    -------
    pa.Table
        Table with alias columns appended.
    """
    out = table
    for newc, oldc in aliases.items():
        if newc in out.column_names:
            continue
        if oldc not in out.column_names:
            out = out.append_column(newc, pa.array([None] * out.num_rows, type=pa.int64()))
            continue
        out = out.append_column(newc, out[oldc])
    return out


def normalize_cst_callsites_spans(
    py_cst_callsites: pa.Table,
    *,
    primary: Literal["callee", "call"] = "callee",
) -> pa.Table:
    """Ensure callsites have canonical (bstart, bend) columns for joins.

    primary="callee" means:
      bstart/bend == callee_bstart/callee_bend (recommended for SCIP occurrence joins)

    Returns
    -------
    pa.Table
        Callsites table with canonical span aliases.
    """
    if primary == "call":
        return _append_alias_cols(py_cst_callsites, {"bstart": "call_bstart", "bend": "call_bend"})
    return _append_alias_cols(py_cst_callsites, {"bstart": "callee_bstart", "bend": "callee_bend"})


def normalize_cst_imports_spans(
    py_cst_imports: pa.Table,
    *,
    primary: Literal["alias", "stmt"] = "alias",
) -> pa.Table:
    """Ensure imports have canonical (bstart, bend) columns.

    Returns
    -------
    pa.Table
        Imports table with canonical span aliases.
    """
    if primary == "stmt":
        return _append_alias_cols(py_cst_imports, {"bstart": "stmt_bstart", "bend": "stmt_bend"})
    return _append_alias_cols(py_cst_imports, {"bstart": "alias_bstart", "bend": "alias_bend"})


def normalize_cst_defs_spans(
    py_cst_defs: pa.Table,
    *,
    primary: Literal["name", "def"] = "name",
) -> pa.Table:
    """Ensure defs have canonical (bstart, bend) columns.

    primary="name" makes bstart/bend match the identifier token span (recommended for SCIP definition joins).

    Returns
    -------
    pa.Table
        Definitions table with canonical span aliases.
    """
    if primary == "def":
        return _append_alias_cols(py_cst_defs, {"bstart": "def_bstart", "bend": "def_bend"})
    return _append_alias_cols(py_cst_defs, {"bstart": "name_bstart", "bend": "name_bend"})
