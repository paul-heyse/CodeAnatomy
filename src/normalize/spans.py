from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa

from .ids import stable_id


@dataclass(frozen=True)
class FileTextIndex:
    """
    Per-file text index used to convert:
      - (line, char) -> byte offsets (bstart/bend)
      - AST lineno/col -> byte offsets

    All byte offsets are in UTF-8 bytes of the decoded document text (canonical coordinate system).

    Important: lines are split with keepends=True so that cumulative byte lengths reproduce
    file-length coordinates (including newlines).
    """
    file_id: str
    path: str
    file_sha256: Optional[str]
    encoding: Optional[str]
    text: str
    lines: List[str]                # splitlines(keepends=True)
    line_start_utf8: List[int]      # length = len(lines)+1; line_start_utf8[i] = byte start of line i


@dataclass(frozen=True)
class RepoTextIndex:
    """
    Repo-wide indices for fast lookup.
    """
    by_file_id: Dict[str, FileTextIndex]
    by_path: Dict[str, FileTextIndex]


def _decode_repo_row_text(rf: dict) -> Optional[str]:
    t = rf.get("text")
    if isinstance(t, str) and t:
        return t
    b = rf.get("bytes")
    if isinstance(b, (bytes, bytearray)):
        enc = rf.get("encoding") or "utf-8"
        try:
            return bytes(b).decode(enc, errors="replace")
        except Exception:
            return bytes(b).decode("utf-8", errors="replace")
    return None


def _build_line_index_utf8(text: str) -> Tuple[List[str], List[int]]:
    lines = text.splitlines(keepends=True)
    starts = [0]
    acc = 0
    for ln in lines:
        acc += len(ln.encode("utf-8"))
        starts.append(acc)
    return lines, starts


def build_repo_text_index(repo_files: pa.Table) -> RepoTextIndex:
    """
    Build lookup structures from repo_files output.

    Expects columns:
      file_id, path, file_sha256, encoding, text/bytes
    """
    by_file_id: Dict[str, FileTextIndex] = {}
    by_path: Dict[str, FileTextIndex] = {}

    for rf in repo_files.to_pylist():
        file_id = rf["file_id"]
        path = rf["path"]
        file_sha256 = rf.get("file_sha256")
        encoding = rf.get("encoding")

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

def normalize_position_encoding(value) -> int:
    """
    Accepts:
      - int enums (1 UTF8 bytes, 2 UTF16 code units, 3 UTF32 codepoints)
      - strings like "POSITION_ENCODING_UTF16", "UTF16", "2", etc.
    Returns int in {1,2,3} (defaults to 3 for Python/scip-python).
    """
    if value is None:
        return 3
    if isinstance(value, int):
        return value if value in (1, 2, 3) else 3
    if isinstance(value, str):
        s = value.strip().upper()
        if s.isdigit():
            v = int(s)
            return v if v in (1, 2, 3) else 3
        if "UTF8" in s:
            return 1
        if "UTF16" in s:
            return 2
        if "UTF32" in s:
            return 3
    return 3


def code_unit_offset_to_py_index(line: str, offset: int, position_encoding: int) -> int:
    """
    Convert a SCIP 'character' offset to a Python string index for a *single line*.

    position_encoding:
      1 UTF8 byte offsets
      2 UTF16 code unit offsets
      3 UTF32 code unit offsets (Pythonic codepoints)
    """
    if offset <= 0:
        return 0

    if position_encoding == 3:
        # UTF32 code units ~= python str indexing by codepoint
        return min(offset, len(line))

    if position_encoding == 1:
        # offset is UTF-8 bytes; decode prefix to count python codepoints
        b = line.encode("utf-8")
        off = min(offset, len(b))
        return len(b[:off].decode("utf-8", errors="strict"))

    if position_encoding == 2:
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
) -> Optional[int]:
    """
    Convert (0-based line, char offset in position_encoding units) -> byte offset in UTF-8 bytes.
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
    start_line: int,
    start_char: int,
    end_line: int,
    end_char: int,
    position_encoding: int,
) -> Tuple[Optional[int], Optional[int]]:
    """
    Convert SCIP normalized Range4 (0-based) into a (bstart, bend) UTF-8 byte span.
    """
    bstart = line_char_to_byte_offset(fidx, start_line, start_char, position_encoding)
    bend = line_char_to_byte_offset(fidx, end_line, end_char, position_encoding)
    return bstart, bend


# -----------------------------
# AST span conversion
# -----------------------------

def ast_range_to_byte_span(
    fidx: FileTextIndex,
    lineno_1: Optional[int],
    col_utf8_bytes: Optional[int],
    end_lineno_1: Optional[int],
    end_col_utf8_bytes: Optional[int],
) -> Tuple[Optional[int], Optional[int]]:
    """
    Convert CPython AST (1-based lineno, col offsets in UTF-8 bytes) to (bstart,bend).

    If end coordinates are missing, we fall back to (bstart,bstart).
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


# -----------------------------
# Table-level normalizers
# -----------------------------

def add_ast_byte_spans(
    repo_index: RepoTextIndex,
    py_ast_nodes: pa.Table,
    *,
    file_id_col: str = "file_id",
    lineno_col: str = "lineno",
    col_col: str = "col_offset",
    end_lineno_col: str = "end_lineno",
    end_col_col: str = "end_col_offset",
    out_bstart: str = "bstart",
    out_bend: str = "bend",
    out_ok: str = "span_ok",
) -> pa.Table:
    """
    Adds (bstart,bend,span_ok) to py_ast_nodes.

    This makes AST nodes joinable on byte spans (when applicable).
    """
    if out_bstart in py_ast_nodes.column_names and out_bend in py_ast_nodes.column_names:
        return py_ast_nodes

    fids = py_ast_nodes[file_id_col].to_pylist()
    linenos = py_ast_nodes[lineno_col].to_pylist()
    cols = py_ast_nodes[col_col].to_pylist()
    end_linenos = py_ast_nodes[end_lineno_col].to_pylist() if end_lineno_col in py_ast_nodes.column_names else [None] * py_ast_nodes.num_rows
    end_cols = py_ast_nodes[end_col_col].to_pylist() if end_col_col in py_ast_nodes.column_names else [None] * py_ast_nodes.num_rows

    bstarts: list[Optional[int]] = []
    bends: list[Optional[int]] = []
    oks: list[bool] = []

    for fid, ln, co, eln, eco in zip(fids, linenos, cols, end_linenos, end_cols):
        fidx = repo_index.by_file_id.get(fid)
        if fidx is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        bs, be = ast_range_to_byte_span(fidx, ln, co, eln, eco)
        bstarts.append(bs)
        bends.append(be)
        oks.append(bs is not None and be is not None)

    out = py_ast_nodes
    out = out.append_column(out_bstart, pa.array(bstarts, type=pa.int64()))
    out = out.append_column(out_bend, pa.array(bends, type=pa.int64()))
    out = out.append_column(out_ok, pa.array(oks, type=pa.bool_()))
    return out


def add_scip_occurrence_byte_spans(
    repo_index: RepoTextIndex,
    scip_documents: pa.Table,
    scip_occurrences: pa.Table,
    *,
    document_id_col: str = "document_id",
    doc_posenc_col: str = "position_encoding",
    occ_path_col: str = "path",
    occ_start_line: str = "start_line",
    occ_start_char: str = "start_char",
    occ_end_line: str = "end_line",
    occ_end_char: str = "end_char",
    enc_start_line: str = "enc_start_line",
    enc_start_char: str = "enc_start_char",
    enc_end_line: str = "enc_end_line",
    enc_end_char: str = "enc_end_char",
    out_bstart: str = "bstart",
    out_bend: str = "bend",
    out_enc_bstart: str = "enc_bstart",
    out_enc_bend: str = "enc_bend",
    out_ok: str = "span_ok",
) -> Tuple[pa.Table, pa.Table]:
    """
    Adds byte spans to scip_occurrences using Document.position_encoding.

    Returns: (occurrences_with_spans, span_errors_table)

    span_errors_table is intended for observability/debugging (invalid line/char, missing file text, etc.)
    """
    # Build doc_id -> posenc map
    doc_posenc: Dict[str, int] = {}
    for row in scip_documents.to_pylist():
        did = row.get(document_id_col)
        pe = normalize_position_encoding(row.get(doc_posenc_col))
        if did:
            doc_posenc[str(did)] = pe

    # Pull arrays
    dids = scip_occurrences[document_id_col].to_pylist()
    paths = scip_occurrences[occ_path_col].to_pylist()

    sls = scip_occurrences[occ_start_line].to_pylist()
    scs = scip_occurrences[occ_start_char].to_pylist()
    els = scip_occurrences[occ_end_line].to_pylist()
    ecs = scip_occurrences[occ_end_char].to_pylist()

    has_enc = all(c in scip_occurrences.column_names for c in (enc_start_line, enc_start_char, enc_end_line, enc_end_char))
    esls = scip_occurrences[enc_start_line].to_pylist() if has_enc else [None] * scip_occurrences.num_rows
    escs = scip_occurrences[enc_start_char].to_pylist() if has_enc else [None] * scip_occurrences.num_rows
    eels = scip_occurrences[enc_end_line].to_pylist() if has_enc else [None] * scip_occurrences.num_rows
    eecs = scip_occurrences[enc_end_char].to_pylist() if has_enc else [None] * scip_occurrences.num_rows

    bstarts: list[Optional[int]] = []
    bends: list[Optional[int]] = []
    ebstarts: list[Optional[int]] = []
    ebends: list[Optional[int]] = []
    oks: list[bool] = []

    err_rows: list[dict] = []
    ERR_SCHEMA = pa.schema(
        [
            ("document_id", pa.string()),
            ("path", pa.string()),
            ("reason", pa.string()),
        ]
    )

    for did, path, sl, sc, el, ec, esl, esc, eel, eec in zip(
        dids, paths, sls, scs, els, ecs, esls, escs, eels, eecs
    ):
        did_s = str(did) if did is not None else ""
        posenc = doc_posenc.get(did_s, 3)

        fidx = repo_index.by_path.get(str(path)) if path is not None else None
        if fidx is None:
            bstarts.append(None)
            bends.append(None)
            ebstarts.append(None)
            ebends.append(None)
            oks.append(False)
            err_rows.append({"document_id": did_s, "path": str(path), "reason": "missing_repo_text_for_path"})
            continue

        if sl is None or sc is None or el is None or ec is None:
            bstarts.append(None)
            bends.append(None)
            ebstarts.append(None)
            ebends.append(None)
            oks.append(False)
            err_rows.append({"document_id": did_s, "path": fidx.path, "reason": "missing_range_fields"})
            continue

        bs, be = scip_range_to_byte_span(fidx, int(sl), int(sc), int(el), int(ec), posenc)
        bstarts.append(bs)
        bends.append(be)

        # enclosing range optional
        if esl is None or esc is None or eel is None or eec is None:
            ebstarts.append(None)
            ebends.append(None)
        else:
            ebs, ebe = scip_range_to_byte_span(fidx, int(esl), int(esc), int(eel), int(eec), posenc)
            ebstarts.append(ebs)
            ebends.append(ebe)

        ok = (bs is not None and be is not None)
        oks.append(ok)
        if not ok:
            err_rows.append({"document_id": did_s, "path": fidx.path, "reason": "range_to_byte_span_failed"})

    out = scip_occurrences
    if out_bstart not in out.column_names:
        out = out.append_column(out_bstart, pa.array(bstarts, type=pa.int64()))
    if out_bend not in out.column_names:
        out = out.append_column(out_bend, pa.array(bends, type=pa.int64()))
    if out_enc_bstart not in out.column_names:
        out = out.append_column(out_enc_bstart, pa.array(ebstarts, type=pa.int64()))
    if out_enc_bend not in out.column_names:
        out = out.append_column(out_enc_bend, pa.array(ebends, type=pa.int64()))
    if out_ok not in out.column_names:
        out = out.append_column(out_ok, pa.array(oks, type=pa.bool_()))

    err_t = pa.Table.from_pylist(err_rows, schema=ERR_SCHEMA) if err_rows else pa.Table.from_arrays(
        [pa.array([], type=pa.string()), pa.array([], type=pa.string()), pa.array([], type=pa.string())],
        names=["document_id", "path", "reason"],
    )
    return out, err_t


# -----------------------------
# CST span canonicalization
# -----------------------------

def _append_alias_cols(table: pa.Table, aliases: Dict[str, str]) -> pa.Table:
    """
    aliases: new_col -> existing_col
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
    primary: str = "callee",   # "callee" or "call"
) -> pa.Table:
    """
    Ensure callsites have canonical (bstart,bend) for joins.

    primary="callee" means:
      bstart/bend == callee_bstart/callee_bend (recommended for SCIP occurrence joins)
    """
    if primary == "call":
        return _append_alias_cols(py_cst_callsites, {"bstart": "call_bstart", "bend": "call_bend"})
    return _append_alias_cols(py_cst_callsites, {"bstart": "callee_bstart", "bend": "callee_bend"})


def normalize_cst_imports_spans(
    py_cst_imports: pa.Table,
    *,
    primary: str = "alias",  # "alias" or "stmt"
) -> pa.Table:
    """
    Ensure imports have canonical (bstart,bend).
    """
    if primary == "stmt":
        return _append_alias_cols(py_cst_imports, {"bstart": "stmt_bstart", "bend": "stmt_bend"})
    return _append_alias_cols(py_cst_imports, {"bstart": "alias_bstart", "bend": "alias_bend"})


def normalize_cst_defs_spans(
    py_cst_defs: pa.Table,
    *,
    primary: str = "name",  # "name" or "def"
) -> pa.Table:
    """
    Ensure defs have canonical (bstart,bend).

    primary="name" makes bstart/bend match the identifier token span (recommended for SCIP definition joins).
    """
    if primary == "def":
        return _append_alias_cols(py_cst_defs, {"bstart": "def_bstart", "bend": "def_bend"})
    return _append_alias_cols(py_cst_defs, {"bstart": "name_bstart", "bend": "name_bend"})
