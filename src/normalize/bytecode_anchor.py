"""Normalize bytecode instruction spans to UTF-8 byte offsets."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from normalize.spans import FileTextIndex, RepoTextIndex, ast_range_to_byte_span


def _row_value_int(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


@dataclass(frozen=True)
class BytecodeSpanColumns:
    """Column names for instruction span normalization."""

    file_id: str = "file_id"
    path: str = "path"
    start_line: str = "pos_start_line"
    start_col: str = "pos_start_col"
    end_line: str = "pos_end_line"
    end_col: str = "pos_end_col"
    out_bstart: str = "bstart"
    out_bend: str = "bend"
    out_ok: str = "span_ok"


def _file_index(repo_index: RepoTextIndex, file_id: object, path: object) -> FileTextIndex | None:
    if isinstance(file_id, str) and file_id in repo_index.by_file_id:
        return repo_index.by_file_id[file_id]
    if isinstance(path, str) and path in repo_index.by_path:
        return repo_index.by_path[path]
    return None


def anchor_instructions(
    repo_index: RepoTextIndex,
    py_bc_instructions: pa.Table,
    *,
    columns: BytecodeSpanColumns | None = None,
) -> pa.Table:
    """Append byte-span columns to bytecode instruction rows.

    Parameters
    ----------
    repo_index:
        Repo text index for line/column to byte-offset conversion.
    py_bc_instructions:
        Bytecode instruction table with position columns.
    columns:
        Column naming configuration.

    Returns
    -------
    pa.Table
        Instruction table with bstart/bend/span_ok columns appended.
    """
    cols = columns or BytecodeSpanColumns()
    if (
        cols.out_bstart in py_bc_instructions.column_names
        and cols.out_bend in py_bc_instructions.column_names
    ):
        return py_bc_instructions

    bstarts: list[int | None] = []
    bends: list[int | None] = []
    oks: list[bool] = []

    for row in py_bc_instructions.to_pylist():
        fidx = _file_index(repo_index, row.get(cols.file_id), row.get(cols.path))
        if fidx is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        ln_i = _row_value_int(row.get(cols.start_line))
        col_i = _row_value_int(row.get(cols.start_col))
        eln_i = _row_value_int(row.get(cols.end_line))
        ecol_i = _row_value_int(row.get(cols.end_col))
        if ln_i is None or col_i is None or eln_i is None or ecol_i is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        bstart, bend = ast_range_to_byte_span(fidx, ln_i, col_i, eln_i, ecol_i)
        bstarts.append(bstart)
        bends.append(bend)
        oks.append(bstart is not None and bend is not None)

    return (
        py_bc_instructions.append_column(cols.out_bstart, pa.array(bstarts, type=pa.int64()))
        .append_column(cols.out_bend, pa.array(bends, type=pa.int64()))
        .append_column(cols.out_ok, pa.array(oks, type=pa.bool_()))
    )
