"""Normalize bytecode instruction spans to UTF-8 byte offsets."""

from __future__ import annotations

from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from arrowdsl.iter import iter_arrays
from arrowdsl.pyarrow_protocols import ArrayLike, DataTypeLike, TableLike
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


def _column_or_null(table: TableLike, col: str, dtype: DataTypeLike) -> ArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


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
    py_bc_instructions: TableLike,
    *,
    columns: BytecodeSpanColumns | None = None,
) -> TableLike:
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
    TableLike
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

    arrays = [
        _column_or_null(py_bc_instructions, cols.file_id, pa.string()),
        _column_or_null(py_bc_instructions, cols.path, pa.string()),
        _column_or_null(py_bc_instructions, cols.start_line, pa.int64()),
        _column_or_null(py_bc_instructions, cols.start_col, pa.int64()),
        _column_or_null(py_bc_instructions, cols.end_line, pa.int64()),
        _column_or_null(py_bc_instructions, cols.end_col, pa.int64()),
    ]
    for file_id, path, start_line, start_col, end_line, end_col in iter_arrays(arrays):
        fidx = _file_index(repo_index, file_id, path)
        if fidx is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        ln_i = _row_value_int(start_line)
        col_i = _row_value_int(start_col)
        eln_i = _row_value_int(end_line)
        ecol_i = _row_value_int(end_col)
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
