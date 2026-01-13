"""Normalize bytecode instruction spans to UTF-8 byte offsets."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.ids import iter_arrays
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import column_or_null, set_or_append_column
from normalize.spans import ast_range_to_byte_span
from normalize.text_index import RepoTextIndex, file_index, row_value_int


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
        column_or_null(py_bc_instructions, cols.file_id, pa.string()),
        column_or_null(py_bc_instructions, cols.path, pa.string()),
        column_or_null(py_bc_instructions, cols.start_line, pa.int64()),
        column_or_null(py_bc_instructions, cols.start_col, pa.int64()),
        column_or_null(py_bc_instructions, cols.end_line, pa.int64()),
        column_or_null(py_bc_instructions, cols.end_col, pa.int64()),
    ]
    for file_id, path, start_line, start_col, end_line, end_col in iter_arrays(arrays):
        fidx = file_index(repo_index, file_id=file_id, path=path)
        if fidx is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        ln_i = row_value_int(start_line)
        col_i = row_value_int(start_col)
        eln_i = row_value_int(end_line)
        ecol_i = row_value_int(end_col)
        if ln_i is None or col_i is None or eln_i is None or ecol_i is None:
            bstarts.append(None)
            bends.append(None)
            oks.append(False)
            continue
        bstart, bend = ast_range_to_byte_span(fidx, ln_i, col_i, eln_i, ecol_i)
        bstarts.append(bstart)
        bends.append(bend)
        oks.append(bstart is not None and bend is not None)

    out = set_or_append_column(
        py_bc_instructions, cols.out_bstart, pa.array(bstarts, type=pa.int64())
    )
    out = set_or_append_column(out, cols.out_bend, pa.array(bends, type=pa.int64()))
    return set_or_append_column(out, cols.out_ok, pa.array(oks, type=pa.bool_()))
