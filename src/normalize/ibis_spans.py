"""Ibis-based span normalization helpers."""

from __future__ import annotations

from typing import cast

import ibis
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, NumericValue, Table, Value

from arrowdsl.compute.expr_core import ENC_UTF32
from arrowdsl.core.interop import TableLike
from ibis_engine.builtin_udfs import col_to_byte, position_encoding_norm
from ibis_engine.ids import masked_stable_id_expr
from ibis_engine.plan_bridge import table_to_ibis
from normalize.span_pipeline import span_error_table


def add_ast_byte_spans_ibis(
    line_index: TableLike,
    py_ast_nodes: TableLike,
    *,
    backend: BaseBackend,
) -> TableLike:
    """Append AST byte-span columns using line index joins.

    Returns
    -------
    TableLike
        AST nodes with byte-span columns added.
    """
    ast = _table_expr(py_ast_nodes, backend=backend, name="ast_nodes")
    line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
    start_idx = _line_index_view(line_idx, prefix="start")
    end_idx = _line_index_view(line_idx, prefix="end")

    one = cast("NumericValue", ibis.literal(1))
    start_line = (cast("NumericValue", ast.lineno) - one).cast("int32")
    end_line = (cast("NumericValue", ast.end_lineno) - one).cast("int32")
    joined = ast.join(
        start_idx,
        [ast.file_id == start_idx.start_file_id, start_line == start_idx.start_line_no],
        how="left",
    )
    joined = joined.join(
        end_idx,
        [ast.file_id == end_idx.end_file_id, end_line == end_idx.end_line_no],
        how="left",
    )
    enc = ibis.literal(ENC_UTF32)
    bstart = _line_offset_expr(
        start_idx.start_line_start_byte,
        start_idx.start_line_text,
        ast.col_offset,
        enc,
    )
    bend_raw = _line_offset_expr(
        end_idx.end_line_start_byte,
        end_idx.end_line_text,
        ast.end_col_offset,
        enc,
    )
    end_ok = ast.end_lineno.notnull() & ast.end_col_offset.notnull()
    bend = ibis.ifelse(end_ok, bend_raw, bstart)
    span_ok = bstart.notnull() & bend.notnull()
    span_id = masked_stable_id_expr(
        "span",
        parts=(ast.path, bstart, bend),
        required=(ast.path, bstart, bend),
    )
    return joined.mutate(bstart=bstart, bend=bend, span_ok=span_ok, span_id=span_id).to_pyarrow()


def anchor_instructions_ibis(
    line_index: TableLike,
    py_bc_instructions: TableLike,
    *,
    backend: BaseBackend,
) -> TableLike:
    """Anchor bytecode instruction spans using line index joins.

    Returns
    -------
    TableLike
        Instruction table with span annotations.
    """
    instr = _table_expr(py_bc_instructions, backend=backend, name="py_bc_instructions")
    line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
    start_idx = _line_index_view(line_idx, prefix="start")
    end_idx = _line_index_view(line_idx, prefix="end")

    one = cast("NumericValue", ibis.literal(1))
    start_line = (cast("NumericValue", instr.pos_start_line) - one).cast("int32")
    end_line = (cast("NumericValue", instr.pos_end_line) - one).cast("int32")
    joined = instr.join(
        start_idx,
        [
            instr.file_id == start_idx.start_file_id,
            start_line == start_idx.start_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        end_idx,
        [instr.file_id == end_idx.end_file_id, end_line == end_idx.end_line_no],
        how="left",
    )
    enc = ibis.literal(ENC_UTF32)
    bstart = _line_offset_expr(
        start_idx.start_line_start_byte,
        start_idx.start_line_text,
        instr.pos_start_col,
        enc,
    )
    bend_raw = _line_offset_expr(
        end_idx.end_line_start_byte,
        end_idx.end_line_text,
        instr.pos_end_col,
        enc,
    )
    end_ok = instr.pos_end_line.notnull() & instr.pos_end_col.notnull()
    bend = ibis.ifelse(end_ok, bend_raw, bstart)
    span_ok = bstart.notnull() & bend.notnull()
    expr = joined.mutate(bstart=bstart, bend=bend, span_ok=span_ok)
    return expr.to_pyarrow()


def add_scip_occurrence_byte_spans_ibis(
    line_index: TableLike,
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    *,
    backend: BaseBackend,
) -> tuple[TableLike, TableLike]:
    """Add byte spans to SCIP occurrences using line index joins.

    Returns
    -------
    tuple[TableLike, TableLike]
        Occurrence table with spans plus span error rows.
    """
    occ = _table_expr(scip_occurrences, backend=backend, name="scip_occurrences")
    docs = _table_expr(scip_documents, backend=backend, name="scip_documents")
    line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
    joined, posenc = _scip_occurrence_join(occ, docs, line_idx)
    occ_expr, errors_expr = _scip_occurrence_span_exprs(joined, posenc)
    occ_table = occ_expr.to_pyarrow()
    errors_table = errors_expr.to_pyarrow()
    if errors_table.num_rows == 0:
        return occ_table, span_error_table([])
    return occ_table, errors_table


def _scip_occurrence_join(occ: Table, docs: Table, line_idx: Table) -> tuple[Table, Value]:
    docs_sel = docs.select(
        document_id=docs.document_id,
        doc_path=docs.path,
        position_encoding=docs.position_encoding,
    )
    occ_docs = occ.join(
        docs_sel,
        [occ.document_id == docs_sel.document_id],
        how="left",
    )
    path_expr = ibis.coalesce(occ_docs.path, occ_docs.doc_path)
    occ_docs = occ_docs.mutate(resolved_path=path_expr)
    posenc = position_encoding_norm(occ_docs.position_encoding.cast("string"))

    start_idx = _line_index_view(line_idx, prefix="start")
    end_idx = _line_index_view(line_idx, prefix="end")
    enc_start_idx = _line_index_view(line_idx, prefix="enc_start")
    enc_end_idx = _line_index_view(line_idx, prefix="enc_end")

    joined = occ_docs.join(
        start_idx,
        [
            occ_docs.resolved_path == start_idx.start_path,
            occ_docs.start_line.cast("int32") == start_idx.start_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        end_idx,
        [
            occ_docs.resolved_path == end_idx.end_path,
            occ_docs.end_line.cast("int32") == end_idx.end_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        enc_start_idx,
        [
            occ_docs.resolved_path == enc_start_idx.enc_start_path,
            occ_docs.enc_start_line.cast("int32") == enc_start_idx.enc_start_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        enc_end_idx,
        [
            occ_docs.resolved_path == enc_end_idx.enc_end_path,
            occ_docs.enc_end_line.cast("int32") == enc_end_idx.enc_end_line_no,
        ],
        how="left",
    )
    return joined, posenc


def _scip_occurrence_span_exprs(joined: Table, posenc: Value) -> tuple[Table, Table]:
    bstart = _line_offset_expr(
        joined.start_line_start_byte,
        joined.start_line_text,
        joined.start_char,
        posenc,
    )
    bend = _line_offset_expr(
        joined.end_line_start_byte,
        joined.end_line_text,
        joined.end_char,
        posenc,
    )
    enc_bstart_raw = _line_offset_expr(
        joined.enc_start_line_start_byte,
        joined.enc_start_line_text,
        joined.enc_start_char,
        posenc,
    )
    enc_bend_raw = _line_offset_expr(
        joined.enc_end_line_start_byte,
        joined.enc_end_line_text,
        joined.enc_end_char,
        posenc,
    )
    enc_valid = joined.enc_range_len.notnull() & (joined.enc_range_len > ibis.literal(0))
    enc_bstart = ibis.ifelse(enc_valid, enc_bstart_raw, ibis.null())
    enc_bend = ibis.ifelse(enc_valid, enc_bend_raw, ibis.null())
    span_ok = bstart.notnull() & bend.notnull()
    span_id = masked_stable_id_expr(
        "span",
        parts=(joined.resolved_path, bstart, bend),
        required=(joined.resolved_path, bstart, bend),
    )
    occ_expr = joined.mutate(
        bstart=bstart,
        bend=bend,
        enc_bstart=enc_bstart,
        enc_bend=enc_bend,
        span_ok=span_ok,
        span_id=span_id,
    )
    errors_expr = _scip_span_errors_expr(
        joined,
        span_ok=span_ok,
        path_expr=joined.resolved_path,
    )
    return occ_expr, errors_expr


def _scip_span_errors_expr(
    occ: Table,
    *,
    span_ok: Value,
    path_expr: Value,
) -> Table:
    missing_repo = occ.start_line_text.isnull() | occ.end_line_text.isnull()
    invalid_len = occ.range_len.isnull() | (occ.range_len <= ibis.literal(0))
    missing_fields = (
        occ.start_line.isnull()
        | occ.start_char.isnull()
        | occ.end_line.isnull()
        | occ.end_char.isnull()
    )
    reason = (
        ibis.case()
        .when(missing_repo, "missing_repo_text_for_path")
        .when(invalid_len, "invalid_range_len")
        .when(missing_fields, "missing_range_fields")
        .when(~cast("BooleanValue", span_ok), "range_to_byte_span_failed")
        .else_(ibis.null())
        .end()
    )
    return (
        occ.mutate(reason=reason, path=path_expr)
        .filter(reason.notnull())
        .select(document_id=occ.document_id, path=path_expr, reason=reason)
    )


def _line_offset_expr(
    line_start: Value,
    line_text: Value,
    column: Value,
    encoding: Value,
) -> Value:
    offset = column.cast("int64")
    byte_in_line = col_to_byte(line_text, offset, encoding)
    left = cast("NumericValue", line_start)
    right = cast("NumericValue", byte_in_line)
    return left + right


def _line_index_view(line_index: Table, *, prefix: str) -> Table:
    return line_index.select(
        **{
            f"{prefix}_file_id": line_index.file_id,
            f"{prefix}_path": line_index.path,
            f"{prefix}_line_no": line_index.line_no,
            f"{prefix}_line_start_byte": line_index.line_start_byte,
            f"{prefix}_line_text": line_index.line_text,
        }
    )


def _table_expr(table: TableLike, *, backend: BaseBackend, name: str) -> Table:
    plan = table_to_ibis(table, backend=backend, name=name)
    return plan.expr


__all__ = [
    "add_ast_byte_spans_ibis",
    "add_scip_occurrence_byte_spans_ibis",
    "anchor_instructions_ibis",
]
