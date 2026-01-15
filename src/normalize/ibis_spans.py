"""Ibis-based span normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import ibis
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, NumericValue, Table, Value

from arrowdsl.compute.position_encoding import ENC_UTF8, ENC_UTF16, ENC_UTF32
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

    defaults = SpanCoordDefaults(base=1, unit="utf32", end_exclusive=True)
    line_base, col_unit, end_exclusive = _span_base_values(
        ast.line_base,
        ast.col_unit,
        ast.end_exclusive,
        defaults=defaults,
    )
    start_line, end_line = _span_line_values(ast.lineno, ast.end_lineno, line_base)
    joined = _join_line_index(
        ast,
        start_idx,
        end_idx,
        spec=SpanJoinSpec(
            file_id=ast.file_id,
            start_line=start_line,
            end_line=end_line,
        ),
    )
    bstart, bend, span_ok = _span_offsets(
        SpanOffsetSpec(
            start_idx=start_idx,
            end_idx=end_idx,
            start_col=ast.col_offset,
            end_col=ast.end_col_offset,
            end_line_value=ast.end_lineno,
            col_unit=col_unit,
            end_exclusive=end_exclusive,
        )
    )
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

    defaults = SpanCoordDefaults(base=1, unit="utf32", end_exclusive=True)
    line_base, col_unit, end_exclusive = _span_base_values(
        instr.line_base,
        instr.col_unit,
        instr.end_exclusive,
        defaults=defaults,
    )
    start_line, end_line = _span_line_values(instr.pos_start_line, instr.pos_end_line, line_base)
    joined = _join_line_index(
        instr,
        start_idx,
        end_idx,
        spec=SpanJoinSpec(
            file_id=instr.file_id,
            start_line=start_line,
            end_line=end_line,
        ),
    )
    bstart, bend, span_ok = _span_offsets(
        SpanOffsetSpec(
            start_idx=start_idx,
            end_idx=end_idx,
            start_col=instr.pos_start_col,
            end_col=instr.pos_end_col,
            end_line_value=instr.pos_end_line,
            col_unit=col_unit,
            end_exclusive=end_exclusive,
        )
    )
    return joined.mutate(bstart=bstart, bend=bend, span_ok=span_ok).to_pyarrow()


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
    joined = _scip_occurrence_join(occ, docs, line_idx)
    occ_expr, errors_expr = _scip_occurrence_span_exprs(joined)
    occ_table = occ_expr.to_pyarrow()
    errors_table = errors_expr.to_pyarrow()
    if errors_table.num_rows == 0:
        return occ_table, span_error_table([])
    return occ_table, errors_table


def _scip_occurrence_join(occ: Table, docs: Table, line_idx: Table) -> Table:
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
    line_base = _line_base_value(occ_docs.line_base, default_base=0)
    end_exclusive = _end_exclusive_value(occ_docs.end_exclusive, default_exclusive=True)
    posenc = position_encoding_norm(occ_docs.position_encoding.cast("string"))
    occ_unit = occ_docs.col_unit.cast("string").lower()
    posenc_unit = _col_unit_from_encoding(posenc)
    col_unit = ibis.coalesce(occ_unit, posenc_unit)
    occ_docs = occ_docs.mutate(
        resolved_path=path_expr,
        line_base_norm=line_base,
        end_exclusive_norm=end_exclusive,
        col_unit_norm=col_unit,
        start_line0=_zero_based_line(occ_docs.start_line, line_base),
        end_line0=_zero_based_line(occ_docs.end_line, line_base),
        enc_start_line0=_zero_based_line(occ_docs.enc_start_line, line_base),
        enc_end_line0=_zero_based_line(occ_docs.enc_end_line, line_base),
        end_char_norm=_normalize_end_col(occ_docs.end_char, end_exclusive),
        enc_end_char_norm=_normalize_end_col(occ_docs.enc_end_char, end_exclusive),
    )

    start_idx = _line_index_view(line_idx, prefix="start")
    end_idx = _line_index_view(line_idx, prefix="end")
    enc_start_idx = _line_index_view(line_idx, prefix="enc_start")
    enc_end_idx = _line_index_view(line_idx, prefix="enc_end")

    joined = occ_docs.join(
        start_idx,
        [
            occ_docs.resolved_path == start_idx.start_path,
            occ_docs.start_line0 == start_idx.start_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        end_idx,
        [
            occ_docs.resolved_path == end_idx.end_path,
            occ_docs.end_line0 == end_idx.end_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        enc_start_idx,
        [
            occ_docs.resolved_path == enc_start_idx.enc_start_path,
            occ_docs.enc_start_line0 == enc_start_idx.enc_start_line_no,
        ],
        how="left",
    )
    return joined.join(
        enc_end_idx,
        [
            occ_docs.resolved_path == enc_end_idx.enc_end_path,
            occ_docs.enc_end_line0 == enc_end_idx.enc_end_line_no,
        ],
        how="left",
    )


def _scip_occurrence_span_exprs(joined: Table) -> tuple[Table, Table]:
    bstart = _line_offset_expr(
        joined.start_line_start_byte,
        joined.start_line_text,
        joined.start_char,
        joined.col_unit_norm,
    )
    bend = _line_offset_expr(
        joined.end_line_start_byte,
        joined.end_line_text,
        joined.end_char_norm,
        joined.col_unit_norm,
    )
    enc_bstart_raw = _line_offset_expr(
        joined.enc_start_line_start_byte,
        joined.enc_start_line_text,
        joined.enc_start_char,
        joined.col_unit_norm,
    )
    enc_bend_raw = _line_offset_expr(
        joined.enc_end_line_start_byte,
        joined.enc_end_line_text,
        joined.enc_end_char_norm,
        joined.col_unit_norm,
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
    reason = ibis.cases(
        (missing_repo, ibis.literal("missing_repo_text_for_path")),
        (invalid_len, ibis.literal("invalid_range_len")),
        (missing_fields, ibis.literal("missing_range_fields")),
        (~cast("BooleanValue", span_ok), ibis.literal("range_to_byte_span_failed")),
        else_=ibis.null(),
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
    col_unit: Value,
) -> Value:
    offset = column.cast("int64")
    byte_in_line = col_to_byte(line_text, offset, col_unit.cast("string"))
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


@dataclass(frozen=True)
class SpanCoordDefaults:
    base: int
    unit: str
    end_exclusive: bool


@dataclass(frozen=True)
class SpanJoinSpec:
    file_id: Value
    start_line: Value
    end_line: Value


@dataclass(frozen=True)
class SpanOffsetSpec:
    start_idx: Table
    end_idx: Table
    start_col: Value
    end_col: Value
    end_line_value: Value
    col_unit: Value
    end_exclusive: Value


def _span_base_values(
    line_base: Value,
    col_unit: Value,
    end_exclusive: Value,
    *,
    defaults: SpanCoordDefaults,
) -> tuple[Value, Value, Value]:
    return (
        _line_base_value(line_base, default_base=defaults.base),
        _col_unit_value(col_unit, default_unit=defaults.unit),
        _end_exclusive_value(end_exclusive, default_exclusive=defaults.end_exclusive),
    )


def _span_line_values(start_line: Value, end_line: Value, line_base: Value) -> tuple[Value, Value]:
    return (
        _zero_based_line(start_line, line_base),
        _zero_based_line(end_line, line_base),
    )


def _join_line_index(
    table: Table,
    start_idx: Table,
    end_idx: Table,
    *,
    spec: SpanJoinSpec,
) -> Table:
    joined = table.join(
        start_idx,
        [
            spec.file_id == start_idx.start_file_id,
            spec.start_line == start_idx.start_line_no,
        ],
        how="left",
    )
    return joined.join(
        end_idx,
        [spec.file_id == end_idx.end_file_id, spec.end_line == end_idx.end_line_no],
        how="left",
    )


def _span_offsets(spec: SpanOffsetSpec) -> tuple[Value, Value, Value]:
    end_col_norm = _normalize_end_col(spec.end_col, spec.end_exclusive)
    bstart = _line_offset_expr(
        spec.start_idx.start_line_start_byte,
        spec.start_idx.start_line_text,
        spec.start_col,
        spec.col_unit,
    )
    bend_raw = _line_offset_expr(
        spec.end_idx.end_line_start_byte,
        spec.end_idx.end_line_text,
        end_col_norm,
        spec.col_unit,
    )
    end_ok = spec.end_line_value.notnull() & end_col_norm.notnull()
    bend = ibis.ifelse(end_ok, bend_raw, bstart)
    span_ok = bstart.notnull() & bend.notnull()
    return bstart, bend, span_ok


def _line_base_value(line_base: Value, *, default_base: int) -> NumericValue:
    return cast("NumericValue", ibis.coalesce(line_base.cast("int32"), ibis.literal(default_base)))


def _zero_based_line(line_value: Value, line_base: Value) -> NumericValue:
    left = cast("NumericValue", line_value.cast("int32"))
    right = cast("NumericValue", line_base.cast("int32"))
    result = left - right
    return cast("NumericValue", result.cast("int32"))


def _end_exclusive_value(end_exclusive: Value, *, default_exclusive: bool) -> BooleanValue:
    result = ibis.coalesce(end_exclusive.cast("boolean"), ibis.literal(default_exclusive))
    return cast("BooleanValue", result)


def _normalize_end_col(end_col: Value, end_exclusive: Value) -> NumericValue:
    col = cast("NumericValue", end_col.cast("int64"))
    increment = cast("NumericValue", ibis.literal(1, type="int64"))
    adjusted = col + increment
    result = ibis.ifelse(end_exclusive, col, adjusted)
    return cast("NumericValue", result)


def _col_unit_value(col_unit: Value, *, default_unit: str) -> Value:
    return ibis.coalesce(col_unit.cast("string"), ibis.literal(default_unit))


def _col_unit_from_encoding(encoding: Value) -> Value:
    return ibis.cases(
        (encoding == ibis.literal(ENC_UTF8), ibis.literal("utf8")),
        (encoding == ibis.literal(ENC_UTF16), ibis.literal("utf16")),
        (encoding == ibis.literal(ENC_UTF32), ibis.literal("utf32")),
        else_=ibis.literal("utf32"),
    )


__all__ = [
    "add_ast_byte_spans_ibis",
    "add_scip_occurrence_byte_spans_ibis",
    "anchor_instructions_ibis",
]
