"""Ibis-based span normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, NumericValue, Table, Value

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.semantic_types import SPAN_STORAGE
from datafusion_engine.nested_tables import ViewReference
from ibis_engine.builtin_udfs import col_to_byte
from ibis_engine.ids import masked_stable_id_expr
from ibis_engine.schema_utils import ibis_null_literal
from ibis_engine.sources import SourceToIbisOptions, table_to_ibis
from normalize.ibis_exprs import position_encoding_norm_expr
from normalize.schemas import SPAN_ERROR_SCHEMA
from normalize.text_index import ENC_UTF8, ENC_UTF16, ENC_UTF32

SpanSource = TableLike | Table | ViewReference


@dataclass(frozen=True)
class SpanStructInputs:
    bstart: Value
    bend: Value
    start_line0: Value | None = None
    end_line0: Value | None = None
    start_col: Value | None = None
    end_col: Value | None = None
    col_unit: Value | None = None
    end_exclusive: Value | None = None


def _empty_span_errors_table() -> TableLike:
    return empty_table(SPAN_ERROR_SCHEMA)


def _span_struct_expr(inputs: SpanStructInputs) -> Value:
    span_ok = inputs.bstart.notnull() & inputs.bend.notnull()
    null_i32 = ibis_null_literal(pa.int32())
    null_bool = ibis_null_literal(pa.bool_())
    null_str = ibis_null_literal(pa.string())
    start_line_expr = (
        inputs.start_line0 if inputs.start_line0 is not None else null_i32
    ).cast("int32")
    end_line_expr = (inputs.end_line0 if inputs.end_line0 is not None else null_i32).cast(
        "int32"
    )
    start_col_expr = (inputs.start_col if inputs.start_col is not None else null_i32).cast(
        "int32"
    )
    end_col_expr = (inputs.end_col if inputs.end_col is not None else null_i32).cast("int32")
    col_unit_expr = (inputs.col_unit if inputs.col_unit is not None else null_str).cast(
        "string"
    )
    end_exclusive_expr = (
        inputs.end_exclusive if inputs.end_exclusive is not None else null_bool
    ).cast(
        "boolean"
    )
    bstart_i64 = cast("NumericValue", inputs.bstart.cast("int64"))
    bend_i64 = cast("NumericValue", inputs.bend.cast("int64"))
    byte_start = ibis.ifelse(span_ok, bstart_i64.cast("int32"), null_i32)
    byte_len_value = bend_i64 - bstart_i64
    byte_len = ibis.ifelse(span_ok, byte_len_value.cast("int32"), null_i32)
    return ibis.struct(
        {
            "start": ibis.struct({"line0": start_line_expr, "col": start_col_expr}),
            "end": ibis.struct({"line0": end_line_expr, "col": end_col_expr}),
            "end_exclusive": end_exclusive_expr,
            "col_unit": col_unit_expr,
            "byte_span": ibis.struct({"byte_start": byte_start, "byte_len": byte_len}),
        }
    ).cast(SPAN_STORAGE)


def add_ast_span_struct_ibis(
    line_index: SpanSource,
    py_ast_nodes: SpanSource,
    *,
    backend: BaseBackend,
) -> Table:
    """Append AST span structs using line index joins.

    Returns
    -------
    ibis.expr.types.Table
        AST nodes with span structs added.
    """
    ast = _table_expr(py_ast_nodes, backend=backend, name="ast_nodes")
    line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
    start_idx = _line_index_view(line_idx, prefix="start")
    end_idx = _line_index_view(line_idx, prefix="end")

    defaults = SpanCoordDefaults(base=1, unit="utf32", end_exclusive=True)
    base = _span_base_values(
        ast.line_base,
        ast.col_unit,
        ast.end_exclusive,
        defaults=defaults,
    )
    lines = _span_line_values(ast.lineno, ast.end_lineno, base.line_base)
    joined = _join_line_index(
        ast,
        start_idx,
        end_idx,
        spec=SpanJoinSpec(
            file_id=ast.file_id,
            start_line=lines.start_line0,
            end_line=lines.end_line0,
        ),
    )
    offsets = _span_offsets(
        SpanOffsetSpec(
            start_idx=start_idx,
            end_idx=end_idx,
            start_col=ast.col_offset,
            end_col=ast.end_col_offset,
            end_line_value=ast.end_lineno,
            col_unit=base.col_unit,
            end_exclusive=base.end_exclusive,
        )
    )
    end_col = _normalize_end_col(ast.end_col_offset, base.end_exclusive)
    span_inputs = SpanStructInputs(
        bstart=offsets.bstart,
        bend=offsets.bend,
        start_line0=lines.start_line0,
        end_line0=lines.end_line0,
        start_col=ast.col_offset,
        end_col=end_col,
        col_unit=base.col_unit,
        end_exclusive=base.end_exclusive,
    )
    span = _span_struct_expr(span_inputs)
    span_id = masked_stable_id_expr(
        "span",
        parts=(ast.path, offsets.bstart, offsets.bend),
        required=(ast.path, offsets.bstart, offsets.bend),
    )
    return joined.mutate(span=span, span_ok=offsets.span_ok, span_id=span_id)


def anchor_instructions_span_struct_ibis(
    line_index: SpanSource,
    py_bc_instructions: SpanSource,
    *,
    backend: BaseBackend,
) -> Table:
    """Anchor bytecode instruction spans using line index joins.

    Returns
    -------
    ibis.expr.types.Table
        Instruction table with span structs added.
    """
    instr = _table_expr(py_bc_instructions, backend=backend, name="py_bc_instructions")
    line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
    start_idx = _line_index_view(line_idx, prefix="start")
    end_idx = _line_index_view(line_idx, prefix="end")

    defaults = SpanCoordDefaults(base=1, unit="utf32", end_exclusive=True)
    base = _span_base_values(
        instr.line_base,
        instr.col_unit,
        instr.end_exclusive,
        defaults=defaults,
    )
    lines = _span_line_values(
        instr.pos_start_line,
        instr.pos_end_line,
        base.line_base,
    )
    joined = _join_line_index(
        instr,
        start_idx,
        end_idx,
        spec=SpanJoinSpec(
            file_id=instr.file_id,
            start_line=lines.start_line0,
            end_line=lines.end_line0,
        ),
    )
    offsets = _span_offsets(
        SpanOffsetSpec(
            start_idx=start_idx,
            end_idx=end_idx,
            start_col=instr.pos_start_col,
            end_col=instr.pos_end_col,
            end_line_value=instr.pos_end_line,
            col_unit=base.col_unit,
            end_exclusive=base.end_exclusive,
        )
    )
    end_col = _normalize_end_col(instr.pos_end_col, base.end_exclusive)
    span_inputs = SpanStructInputs(
        bstart=offsets.bstart,
        bend=offsets.bend,
        start_line0=lines.start_line0,
        end_line0=lines.end_line0,
        start_col=instr.pos_start_col,
        end_col=end_col,
        col_unit=base.col_unit,
        end_exclusive=base.end_exclusive,
    )
    span = _span_struct_expr(span_inputs)
    return joined.mutate(span=span, span_ok=offsets.span_ok)


def add_scip_occurrence_span_struct_ibis(
    line_index: SpanSource,
    scip_documents: SpanSource,
    scip_occurrences: SpanSource,
    *,
    backend: BaseBackend,
) -> tuple[Table, Table]:
    """Add span structs to SCIP occurrences using line index joins.

    Returns
    -------
    tuple[ibis.expr.types.Table, ibis.expr.types.Table]
        Occurrence table with spans plus span error rows.
    """
    occ = _table_expr(scip_occurrences, backend=backend, name="scip_occurrences")
    docs = _table_expr(scip_documents, backend=backend, name="scip_documents")
    line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
    joined = _scip_occurrence_join(occ, docs, line_idx)
    return _scip_occurrence_span_exprs(joined)


def normalize_cst_callsites_span_struct_ibis(
    py_cst_callsites: SpanSource,
    *,
    backend: BaseBackend,
    primary: Literal["callee", "call"] = "callee",
) -> Table:
    """Ensure callsites expose canonical span structs.

    Parameters
    ----------
    py_cst_callsites
        Callsite table or SQL fragment input.
    backend
        Ibis backend to use for expression execution.
    primary
        Which span to alias as ``bstart``/``bend`` ("callee" or "call").

    Returns
    -------
    ibis.expr.types.Table
        Callsites table with canonical span structs.
    """
    callsites = _table_expr(py_cst_callsites, backend=backend, name="cst_callsites")
    if primary == "call":
        bstart = callsites.call_bstart
        bend = callsites.call_bend
    else:
        bstart = callsites.callee_bstart
        bend = callsites.callee_bend
    span = _span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bend,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    return callsites.mutate(bstart=bstart, bend=bend, span=span)


def normalize_cst_imports_span_struct_ibis(
    py_cst_imports: SpanSource,
    *,
    backend: BaseBackend,
    primary: Literal["alias", "stmt"] = "alias",
) -> Table:
    """Ensure imports expose canonical span structs.

    Parameters
    ----------
    py_cst_imports
        Import table or SQL fragment input.
    backend
        Ibis backend to use for expression execution.
    primary
        Which span to alias as ``bstart``/``bend`` ("alias" or "stmt").

    Returns
    -------
    ibis.expr.types.Table
        Imports table with canonical span structs.
    """
    imports = _table_expr(py_cst_imports, backend=backend, name="cst_imports")
    if primary == "stmt":
        bstart = imports.stmt_bstart
        bend = imports.stmt_bend
    else:
        bstart = imports.alias_bstart
        bend = imports.alias_bend
    span = _span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bend,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    return imports.mutate(bstart=bstart, bend=bend, span=span)


def normalize_cst_defs_span_struct_ibis(
    py_cst_defs: SpanSource,
    *,
    backend: BaseBackend,
    primary: Literal["name", "def"] = "name",
) -> Table:
    """Ensure defs expose canonical span structs.

    Parameters
    ----------
    py_cst_defs
        Definition table or SQL fragment input.
    backend
        Ibis backend to use for expression execution.
    primary
        Which span to alias as ``bstart``/``bend`` ("name" or "def").

    Returns
    -------
    ibis.expr.types.Table
        Definitions table with canonical span structs.
    """
    defs = _table_expr(py_cst_defs, backend=backend, name="cst_defs")
    if primary == "def":
        bstart = defs.def_bstart
        bend = defs.def_bend
    else:
        bstart = defs.name_bstart
        bend = defs.name_bend
    span = _span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bend,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    return defs.mutate(bstart=bstart, bend=bend, span=span)


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
    posenc = position_encoding_norm_expr(occ_docs.position_encoding)
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
    span_inputs = SpanStructInputs(
        bstart=bstart,
        bend=bend,
        start_line0=joined.start_line0,
        end_line0=joined.end_line0,
        start_col=joined.start_char,
        end_col=joined.end_char_norm,
        col_unit=joined.col_unit_norm,
        end_exclusive=joined.end_exclusive_norm,
    )
    span = _span_struct_expr(span_inputs)
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
        span=span,
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


def _table_expr(source: SpanSource, *, backend: BaseBackend, name: str) -> Table:
    if isinstance(source, ViewReference):
        return _view_reference_expr(backend, source)
    if isinstance(source, Table):
        return source
    view_name = None if _is_datafusion_backend(backend) else name
    plan = table_to_ibis(source, options=SourceToIbisOptions(backend=backend, name=view_name))
    return plan.expr


def _is_datafusion_backend(backend: BaseBackend) -> bool:
    name = getattr(backend, "name", "")
    return str(name).lower() == "datafusion"


def _view_reference_expr(backend: BaseBackend, fragment: ViewReference) -> Table:
    return backend.table(fragment.name)


@dataclass(frozen=True)
class SpanCoordDefaults:
    base: int
    unit: str
    end_exclusive: bool


@dataclass(frozen=True)
class SpanBaseValues:
    line_base: Value
    col_unit: Value
    end_exclusive: Value


@dataclass(frozen=True)
class SpanLineValues:
    start_line0: Value
    end_line0: Value


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


@dataclass(frozen=True)
class SpanOffsets:
    bstart: Value
    bend: Value
    span_ok: Value


def _span_base_values(
    line_base: Value,
    col_unit: Value,
    end_exclusive: Value,
    *,
    defaults: SpanCoordDefaults,
) -> SpanBaseValues:
    return SpanBaseValues(
        line_base=_line_base_value(line_base, default_base=defaults.base),
        col_unit=_col_unit_value(col_unit, default_unit=defaults.unit),
        end_exclusive=_end_exclusive_value(end_exclusive, default_exclusive=defaults.end_exclusive),
    )


def _span_line_values(
    start_line: Value,
    end_line: Value,
    line_base: Value,
) -> SpanLineValues:
    return SpanLineValues(
        start_line0=_zero_based_line(start_line, line_base),
        end_line0=_zero_based_line(end_line, line_base),
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


def _span_offsets(spec: SpanOffsetSpec) -> SpanOffsets:
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
    return SpanOffsets(bstart=bstart, bend=bend, span_ok=span_ok)


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
    "add_ast_span_struct_ibis",
    "add_scip_occurrence_span_struct_ibis",
    "anchor_instructions_span_struct_ibis",
    "normalize_cst_callsites_span_struct_ibis",
    "normalize_cst_defs_span_struct_ibis",
    "normalize_cst_imports_span_struct_ibis",
]
