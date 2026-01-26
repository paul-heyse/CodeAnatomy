"""Ibis-based span normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

import ibis
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, Table, Value

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import empty_table
from datafusion_engine.nested_tables import ViewReference
from ibis_engine.hash_exprs import HashExprSpec, masked_stable_id_expr_from_spec
from ibis_engine.sources import SourceToIbisOptions, table_to_ibis
from normalize.schemas import SPAN_ERROR_SCHEMA
from normalize.span_logic import (
    SpanStructInputs,
    end_exclusive_value,
    line_base_value,
    line_offset_expr,
    normalize_col_unit_expr,
    normalize_end_col,
    span_struct_expr,
    zero_based_line,
)

SpanSource = TableLike | Table | ViewReference


def _empty_span_errors_table() -> TableLike:
    return empty_table(SPAN_ERROR_SCHEMA)


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
    end_col = normalize_end_col(ast.end_col_offset, base.end_exclusive)
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
    span = span_struct_expr(span_inputs)
    with_offsets = joined.mutate(
        __span_bstart=offsets.bstart,
        __span_bend=offsets.bend,
    )
    span_id = masked_stable_id_expr_from_spec(
        with_offsets,
        spec=HashExprSpec(
            prefix="span",
            cols=("path", "__span_bstart", "__span_bend"),
            null_sentinel="None",
        ),
        required=("path", "__span_bstart", "__span_bend"),
    )
    return with_offsets.mutate(
        span=span,
        span_ok=offsets.span_ok,
        span_id=span_id,
    ).drop("__span_bstart", "__span_bend")


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
    end_col = normalize_end_col(instr.pos_end_col, base.end_exclusive)
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
    span = span_struct_expr(span_inputs)
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
    span = span_struct_expr(
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
    span = span_struct_expr(
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
    span = span_struct_expr(
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
    line_base = line_base_value(occ_docs.line_base, default_base=0)
    end_exclusive = end_exclusive_value(occ_docs.end_exclusive, default_exclusive=True)
    col_unit = normalize_col_unit_expr(
        occ_docs.col_unit,
        position_encoding=occ_docs.position_encoding,
    )
    occ_docs = occ_docs.mutate(
        resolved_path=path_expr,
        line_base_norm=line_base,
        end_exclusive_norm=end_exclusive,
        col_unit_norm=col_unit,
        start_line0=zero_based_line(occ_docs.start_line, line_base),
        end_line0=zero_based_line(occ_docs.end_line, line_base),
        enc_start_line0=zero_based_line(occ_docs.enc_start_line, line_base),
        enc_end_line0=zero_based_line(occ_docs.enc_end_line, line_base),
        end_char_norm=normalize_end_col(occ_docs.end_char, end_exclusive),
        enc_end_char_norm=normalize_end_col(occ_docs.enc_end_char, end_exclusive),
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
    bstart = line_offset_expr(
        joined.start_line_start_byte,
        joined.start_line_text,
        joined.start_char,
        joined.col_unit_norm,
    )
    bend = line_offset_expr(
        joined.end_line_start_byte,
        joined.end_line_text,
        joined.end_char_norm,
        joined.col_unit_norm,
    )
    enc_bstart_raw = line_offset_expr(
        joined.enc_start_line_start_byte,
        joined.enc_start_line_text,
        joined.enc_start_char,
        joined.col_unit_norm,
    )
    enc_bend_raw = line_offset_expr(
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
    span = span_struct_expr(span_inputs)
    occ_expr = joined.mutate(
        bstart=bstart,
        bend=bend,
        enc_bstart=enc_bstart,
        enc_bend=enc_bend,
        span=span,
        span_ok=span_ok,
    )
    span_id = masked_stable_id_expr_from_spec(
        occ_expr,
        spec=HashExprSpec(
            prefix="span",
            cols=("resolved_path", "bstart", "bend"),
            null_sentinel="None",
        ),
        required=("resolved_path", "bstart", "bend"),
    )
    occ_expr = occ_expr.mutate(span_id=span_id)
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
        line_base=line_base_value(line_base, default_base=defaults.base),
        col_unit=normalize_col_unit_expr(col_unit, default_unit=defaults.unit),
        end_exclusive=end_exclusive_value(end_exclusive, default_exclusive=defaults.end_exclusive),
    )


def _span_line_values(
    start_line: Value,
    end_line: Value,
    line_base: Value,
) -> SpanLineValues:
    return SpanLineValues(
        start_line0=zero_based_line(start_line, line_base),
        end_line0=zero_based_line(end_line, line_base),
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
    end_col_norm = normalize_end_col(spec.end_col, spec.end_exclusive)
    bstart = line_offset_expr(
        spec.start_idx.start_line_start_byte,
        spec.start_idx.start_line_text,
        spec.start_col,
        spec.col_unit,
    )
    bend_raw = line_offset_expr(
        spec.end_idx.end_line_start_byte,
        spec.end_idx.end_line_text,
        end_col_norm,
        spec.col_unit,
    )
    end_ok = spec.end_line_value.notnull() & end_col_norm.notnull()
    bend = ibis.ifelse(end_ok, bend_raw, bstart)
    span_ok = bstart.notnull() & bend.notnull()
    return SpanOffsets(bstart=bstart, bend=bend, span_ok=span_ok)


__all__ = [
    "add_ast_span_struct_ibis",
    "add_scip_occurrence_span_struct_ibis",
    "anchor_instructions_span_struct_ibis",
    "normalize_cst_callsites_span_struct_ibis",
    "normalize_cst_defs_span_struct_ibis",
    "normalize_cst_imports_span_struct_ibis",
]
