"""View builders for normalize outputs."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import ArrayValue, BooleanValue, NumericValue, StringValue, Table, Value

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import Ordering, OrderingKey
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.metadata import infer_ordering_keys, ordering_from_schema
from datafusion_engine.extract_registry import dataset_schema as extract_dataset_schema
from datafusion_engine.normalize_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from datafusion_engine.schema_registry import (
    DIAG_DETAILS_TYPE,
    SCIP_VIEW_SCHEMA_MAP,
    schema_for,
)
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.expr_compiler import OperationSupportBackend, preflight_portability
from ibis_engine.hash_exprs import (
    HashExprSpec,
    masked_stable_id_expr_from_spec,
    stable_id_expr_from_spec,
    stable_key_hash_expr_from_spec,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import (
    bind_expr_schema,
    coalesce_columns,
    ibis_null_literal,
    validate_expr_schema,
)
from ibis_engine.sources import (
    SourceToIbisOptions,
    register_ibis_table,
)
from normalize.registry_runtime import dataset_input_schema, dataset_schema
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

TYPE_EXPRS_NAME = "type_exprs_norm_v1"
TYPE_NODES_NAME = "type_nodes_v1"
CFG_BLOCKS_NAME = "py_bc_blocks_norm_v1"
CFG_EDGES_NAME = "py_bc_cfg_edges_norm_v1"
DEF_USE_NAME = "py_bc_def_use_events_v1"
REACHES_NAME = "py_bc_reaches_v1"
DIAG_NAME = "diagnostics_norm_v1"

_DEF_USE_OPS: tuple[str, ...] = ("IMPORT_NAME", "IMPORT_FROM")
_DEF_USE_PREFIXES: tuple[str, ...] = ("STORE_", "DELETE_")
_USE_PREFIXES: tuple[str, ...] = ("LOAD_",)


def _drop_columns(table: Table, names: Sequence[str]) -> Table:
    cols = [name for name in names if name in table.columns]
    return table.drop(*cols) if cols else table


def type_exprs_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for normalized type expressions.

    Returns
    -------
    IbisPlan | None
        Ibis plan for normalized type expressions.
    """
    input_schema = dataset_input_schema(TYPE_EXPRS_NAME)
    table = catalog.resolve_expr("cst_type_exprs", ctx=ctx, schema=input_schema)
    validate_expr_schema(table, expected=input_schema, allow_extra_columns=ctx.debug)
    expr_text = table.expr_text.cast("string")
    trimmed = expr_text.strip()
    non_empty = trimmed.notnull() & (trimmed.length() > ibis.literal(0))
    filtered = table.filter(non_empty)
    trimmed = filtered.expr_text.cast("string").strip()
    filtered = filtered.mutate(type_repr=trimmed)
    type_expr_id = masked_stable_id_expr_from_spec(
        filtered,
        spec=HashExprSpec(
            prefix=TYPE_EXPR_ID_SPEC.prefix,
            cols=("path", "bstart", "bend"),
            null_sentinel=TYPE_EXPR_ID_SPEC.null_sentinel,
        ),
        required=("path", "bstart", "bend"),
    )
    type_id = stable_id_expr_from_spec(
        filtered,
        spec=HashExprSpec(
            prefix=TYPE_ID_SPEC.prefix,
            cols=("type_repr",),
            null_sentinel=TYPE_ID_SPEC.null_sentinel,
        ),
    )
    span = span_struct_expr(
        SpanStructInputs(
            bstart=filtered.bstart,
            bend=filtered.bend,
            col_unit=filtered.col_unit if "col_unit" in filtered.columns else None,
            end_exclusive=filtered.end_exclusive if "end_exclusive" in filtered.columns else None,
        )
    )
    updates: dict[str, Value] = {
        "type_expr_id": type_expr_id,
        "type_id": type_id,
        "span": span,
    }
    if ctx.debug:
        updates["type_expr_key"] = stable_key_hash_expr_from_spec(
            filtered,
            spec=HashExprSpec(
                prefix=TYPE_EXPR_ID_SPEC.prefix,
                cols=("path", "bstart", "bend"),
                null_sentinel=TYPE_EXPR_ID_SPEC.null_sentinel,
                as_string=True,
            ),
            use_128=False,
        )
        updates["type_id_key"] = stable_key_hash_expr_from_spec(
            filtered,
            spec=HashExprSpec(
                prefix=TYPE_ID_SPEC.prefix,
                cols=("type_repr",),
                null_sentinel=TYPE_ID_SPEC.null_sentinel,
                as_string=True,
            ),
            use_128=False,
        )
    enriched = filtered.mutate(**updates)
    enriched = _drop_columns(
        enriched,
        ("bstart", "bend", "line_base", "col_unit", "end_exclusive"),
    )
    validate_expr_schema(
        enriched,
        expected=dataset_schema(TYPE_EXPRS_NAME),
        allow_extra_columns=ctx.debug,
    )
    enriched = bind_expr_schema(
        enriched,
        schema=dataset_schema(TYPE_EXPRS_NAME),
        allow_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=enriched, ordering=Ordering.unordered())


def type_nodes_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for normalized type nodes.

    Returns
    -------
    IbisPlan | None
        Ibis plan for normalized type nodes.
    """
    type_node_columns = _type_node_columns(ctx)
    expr_schema = dataset_schema(TYPE_EXPRS_NAME)
    expr_rows = _expr_type_rows(
        catalog.resolve_expr(
            "type_exprs_norm_v1",
            ctx=ctx,
            schema=expr_schema,
        ),
        ctx=ctx,
        type_node_columns=type_node_columns,
    )
    scip_schema = schema_for(SCIP_VIEW_SCHEMA_MAP["scip_symbol_information"])
    scip_rows = _scip_type_rows(
        catalog.resolve_expr(
            "scip_symbol_information",
            ctx=ctx,
            schema=scip_schema,
        ),
        ctx=ctx,
        type_node_columns=type_node_columns,
    )
    combined = _prefer_type_rows(expr_rows, scip_rows)
    target_schema = dataset_schema(TYPE_NODES_NAME)
    validate_expr_schema(
        combined,
        expected=target_schema,
        allow_extra_columns=ctx.debug,
    )
    combined = bind_expr_schema(
        combined,
        schema=target_schema,
        allow_extra_columns=ctx.debug,
    )
    ordering_keys = _ordering_keys_for_schema(target_schema)
    ordering = Ordering.explicit(ordering_keys) if ordering_keys else Ordering.unordered()
    return IbisPlan(expr=combined, ordering=ordering)


def _type_node_columns(ctx: ExecutionContext) -> list[str]:
    columns = ["type_id", "type_repr", "type_form", "origin"]
    if ctx.debug:
        columns.append("type_id_key")
    return columns


def _ordering_keys_for_schema(schema: pa.Schema) -> tuple[OrderingKey, ...]:
    ordering = ordering_from_schema(schema)
    if ordering.keys:
        return ordering.keys
    return infer_ordering_keys(schema.names)


def _expr_type_rows(
    exprs: Table,
    *,
    ctx: ExecutionContext,
    type_node_columns: Sequence[str],
) -> Table:
    expr_trimmed = exprs.type_repr.cast("string").strip()
    expr_non_empty = expr_trimmed.notnull() & (expr_trimmed.length() > ibis.literal(0))
    expr_valid = expr_non_empty & exprs.type_id.notnull()
    expr_rows = exprs.filter(expr_valid).mutate(
        type_repr=expr_trimmed,
        type_form=ibis.literal("annotation"),
        origin=ibis.literal("annotation"),
    )
    if ctx.debug:
        expr_rows = expr_rows.mutate(
            type_id_key=stable_key_hash_expr_from_spec(
                expr_rows,
                spec=HashExprSpec(
                    prefix=TYPE_ID_SPEC.prefix,
                    cols=("type_repr",),
                    null_sentinel=TYPE_ID_SPEC.null_sentinel,
                    as_string=True,
                ),
                use_128=False,
            )
        )
    return expr_rows.select([expr_rows[col] for col in type_node_columns])


def _scip_type_rows(
    scip: Table,
    *,
    ctx: ExecutionContext,
    type_node_columns: Sequence[str],
) -> Table | None:
    if "type_repr" not in scip.columns:
        return None
    expected_schema = schema_for(SCIP_VIEW_SCHEMA_MAP["scip_symbol_information"])
    validate_expr_schema(scip, expected=expected_schema, allow_extra_columns=True)
    scip_trimmed = scip.type_repr.cast("string").strip()
    scip_non_empty = scip_trimmed.notnull() & (scip_trimmed.length() > ibis.literal(0))
    scip_rows = scip.filter(scip_non_empty).mutate(
        type_repr=scip_trimmed,
        type_form=ibis.literal("scip"),
        origin=ibis.literal("inferred"),
    )
    scip_rows = scip_rows.mutate(
        type_id=stable_id_expr_from_spec(
            scip_rows,
            spec=HashExprSpec(
                prefix=TYPE_ID_SPEC.prefix,
                cols=("type_repr",),
                null_sentinel=TYPE_ID_SPEC.null_sentinel,
            ),
        )
    )
    if ctx.debug:
        scip_rows = scip_rows.mutate(
            type_id_key=stable_key_hash_expr_from_spec(
                scip_rows,
                spec=HashExprSpec(
                    prefix=TYPE_ID_SPEC.prefix,
                    cols=("type_repr",),
                    null_sentinel=TYPE_ID_SPEC.null_sentinel,
                    as_string=True,
                ),
                use_128=False,
            )
        )
    return scip_rows.select([scip_rows[col] for col in type_node_columns])


def _prefer_type_rows(expr_rows: Table, scip_rows: Table | None) -> Table:
    if scip_rows is None:
        return expr_rows
    scip_preview = scip_rows.limit(1).to_pyarrow()
    if scip_preview.num_rows > 0:
        return scip_rows
    return expr_rows


def cfg_blocks_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for normalized CFG blocks.

    Returns
    -------
    IbisPlan | None
        Ibis plan for normalized CFG blocks.
    """
    input_schema = dataset_input_schema(CFG_BLOCKS_NAME)
    blocks = catalog.resolve_expr("py_bc_blocks", ctx=ctx, schema=input_schema)
    validate_expr_schema(blocks, expected=input_schema, allow_extra_columns=ctx.debug)
    code_units = catalog.resolve_expr(
        "py_bc_code_units",
        ctx=ctx,
        schema=schema_for("py_bc_code_units"),
    )
    if "code_unit_id" in blocks.columns and "code_unit_id" in code_units.columns:
        code_units = code_units.select(
            code_unit_id=code_units.code_unit_id,
            code_unit_file_id=code_units.file_id,
            code_unit_path=code_units.path,
        )
        joined = blocks.left_join(
            code_units,
            predicates=[blocks.code_unit_id == code_units.code_unit_id],
        )
        joined = joined.mutate(
            file_id=coalesce_columns(
                joined,
                ("file_id", "code_unit_file_id"),
                default=ibis_null_literal(pa.string()),
            ),
            path=coalesce_columns(
                joined,
                ("path", "code_unit_path"),
                default=ibis_null_literal(pa.string()),
            ),
        )
    else:
        joined = blocks
    bstart = joined.start_offset.cast("int64")
    bend = ibis.coalesce(joined.end_offset.cast("int64"), bstart)
    span = span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bend,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    joined = joined.mutate(span=span)
    validate_expr_schema(joined, expected=dataset_schema(CFG_BLOCKS_NAME))
    joined = bind_expr_schema(joined, schema=dataset_schema(CFG_BLOCKS_NAME))
    return IbisPlan(expr=joined, ordering=Ordering.unordered())


def cfg_edges_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for normalized CFG edges.

    Returns
    -------
    IbisPlan | None
        Ibis plan for normalized CFG edges.
    """
    input_schema = dataset_input_schema(CFG_EDGES_NAME)
    edges = catalog.resolve_expr("py_bc_cfg_edges", ctx=ctx, schema=input_schema)
    validate_expr_schema(edges, expected=input_schema, allow_extra_columns=ctx.debug)
    code_units = catalog.resolve_expr(
        "py_bc_code_units",
        ctx=ctx,
        schema=schema_for("py_bc_code_units"),
    )
    if "code_unit_id" in edges.columns and "code_unit_id" in code_units.columns:
        code_units = code_units.select(
            code_unit_id=code_units.code_unit_id,
            code_unit_file_id=code_units.file_id,
            code_unit_path=code_units.path,
        )
        joined = edges.left_join(
            code_units,
            predicates=[edges.code_unit_id == code_units.code_unit_id],
        )
        joined = joined.mutate(
            file_id=coalesce_columns(
                joined,
                ("file_id", "code_unit_file_id"),
                default=ibis_null_literal(pa.string()),
            ),
            path=coalesce_columns(
                joined,
                ("path", "code_unit_path"),
                default=ibis_null_literal(pa.string()),
            ),
        )
    else:
        joined = edges
    validate_expr_schema(joined, expected=dataset_schema(CFG_EDGES_NAME))
    joined = bind_expr_schema(joined, schema=dataset_schema(CFG_EDGES_NAME))
    return IbisPlan(expr=joined, ordering=Ordering.unordered())


def def_use_events_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for bytecode def/use events.

    Returns
    -------
    IbisPlan | None
        Ibis plan for bytecode def/use events.
    """
    input_schema = dataset_input_schema(DEF_USE_NAME)
    table = catalog.resolve_expr("py_bc_instructions", ctx=ctx, schema=input_schema)
    validate_expr_schema(table, expected=input_schema, allow_extra_columns=ctx.debug)
    symbol = coalesce_columns(table, ("argval_str", "argrepr"))
    kind = _def_use_kind_expr(table.opname)
    base = table.mutate(symbol=symbol, kind=kind)
    event_id = stable_id_expr_from_spec(
        base,
        spec=HashExprSpec(
            prefix=DEF_USE_EVENT_ID_SPEC.prefix,
            cols=("code_unit_id", "instr_id", "kind", "symbol"),
            null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
        ),
    )
    bstart = table.offset.cast("int64")
    span = span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bstart,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    valid = base.symbol.notnull() & base.kind.notnull()
    updates: dict[str, Value] = {
        "event_id": event_id,
        "span": span,
    }
    if ctx.debug:
        updates["event_key"] = stable_key_hash_expr_from_spec(
            base,
            spec=HashExprSpec(
                prefix=DEF_USE_EVENT_ID_SPEC.prefix,
                cols=("code_unit_id", "instr_id", "kind", "symbol"),
                null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
                as_string=True,
            ),
            use_128=False,
        )
    enriched = base.filter(valid).mutate(**updates)
    validate_expr_schema(
        enriched,
        expected=dataset_schema(DEF_USE_NAME),
        allow_extra_columns=ctx.debug,
    )
    enriched = bind_expr_schema(
        enriched,
        schema=dataset_schema(DEF_USE_NAME),
        allow_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=enriched, ordering=Ordering.unordered())


def reaching_defs_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for reaching-def edges.

    Returns
    -------
    IbisPlan | None
        Ibis plan for reaching-def edges.
    """
    input_schema = dataset_schema(DEF_USE_NAME)
    table = catalog.resolve_expr("py_bc_def_use_events_v1", ctx=ctx, schema=input_schema)
    required = {"kind", "code_unit_id", "symbol", "event_id"}
    if not required.issubset(set(table.columns)):
        return register_ibis_table(
            empty_table(dataset_schema(REACHES_NAME)),
            options=SourceToIbisOptions(
                backend=backend,
                name=None,
                ordering=Ordering.unordered(),
                runtime_profile=ctx.runtime.datafusion,
            ),
        )
    defs = table.filter(table.kind == ibis.literal("def")).select(
        code_unit_id=table.code_unit_id,
        symbol=table.symbol,
        def_event_id=table.event_id,
    )
    uses = table.filter(table.kind == ibis.literal("use")).select(
        code_unit_id=table.code_unit_id,
        symbol=table.symbol,
        use_event_id=table.event_id,
        path=table.path if "path" in table.columns else ibis_null_literal(pa.string()),
        file_id=table.file_id if "file_id" in table.columns else ibis_null_literal(pa.string()),
    )
    joined = defs.join(
        uses,
        predicates=[defs.code_unit_id == uses.code_unit_id, defs.symbol == uses.symbol],
        how="inner",
    )
    edge_id = stable_id_expr_from_spec(
        joined,
        spec=HashExprSpec(
            prefix=REACH_EDGE_ID_SPEC.prefix,
            cols=("def_event_id", "use_event_id"),
            null_sentinel=REACH_EDGE_ID_SPEC.null_sentinel,
        ),
    )
    updates: dict[str, Value] = {"edge_id": edge_id}
    if ctx.debug:
        updates["edge_key"] = stable_key_hash_expr_from_spec(
            joined,
            spec=HashExprSpec(
                prefix=REACH_EDGE_ID_SPEC.prefix,
                cols=("def_event_id", "use_event_id"),
                null_sentinel=REACH_EDGE_ID_SPEC.null_sentinel,
                as_string=True,
            ),
            use_128=False,
        )
    enriched = joined.mutate(**updates)
    validate_expr_schema(
        enriched,
        expected=dataset_schema(REACHES_NAME),
        allow_extra_columns=ctx.debug,
    )
    enriched = bind_expr_schema(
        enriched,
        schema=dataset_schema(REACHES_NAME),
        allow_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=enriched, ordering=Ordering.unordered())


def _line_index_view(line_index: Table, *, prefix: str) -> Table:
    return line_index.select(
        **{
            f"{prefix}_file_id": line_index.file_id,
            f"{prefix}_path": line_index.path,
            f"{prefix}_line_no": line_index.line_no,
            f"{prefix}_line_start_byte": line_index.line_start_byte,
            f"{prefix}_line_end_byte": line_index.line_end_byte,
            f"{prefix}_line_text": line_index.line_text,
        }
    )


def _non_empty_string(value: Value, *, default: str) -> Value:
    text = value.cast("string")
    return ibis.ifelse(
        text.notnull() & (text.length() > ibis.literal(0)),
        text,
        ibis.literal(default),
    )


def _scip_severity_expr(value: Value) -> Value:
    text = value.cast("string").upper()
    values = [ibis.literal(level) for level in ("ERROR", "WARNING", "INFO", "HINT")]
    return ibis.cases(
        (text == ibis.literal("1"), ibis.literal("ERROR")),
        (text == ibis.literal("2"), ibis.literal("WARNING")),
        (text == ibis.literal("3"), ibis.literal("INFO")),
        (text == ibis.literal("4"), ibis.literal("HINT")),
        (text.isin(values), text),
        else_=ibis.literal("ERROR"),
    )


def _cst_diag_expr(cst: Table, line_index: Table) -> Table:
    line_base = line_base_value(cst.line_base, default_base=1)
    col_unit = normalize_col_unit_expr(cst.col_unit, default_unit="utf32")
    end_exclusive = end_exclusive_value(cst.end_exclusive, default_exclusive=True)
    start_line0 = zero_based_line(cst.raw_line, line_base)
    start_idx = _line_index_view(line_index, prefix="cst")
    joined = cst.join(
        start_idx,
        [cst.path == start_idx.cst_path, start_line0 == start_idx.cst_line_no],
        how="left",
    )
    bstart = line_offset_expr(
        joined.cst_line_start_byte,
        joined.cst_line_text,
        joined.raw_column,
        col_unit,
    )
    path_expr = ibis.coalesce(joined.path, joined.cst_path)
    file_id_expr = ibis.coalesce(joined.file_id, joined.cst_file_id)
    end_col = normalize_end_col(joined.raw_column, end_exclusive)
    span = span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bstart,
            start_line0=start_line0,
            end_line0=start_line0,
            start_col=joined.raw_column,
            end_col=end_col,
            col_unit=col_unit,
            end_exclusive=end_exclusive,
        )
    )
    base = joined.select(
        file_id=file_id_expr,
        path=path_expr,
        bstart=bstart,
        bend=bstart,
        span=span,
        severity=ibis.literal("ERROR"),
        message=_non_empty_string(joined.message, default="LibCST parse error"),
        diag_source=ibis.literal("libcst"),
        code=ibis_null_literal(pa.string()),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
    )
    return base.filter(base.bstart.notnull() & base.bend.notnull() & base.path.notnull())


def _ts_diag_expr(table: Table, *, severity: str, message: str) -> Table:
    bstart = coalesce_columns(
        table,
        ("bstart", "start_byte"),
        default=ibis_null_literal(pa.int64()),
    ).cast("int64")
    bend = coalesce_columns(
        table,
        ("bend", "end_byte"),
        default=ibis_null_literal(pa.int64()),
    ).cast("int64")
    span = span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bend,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    file_id = table.file_id if "file_id" in table.columns else ibis_null_literal(pa.string())
    path = table.path if "path" in table.columns else ibis_null_literal(pa.string())
    base = table.select(
        file_id=file_id,
        path=path,
        bstart=bstart,
        bend=bend,
        span=span,
        severity=ibis.literal(severity),
        message=ibis.literal(message),
        diag_source=ibis.literal("treesitter"),
        code=ibis_null_literal(pa.string()),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
    )
    return base.filter(base.bstart.notnull() & base.bend.notnull() & base.path.notnull())


@dataclass(frozen=True)
class _ScipDiagContext:
    joined: Table
    path_expr: Value
    col_unit: Value
    end_char: Value
    line_base: Value
    end_exclusive: Value


def _scip_diag_expr(diags: Table, docs: Table, line_index: Table) -> Table:
    ctx = _scip_diag_context(diags, docs, line_index)
    bstart = line_offset_expr(
        ctx.joined.scip_start_line_start_byte,
        ctx.joined.scip_start_line_text,
        ctx.joined.start_char,
        ctx.col_unit,
    )
    bend = line_offset_expr(
        ctx.joined.scip_end_line_start_byte,
        ctx.joined.scip_end_line_text,
        ctx.end_char,
        ctx.col_unit,
    )
    span = span_struct_expr(
        SpanStructInputs(
            bstart=bstart,
            bend=bend,
            start_line0=ctx.joined.scip_start_line0,
            end_line0=ctx.joined.scip_end_line0,
            start_col=ctx.joined.start_char,
            end_col=ctx.end_char,
            col_unit=ctx.col_unit,
            end_exclusive=ctx.end_exclusive,
        )
    )
    file_id = coalesce_columns(
        ctx.joined,
        ("file_id", "scip_start_file_id"),
        default=ibis_null_literal(pa.string()),
    )
    base = ctx.joined.select(
        file_id=file_id,
        path=ctx.path_expr,
        bstart=bstart,
        bend=bend,
        span=span,
        severity=_scip_severity_expr(ctx.joined.severity),
        message=_non_empty_string(ctx.joined.message, default="SCIP diagnostic"),
        diag_source=ibis.literal("scip"),
        code=ctx.joined.code.cast("string"),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
    )
    return base.filter(base.bstart.notnull() & base.bend.notnull() & base.path.notnull())


def _symtable_bytecode_diag_expr(scopes: Table, code_units: Table, line_index: Table) -> Table:
    """Return diagnostics for symtable vs bytecode mismatches.

    Returns
    -------
    ibis.expr.types.Table
        Diagnostics rows for symtable/bytecode consistency checks.
    """
    joined = _symtable_bytecode_join(scopes, code_units=code_units, line_index=line_index)
    base, param_mismatch, freevars_mismatch = _symtable_bytecode_base(joined)
    param_message = ibis.concat(
        ibis.literal("symtable param count mismatch: symtable="),
        base.sym_param_count.cast("string"),
        ibis.literal(" bytecode="),
        base.bc_param_count.cast("string"),
        ibis.literal(" scope_id="),
        base.scope_id.cast("string"),
        ibis.literal(" code_unit_id="),
        base.code_unit_id.cast("string"),
    )
    free_message = ibis.concat(
        ibis.literal("symtable freevars mismatch: symtable="),
        base.sym_free_count.cast("string"),
        ibis.literal(" bytecode="),
        base.bc_free_count.cast("string"),
        ibis.literal(" scope_id="),
        base.scope_id.cast("string"),
        ibis.literal(" code_unit_id="),
        base.code_unit_id.cast("string"),
    )
    param_diag = _symtable_diag_row(
        base,
        message=param_message,
        code="SYM_BC_PARAM_COUNT_MISMATCH",
    ).filter(param_mismatch & base.code_unit_id.notnull())
    free_diag = _symtable_diag_row(
        base,
        message=free_message,
        code="SYM_BC_FREEVARS_MISMATCH",
    ).filter(freevars_mismatch & base.code_unit_id.notnull())
    combined = param_diag.union(free_diag, distinct=False)
    return combined.filter(
        combined.bstart.notnull() & combined.bend.notnull() & combined.path.notnull()
    )


def _symtable_bytecode_join(
    scopes: Table,
    *,
    code_units: Table,
    line_index: Table,
) -> Table:
    function_scopes = scopes.filter(scopes.scope_type == ibis.literal("FUNCTION"))
    code_qualpath = ibis.coalesce(code_units.co_qualname, code_units.qualpath)
    joined = function_scopes.join(
        code_units,
        [
            function_scopes.file_id == code_units.file_id,
            function_scopes.qualpath == code_qualpath,
        ],
        how="left",
    )
    line_view = _line_index_view(line_index, prefix="sym")
    return joined.join(
        line_view,
        [joined.file_id == line_view.sym_file_id, joined.lineno == line_view.sym_line_no],
        how="left",
    )


def _symtable_bytecode_base(
    joined: Table,
) -> tuple[Table, BooleanValue, BooleanValue]:
    """Build the symtable/bytecode comparison base table.

    Returns
    -------
    tuple[Table, BooleanValue, BooleanValue]
        Base table plus parameter/freevar mismatch flags.
    """
    unpacked = _unpack_symtable_structs(joined)
    bstart = unpacked.sym_line_start_byte.cast("int64")
    sym_line_text = cast("StringValue", unpacked.sym_line_text)
    bend = ibis.coalesce(
        unpacked.sym_line_end_byte.cast("int64"),
        bstart + sym_line_text.length().cast("int64"),
    )
    sym_param_count, bc_param_count = _symtable_param_counts(unpacked)
    _sym_frees, _bc_freevars, sym_free_count, bc_free_count, freevars_mismatch = (
        _symtable_freevar_counts(unpacked)
    )
    param_mismatch = sym_param_count != bc_param_count
    base = unpacked.select(
        file_id=unpacked.file_id,
        path=unpacked.path,
        bstart=bstart,
        bend=bend,
        scope_id=unpacked.scope_id,
        code_unit_id=unpacked.code_unit_id,
        sym_param_count=sym_param_count,
        bc_param_count=bc_param_count,
        sym_free_count=sym_free_count,
        bc_free_count=bc_free_count,
    )
    return base, param_mismatch, freevars_mismatch


def _unpack_symtable_structs(joined: Table) -> Table:
    """Unpack nested symtable struct columns into top-level fields.

    Returns
    -------
    Table
        Table with nested struct fields unpacked when present.
    """
    expr = joined
    if "function_partitions" in expr.columns:
        expr = expr.unpack("function_partitions")
    if "flags_detail" in expr.columns:
        expr = expr.unpack("flags_detail")
    return expr


def _symtable_param_counts(
    joined: Table,
) -> tuple[NumericValue, NumericValue]:
    empty_list = _empty_string_list()
    if "parameters" in joined.columns:
        sym_params = ibis.coalesce(joined.parameters, empty_list)
    else:
        sym_params = empty_list
    sym_param_count = cast("ArrayValue", sym_params).length()
    argcount = _coalesce_int64(joined.argcount)
    posonly = _coalesce_int64(joined.posonlyargcount)
    kwonly = _coalesce_int64(joined.kwonlyargcount)
    has_varargs = (
        ibis.coalesce(joined.has_varargs, ibis.literal(value=False))
        if "has_varargs" in joined.columns
        else ibis.literal(value=False)
    )
    has_varkeywords = (
        ibis.coalesce(joined.has_varkeywords, ibis.literal(value=False))
        if "has_varkeywords" in joined.columns
        else ibis.literal(value=False)
    )
    var_extra = cast(
        "NumericValue",
        ibis.ifelse(has_varargs, ibis.literal(value=1), ibis.literal(value=0)).cast("int64"),
    ) + cast(
        "NumericValue",
        ibis.ifelse(has_varkeywords, ibis.literal(value=1), ibis.literal(value=0)).cast("int64"),
    )
    bc_param_count = argcount + posonly + kwonly + var_extra
    return sym_param_count, bc_param_count


def _symtable_freevar_counts(
    joined: Table,
) -> tuple[Value, Value, NumericValue, NumericValue, BooleanValue]:
    empty_list = _empty_string_list()
    sym_frees = ibis.coalesce(joined.frees, empty_list) if "frees" in joined.columns else empty_list
    bc_freevars = ibis.coalesce(joined.freevars, empty_list)
    sym_free_count = cast("ArrayValue", sym_frees).length()
    bc_free_count = cast("ArrayValue", bc_freevars).length()
    freevars_mismatch = sym_frees != bc_freevars
    return sym_frees, bc_freevars, sym_free_count, bc_free_count, freevars_mismatch


def _symtable_diag_row(base: Table, *, message: StringValue, code: str) -> Table:
    span = span_struct_expr(
        SpanStructInputs(
            bstart=base.bstart,
            bend=base.bend,
            col_unit=ibis.literal("byte"),
            end_exclusive=ibis.literal(value=True),
        )
    )
    return base.select(
        file_id=base.file_id,
        path=base.path,
        bstart=base.bstart,
        bend=base.bend,
        span=span,
        severity=ibis.literal("WARNING"),
        message=message,
        diag_source=ibis.literal("symtable_bytecode"),
        code=ibis.literal(code),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
    )


def _empty_string_list() -> Value:
    return ibis.literal([], type=pa.list_(pa.string()))


def _coalesce_int64(value: Value) -> NumericValue:
    return cast("NumericValue", ibis.coalesce(value, ibis.literal(value=0)).cast("int64"))


def _scip_diag_context(diags: Table, docs: Table, line_index: Table) -> _ScipDiagContext:
    docs_sel = docs.select(
        document_id=docs.document_id,
        doc_path=docs.path,
        position_encoding=docs.position_encoding,
    )
    diag_docs = diags.join(
        docs_sel,
        [diags.document_id == docs_sel.document_id],
        how="left",
    )
    path_expr = ibis.coalesce(diag_docs.path, diag_docs.doc_path)
    line_base = line_base_value(diag_docs.line_base, default_base=0)
    end_exclusive = end_exclusive_value(diag_docs.end_exclusive, default_exclusive=True)
    col_unit = normalize_col_unit_expr(
        diag_docs.col_unit.cast("string").lower(),
        position_encoding=diag_docs.position_encoding,
    )
    start_line0 = zero_based_line(diag_docs.start_line, line_base)
    end_line0 = zero_based_line(diag_docs.end_line, line_base)
    end_char = normalize_end_col(diag_docs.end_char, end_exclusive)
    diag_docs = diag_docs.mutate(
        scip_path=path_expr,
        scip_line_base=line_base,
        scip_end_exclusive=end_exclusive,
        scip_col_unit=col_unit,
        scip_end_char=end_char,
        scip_start_line0=start_line0,
        scip_end_line0=end_line0,
    )
    start_idx = _line_index_view(line_index, prefix="scip_start")
    end_idx = _line_index_view(line_index, prefix="scip_end")
    joined = diag_docs.join(
        start_idx,
        [
            diag_docs.scip_path == start_idx.scip_start_path,
            diag_docs.scip_start_line0 == start_idx.scip_start_line_no,
        ],
        how="left",
    )
    joined = joined.join(
        end_idx,
        [
            joined.scip_path == end_idx.scip_end_path,
            joined.scip_end_line0 == end_idx.scip_end_line_no,
        ],
        how="left",
    )
    return _ScipDiagContext(
        joined=joined,
        path_expr=joined.scip_path,
        col_unit=joined.scip_col_unit,
        end_char=joined.scip_end_char,
        line_base=joined.scip_line_base,
        end_exclusive=joined.scip_end_exclusive,
    )


def diagnostics_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for normalized diagnostics.

    Returns
    -------
    IbisPlan | None
        Ibis plan for normalized diagnostics.
    """
    diag_schema = dataset_schema(DIAG_NAME)
    line_index = _resolve_input(
        catalog, ctx=ctx, name="file_line_index", schema="file_line_index_v1"
    )
    exprs = _diagnostic_exprs(catalog, ctx=ctx, line_index=line_index)

    if not exprs:
        return register_ibis_table(
            empty_table(diag_schema),
            options=SourceToIbisOptions(
                backend=backend,
                name=None,
                ordering=Ordering.unordered(),
                runtime_profile=ctx.runtime.datafusion,
            ),
        )

    combined = exprs[0]
    for expr in exprs[1:]:
        combined = combined.union(expr, distinct=False)

    diag_id = stable_id_expr_from_spec(
        combined,
        spec=HashExprSpec(
            prefix=DIAG_ID_SPEC.prefix,
            cols=("path", "bstart", "bend", "diag_source", "message"),
            null_sentinel=DIAG_ID_SPEC.null_sentinel,
        ),
    )
    updates: dict[str, Value] = {"diag_id": diag_id}
    if ctx.debug:
        updates["diag_key"] = stable_key_hash_expr_from_spec(
            combined,
            spec=HashExprSpec(
                prefix=DIAG_ID_SPEC.prefix,
                cols=("path", "bstart", "bend", "diag_source", "message"),
                null_sentinel=DIAG_ID_SPEC.null_sentinel,
                as_string=True,
            ),
            use_128=False,
        )
    enriched = combined.mutate(**updates)
    enriched = _drop_columns(
        enriched,
        ("bstart", "bend", "line_base", "col_unit", "end_exclusive"),
    )
    validate_expr_schema(
        enriched,
        expected=diag_schema,
        allow_extra_columns=ctx.debug,
    )
    enriched = bind_expr_schema(
        enriched,
        schema=diag_schema,
        allow_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=enriched, ordering=Ordering.unordered())


def _resolve_input(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    name: str,
    schema: str,
) -> Table:
    resolved_schema = extract_dataset_schema(schema)
    table = catalog.resolve_expr(name, ctx=ctx, schema=resolved_schema)
    validate_expr_schema(table, expected=resolved_schema, allow_extra_columns=ctx.debug)
    return table


def _diagnostic_exprs(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    line_index: Table,
) -> list[Table]:
    cst = _resolve_input(catalog, ctx=ctx, name="cst_parse_errors", schema="py_cst_parse_errors_v1")
    ts_errors = _resolve_input(catalog, ctx=ctx, name="ts_errors", schema="ts_errors_v1")
    ts_missing = _resolve_input(catalog, ctx=ctx, name="ts_missing", schema="ts_missing_v1")
    scip_diags = _resolve_input(
        catalog, ctx=ctx, name="scip_diagnostics", schema="scip_diagnostics_v1"
    )
    scip_docs = _resolve_input(catalog, ctx=ctx, name="scip_documents", schema="scip_documents_v1")
    symtable_scopes = _resolve_input(
        catalog, ctx=ctx, name="symtable_scopes", schema="symtable_scopes"
    )
    code_units = _resolve_input(
        catalog, ctx=ctx, name="py_bc_code_units", schema="py_bc_code_units"
    )
    return [
        _cst_diag_expr(cst, line_index),
        _ts_diag_expr(ts_errors, severity="ERROR", message="tree-sitter error node"),
        _ts_diag_expr(ts_missing, severity="WARNING", message="tree-sitter missing node"),
        _scip_diag_expr(scip_diags, scip_docs, line_index),
        _symtable_bytecode_diag_expr(symtable_scopes, code_units, line_index),
    ]


def span_errors_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan | None:
    """Build an Ibis plan for span error rows.

    Returns
    -------
    IbisPlan | None
        Ibis plan for span error rows.
    """
    schema = dataset_schema("span_errors_v1")
    table = catalog.resolve_expr("span_errors_v1", ctx=ctx, schema=schema)
    validate_expr_schema(table, expected=schema)
    table = bind_expr_schema(table, schema=schema)
    return IbisPlan(expr=table, ordering=Ordering.unordered())


def _def_use_kind_expr(opname: Value) -> Value:
    op_str = opname.cast("string")
    def_values = [ibis.literal(value) for value in _DEF_USE_OPS]
    is_def = op_str.isin(def_values) | op_str.startswith(_DEF_USE_PREFIXES[0])
    is_def |= op_str.startswith(_DEF_USE_PREFIXES[1])
    is_use = op_str.startswith(_USE_PREFIXES[0])
    return ibis.ifelse(
        is_def,
        ibis.literal("def"),
        ibis.ifelse(is_use, ibis.literal("use"), ibis.null()),
    )


def _backend_dialect(backend: BaseBackend) -> str | None:
    dialect = getattr(backend, "dialect", None)
    if isinstance(dialect, str):
        return dialect
    return None


@dataclass(frozen=True)
class OpFallbackPayload:
    builder_name: str
    missing_ops: tuple[str, ...]
    fallback_reason: str | None
    error: str | None = None


def _record_op_fallback(
    ctx: ExecutionContext,
    *,
    backend: BaseBackend,
    payload: OpFallbackPayload,
) -> None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return
    record = {
        "event_time_unix_ms": int(time.time() * 1000),
        "stage": "normalize",
        "builder": payload.builder_name,
        "backend": type(backend).__name__,
        "missing_ops": list(payload.missing_ops),
        "fallback_reason": payload.fallback_reason,
        "error": payload.error,
    }
    from datafusion_engine.diagnostics import record_artifact

    record_artifact(profile, "ibis_op_fallback_v1", record)


def _apply_portability_fallback(
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    builder_name: str,
) -> IbisPlan:
    dialect = _backend_dialect(backend)
    try:
        result = preflight_portability(
            plan.expr,
            backend=cast("OperationSupportBackend", backend),
            dialect=dialect,
        )
    except ValueError as exc:
        _record_op_fallback(
            ctx,
            backend=backend,
            payload=OpFallbackPayload(
                builder_name=builder_name,
                missing_ops=(),
                fallback_reason="fallback_failed",
                error=str(exc),
            ),
        )
        raise
    if result.missing_ops:
        _record_op_fallback(
            ctx,
            backend=backend,
            payload=OpFallbackPayload(
                builder_name=builder_name,
                missing_ops=result.missing_ops,
                fallback_reason=result.fallback_reason,
            ),
        )
    if result.expr is plan.expr:
        return plan
    return IbisPlan(expr=result.expr, ordering=plan.ordering)


IbisPlanDeriver = Callable[[IbisPlanCatalog, ExecutionContext, BaseBackend], IbisPlan | None]


def plan_builders_ibis() -> Mapping[str, IbisPlanDeriver]:
    """Return registered Ibis plan builders.

    Returns
    -------
    Mapping[str, IbisPlanDeriver]
        Registered Ibis plan builder mapping.
    """
    return {
        "type_exprs": type_exprs_plan_ibis,
        "type_nodes": type_nodes_plan_ibis,
        "cfg_blocks": cfg_blocks_plan_ibis,
        "cfg_edges": cfg_edges_plan_ibis,
        "def_use_events": def_use_events_plan_ibis,
        "reaching_defs": reaching_defs_plan_ibis,
        "diagnostics": diagnostics_plan_ibis,
        "span_errors": span_errors_plan_ibis,
    }


def resolve_plan_builder_ibis(name: str) -> IbisPlanDeriver:
    """Return a registered Ibis plan builder by name.

    Returns
    -------
    IbisPlanDeriver
        Plan builder for the requested name.

    Raises
    ------
    KeyError
        Raised when the plan builder name is unknown.
    """
    builders = plan_builders_ibis()
    try:
        resolved_builder = builders[name]
    except KeyError as exc:
        msg = f"Unknown normalize Ibis plan builder: {name!r}."
        raise KeyError(msg) from exc

    def _wrapped(
        catalog: IbisPlanCatalog,
        ctx: ExecutionContext,
        backend: BaseBackend,
    ) -> IbisPlan | None:
        plan = resolved_builder(catalog, ctx, backend)
        if plan is None:
            return None
        return _apply_portability_fallback(
            plan,
            ctx=ctx,
            backend=backend,
            builder_name=name,
        )

    return _wrapped


__all__ = [
    "IbisPlanCatalog",
    "IbisPlanDeriver",
    "cfg_blocks_plan_ibis",
    "cfg_edges_plan_ibis",
    "def_use_events_plan_ibis",
    "diagnostics_plan_ibis",
    "plan_builders_ibis",
    "reaching_defs_plan_ibis",
    "resolve_plan_builder_ibis",
    "span_errors_plan_ibis",
    "type_exprs_plan_ibis",
    "type_nodes_plan_ibis",
]
