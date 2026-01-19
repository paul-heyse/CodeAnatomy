"""Ibis plan builders for normalize outputs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, NumericValue, Table, Value

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.core.ordering import Ordering
from arrowdsl.core.ordering_policy import ordering_keys_for_schema
from arrowdsl.core.position_encoding import ENC_UTF8, ENC_UTF16, ENC_UTF32
from arrowdsl.schema.build import empty_table
from extract.registry_specs import dataset_schema as extract_dataset_schema
from ibis_engine.builtin_udfs import col_to_byte, position_encoding_norm
from ibis_engine.ids import masked_stable_id_expr, stable_id_expr, stable_key_expr
from ibis_engine.plan import IbisPlan
from ibis_engine.scan_io import DatasetSource
from ibis_engine.schema_utils import (
    align_table_to_schema,
    coalesce_columns,
    ensure_columns,
    ibis_null_literal,
)
from ibis_engine.sources import SourceToIbisOptions, namespace_recorder_from_ctx, source_to_ibis
from normalize.registry_fields import DIAG_DETAILS_TYPE
from normalize.registry_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from normalize.registry_specs import (
    dataset_input_schema,
    dataset_schema,
)
from normalize.text_index import RepoTextIndex

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

IbisPlanSource = IbisPlan | Table | TableLike


@dataclass
class IbisPlanCatalog:
    """Catalog wrapper for Ibis plan inputs."""

    backend: BaseBackend
    tables: dict[str, IbisPlanSource] = field(default_factory=dict)
    repo_text_index: RepoTextIndex | None = None

    def resolve_expr(
        self,
        name: str,
        *,
        ctx: ExecutionContext,
        schema: pa.Schema,
    ) -> Table:
        """Resolve an input table into an Ibis expression.

        Returns
        -------
        ibis.expr.types.Table
            Ibis table expression for the named input.

        Raises
        ------
        TypeError
            Raised when a DatasetSource must be materialized before use.
        """
        source = self.tables.get(name)
        if source is None:
            empty = empty_table(schema)
            return ibis.memtable(empty)
        if isinstance(source, IbisPlan):
            return source.expr
        if isinstance(source, Table):
            return source
        if isinstance(source, DatasetSource):
            msg = f"DatasetSource {name!r} must be materialized before normalize builds."
            raise TypeError(msg)
        plan = source_to_ibis(
            cast("TableLike", source),
            options=SourceToIbisOptions(
                backend=self.backend,
                name=name,
                ordering=Ordering.unordered(),
                namespace_recorder=namespace_recorder_from_ctx(ctx),
            ),
        )
        self.tables[name] = plan
        return plan.expr

    def add(self, name: str, plan: IbisPlan) -> None:
        """Add a derived Ibis plan to the catalog."""
        self.tables[name] = plan


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
    table = ensure_columns(table, schema=input_schema)
    expr_text = table.expr_text.cast("string")
    trimmed = expr_text.strip()
    non_empty = trimmed.notnull() & (trimmed.length() > ibis.literal(0))
    filtered = table.filter(non_empty)
    type_expr_id = masked_stable_id_expr(
        TYPE_EXPR_ID_SPEC.prefix,
        parts=(filtered.path, filtered.bstart, filtered.bend),
        required=(filtered.path, filtered.bstart, filtered.bend),
        null_sentinel=TYPE_EXPR_ID_SPEC.null_sentinel,
    )
    type_id = stable_id_expr(
        TYPE_ID_SPEC.prefix,
        trimmed,
        null_sentinel=TYPE_ID_SPEC.null_sentinel,
    )
    updates: dict[str, Value] = {
        "type_repr": trimmed,
        "type_expr_id": type_expr_id,
        "type_id": type_id,
    }
    if ctx.debug:
        updates["type_expr_key"] = stable_key_expr(
            filtered.path,
            filtered.bstart,
            filtered.bend,
            prefix=TYPE_EXPR_ID_SPEC.prefix,
            null_sentinel=TYPE_EXPR_ID_SPEC.null_sentinel,
        )
        updates["type_id_key"] = stable_key_expr(
            trimmed,
            prefix=TYPE_ID_SPEC.prefix,
            null_sentinel=TYPE_ID_SPEC.null_sentinel,
        )
    enriched = filtered.mutate(**updates)
    aligned = align_table_to_schema(
        enriched,
        schema=dataset_schema(TYPE_EXPRS_NAME),
        keep_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


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
    expr_rows = _expr_type_rows(
        catalog.resolve_expr(
            "type_exprs_norm_v1",
            ctx=ctx,
            schema=_expr_type_schema(),
        ),
        ctx=ctx,
        type_node_columns=type_node_columns,
    )
    scip_rows = _scip_type_rows(
        catalog.resolve_expr(
            "scip_symbol_information",
            ctx=ctx,
            schema=_scip_type_schema(),
        ),
        ctx=ctx,
        type_node_columns=type_node_columns,
    )
    combined = _prefer_type_rows(expr_rows, scip_rows)
    target_schema = dataset_schema(TYPE_NODES_NAME)
    aligned = align_table_to_schema(
        combined,
        schema=target_schema,
        keep_extra_columns=ctx.debug,
    )
    ordering_keys = ordering_keys_for_schema(target_schema)
    ordering = Ordering.explicit(ordering_keys) if ordering_keys else Ordering.unordered()
    return IbisPlan(expr=aligned, ordering=ordering)


def _expr_type_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("type_id", pa.string()),
            pa.field("type_repr", pa.string()),
        ]
    )


def _scip_type_schema() -> pa.Schema:
    return pa.schema([pa.field("type_repr", pa.string())])


def _type_node_columns(ctx: ExecutionContext) -> list[str]:
    columns = ["type_id", "type_repr", "type_form", "origin"]
    if ctx.debug:
        columns.append("type_id_key")
    return columns


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
            type_id_key=stable_key_expr(
                expr_trimmed,
                prefix=TYPE_ID_SPEC.prefix,
                null_sentinel=TYPE_ID_SPEC.null_sentinel,
            )
        )
    return expr_rows.select(*[expr_rows[name] for name in type_node_columns])


def _scip_type_rows(
    scip: Table,
    *,
    ctx: ExecutionContext,
    type_node_columns: Sequence[str],
) -> Table | None:
    if "type_repr" not in scip.columns:
        return None
    scip = ensure_columns(scip, schema=_scip_type_schema())
    scip_trimmed = scip.type_repr.cast("string").strip()
    scip_non_empty = scip_trimmed.notnull() & (scip_trimmed.length() > ibis.literal(0))
    scip_rows = scip.filter(scip_non_empty).mutate(
        type_repr=scip_trimmed,
        type_id=stable_id_expr(TYPE_ID_SPEC.prefix, scip_trimmed),
        type_form=ibis.literal("scip"),
        origin=ibis.literal("inferred"),
    )
    if ctx.debug:
        scip_rows = scip_rows.mutate(
            type_id_key=stable_key_expr(
                scip_trimmed,
                prefix=TYPE_ID_SPEC.prefix,
                null_sentinel=TYPE_ID_SPEC.null_sentinel,
            )
        )
    return scip_rows.select(*[scip_rows[name] for name in type_node_columns])


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
    blocks = ensure_columns(blocks, schema=input_schema)
    meta_schema = pa.schema(
        [
            pa.field("code_unit_id", pa.string()),
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
        ]
    )
    code_units = catalog.resolve_expr("py_bc_code_units", ctx=ctx, schema=meta_schema)
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
    aligned = align_table_to_schema(joined, schema=dataset_schema(CFG_BLOCKS_NAME))
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


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
    edges = ensure_columns(edges, schema=input_schema)
    meta_schema = pa.schema(
        [
            pa.field("code_unit_id", pa.string()),
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
        ]
    )
    code_units = catalog.resolve_expr("py_bc_code_units", ctx=ctx, schema=meta_schema)
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
    aligned = align_table_to_schema(joined, schema=dataset_schema(CFG_EDGES_NAME))
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


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
    table = ensure_columns(table, schema=input_schema)
    symbol = coalesce_columns(table, ("argval_str", "argrepr"))
    kind = _def_use_kind_expr(table.opname)
    event_id = stable_id_expr(
        DEF_USE_EVENT_ID_SPEC.prefix,
        table.code_unit_id,
        table.instr_id,
        kind,
        symbol,
        null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
    )
    valid = symbol.notnull() & kind.notnull()
    updates: dict[str, Value] = {"symbol": symbol, "kind": kind, "event_id": event_id}
    if ctx.debug:
        updates["event_key"] = stable_key_expr(
            table.code_unit_id,
            table.instr_id,
            kind,
            symbol,
            prefix=DEF_USE_EVENT_ID_SPEC.prefix,
            null_sentinel=DEF_USE_EVENT_ID_SPEC.null_sentinel,
        )
    enriched = table.filter(valid).mutate(**updates)
    aligned = align_table_to_schema(
        enriched,
        schema=dataset_schema(DEF_USE_NAME),
        keep_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


def reaching_defs_plan_ibis(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
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
        empty = ibis.memtable(empty_table(dataset_schema(REACHES_NAME)))
        return IbisPlan(expr=empty, ordering=Ordering.unordered())
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
    edge_id = stable_id_expr(
        REACH_EDGE_ID_SPEC.prefix,
        joined.def_event_id,
        joined.use_event_id,
        null_sentinel=REACH_EDGE_ID_SPEC.null_sentinel,
    )
    updates: dict[str, Value] = {"edge_id": edge_id}
    if ctx.debug:
        updates["edge_key"] = stable_key_expr(
            joined.def_event_id,
            joined.use_event_id,
            prefix=REACH_EDGE_ID_SPEC.prefix,
            null_sentinel=REACH_EDGE_ID_SPEC.null_sentinel,
        )
    enriched = joined.mutate(**updates)
    aligned = align_table_to_schema(
        enriched,
        schema=dataset_schema(REACHES_NAME),
        keep_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


def _line_base_value(line_base: Value, *, default_base: int) -> NumericValue:
    result = ibis.coalesce(line_base.cast("int32"), ibis.literal(default_base))
    return cast("NumericValue", result)


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


def _line_offset_expr(
    line_start: Value,
    line_text: Value,
    column: Value,
    col_unit: Value,
) -> Value:
    offset = column.cast("int64")
    byte_in_line = col_to_byte(line_text, offset, col_unit.cast("string"))
    left = line_start.cast("int64")
    right = byte_in_line.cast("int64")
    return left + right


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
    line_base = _line_base_value(cst.line_base, default_base=1)
    col_unit = _col_unit_value(cst.col_unit, default_unit="utf32")
    end_exclusive = _end_exclusive_value(cst.end_exclusive, default_exclusive=True)
    start_line0 = _zero_based_line(cst.raw_line, line_base)
    start_idx = _line_index_view(line_index, prefix="cst")
    joined = cst.join(
        start_idx,
        [cst.path == start_idx.cst_path, start_line0 == start_idx.cst_line_no],
        how="left",
    )
    bstart = _line_offset_expr(
        joined.cst_line_start_byte,
        joined.cst_line_text,
        joined.raw_column,
        col_unit,
    )
    path_expr = ibis.coalesce(joined.path, joined.cst_path)
    file_id_expr = ibis.coalesce(joined.file_id, joined.cst_file_id)
    base = joined.select(
        file_id=file_id_expr,
        path=path_expr,
        bstart=bstart,
        bend=bstart,
        severity=ibis.literal("ERROR"),
        message=_non_empty_string(joined.message, default="LibCST parse error"),
        diag_source=ibis.literal("libcst"),
        code=ibis_null_literal(pa.string()),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
        line_base=line_base,
        col_unit=col_unit.cast("string"),
        end_exclusive=end_exclusive,
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
    file_id = table.file_id if "file_id" in table.columns else ibis_null_literal(pa.string())
    path = table.path if "path" in table.columns else ibis_null_literal(pa.string())
    base = table.select(
        file_id=file_id,
        path=path,
        bstart=bstart,
        bend=bend,
        severity=ibis.literal(severity),
        message=ibis.literal(message),
        diag_source=ibis.literal("treesitter"),
        code=ibis_null_literal(pa.string()),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
        line_base=ibis_null_literal(pa.int32()),
        col_unit=ibis_null_literal(pa.string()),
        end_exclusive=ibis_null_literal(pa.bool_()),
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
    bstart = _line_offset_expr(
        ctx.joined.scip_start_line_start_byte,
        ctx.joined.scip_start_line_text,
        ctx.joined.start_char,
        ctx.col_unit,
    )
    bend = _line_offset_expr(
        ctx.joined.scip_end_line_start_byte,
        ctx.joined.scip_end_line_text,
        ctx.end_char,
        ctx.col_unit,
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
        severity=_scip_severity_expr(ctx.joined.severity),
        message=_non_empty_string(ctx.joined.message, default="SCIP diagnostic"),
        diag_source=ibis.literal("scip"),
        code=ctx.joined.code.cast("string"),
        details=ibis_null_literal(DIAG_DETAILS_TYPE),
        line_base=ctx.line_base,
        col_unit=ctx.col_unit.cast("string"),
        end_exclusive=ctx.end_exclusive,
    )
    return base.filter(base.bstart.notnull() & base.bend.notnull() & base.path.notnull())


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
    line_base = _line_base_value(diag_docs.line_base, default_base=0)
    end_exclusive = _end_exclusive_value(diag_docs.end_exclusive, default_exclusive=True)
    posenc = position_encoding_norm(diag_docs.position_encoding.cast("string"))
    col_unit = ibis.coalesce(
        diag_docs.col_unit.cast("string").lower(),
        _col_unit_from_encoding(posenc),
    )
    start_line0 = _zero_based_line(diag_docs.start_line, line_base)
    end_line0 = _zero_based_line(diag_docs.end_line, line_base)
    end_char = _normalize_end_col(diag_docs.end_char, end_exclusive)
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
    _backend: BaseBackend,
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
        empty = ibis.memtable(empty_table(diag_schema))
        return IbisPlan(expr=empty, ordering=Ordering.unordered())

    combined = exprs[0]
    for expr in exprs[1:]:
        combined = combined.union(expr, distinct=False)

    diag_id = stable_id_expr(
        DIAG_ID_SPEC.prefix,
        combined.path,
        combined.bstart,
        combined.bend,
        combined.diag_source,
        combined.message,
        null_sentinel=DIAG_ID_SPEC.null_sentinel,
    )
    updates: dict[str, Value] = {"diag_id": diag_id}
    if ctx.debug:
        updates["diag_key"] = stable_key_expr(
            combined.path,
            combined.bstart,
            combined.bend,
            combined.diag_source,
            combined.message,
            prefix=DIAG_ID_SPEC.prefix,
            null_sentinel=DIAG_ID_SPEC.null_sentinel,
        )
    enriched = combined.mutate(**updates)
    aligned = align_table_to_schema(
        enriched,
        schema=diag_schema,
        keep_extra_columns=ctx.debug,
    )
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


def _resolve_input(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    name: str,
    schema: str,
) -> Table:
    resolved_schema = extract_dataset_schema(schema)
    table = catalog.resolve_expr(name, ctx=ctx, schema=resolved_schema)
    return ensure_columns(table, schema=resolved_schema)


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
    return [
        _cst_diag_expr(cst, line_index),
        _ts_diag_expr(ts_errors, severity="ERROR", message="tree-sitter error node"),
        _ts_diag_expr(ts_missing, severity="WARNING", message="tree-sitter missing node"),
        _scip_diag_expr(scip_diags, scip_docs, line_index),
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
    aligned = align_table_to_schema(table, schema=schema)
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


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
    builder = builders.get(name)
    if builder is None:
        msg = f"Unknown normalize Ibis plan builder: {name!r}."
        raise KeyError(msg)
    return builder


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
