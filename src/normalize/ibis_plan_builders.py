"""Ibis plan builders for normalize outputs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table, Value

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.interop import Table as ArrowTable
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.scan_io import PlanSource, plan_from_source
from arrowdsl.schema.build import empty_table
from ibis_engine.ids import masked_stable_id_expr, stable_id_expr
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import SourceToIbisOptions, source_to_ibis
from ibis_engine.schema_utils import (
    align_table_to_schema,
    coalesce_columns,
    ensure_columns,
    ibis_null_literal,
)
from normalize.diagnostics_plans import DiagnosticsSources, diagnostics_plan
from normalize.registry_ids import (
    DEF_USE_EVENT_ID_SPEC,
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

IbisPlanSource = PlanSource | IbisPlan | Table


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
        """
        source = self.tables.get(name)
        if source is None:
            empty = empty_table(schema)
            return ibis.memtable(empty)
        if isinstance(source, IbisPlan):
            return source.expr
        if isinstance(source, Table):
            return source
        plan_source = plan_from_source(cast("PlanSource", source), ctx=ctx, label=name)
        plan = source_to_ibis(
            plan_source,
            options=SourceToIbisOptions(
                ctx=ctx,
                backend=self.backend,
                name=name,
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
    enriched = filtered.mutate(type_repr=trimmed, type_expr_id=type_expr_id, type_id=type_id)
    aligned = align_table_to_schema(enriched, schema=dataset_schema(TYPE_EXPRS_NAME))
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
    expr_schema = pa.schema(
        [
            pa.field("type_id", pa.string()),
            pa.field("type_repr", pa.string()),
        ]
    )
    scip_schema = pa.schema([pa.field("type_repr", pa.string())])
    exprs = catalog.resolve_expr("type_exprs_norm_v1", ctx=ctx, schema=expr_schema)
    scip = catalog.resolve_expr("scip_symbol_information", ctx=ctx, schema=scip_schema)

    scip_trimmed = scip.type_repr.cast("string").strip()
    scip_non_empty = scip_trimmed.notnull() & (scip_trimmed.length() > ibis.literal(0))
    scip_rows = scip.filter(scip_non_empty).mutate(
        type_repr=scip_trimmed,
        type_id=stable_id_expr(TYPE_ID_SPEC.prefix, scip_trimmed),
        type_form=ibis.literal("scip"),
        origin=ibis.literal("inferred"),
    )

    expr_trimmed = exprs.type_repr.cast("string").strip()
    expr_non_empty = expr_trimmed.notnull() & (expr_trimmed.length() > ibis.literal(0))
    expr_valid = expr_non_empty & exprs.type_id.notnull()
    expr_rows = exprs.filter(expr_valid).mutate(
        type_repr=expr_trimmed,
        type_form=ibis.literal("annotation"),
        origin=ibis.literal("annotation"),
    )

    scip_count = scip_rows.count()
    use_scip = scip_count > ibis.literal(0)
    combined = scip_rows.filter(use_scip).union(expr_rows.filter(~use_scip))
    aligned = align_table_to_schema(combined, schema=dataset_schema(TYPE_NODES_NAME))
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


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
    enriched = table.filter(valid).mutate(symbol=symbol, kind=kind, event_id=event_id)
    aligned = align_table_to_schema(enriched, schema=dataset_schema(DEF_USE_NAME))
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
    enriched = joined.mutate(edge_id=edge_id)
    aligned = align_table_to_schema(enriched, schema=dataset_schema(REACHES_NAME))
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


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
    if catalog.repo_text_index is None:
        empty = ibis.memtable(empty_table(dataset_schema(DIAG_NAME)))
        return IbisPlan(expr=empty, ordering=Ordering.unordered())
    sources = DiagnosticsSources(
        cst_parse_errors=_resolve_table_like(catalog, "cst_parse_errors"),
        ts_errors=_resolve_table_like(catalog, "ts_errors"),
        ts_missing=_resolve_table_like(catalog, "ts_missing"),
        scip_diagnostics=_resolve_table_like(catalog, "scip_diagnostics"),
        scip_documents=_resolve_table_like(catalog, "scip_documents"),
    )
    plan = diagnostics_plan(catalog.repo_text_index, sources=sources, ctx=ctx)
    ibis_plan = source_to_ibis(
        plan,
        options=SourceToIbisOptions(
            ctx=ctx,
            backend=backend,
            name=DIAG_NAME,
        ),
    )
    return IbisPlan(expr=ibis_plan.expr, ordering=ibis_plan.ordering)


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


def _resolve_table_like(catalog: IbisPlanCatalog, name: str) -> TableLike | None:
    source = catalog.tables.get(name)
    if isinstance(source, IbisPlan):
        return source.expr.to_pyarrow()
    if isinstance(source, Table):
        return source.to_pyarrow()
    if isinstance(source, ArrowTable):
        return source
    return None


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
