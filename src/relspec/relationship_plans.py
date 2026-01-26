"""Relationship plan builders for inference-first scheduling."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.metadata import ordering_from_schema
from cpg.kind_catalog import (
    EDGE_KIND_PY_CALLS_QNAME,
    EDGE_KIND_PY_CALLS_SYMBOL,
    EDGE_KIND_PY_DEFINES_SYMBOL,
    EDGE_KIND_PY_IMPORTS_SYMBOL,
    EDGE_KIND_PY_REFERENCES_SYMBOL,
)
from datafusion_engine.schema_registry import schema_for
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.hash_exprs import HashExprSpec, stable_id_expr_from_spec
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import (
    bind_expr_schema,
    coalesce_columns,
    ensure_columns,
    ibis_dtype_from_arrow,
    ibis_null_literal,
)
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from relspec.contracts import (
    rel_callsite_qname_schema,
    rel_callsite_symbol_schema,
    rel_def_symbol_schema,
    rel_import_symbol_schema,
    rel_name_symbol_schema,
    relation_output_schema,
)

if TYPE_CHECKING:
    from ibis.backends import BaseBackend
    from ibis.expr.types import Table, Value

    from arrowdsl.core.execution_context import ExecutionContext

REL_NAME_SYMBOL_OUTPUT = "rel_name_symbol_v1"
REL_IMPORT_SYMBOL_OUTPUT = "rel_import_symbol_v1"
REL_DEF_SYMBOL_OUTPUT = "rel_def_symbol_v1"
REL_CALLSITE_SYMBOL_OUTPUT = "rel_callsite_symbol_v1"
REL_CALLSITE_QNAME_OUTPUT = "rel_callsite_qname_v1"
RELATION_OUTPUT_NAME = "relation_output_v1"


def build_rel_name_symbol_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    task_name: str,
    task_priority: int,
) -> IbisPlan:
    """Return plan for name-to-symbol relationships.

    Returns
    -------
    IbisPlan
        Relation plan for name-to-symbol edges.
    """
    refs = _resolve_table(catalog, ctx=ctx, name="cst_refs")
    output = refs.mutate(
        ref_id=_col(refs, "ref_id", pa.string()),
        symbol=_col(refs, "ref_text", pa.string()),
        symbol_roles=_col(refs, "symbol_roles", pa.int32()),
        path=_col(refs, "path", pa.string()),
        edge_owner_file_id=_coalesce(refs, ("edge_owner_file_id", "file_id"), pa.string()),
        bstart=_col(refs, "bstart", pa.int64()),
        bend=_col(refs, "bend", pa.int64()),
        resolution_method=ibis.literal("cst_ref_text"),
        confidence=ibis.literal(0.5),
        score=ibis.literal(0.5),
        task_name=ibis.literal(task_name),
        task_priority=ibis.literal(task_priority),
    )
    return _select_plan(output, schema=rel_name_symbol_schema(), allow_extra_columns=ctx.debug)


def build_rel_import_symbol_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    task_name: str,
    task_priority: int,
) -> IbisPlan:
    """Return plan for import-to-symbol relationships.

    Returns
    -------
    IbisPlan
        Relation plan for import-to-symbol edges.
    """
    imports = _resolve_table(catalog, ctx=ctx, name="cst_imports")
    symbol = coalesce_columns(
        imports,
        ("name", "module"),
        default=ibis_null_literal(pa.string()),
    )
    bstart = _coalesce(imports, ("alias_bstart", "stmt_bstart"), pa.int64())
    bend = _coalesce(imports, ("alias_bend", "stmt_bend"), pa.int64())
    output = imports.mutate(
        import_alias_id=_coalesce(imports, ("import_alias_id", "import_id"), pa.string()),
        symbol=symbol,
        symbol_roles=_col(imports, "symbol_roles", pa.int32()),
        path=_col(imports, "path", pa.string()),
        edge_owner_file_id=_coalesce(imports, ("edge_owner_file_id", "file_id"), pa.string()),
        bstart=bstart,
        bend=bend,
        resolution_method=ibis.literal("cst_import_name"),
        confidence=ibis.literal(0.5),
        score=ibis.literal(0.5),
        task_name=ibis.literal(task_name),
        task_priority=ibis.literal(task_priority),
    )
    return _select_plan(output, schema=rel_import_symbol_schema(), allow_extra_columns=ctx.debug)


def build_rel_def_symbol_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    task_name: str,
    task_priority: int,
) -> IbisPlan:
    """Return plan for def-to-symbol relationships.

    Returns
    -------
    IbisPlan
        Relation plan for def-to-symbol edges.
    """
    defs = _resolve_table(catalog, ctx=ctx, name="cst_defs")
    bstart = _coalesce(defs, ("name_bstart", "def_bstart"), pa.int64())
    bend = _coalesce(defs, ("name_bend", "def_bend"), pa.int64())
    output = defs.mutate(
        def_id=_col(defs, "def_id", pa.string()),
        symbol=_coalesce(defs, ("name",), pa.string()),
        symbol_roles=_col(defs, "symbol_roles", pa.int32()),
        path=_col(defs, "path", pa.string()),
        edge_owner_file_id=_coalesce(defs, ("edge_owner_file_id", "file_id"), pa.string()),
        bstart=bstart,
        bend=bend,
        resolution_method=ibis.literal("cst_def_name"),
        confidence=ibis.literal(0.6),
        score=ibis.literal(0.6),
        task_name=ibis.literal(task_name),
        task_priority=ibis.literal(task_priority),
    )
    return _select_plan(output, schema=rel_def_symbol_schema(), allow_extra_columns=ctx.debug)


def build_rel_callsite_symbol_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    task_name: str,
    task_priority: int,
) -> IbisPlan:
    """Return plan for callsite-to-symbol relationships.

    Returns
    -------
    IbisPlan
        Relation plan for callsite-to-symbol edges.
    """
    callsites = _resolve_table(catalog, ctx=ctx, name="cst_callsites")
    symbol = coalesce_columns(
        callsites,
        ("callee_text", "callee_dotted"),
        default=ibis_null_literal(pa.string()),
    )
    output = callsites.mutate(
        call_id=_col(callsites, "call_id", pa.string()),
        symbol=symbol,
        symbol_roles=_col(callsites, "symbol_roles", pa.int32()),
        path=_col(callsites, "path", pa.string()),
        edge_owner_file_id=_coalesce(callsites, ("edge_owner_file_id", "file_id"), pa.string()),
        call_bstart=_col(callsites, "call_bstart", pa.int64()),
        call_bend=_col(callsites, "call_bend", pa.int64()),
        resolution_method=ibis.literal("cst_callsite"),
        confidence=ibis.literal(0.6),
        score=ibis.literal(0.6),
        task_name=ibis.literal(task_name),
        task_priority=ibis.literal(task_priority),
    )
    return _select_plan(
        output,
        schema=rel_callsite_symbol_schema(),
        allow_extra_columns=ctx.debug,
    )


def build_rel_callsite_qname_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    task_name: str,
    task_priority: int,
) -> IbisPlan:
    """Return plan for callsite-to-qname relationships.

    Returns
    -------
    IbisPlan
        Relation plan for callsite-to-qname edges.
    """
    candidates = _resolve_table(catalog, ctx=ctx, name="callsite_qname_candidates_v1")
    qname_id = stable_id_expr_from_spec(
        candidates,
        spec=HashExprSpec(prefix="qname", cols=("qname",), null_sentinel="None"),
    )
    output = candidates.mutate(
        call_id=_col(candidates, "call_id", pa.string()),
        qname_id=qname_id,
        qname_source=_col(candidates, "qname_source", pa.string()),
        path=_col(candidates, "path", pa.string()),
        edge_owner_file_id=_col(candidates, "edge_owner_file_id", pa.string()),
        call_bstart=_col(candidates, "call_bstart", pa.int64()),
        call_bend=_col(candidates, "call_bend", pa.int64()),
        confidence=ibis.literal(0.5),
        score=ibis.literal(0.5),
        ambiguity_group_id=_col(candidates, "ambiguity_group_id", pa.string()),
        task_name=ibis.literal(task_name),
        task_priority=ibis.literal(task_priority),
    )
    return _select_plan(
        output,
        schema=rel_callsite_qname_schema(),
        allow_extra_columns=ctx.debug,
    )


def build_relation_output_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan:
    """Return the unified relation_output plan.

    Returns
    -------
    IbisPlan
        Unified relation output plan.
    """
    rel_name = _resolve_table(catalog, ctx=ctx, name=REL_NAME_SYMBOL_OUTPUT)
    rel_import = _resolve_table(catalog, ctx=ctx, name=REL_IMPORT_SYMBOL_OUTPUT)
    rel_def = _resolve_table(catalog, ctx=ctx, name=REL_DEF_SYMBOL_OUTPUT)
    rel_call_symbol = _resolve_table(catalog, ctx=ctx, name=REL_CALLSITE_SYMBOL_OUTPUT)
    rel_call_qname = _resolve_table(catalog, ctx=ctx, name=REL_CALLSITE_QNAME_OUTPUT)
    parts = [
        _relation_output_from_name(rel_name),
        _relation_output_from_import(rel_import),
        _relation_output_from_def(rel_def),
        _relation_output_from_call_symbol(rel_call_symbol),
        _relation_output_from_call_qname(rel_call_qname),
    ]
    combined = _union_exprs(parts)
    output_schema = relation_output_schema()
    combined = ensure_columns(combined, schema=output_schema, only_missing=True)
    combined = combined.select(*output_schema.names)
    combined = bind_expr_schema(combined, schema=output_schema, allow_extra_columns=ctx.debug)
    return IbisPlan(expr=combined, ordering=ordering_from_schema(output_schema))


@dataclass(frozen=True)
class RelationOutputSpec:
    """Specification for relation output normalization."""

    src_col: str
    dst_col: str
    bstart_col: str
    bend_col: str
    kind: str
    origin: str
    qname_source: Value | None = None
    ambiguity_group_id: Value | None = None


def _relation_output_from_name(expr: Table) -> Table:
    return _relation_output_base(
        expr,
        RelationOutputSpec(
            src_col="ref_id",
            dst_col="symbol",
            bstart_col="bstart",
            bend_col="bend",
            kind=str(EDGE_KIND_PY_REFERENCES_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_import(expr: Table) -> Table:
    return _relation_output_base(
        expr,
        RelationOutputSpec(
            src_col="import_alias_id",
            dst_col="symbol",
            bstart_col="bstart",
            bend_col="bend",
            kind=str(EDGE_KIND_PY_IMPORTS_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_def(expr: Table) -> Table:
    return _relation_output_base(
        expr,
        RelationOutputSpec(
            src_col="def_id",
            dst_col="symbol",
            bstart_col="bstart",
            bend_col="bend",
            kind=str(EDGE_KIND_PY_DEFINES_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_call_symbol(expr: Table) -> Table:
    return _relation_output_base(
        expr,
        RelationOutputSpec(
            src_col="call_id",
            dst_col="symbol",
            bstart_col="call_bstart",
            bend_col="call_bend",
            kind=str(EDGE_KIND_PY_CALLS_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_call_qname(expr: Table) -> Table:
    return _relation_output_base(
        expr,
        RelationOutputSpec(
            src_col="call_id",
            dst_col="qname_id",
            bstart_col="call_bstart",
            bend_col="call_bend",
            kind=str(EDGE_KIND_PY_CALLS_QNAME),
            origin="cst",
            qname_source=_col(expr, "qname_source", pa.string()),
            ambiguity_group_id=_col(expr, "ambiguity_group_id", pa.string()),
        ),
    )


def _relation_output_base(expr: Table, spec: RelationOutputSpec) -> Table:
    return expr.select(
        src=_col(expr, spec.src_col, pa.string()),
        dst=_col(expr, spec.dst_col, pa.string()),
        path=_col(expr, "path", pa.string()),
        edge_owner_file_id=_coalesce(expr, ("edge_owner_file_id", "file_id"), pa.string()),
        bstart=_col(expr, spec.bstart_col, pa.int64()),
        bend=_col(expr, spec.bend_col, pa.int64()),
        origin=ibis.literal(spec.origin),
        resolution_method=_col(expr, "resolution_method", pa.string()),
        binding_kind=_col(expr, "binding_kind", pa.string()),
        def_site_kind=_col(expr, "def_site_kind", pa.string()),
        use_kind=_col(expr, "use_kind", pa.string()),
        kind=ibis.literal(spec.kind),
        reason=_col(expr, "reason", pa.string()),
        confidence=_col(expr, "confidence", pa.float32()),
        score=_col(expr, "score", pa.float32()),
        symbol_roles=_col(expr, "symbol_roles", pa.int32()),
        qname_source=spec.qname_source or _col(expr, "qname_source", pa.string()),
        ambiguity_group_id=spec.ambiguity_group_id or _col(expr, "ambiguity_group_id", pa.string()),
        diag_source=_col(expr, "diag_source", pa.string()),
        severity=_col(expr, "severity", pa.string()),
        task_name=_col(expr, "task_name", pa.string()),
        task_priority=_col(expr, "task_priority", pa.int32()),
    )


def _resolve_table(catalog: IbisPlanCatalog, *, ctx: ExecutionContext, name: str) -> Table:
    plan = catalog.resolve_plan(name, ctx=ctx, label=name)
    if plan is not None:
        return plan.expr
    try:
        schema = schema_for(name)
    except KeyError as exc:
        msg = f"Missing schema for relationship input {name!r}."
        raise KeyError(msg) from exc
    empty = empty_table(schema)
    plan = register_ibis_table(
        empty,
        options=SourceToIbisOptions(
            backend=catalog.backend,
            name=name,
            ordering=Ordering.unordered(),
            schema=schema,
        ),
    )
    catalog.add(name, plan)
    return plan.expr


def _select_plan(
    expr: Table,
    *,
    schema: pa.Schema,
    allow_extra_columns: bool,
) -> IbisPlan:
    expr = ensure_columns(expr, schema=schema, only_missing=True)
    expr = expr.select(*schema.names)
    expr = bind_expr_schema(expr, schema=schema, allow_extra_columns=allow_extra_columns)
    return IbisPlan(expr=expr, ordering=ordering_from_schema(schema))


def _union_exprs(exprs: Iterable[Table]) -> Table:
    iterator = iter(exprs)
    combined = next(iterator)
    for expr in iterator:
        combined = combined.union(expr)
    return combined


def _col(expr: Table, name: str, dtype: pa.DataType) -> Value:
    if name in expr.columns:
        return ibis.cast(expr[name], ibis_dtype_from_arrow(dtype))
    return ibis_null_literal(dtype)


def _coalesce(expr: Table, columns: Sequence[str], dtype: pa.DataType) -> Value:
    return coalesce_columns(expr, columns, default=ibis_null_literal(dtype))


__all__ = [
    "RELATION_OUTPUT_NAME",
    "REL_CALLSITE_QNAME_OUTPUT",
    "REL_CALLSITE_SYMBOL_OUTPUT",
    "REL_DEF_SYMBOL_OUTPUT",
    "REL_IMPORT_SYMBOL_OUTPUT",
    "REL_NAME_SYMBOL_OUTPUT",
    "build_rel_callsite_qname_plan",
    "build_rel_callsite_symbol_plan",
    "build_rel_def_symbol_plan",
    "build_rel_import_symbol_plan",
    "build_rel_name_symbol_plan",
    "build_relation_output_plan",
]
