"""Adapter helpers for bridging ArrowDSL plans into Ibis plans."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol, cast

import ibis
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from ibis_engine.plan import IbisPlan


class ViewBackend(Protocol):
    """Protocol for backends supporting view registration."""

    def create_view(self, name: str, expr: IbisTable, *, overwrite: bool = False) -> None:
        """Register an Ibis view on the backend."""
        ...


@dataclass(frozen=True)
class PlanBridgeResult:
    """Ibis plan plus captured ordering metadata."""

    plan: IbisPlan
    ordering: Ordering


@dataclass(frozen=True)
class SourceToIbisOptions:
    """Options for bridging plan sources into Ibis plans."""

    ctx: ExecutionContext
    backend: BaseBackend
    name: str | None = None
    ordering: Ordering | None = None
    overwrite: bool = True


def plan_to_ibis(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None = None,
    overwrite: bool = True,
) -> IbisPlan:
    """Materialize a Plan and register it as an Ibis view.

    Returns
    -------
    IbisPlan
        Ibis plan backed by a backend view.
    """
    table = plan.to_table(ctx=ctx)
    return table_to_ibis(
        table,
        backend=backend,
        name=name or plan.label,
        ordering=plan.ordering,
        overwrite=overwrite,
    )


def source_to_ibis(
    source: Plan | IbisPlan | IbisTable | TableLike | RecordBatchReaderLike,
    *,
    options: SourceToIbisOptions,
) -> IbisPlan:
    """Bridge a plan source into an Ibis plan.

    Returns
    -------
    IbisPlan
        Ibis plan registered on the backend.
    """
    if isinstance(source, IbisPlan):
        return source
    if isinstance(source, IbisTable):
        return IbisPlan(expr=source, ordering=options.ordering or Ordering.unordered())
    if isinstance(source, Plan):
        return plan_to_ibis(
            source,
            ctx=options.ctx,
            backend=options.backend,
            name=options.name,
            overwrite=options.overwrite,
        )
    table = _ensure_table(source)
    return table_to_ibis(
        table,
        backend=options.backend,
        name=options.name,
        ordering=options.ordering or Ordering.unordered(),
        overwrite=options.overwrite,
    )


def table_to_ibis(
    table: TableLike,
    *,
    backend: BaseBackend,
    name: str | None = None,
    ordering: Ordering | None = None,
    overwrite: bool = True,
) -> IbisPlan:
    """Register a table-like value as an Ibis view and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered view.
    """
    expr = ibis.memtable(table)
    view_name = _resolve_name(name)
    if view_name is None:
        return IbisPlan(expr=expr, ordering=ordering or Ordering.unordered())
    backend_view = cast("ViewBackend", backend)
    backend_view.create_view(view_name, expr, overwrite=overwrite)
    registered = backend.table(view_name)
    return IbisPlan(expr=registered, ordering=ordering or Ordering.unordered())


def reader_to_ibis(
    reader: RecordBatchReaderLike,
    *,
    backend: BaseBackend,
    name: str | None = None,
    ordering: Ordering | None = None,
    overwrite: bool = True,
) -> IbisPlan:
    """Register a RecordBatchReader as an Ibis view and return a plan.

    Returns
    -------
    IbisPlan
        Ibis plan backed by the registered view.
    """
    table = _ensure_table(reader)
    return table_to_ibis(
        table,
        backend=backend,
        name=name,
        ordering=ordering,
        overwrite=overwrite,
    )


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _resolve_name(name: str | None) -> str | None:
    if not name:
        return None
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    return sanitized or "view"


def collect_plan_bridge(
    plans: Iterable[Plan],
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    prefix: str,
) -> dict[str, IbisPlan]:
    """Materialize and register multiple plans under stable names.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plans keyed by the generated view names.
    """
    out: dict[str, IbisPlan] = {}
    for idx, plan in enumerate(plans):
        name = f"{prefix}_{idx}"
        out[name] = plan_to_ibis(plan, ctx=ctx, backend=backend, name=name)
    return out


__all__ = [
    "PlanBridgeResult",
    "collect_plan_bridge",
    "plan_to_ibis",
    "reader_to_ibis",
    "source_to_ibis",
    "table_to_ibis",
]
