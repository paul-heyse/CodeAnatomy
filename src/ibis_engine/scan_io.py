"""Ibis plan helpers for dataset sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, cast

from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.core.ordering import Ordering
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.sources import SourceToIbisOptions, namespace_recorder_from_ctx, source_to_ibis


class DatasetSpecLike(Protocol):
    """Protocol for dataset specs used in Ibis plan compilation."""

    def query(self) -> IbisQuerySpec:
        """Return the query spec for the dataset."""
        ...

    def ordering(self) -> Ordering:
        """Return the ordering metadata for the dataset."""
        ...


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + dataset spec pairing for Ibis plan compilation."""

    dataset: TableLike | RecordBatchReaderLike | IbisTable
    spec: DatasetSpecLike


type PlanSource = IbisPlan | IbisTable | TableLike | RecordBatchReaderLike | DatasetSource


def plan_from_dataset(
    dataset: TableLike | RecordBatchReaderLike | IbisTable,
    *,
    spec: DatasetSpecLike,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None = None,
) -> IbisPlan:
    """Return an Ibis plan for a dataset spec.

    Returns
    -------
    IbisPlan
        Ibis plan with the query spec applied.
    """
    plan = _plan_from_source(
        dataset,
        ctx=ctx,
        backend=backend,
        name=name,
    )
    expr = apply_query_spec(plan.expr, spec=spec.query())
    return IbisPlan(expr=expr, ordering=spec.ordering())


def plan_from_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None = None,
) -> IbisPlan:
    """Return an Ibis plan for a source value.

    Returns
    -------
    IbisPlan
        Ibis plan for the source.
    """
    if isinstance(source, DatasetSource):
        return plan_from_dataset(
            source.dataset,
            spec=source.spec,
            ctx=ctx,
            backend=backend,
            name=name,
        )
    return _plan_from_source(source, ctx=ctx, backend=backend, name=name)


def _plan_from_source(
    source: IbisPlan | IbisTable | TableLike | RecordBatchReaderLike,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None,
) -> IbisPlan:
    ordering = _ordering_for_ctx(ctx)
    if isinstance(source, IbisPlan):
        return source
    if isinstance(source, IbisTable):
        return IbisPlan(expr=source, ordering=ordering)
    options = SourceToIbisOptions(
        backend=backend,
        name=name,
        ordering=ordering,
        namespace_recorder=namespace_recorder_from_ctx(ctx),
    )
    return source_to_ibis(cast("TableLike | RecordBatchReaderLike", source), options=options)


def _ordering_for_ctx(ctx: ExecutionContext) -> Ordering:
    if ctx.runtime.scan.implicit_ordering:
        return Ordering.implicit()
    return Ordering.unordered()


__all__ = [
    "DatasetSource",
    "DatasetSpecLike",
    "PlanSource",
    "plan_from_dataset",
    "plan_from_source",
]
