"""Shared catalog helpers for resolving Ibis plan inputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table
from datafusion_engine.nested_tables import ViewReference
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import (
    DatasetSource,
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_table,
    source_to_ibis,
)

IbisPlanSource = IbisPlan | Table | TableLike | ViewReference | DatasetSource


@dataclass
class IbisPlanCatalog:
    """Catalog wrapper for resolving Ibis plan inputs."""

    backend: BaseBackend
    tables: dict[str, IbisPlanSource] = field(default_factory=dict)

    def resolve_plan(
        self,
        name: str,
        *,
        ctx: ExecutionContext,
        label: str | None = None,
    ) -> IbisPlan | None:
        """Resolve a catalog entry into an Ibis plan.

        Returns
        -------
        IbisPlan | None
            Ibis plan for the named entry when available.

        Raises
        ------
        TypeError
            Raised when a DatasetSource requires materialization.
        """
        source = self.tables.get(name)
        if source is None:
            return None
        if isinstance(source, IbisPlan):
            return source
        if isinstance(source, ViewReference):
            expr = _view_reference_expr(self.backend, source)
            plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
            self.tables[name] = plan
            return plan
        if isinstance(source, Table):
            return IbisPlan(expr=source, ordering=Ordering.unordered())
        if isinstance(source, DatasetSource):
            msg = f"DatasetSource {name!r} must be materialized before Ibis compilation."
            raise TypeError(msg)
        plan = source_to_ibis(
            cast("TableLike", source),
            options=SourceToIbisOptions(
                backend=self.backend,
                name=label or name,
                ordering=Ordering.unordered(),
                namespace_recorder=namespace_recorder_from_ctx(ctx),
            ),
        )
        self.tables[name] = plan
        return plan

    def resolve_expr(
        self,
        name: str,
        *,
        ctx: ExecutionContext,
        schema: pa.Schema | None = None,
    ) -> Table:
        """Resolve a catalog entry into an Ibis expression.

        Returns
        -------
        ibis.expr.types.Table
            Resolved table expression.

        Raises
        ------
        KeyError
            Raised when the dataset is missing and no schema was provided.
        TypeError
            Raised when a DatasetSource must be materialized before use.
        """
        try:
            plan = self.resolve_plan(name, ctx=ctx, label=name)
        except TypeError as exc:
            raise TypeError(str(exc)) from exc
        if plan is not None:
            return plan.expr
        if schema is None:
            msg = f"Unknown dataset reference: {name!r}."
            raise KeyError(msg)
        empty = empty_table(schema)
        plan = register_ibis_table(
            empty,
            options=SourceToIbisOptions(
                backend=self.backend,
                name=None,
                ordering=Ordering.unordered(),
                namespace_recorder=namespace_recorder_from_ctx(ctx),
            ),
        )
        return plan.expr

    def add(self, name: str, plan: IbisPlan) -> None:
        """Add a derived Ibis plan to the catalog."""
        self.tables[name] = plan


def _view_reference_expr(backend: BaseBackend, fragment: ViewReference) -> Table:
    return backend.table(fragment.name)


__all__ = ["IbisPlanCatalog", "IbisPlanSource"]
