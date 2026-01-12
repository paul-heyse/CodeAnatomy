"""Catalog helpers for plan/table sources."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.source import PlanSource, plan_from_source

TableGetter = Callable[[Mapping[str, TableLike]], TableLike | None]
TableDeriver = Callable[["TableCatalog", ExecutionContext], TableLike | None]
PlanGetter = Callable[[Mapping[str, PlanSource]], PlanSource | None]
PlanDeriver = Callable[["PlanCatalog", ExecutionContext], PlanSource | None]


@dataclass(frozen=True)
class TableRef:
    """Reference to a table, optionally derived on demand."""

    name: str
    derive: TableDeriver | None = None

    def getter(self) -> TableGetter:
        """Return a TableGetter for use in spec objects.

        Returns
        -------
        TableGetter
            Getter that looks up the table by name.
        """

        def _get(tables: Mapping[str, TableLike]) -> TableLike | None:
            return tables.get(self.name)

        return _get


@dataclass
class TableCatalog:
    """Mutable table registry for build steps and derived tables."""

    tables: dict[str, TableLike] = field(default_factory=dict)

    def add(self, name: str, table: TableLike | None) -> None:
        """Add a table when present."""
        if table is not None:
            self.tables[name] = table

    def extend(self, tables: Mapping[str, TableLike]) -> None:
        """Add multiple tables to the catalog."""
        self.tables.update(tables)

    def resolve(self, ref: TableRef, *, ctx: ExecutionContext) -> TableLike | None:
        """Resolve a table, deriving it if needed.

        Returns
        -------
        TableLike | None
            Resolved table or ``None`` when unavailable.
        """
        if ref.name in self.tables:
            return self.tables[ref.name]
        if ref.derive is None:
            return None
        table = ref.derive(self, ctx)
        if table is not None:
            self.tables[ref.name] = table
        return table

    def snapshot(self) -> dict[str, TableLike]:
        """Return a shallow copy of the catalog tables.

        Returns
        -------
        dict[str, TableLike]
            Shallow copy of catalog tables.
        """
        return dict(self.tables)


@dataclass(frozen=True)
class PlanRef:
    """Reference to a plan source, optionally derived on demand."""

    name: str
    derive: PlanDeriver | None = None

    def getter(self) -> PlanGetter:
        """Return a PlanGetter for use in plan spec objects.

        Returns
        -------
        PlanGetter
            Getter that looks up the plan source by name.
        """

        def _get(tables: Mapping[str, PlanSource]) -> PlanSource | None:
            return tables.get(self.name)

        return _get


@dataclass
class PlanCatalog:
    """Mutable plan registry for build steps and derived sources."""

    tables: dict[str, PlanSource] = field(default_factory=dict)

    def add(self, name: str, table: PlanSource | None) -> None:
        """Add a plan source when present."""
        if table is not None:
            self.tables[name] = table

    def extend(self, tables: Mapping[str, PlanSource]) -> None:
        """Add multiple plan sources to the catalog."""
        self.tables.update(tables)

    def resolve(self, ref: PlanRef, *, ctx: ExecutionContext) -> Plan | None:
        """Resolve a plan source, deriving it if needed.

        Returns
        -------
        Plan | None
            Resolved plan or ``None`` when unavailable.
        """
        if ref.name in self.tables:
            plan = plan_from_source(self.tables[ref.name], ctx=ctx, label=ref.name)
            self.tables[ref.name] = plan
            return plan
        if ref.derive is None:
            return None
        derived = ref.derive(self, ctx)
        if derived is None:
            return None
        plan = plan_from_source(derived, ctx=ctx, label=ref.name)
        self.tables[ref.name] = plan
        return plan

    def snapshot(self) -> dict[str, PlanSource]:
        """Return a shallow copy of the catalog sources.

        Returns
        -------
        dict[str, PlanSource]
            Shallow copy of catalog sources.
        """
        return dict(self.tables)


__all__ = [
    "PlanCatalog",
    "PlanDeriver",
    "PlanGetter",
    "PlanRef",
    "PlanSource",
    "TableCatalog",
    "TableDeriver",
    "TableGetter",
    "TableRef",
]
