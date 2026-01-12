"""Shared table catalog and references for CPG builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike

type TableGetter = Callable[[Mapping[str, TableLike]], TableLike | None]
type TableDeriver = Callable[[TableCatalog, ExecutionContext], TableLike | None]


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


__all__ = ["TableCatalog", "TableGetter", "TableRef"]
