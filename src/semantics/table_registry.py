"""Mutable table-info registry for semantic compilation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from semantics.compiler import TableInfo


@dataclass
class TableRegistry:
    """Mutable registry for analyzed semantic tables."""

    _tables: dict[str, TableInfo] = field(default_factory=dict)

    def register(self, name: str, info: TableInfo) -> None:
        """Store table info under ``name``."""
        if name in self._tables:
            msg = f"Table already registered: {name!r}"
            raise ValueError(msg)
        self._tables[name] = info

    def get(self, name: str) -> TableInfo | None:
        """Return table info when registered."""
        return self._tables.get(name)

    def all_names(self) -> tuple[str, ...]:
        """Return sorted table names currently registered."""
        return tuple(sorted(self._tables))

    def names(self) -> tuple[str, ...]:
        """Return sorted table names currently registered."""
        return self.all_names()

    def resolve(self, name: str, factory: Callable[[], TableInfo]) -> TableInfo:
        """Return existing table info or register a newly-created one."""
        existing = self.get(name)
        if existing is not None:
            return existing
        created = factory()
        self.register(name, created)
        return created


__all__ = ["TableRegistry"]
