"""Shared batch-builder helpers for semantic diagnostics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast

import pyarrow as pa


@dataclass
class DiagnosticBatchBuilder:
    """Reusable builder for Arrow diagnostic tables."""

    _rows: list[dict[str, object]] = field(default_factory=list)

    def add(self, row: Mapping[str, object]) -> None:
        """Append a diagnostic row payload."""
        self._rows.append(dict(row))

    def add_many(self, rows: Sequence[Mapping[str, object]]) -> None:
        """Append multiple diagnostic row payloads."""
        for row in rows:
            self.add(row)

    def add_arrow_table(self, table: pa.Table) -> None:
        """Append rows decoded from an Arrow table."""
        self.add_many(cast("list[dict[str, object]]", table.to_pylist()))

    def build_table(self, *, schema: pa.Schema) -> pa.Table:
        """Build a PyArrow table using the provided schema.

        Returns:
        -------
        pa.Table
            Table materialized from accumulated rows.
        """
        if not self._rows:
            return pa.Table.from_pylist([], schema=schema)
        return pa.Table.from_pylist(self._rows, schema=schema)

    def rows(self) -> list[dict[str, object]]:
        """Return accumulated row payloads."""
        return list(self._rows)

    def __len__(self) -> int:
        """Return the number of accumulated rows."""
        return len(self._rows)


__all__ = ["DiagnosticBatchBuilder"]
