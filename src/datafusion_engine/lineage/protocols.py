"""Protocol contracts for lineage consumers and producers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class LineageScan(Protocol):
    """Protocol describing scan metadata from lineage artifacts."""

    dataset_name: str
    projected_columns: Sequence[str]
    pushed_filters: Sequence[str]


@runtime_checkable
class LineageJoin(Protocol):
    """Protocol describing join metadata from lineage artifacts."""

    join_type: str
    left_keys: Sequence[str]
    right_keys: Sequence[str]


@runtime_checkable
class LineageExpr(Protocol):
    """Protocol describing expression-level lineage references."""

    kind: str
    referenced_columns: Sequence[Sequence[str]]
    referenced_udfs: Sequence[str]
    text: str | None


@runtime_checkable
class LineageRecorder(Protocol):
    """Protocol for writing lineage diagnostics artifacts."""

    def record(self, payload: Mapping[str, object]) -> None:
        """Record lineage payload."""
        ...


@runtime_checkable
class LineageQuery(Protocol):
    """Protocol for reading lineage metadata for plan surfaces."""

    @property
    def required_udfs(self) -> Sequence[str]:
        """Return required UDF names."""
        ...

    @property
    def required_rewrite_tags(self) -> Sequence[str]:
        """Return required rewrite tags."""
        ...

    @property
    def scans(self) -> Sequence[LineageScan]:
        """Return scan lineage entries."""
        ...

    @property
    def joins(self) -> Sequence[LineageJoin]:
        """Return join lineage entries."""
        ...

    @property
    def exprs(self) -> Sequence[LineageExpr]:
        """Return expression lineage entries."""
        ...

    @property
    def required_columns_by_dataset(self) -> Mapping[str, Sequence[str]]:
        """Return required columns grouped by dataset."""
        ...


__all__ = [
    "LineageExpr",
    "LineageJoin",
    "LineageQuery",
    "LineageRecorder",
    "LineageScan",
]
