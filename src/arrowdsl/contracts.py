"""Contracts and policy specs for Arrow table outputs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from arrowdsl.pyarrow_protocols import ArrayLike, SchemaLike, TableLike

type InvariantFn = Callable[[TableLike], tuple[ArrayLike, str]]

if TYPE_CHECKING:
    from schema_spec.core import TableSchemaSpec


@dataclass(frozen=True)
class SortKey:
    """Sort key specification for deterministic ordering.

    Parameters
    ----------
    column:
        Column name to sort by.
    order:
        Sort order ("ascending" or "descending").
    """

    column: str
    order: Literal["ascending", "descending"] = "ascending"


type DedupeStrategy = Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]


@dataclass(frozen=True)
class DedupeSpec:
    """Dedupe semantics for a table.

    Parameters
    ----------
    keys:
        Key columns that define duplicates.
    tie_breakers:
        Additional sort keys used for deterministic winner selection.
    strategy:
        Dedupe strategy name.
    """

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"


@dataclass(frozen=True)
class Contract:
    """Output contract: schema, invariants, and determinism policy."""

    name: str
    schema: SchemaLike
    schema_spec: TableSchemaSpec | None = None

    key_fields: tuple[str, ...] = ()
    required_non_null: tuple[str, ...] = ()
    invariants: tuple[InvariantFn, ...] = ()

    dedupe: DedupeSpec | None = None
    canonical_sort: tuple[SortKey, ...] = ()

    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None

    def with_versioned_schema(self) -> SchemaLike:
        """Return the schema with contract metadata attached.

        Returns
        -------
        pyarrow.Schema
            Schema containing contract name/version metadata when available.
        """
        meta = dict(self.schema.metadata or {})
        meta[b"contract_name"] = str(self.name).encode("utf-8")
        if self.version is not None:
            meta[b"contract_version"] = str(self.version).encode("utf-8")
        return self.schema.with_metadata(meta)

    def available_fields(self) -> tuple[str, ...]:
        """Return all visible field names (columns + virtual fields).

        Returns
        -------
        tuple[str, ...]
            Field names visible to validators and downstream consumers.
        """
        cols = tuple(self.schema.names)
        return cols + tuple(self.virtual_fields)
