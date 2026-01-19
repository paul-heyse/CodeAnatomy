"""Shared registry row dataclasses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from schema_spec.system import DedupeSpecSpec, SortKeySpec


@dataclass(frozen=True)
class ContractRow:
    """Row configuration for a dataset contract."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None


__all__ = ["ContractRow"]
