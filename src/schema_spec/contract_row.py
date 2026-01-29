"""Shared dataset contract row specification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.schema_validation import ArrowValidationOptions
    from schema_spec.system import DedupeSpecSpec, SortKeySpec


@dataclass(frozen=True)
class ContractRow:
    """Contract specification for dataset rows."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None
    constraints: tuple[str, ...] = ()
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None


__all__ = ["ContractRow"]
