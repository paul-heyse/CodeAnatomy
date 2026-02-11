"""View specification contracts for dataset registration."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from core_types import IdentifierStr
from schema_spec.specs import TableSchemaSpec
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


class ViewSchemaMismatchError(ValueError):
    """Raised when a view schema does not match its specification."""


class ViewSpec(StructBaseStrict, frozen=True):
    """Serializable view specification for schema contracts."""

    name: IdentifierStr
    schema: TableSchemaSpec | None = None


@dataclass(frozen=True)
class ViewRuntimeSpec:
    """Runtime view specification with builder wiring."""

    name: IdentifierStr
    builder: Callable[[SessionContext], DataFrame]
    schema: TableSchemaSpec | None = None


__all__ = ["ViewRuntimeSpec", "ViewSchemaMismatchError", "ViewSpec"]
