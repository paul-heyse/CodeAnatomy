"""Semantic compiler configuration and overrides."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from semantics.column_types import TYPE_PATTERNS, ColumnType

if TYPE_CHECKING:
    from semantics.specs import SemanticTableSpec


@dataclass(frozen=True)
class SemanticConfig:
    """Configuration overrides for semantic schema inference."""

    type_patterns: tuple[tuple[re.Pattern[str], ColumnType], ...] = TYPE_PATTERNS
    table_overrides: dict[str, dict[ColumnType, str]] = field(default_factory=dict)
    spec_registry: dict[str, SemanticTableSpec] | None = None
    disallow_entity_id_patterns: tuple[re.Pattern[str], ...] = (
        re.compile(r"^span_id$"),
        re.compile(r"^code_unit_id$"),
    )

    def overrides_for(self, table_name: str | None) -> Mapping[ColumnType, str]:
        """Return overrides for the table name.

        Returns
        -------
        Mapping[ColumnType, str]
            Overrides for the table when configured.
        """
        if table_name is None:
            return {}
        return self.table_overrides.get(table_name, {})

    def spec_for(self, table_name: str | None) -> SemanticTableSpec | None:
        """Return the semantic spec for the table when configured.

        Returns
        -------
        SemanticTableSpec | None
            Registered semantic spec, if available.
        """
        if table_name is None or self.spec_registry is None:
            return None
        return self.spec_registry.get(table_name)


__all__ = ["SemanticConfig"]
