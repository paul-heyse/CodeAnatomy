"""Runtime validation models for semantic configuration."""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import Field, TypeAdapter

from runtime_models.base import RuntimeBase
from runtime_models.types import NonEmptyStr
from semantics.column_types import ColumnType


class SemanticTypePatternRuntime(RuntimeBase):
    """Validated semantic type pattern entry."""

    pattern: NonEmptyStr
    column_type: ColumnType


class SemanticConfigRuntime(RuntimeBase):
    """Validated semantic config payload."""

    type_patterns: tuple[SemanticTypePatternRuntime, ...] = ()
    table_overrides: dict[str, dict[ColumnType, str]] = Field(default_factory=dict)
    disallow_entity_id_patterns: tuple[NonEmptyStr, ...] = ()

    def overrides_for(self, table_name: str | None) -> Mapping[ColumnType, str]:
        """Return column-type overrides for the given table.

        Returns
        -------
        Mapping[ColumnType, str]
            Overrides for the table when configured.
        """
        if table_name is None:
            return {}
        return self.table_overrides.get(table_name, {})


SEMANTIC_CONFIG_ADAPTER = TypeAdapter(SemanticConfigRuntime)

__all__ = ["SEMANTIC_CONFIG_ADAPTER", "SemanticConfigRuntime", "SemanticTypePatternRuntime"]
