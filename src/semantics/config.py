"""Semantic compiler configuration and overrides."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import msgspec

from core_types import IdentifierStr
from semantics.column_types import TYPE_PATTERNS, ColumnType
from serde_msgspec import StructBaseStrict, to_builtins

if TYPE_CHECKING:
    from semantics.specs import SemanticTableSpec


class SemanticTypePatternSpec(StructBaseStrict, frozen=True):
    """Serializable pattern entry for semantic inference."""

    pattern: str
    column_type: ColumnType


_DEFAULT_TYPE_PATTERN_SPECS: tuple[SemanticTypePatternSpec, ...] = tuple(
    SemanticTypePatternSpec(pattern=pattern.pattern, column_type=col_type)
    for pattern, col_type in TYPE_PATTERNS
)

_DEFAULT_DISALLOW_ENTITY_ID_PATTERNS: tuple[str, ...] = (
    r"^span_id$",
    r"^code_unit_id$",
)


class SemanticConfigSpec(StructBaseStrict, frozen=True):
    """Serializable semantic configuration overrides."""

    type_patterns: tuple[SemanticTypePatternSpec, ...] = _DEFAULT_TYPE_PATTERN_SPECS
    table_overrides: dict[IdentifierStr, dict[ColumnType, str]] = msgspec.field(
        default_factory=dict
    )
    disallow_entity_id_patterns: tuple[str, ...] = _DEFAULT_DISALLOW_ENTITY_ID_PATTERNS


@dataclass(frozen=True)
class SemanticConfig:
    """Configuration overrides for semantic schema inference."""

    type_patterns: tuple[tuple[re.Pattern[str], ColumnType], ...] = TYPE_PATTERNS
    table_overrides: dict[IdentifierStr, dict[ColumnType, str]] = field(default_factory=dict)
    spec_registry: dict[str, SemanticTableSpec] | None = None
    disallow_entity_id_patterns: tuple[re.Pattern[str], ...] = (
        re.compile(r"^span_id$"),
        re.compile(r"^code_unit_id$"),
    )

    def overrides_for(self, table_name: str | None) -> Mapping[ColumnType, str]:
        """Return overrides for the table name.

        Returns:
        -------
        Mapping[ColumnType, str]
            Overrides for the table when configured.
        """
        if table_name is None:
            return {}
        return self.table_overrides.get(table_name, {})

    def spec_for(self, table_name: str | None) -> SemanticTableSpec | None:
        """Return the semantic spec for the table when configured.

        Returns:
        -------
        SemanticTableSpec | None
            Registered semantic spec, if available.
        """
        if table_name is None or self.spec_registry is None:
            return None
        return self.spec_registry.get(table_name)


def semantic_config_from_spec(
    spec: SemanticConfigSpec,
    *,
    spec_registry: dict[str, SemanticTableSpec] | None = None,
) -> SemanticConfig:
    """Resolve a runtime SemanticConfig from a serializable spec.

    Returns:
    -------
    SemanticConfig
        Runtime semantic configuration with compiled patterns.
    """
    from runtime_models.adapters import SEMANTIC_CONFIG_ADAPTER

    payload = to_builtins(spec, str_keys=True)
    resolved = SEMANTIC_CONFIG_ADAPTER.validate_python(payload)
    type_patterns = tuple(
        (re.compile(entry.pattern), entry.column_type) for entry in resolved.type_patterns
    )
    disallow_patterns = tuple(
        re.compile(pattern) for pattern in resolved.disallow_entity_id_patterns
    )
    return SemanticConfig(
        type_patterns=type_patterns,
        table_overrides=resolved.table_overrides,
        spec_registry=spec_registry,
        disallow_entity_id_patterns=disallow_patterns,
    )


__all__ = [
    "SemanticConfig",
    "SemanticConfigSpec",
    "SemanticTypePatternSpec",
    "semantic_config_from_spec",
]
