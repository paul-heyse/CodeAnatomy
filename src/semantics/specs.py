"""Declarative semantic table specs for primary spans and ID derivations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

SpanUnit = Literal["byte"]


@dataclass(frozen=True)
class SpanBinding:
    """Bind a semantic span role to concrete source columns."""

    start_col: str
    end_col: str
    unit: SpanUnit = "byte"
    canonical_start: str = "bstart"
    canonical_end: str = "bend"
    canonical_span: str = "span"


@dataclass(frozen=True)
class IdDerivation:
    """Derive an ID from path + span with a stable namespace."""

    out_col: str
    namespace: str
    path_col: str = "path"
    start_col: str = "bstart"
    end_col: str = "bend"
    null_if_any_null: bool = True
    canonical_entity_id: str | None = "entity_id"


@dataclass(frozen=True)
class ForeignKeyDerivation:
    """Derive a foreign-key ID using a reference span."""

    out_col: str
    target_namespace: str
    path_col: str = "path"
    start_col: str = ""
    end_col: str = ""
    null_if_any_null: bool = True
    guard_null_if: tuple[str, ...] = ()


@dataclass(frozen=True)
class SemanticTableSpec:
    """Declarative spec for semantic normalization."""

    table: str
    path_col: str = "path"
    primary_span: SpanBinding = field(default_factory=lambda: SpanBinding("bstart", "bend"))
    entity_id: IdDerivation = field(
        default_factory=lambda: IdDerivation(out_col="entity_id", namespace="entity")
    )
    foreign_keys: tuple[ForeignKeyDerivation, ...] = ()
    text_cols: tuple[str, ...] = ()


__all__ = [
    "ForeignKeyDerivation",
    "IdDerivation",
    "SemanticTableSpec",
    "SpanBinding",
    "SpanUnit",
]
