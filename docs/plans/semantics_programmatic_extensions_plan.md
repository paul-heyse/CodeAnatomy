# Semantics Programmatic Extensions Plan

**Created:** 2025-01-31
**Status:** Planning
**Scope:** Programmatic, extensible, and robust enhancements to the semantic pipeline
**Prerequisites:** `semantics_integration_wiring_plan.md`, `semantics_quality_reliability_plan.md`

---

## Executive Summary

This document details enhancements to make the semantic pipeline maximally programmatic, extensible, and robust. These extensions build on the foundation established in the integration wiring plan and quality/reliability plan, adding:

1. **Metadata-driven join inference** - Derive join strategies from semantic types, not hardcoded conditions
2. **Statistics-aware optimization** - Feed extraction statistics to DataFusion's cost-based optimizer
3. **Declarative relationship specifications** - Specify intent, let the system determine execution
4. **Plan caching via Substrait** - Portable plan representation for caching and debugging
5. **Incremental computation** - CDF-based incremental relationship updates
6. **Self-documenting architecture** - Auto-generated documentation and visualization

---

## Part 1: Semantic Type System

### 1.1 Core Type Definitions

**File:** `src/semantics/types/core.py` (NEW)

```python
"""Core semantic type definitions for programmatic join inference."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Final, Literal

from utils.registry_protocol import ImmutableRegistry

if TYPE_CHECKING:
    from collections.abc import Sequence


class JoinStrategy(StrEnum):
    """Supported join strategies for semantic types."""

    EQUI = auto()  # Equality join (hash join)
    INTERVAL_OVERLAP = auto()  # Span overlap (range join)
    INTERVAL_CONTAINS = auto()  # Span containment (range join)
    FOREIGN_KEY = auto()  # Foreign key relationship
    FUZZY_MATCH = auto()  # Text similarity (future)


class CompatibilityGroup(StrEnum):
    """Groups of semantically compatible types that can join together."""

    FILE_IDENTITY = auto()  # file_id, path, file_sha256
    BYTE_SPAN = auto()  # bstart, bend, span_start, span_end
    ENTITY_IDENTITY = auto()  # entity_id, def_id, ref_id
    SYMBOL_IDENTITY = auto()  # symbol, scip_symbol
    LINE_POSITION = auto()  # line_no, start_line, end_line
    COLUMN_POSITION = auto()  # col, start_col, end_col


@dataclass(frozen=True)
class SemanticType:
    """A semantic type with join strategy metadata.

    Semantic types encode both the meaning of a column and how it should
    participate in joins. This enables programmatic join inference.

    Parameters
    ----------
    name
        Canonical type name (e.g., "file_id", "span_start").
    join_strategy
        Default join strategy when this type is used.
    compatibility_group
        Types in the same group can join together.
    is_nullable
        Whether this type is commonly nullable.
    requires_normalization
        Whether values need normalization before joining.
    normalization_fn
        Optional SQL expression for normalization.
    """

    name: str
    join_strategy: JoinStrategy
    compatibility_group: CompatibilityGroup
    is_nullable: bool = False
    requires_normalization: bool = False
    normalization_fn: str | None = None

    def can_join_with(self, other: SemanticType) -> bool:
        """Check if this type can join with another type."""
        return self.compatibility_group == other.compatibility_group


@dataclass(frozen=True)
class SemanticTypeMapping:
    """Maps a column to its semantic type with optional transformations."""

    column_name: str
    semantic_type: SemanticType
    transform_expr: str | None = None  # SQL expression to transform column


# Canonical semantic type definitions
FILE_ID_TYPE: Final = SemanticType(
    name="file_id",
    join_strategy=JoinStrategy.EQUI,
    compatibility_group=CompatibilityGroup.FILE_IDENTITY,
)

PATH_TYPE: Final = SemanticType(
    name="path",
    join_strategy=JoinStrategy.EQUI,
    compatibility_group=CompatibilityGroup.FILE_IDENTITY,
    requires_normalization=True,
    normalization_fn="normalize_path({col})",
)

FILE_SHA256_TYPE: Final = SemanticType(
    name="file_sha256",
    join_strategy=JoinStrategy.EQUI,
    compatibility_group=CompatibilityGroup.FILE_IDENTITY,
)

SPAN_START_TYPE: Final = SemanticType(
    name="span_start",
    join_strategy=JoinStrategy.INTERVAL_OVERLAP,
    compatibility_group=CompatibilityGroup.BYTE_SPAN,
)

SPAN_END_TYPE: Final = SemanticType(
    name="span_end",
    join_strategy=JoinStrategy.INTERVAL_OVERLAP,
    compatibility_group=CompatibilityGroup.BYTE_SPAN,
)

ENTITY_ID_TYPE: Final = SemanticType(
    name="entity_id",
    join_strategy=JoinStrategy.EQUI,
    compatibility_group=CompatibilityGroup.ENTITY_IDENTITY,
)

OWNER_ENTITY_ID_TYPE: Final = SemanticType(
    name="owner_entity_id",
    join_strategy=JoinStrategy.FOREIGN_KEY,
    compatibility_group=CompatibilityGroup.ENTITY_IDENTITY,
    is_nullable=True,
)

SYMBOL_TYPE: Final = SemanticType(
    name="symbol",
    join_strategy=JoinStrategy.EQUI,
    compatibility_group=CompatibilityGroup.SYMBOL_IDENTITY,
)

LINE_NO_TYPE: Final = SemanticType(
    name="line_no",
    join_strategy=JoinStrategy.EQUI,
    compatibility_group=CompatibilityGroup.LINE_POSITION,
)


# Type registry for lookup by name
SEMANTIC_TYPE_REGISTRY: Final[ImmutableRegistry[str, SemanticType]] = (
    ImmutableRegistry.from_dict({
        "file_id": FILE_ID_TYPE,
        "path": PATH_TYPE,
        "file_sha256": FILE_SHA256_TYPE,
        "bstart": SPAN_START_TYPE,
        "bend": SPAN_END_TYPE,
        "span_start": SPAN_START_TYPE,
        "span_end": SPAN_END_TYPE,
        "entity_id": ENTITY_ID_TYPE,
        "def_id": ENTITY_ID_TYPE,
        "ref_id": ENTITY_ID_TYPE,
        "owner_def_id": OWNER_ENTITY_ID_TYPE,
        "owner_entity_id": OWNER_ENTITY_ID_TYPE,
        "symbol": SYMBOL_TYPE,
        "scip_symbol": SYMBOL_TYPE,
        "line_no": LINE_NO_TYPE,
        "start_line": LINE_NO_TYPE,
        "end_line": LINE_NO_TYPE,
    })
)


def infer_semantic_type(column_name: str) -> SemanticType | None:
    """Infer semantic type from column name using registry and patterns.

    Parameters
    ----------
    column_name
        The column name to infer type for.

    Returns
    -------
    SemanticType | None
        Inferred semantic type or None if unknown.
    """
    # Direct registry lookup
    if column_name in SEMANTIC_TYPE_REGISTRY:
        return SEMANTIC_TYPE_REGISTRY[column_name]

    # Pattern-based inference
    name_lower = column_name.lower()

    if name_lower.endswith("_id") and "file" in name_lower:
        return FILE_ID_TYPE
    if name_lower.endswith("_id") and "owner" in name_lower:
        return OWNER_ENTITY_ID_TYPE
    if name_lower.endswith("_id"):
        return ENTITY_ID_TYPE
    if "bstart" in name_lower or name_lower.endswith("_start_byte"):
        return SPAN_START_TYPE
    if "bend" in name_lower or name_lower.endswith("_end_byte"):
        return SPAN_END_TYPE
    if "symbol" in name_lower:
        return SYMBOL_TYPE
    if "path" in name_lower:
        return PATH_TYPE

    return None


__all__ = [
    "CompatibilityGroup",
    "JoinStrategy",
    "SemanticType",
    "SemanticTypeMapping",
    "SEMANTIC_TYPE_REGISTRY",
    "infer_semantic_type",
    # Individual types for direct import
    "FILE_ID_TYPE",
    "PATH_TYPE",
    "FILE_SHA256_TYPE",
    "SPAN_START_TYPE",
    "SPAN_END_TYPE",
    "ENTITY_ID_TYPE",
    "OWNER_ENTITY_ID_TYPE",
    "SYMBOL_TYPE",
    "LINE_NO_TYPE",
]
```

**Acceptance Criteria:**
- [ ] All semantic types defined with join strategies
- [ ] Pattern-based inference covers common column naming conventions
- [ ] Registry is immutable and type-safe

---

### 1.2 Annotated Schema

**File:** `src/semantics/types/annotated_schema.py` (NEW)

```python
"""Schema with semantic type annotations for programmatic operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

import pyarrow as pa

from semantics.types.core import (
    CompatibilityGroup,
    JoinStrategy,
    SemanticType,
    SemanticTypeMapping,
    infer_semantic_type,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from datafusion import DataFrame


@dataclass(frozen=True)
class AnnotatedColumn:
    """A column with semantic type annotation."""

    name: str
    arrow_type: pa.DataType
    semantic_type: SemanticType | None
    is_nullable: bool = True
    metadata: Mapping[str, str] = field(default_factory=dict)

    @property
    def join_strategy(self) -> JoinStrategy | None:
        """Get the join strategy for this column."""
        return self.semantic_type.join_strategy if self.semantic_type else None

    @property
    def compatibility_group(self) -> CompatibilityGroup | None:
        """Get the compatibility group for this column."""
        return self.semantic_type.compatibility_group if self.semantic_type else None


@dataclass(frozen=True)
class AnnotatedSchema:
    """Schema with semantic type annotations.

    Provides programmatic access to column semantics for join inference,
    validation, and optimization.
    """

    columns: tuple[AnnotatedColumn, ...]
    source_name: str | None = None

    @classmethod
    def from_arrow_schema(
        cls,
        schema: pa.Schema,
        *,
        source_name: str | None = None,
        type_overrides: Mapping[str, SemanticType] | None = None,
    ) -> AnnotatedSchema:
        """Create annotated schema from Arrow schema with type inference.

        Parameters
        ----------
        schema
            PyArrow schema to annotate.
        source_name
            Optional source table/view name.
        type_overrides
            Manual type annotations that override inference.

        Returns
        -------
        AnnotatedSchema
            Schema with semantic type annotations.
        """
        overrides = type_overrides or {}
        columns = []

        for field in schema:
            # Check for override first
            if field.name in overrides:
                sem_type = overrides[field.name]
            else:
                # Infer from column name
                sem_type = infer_semantic_type(field.name)

            columns.append(
                AnnotatedColumn(
                    name=field.name,
                    arrow_type=field.type,
                    semantic_type=sem_type,
                    is_nullable=field.nullable,
                    metadata=dict(field.metadata) if field.metadata else {},
                )
            )

        return cls(columns=tuple(columns), source_name=source_name)

    @classmethod
    def from_dataframe(
        cls,
        df: DataFrame,
        *,
        source_name: str | None = None,
        type_overrides: Mapping[str, SemanticType] | None = None,
    ) -> AnnotatedSchema:
        """Create annotated schema from DataFusion DataFrame."""
        return cls.from_arrow_schema(
            df.schema(),
            source_name=source_name,
            type_overrides=type_overrides,
        )

    def __iter__(self) -> Iterator[AnnotatedColumn]:
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)

    def __getitem__(self, name: str) -> AnnotatedColumn:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"Column not found: {name}")

    def __contains__(self, name: str) -> bool:
        return any(col.name == name for col in self.columns)

    @property
    def column_names(self) -> tuple[str, ...]:
        """Get all column names."""
        return tuple(col.name for col in self.columns)

    def columns_by_group(
        self,
        group: CompatibilityGroup,
    ) -> tuple[AnnotatedColumn, ...]:
        """Get columns belonging to a compatibility group."""
        return tuple(
            col for col in self.columns
            if col.compatibility_group == group
        )

    def columns_by_strategy(
        self,
        strategy: JoinStrategy,
    ) -> tuple[AnnotatedColumn, ...]:
        """Get columns using a specific join strategy."""
        return tuple(
            col for col in self.columns
            if col.join_strategy == strategy
        )

    def has_file_identity(self) -> bool:
        """Check if schema has file identity columns."""
        return len(self.columns_by_group(CompatibilityGroup.FILE_IDENTITY)) > 0

    def has_byte_span(self) -> bool:
        """Check if schema has byte span columns."""
        span_cols = self.columns_by_group(CompatibilityGroup.BYTE_SPAN)
        # Need both start and end
        strategies = {col.semantic_type.name for col in span_cols if col.semantic_type}
        return "span_start" in strategies or "bstart" in strategies

    def file_identity_column(self) -> AnnotatedColumn | None:
        """Get the primary file identity column (prefer file_id over path)."""
        file_cols = self.columns_by_group(CompatibilityGroup.FILE_IDENTITY)
        if not file_cols:
            return None
        # Prefer file_id
        for col in file_cols:
            if col.name == "file_id":
                return col
        return file_cols[0]

    def span_columns(self) -> tuple[AnnotatedColumn, AnnotatedColumn] | None:
        """Get (start, end) span columns if available."""
        span_cols = self.columns_by_group(CompatibilityGroup.BYTE_SPAN)
        start_col = None
        end_col = None

        for col in span_cols:
            if col.semantic_type and col.semantic_type.name in ("span_start", "bstart"):
                start_col = col
            elif col.semantic_type and col.semantic_type.name in ("span_end", "bend"):
                end_col = col

        if start_col and end_col:
            return (start_col, end_col)
        return None

    def entity_id_column(self) -> AnnotatedColumn | None:
        """Get the primary entity ID column."""
        entity_cols = self.columns_by_group(CompatibilityGroup.ENTITY_IDENTITY)
        # Filter out owner/foreign key columns
        primary_cols = [
            col for col in entity_cols
            if col.join_strategy == JoinStrategy.EQUI
        ]
        return primary_cols[0] if primary_cols else None

    def foreign_key_columns(self) -> tuple[AnnotatedColumn, ...]:
        """Get foreign key columns (nullable owner IDs)."""
        return self.columns_by_strategy(JoinStrategy.FOREIGN_KEY)

    def joinable_columns_with(
        self,
        other: AnnotatedSchema,
    ) -> list[tuple[AnnotatedColumn, AnnotatedColumn]]:
        """Find all joinable column pairs with another schema.

        Returns
        -------
        list[tuple[AnnotatedColumn, AnnotatedColumn]]
            List of (self_column, other_column) pairs that can join.
        """
        pairs = []

        for self_col in self.columns:
            if not self_col.semantic_type:
                continue

            for other_col in other.columns:
                if not other_col.semantic_type:
                    continue

                if self_col.semantic_type.can_join_with(other_col.semantic_type):
                    pairs.append((self_col, other_col))

        return pairs


__all__ = [
    "AnnotatedColumn",
    "AnnotatedSchema",
]
```

**Acceptance Criteria:**
- [ ] Schema annotation from Arrow and DataFrame sources
- [ ] Column lookup by compatibility group and join strategy
- [ ] Joinable column pair discovery

---

## Part 2: Join Strategy Inference

### 2.1 Join Strategy Types

**File:** `src/semantics/joins/strategies.py` (NEW)

```python
"""Join strategy definitions and inference logic."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Literal, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from datafusion import DataFrame, SessionContext
    from datafusion.expr import Expr

    from semantics.types.annotated_schema import AnnotatedColumn, AnnotatedSchema


class JoinStrategyProtocol(Protocol):
    """Protocol for join strategies."""

    @property
    def name(self) -> str:
        """Strategy name for logging/debugging."""
        ...

    @property
    def estimated_selectivity(self) -> float:
        """Estimated selectivity (0-1) for cost estimation."""
        ...

    def build_join_expr(
        self,
        left_alias: str,
        right_alias: str,
    ) -> str:
        """Build SQL join expression."""
        ...

    def applicable(
        self,
        left_schema: AnnotatedSchema,
        right_schema: AnnotatedSchema,
    ) -> bool:
        """Check if strategy is applicable to given schemas."""
        ...


@dataclass(frozen=True)
class EquiJoinStrategy:
    """Equality join on one or more columns."""

    left_columns: tuple[str, ...]
    right_columns: tuple[str, ...]

    @property
    def name(self) -> str:
        return f"equi_join({','.join(self.left_columns)})"

    @property
    def estimated_selectivity(self) -> float:
        # Equi-joins are typically very selective
        return 0.01 ** len(self.left_columns)

    def build_join_expr(self, left_alias: str, right_alias: str) -> str:
        conditions = [
            f"{left_alias}.{lc} = {right_alias}.{rc}"
            for lc, rc in zip(self.left_columns, self.right_columns, strict=True)
        ]
        return " AND ".join(conditions)

    def applicable(
        self,
        left_schema: AnnotatedSchema,
        right_schema: AnnotatedSchema,
    ) -> bool:
        return all(
            lc in left_schema and rc in right_schema
            for lc, rc in zip(self.left_columns, self.right_columns, strict=True)
        )


@dataclass(frozen=True)
class SpanOverlapJoinStrategy:
    """Interval overlap join for byte spans."""

    left_start: str
    left_end: str
    right_start: str
    right_end: str
    file_key: tuple[str, str] | None = None  # (left_col, right_col) for file partitioning

    @property
    def name(self) -> str:
        return f"span_overlap({self.left_start}:{self.left_end})"

    @property
    def estimated_selectivity(self) -> float:
        # Span overlaps within a file are moderately selective
        return 0.05 if self.file_key else 0.001

    def build_join_expr(self, left_alias: str, right_alias: str) -> str:
        # Overlap: A.start < B.end AND B.start < A.end
        overlap_expr = (
            f"{left_alias}.{self.left_start} < {right_alias}.{self.right_end} AND "
            f"{right_alias}.{self.right_start} < {left_alias}.{self.left_end}"
        )

        if self.file_key:
            file_expr = f"{left_alias}.{self.file_key[0]} = {right_alias}.{self.file_key[1]}"
            return f"{file_expr} AND {overlap_expr}"

        return overlap_expr

    def applicable(
        self,
        left_schema: AnnotatedSchema,
        right_schema: AnnotatedSchema,
    ) -> bool:
        has_left_span = (
            self.left_start in left_schema and
            self.left_end in left_schema
        )
        has_right_span = (
            self.right_start in right_schema and
            self.right_end in right_schema
        )

        if self.file_key:
            has_file_key = (
                self.file_key[0] in left_schema and
                self.file_key[1] in right_schema
            )
            return has_left_span and has_right_span and has_file_key

        return has_left_span and has_right_span


@dataclass(frozen=True)
class SpanContainsJoinStrategy:
    """Interval containment join (outer contains inner)."""

    outer_start: str
    outer_end: str
    inner_start: str
    inner_end: str
    file_key: tuple[str, str] | None = None

    @property
    def name(self) -> str:
        return f"span_contains({self.outer_start}:{self.outer_end})"

    @property
    def estimated_selectivity(self) -> float:
        return 0.02 if self.file_key else 0.0005

    def build_join_expr(self, left_alias: str, right_alias: str) -> str:
        # Containment: outer.start <= inner.start AND inner.end <= outer.end
        contains_expr = (
            f"{left_alias}.{self.outer_start} <= {right_alias}.{self.inner_start} AND "
            f"{right_alias}.{self.inner_end} <= {left_alias}.{self.outer_end}"
        )

        if self.file_key:
            file_expr = f"{left_alias}.{self.file_key[0]} = {right_alias}.{self.file_key[1]}"
            return f"{file_expr} AND {contains_expr}"

        return contains_expr

    def applicable(
        self,
        left_schema: AnnotatedSchema,
        right_schema: AnnotatedSchema,
    ) -> bool:
        has_outer = self.outer_start in left_schema and self.outer_end in left_schema
        has_inner = self.inner_start in right_schema and self.inner_end in right_schema

        if self.file_key:
            has_file = self.file_key[0] in left_schema and self.file_key[1] in right_schema
            return has_outer and has_inner and has_file

        return has_outer and has_inner


@dataclass(frozen=True)
class ForeignKeyJoinStrategy:
    """Foreign key join (nullable key to primary key)."""

    foreign_key_column: str
    primary_key_column: str
    null_handling: Literal["filter", "preserve"] = "filter"

    @property
    def name(self) -> str:
        return f"fk_join({self.foreign_key_column})"

    @property
    def estimated_selectivity(self) -> float:
        return 0.8 if self.null_handling == "preserve" else 0.5

    def build_join_expr(self, left_alias: str, right_alias: str) -> str:
        base_expr = f"{left_alias}.{self.foreign_key_column} = {right_alias}.{self.primary_key_column}"

        if self.null_handling == "filter":
            return f"{left_alias}.{self.foreign_key_column} IS NOT NULL AND {base_expr}"

        return base_expr

    def applicable(
        self,
        left_schema: AnnotatedSchema,
        right_schema: AnnotatedSchema,
    ) -> bool:
        return (
            self.foreign_key_column in left_schema and
            self.primary_key_column in right_schema
        )


@dataclass(frozen=True)
class CompositeJoinStrategy:
    """Composite strategy combining multiple strategies."""

    strategies: tuple[JoinStrategyProtocol, ...]
    combination: Literal["and", "or"] = "and"

    @property
    def name(self) -> str:
        names = [s.name for s in self.strategies]
        return f"composite_{self.combination}({', '.join(names)})"

    @property
    def estimated_selectivity(self) -> float:
        if self.combination == "and":
            # Multiply selectivities for AND
            result = 1.0
            for s in self.strategies:
                result *= s.estimated_selectivity
            return result
        else:
            # For OR, use inclusion-exclusion approximation
            result = 0.0
            for s in self.strategies:
                result += s.estimated_selectivity
            return min(result, 1.0)

    def build_join_expr(self, left_alias: str, right_alias: str) -> str:
        exprs = [s.build_join_expr(left_alias, right_alias) for s in self.strategies]
        connector = " AND " if self.combination == "and" else " OR "
        return f"({connector.join(f'({e})' for e in exprs)})"

    def applicable(
        self,
        left_schema: AnnotatedSchema,
        right_schema: AnnotatedSchema,
    ) -> bool:
        if self.combination == "and":
            return all(s.applicable(left_schema, right_schema) for s in self.strategies)
        return any(s.applicable(left_schema, right_schema) for s in self.strategies)


# Type alias for any join strategy
JoinStrategyType = (
    EquiJoinStrategy |
    SpanOverlapJoinStrategy |
    SpanContainsJoinStrategy |
    ForeignKeyJoinStrategy |
    CompositeJoinStrategy
)


__all__ = [
    "CompositeJoinStrategy",
    "EquiJoinStrategy",
    "ForeignKeyJoinStrategy",
    "JoinStrategyProtocol",
    "JoinStrategyType",
    "SpanContainsJoinStrategy",
    "SpanOverlapJoinStrategy",
]
```

**Acceptance Criteria:**
- [ ] All join strategies implement common protocol
- [ ] SQL expression generation for each strategy
- [ ] Applicability checking against schemas
- [ ] Selectivity estimates for cost-based selection

---

### 2.2 Strategy Inference Engine

**File:** `src/semantics/joins/inference.py` (NEW)

```python
"""Automatic join strategy inference from semantic schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

from semantics.joins.strategies import (
    CompositeJoinStrategy,
    EquiJoinStrategy,
    ForeignKeyJoinStrategy,
    JoinStrategyType,
    SpanContainsJoinStrategy,
    SpanOverlapJoinStrategy,
)
from semantics.types.core import CompatibilityGroup, JoinStrategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from semantics.types.annotated_schema import AnnotatedSchema


@dataclass(frozen=True)
class InferredJoinPath:
    """An inferred join path between two schemas."""

    strategy: JoinStrategyType
    confidence: float  # 0-1, higher = more confident
    rationale: str  # Human-readable explanation


@dataclass(frozen=True)
class JoinInferenceResult:
    """Result of join strategy inference."""

    left_schema: AnnotatedSchema
    right_schema: AnnotatedSchema
    paths: tuple[InferredJoinPath, ...]
    recommended: InferredJoinPath | None

    @property
    def has_valid_path(self) -> bool:
        """Check if any valid join path exists."""
        return len(self.paths) > 0


def infer_join_strategies(
    left: AnnotatedSchema,
    right: AnnotatedSchema,
    *,
    intent: Literal["overlap", "contains", "ownership", "reference", "auto"] = "auto",
) -> JoinInferenceResult:
    """Infer valid join strategies between two schemas.

    Parameters
    ----------
    left
        Left side annotated schema.
    right
        Right side annotated schema.
    intent
        Join intent hint:
        - "overlap": Prefer span overlap joins
        - "contains": Prefer span containment joins
        - "ownership": Prefer foreign key joins
        - "reference": Prefer symbol/entity resolution
        - "auto": Infer from schema structure

    Returns
    -------
    JoinInferenceResult
        All valid join paths with recommendation.
    """
    paths: list[InferredJoinPath] = []

    # 1) Check for file identity + span pattern (most common for CPG)
    file_span_path = _infer_file_span_join(left, right, intent)
    if file_span_path:
        paths.append(file_span_path)

    # 2) Check for foreign key relationships
    fk_paths = _infer_foreign_key_joins(left, right)
    paths.extend(fk_paths)

    # 3) Check for symbol identity joins
    symbol_path = _infer_symbol_join(left, right)
    if symbol_path:
        paths.append(symbol_path)

    # 4) Check for pure equi-joins on compatible columns
    equi_paths = _infer_equi_joins(left, right)
    paths.extend(equi_paths)

    # Select recommendation (highest confidence that matches intent)
    recommended = _select_recommended(paths, intent)

    return JoinInferenceResult(
        left_schema=left,
        right_schema=right,
        paths=tuple(paths),
        recommended=recommended,
    )


def _infer_file_span_join(
    left: AnnotatedSchema,
    right: AnnotatedSchema,
    intent: Literal["overlap", "contains", "ownership", "reference", "auto"],
) -> InferredJoinPath | None:
    """Infer file identity + span overlap/contains join."""
    # Both need file identity
    left_file = left.file_identity_column()
    right_file = right.file_identity_column()
    if not left_file or not right_file:
        return None

    # Both need spans
    left_span = left.span_columns()
    right_span = right.span_columns()
    if not left_span or not right_span:
        return None

    file_key = (left_file.name, right_file.name)

    # Determine overlap vs contains based on intent or heuristics
    if intent == "contains":
        strategy = SpanContainsJoinStrategy(
            outer_start=left_span[0].name,
            outer_end=left_span[1].name,
            inner_start=right_span[0].name,
            inner_end=right_span[1].name,
            file_key=file_key,
        )
        rationale = "File identity + span containment (left contains right)"
    else:
        # Default to overlap
        strategy = SpanOverlapJoinStrategy(
            left_start=left_span[0].name,
            left_end=left_span[1].name,
            right_start=right_span[0].name,
            right_end=right_span[1].name,
            file_key=file_key,
        )
        rationale = "File identity + span overlap"

    return InferredJoinPath(
        strategy=strategy,
        confidence=0.95 if intent in ("overlap", "contains") else 0.85,
        rationale=rationale,
    )


def _infer_foreign_key_joins(
    left: AnnotatedSchema,
    right: AnnotatedSchema,
) -> list[InferredJoinPath]:
    """Infer foreign key join paths."""
    paths = []

    # Check left FK -> right PK
    for fk_col in left.foreign_key_columns():
        pk_col = right.entity_id_column()
        if pk_col and fk_col.compatibility_group == pk_col.compatibility_group:
            paths.append(
                InferredJoinPath(
                    strategy=ForeignKeyJoinStrategy(
                        foreign_key_column=fk_col.name,
                        primary_key_column=pk_col.name,
                        null_handling="filter",
                    ),
                    confidence=0.90,
                    rationale=f"Foreign key {fk_col.name} -> {pk_col.name}",
                )
            )

    # Check right FK -> left PK
    for fk_col in right.foreign_key_columns():
        pk_col = left.entity_id_column()
        if pk_col and fk_col.compatibility_group == pk_col.compatibility_group:
            paths.append(
                InferredJoinPath(
                    strategy=ForeignKeyJoinStrategy(
                        foreign_key_column=fk_col.name,
                        primary_key_column=pk_col.name,
                        null_handling="filter",
                    ),
                    confidence=0.90,
                    rationale=f"Foreign key {fk_col.name} -> {pk_col.name}",
                )
            )

    return paths


def _infer_symbol_join(
    left: AnnotatedSchema,
    right: AnnotatedSchema,
) -> InferredJoinPath | None:
    """Infer symbol identity join."""
    left_symbols = left.columns_by_group(CompatibilityGroup.SYMBOL_IDENTITY)
    right_symbols = right.columns_by_group(CompatibilityGroup.SYMBOL_IDENTITY)

    if not left_symbols or not right_symbols:
        return None

    # Use first symbol column from each
    return InferredJoinPath(
        strategy=EquiJoinStrategy(
            left_columns=(left_symbols[0].name,),
            right_columns=(right_symbols[0].name,),
        ),
        confidence=0.85,
        rationale=f"Symbol identity join on {left_symbols[0].name} = {right_symbols[0].name}",
    )


def _infer_equi_joins(
    left: AnnotatedSchema,
    right: AnnotatedSchema,
) -> list[InferredJoinPath]:
    """Infer pure equi-joins on compatible columns."""
    paths = []

    # Find all joinable pairs
    joinable = left.joinable_columns_with(right)

    # Group by compatibility group
    by_group: dict[CompatibilityGroup, list[tuple]] = {}
    for left_col, right_col in joinable:
        if left_col.join_strategy == JoinStrategy.EQUI:
            group = left_col.compatibility_group
            if group not in by_group:
                by_group[group] = []
            by_group[group].append((left_col.name, right_col.name))

    # Create equi-join for each group
    for group, pairs in by_group.items():
        if group == CompatibilityGroup.FILE_IDENTITY:
            # File identity alone is too broad, skip
            continue

        left_cols = tuple(p[0] for p in pairs)
        right_cols = tuple(p[1] for p in pairs)

        paths.append(
            InferredJoinPath(
                strategy=EquiJoinStrategy(
                    left_columns=left_cols,
                    right_columns=right_cols,
                ),
                confidence=0.70,
                rationale=f"Equi-join on {group.name}: {left_cols}",
            )
        )

    return paths


def _select_recommended(
    paths: list[InferredJoinPath],
    intent: Literal["overlap", "contains", "ownership", "reference", "auto"],
) -> InferredJoinPath | None:
    """Select the recommended join path based on confidence and intent."""
    if not paths:
        return None

    # Filter by intent if specified
    if intent == "ownership":
        fk_paths = [p for p in paths if isinstance(p.strategy, ForeignKeyJoinStrategy)]
        if fk_paths:
            return max(fk_paths, key=lambda p: p.confidence)

    if intent in ("overlap", "contains"):
        span_paths = [
            p for p in paths
            if isinstance(p.strategy, (SpanOverlapJoinStrategy, SpanContainsJoinStrategy))
        ]
        if span_paths:
            return max(span_paths, key=lambda p: p.confidence)

    # Default: highest confidence
    return max(paths, key=lambda p: p.confidence)


__all__ = [
    "InferredJoinPath",
    "JoinInferenceResult",
    "infer_join_strategies",
]
```

**Acceptance Criteria:**
- [ ] Automatic inference of file+span join patterns
- [ ] Foreign key relationship detection
- [ ] Symbol identity join inference
- [ ] Intent-based strategy selection

---

## Part 3: Semantic Catalog

### 3.1 Catalog Implementation

**File:** `src/semantics/catalog/catalog.py` (NEW)

```python
"""Semantic catalog for managing views with rich metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from semantics.joins.inference import JoinInferenceResult, infer_join_strategies
from semantics.types.annotated_schema import AnnotatedSchema

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from datafusion import DataFrame, SessionContext

    from datafusion_engine.plan.bundle import DataFrameBuilder, DataFusionPlanBundle
    from semantics.quality import EvidenceTier


@dataclass(frozen=True)
class SemanticViewEntry:
    """Entry in the semantic catalog with full metadata."""

    name: str
    semantic_schema: AnnotatedSchema
    evidence_tier: EvidenceTier
    upstream_deps: frozenset[str]
    plan_fingerprint: str | None = None
    row_count_estimate: int | None = None
    builder: DataFrameBuilder | None = None


@dataclass(frozen=True)
class JoinablePair:
    """A pair of views that can be joined."""

    left_view: str
    right_view: str
    inference_result: JoinInferenceResult


class SemanticCatalog:
    """Catalog of semantic views with join inference capabilities.

    The semantic catalog extends DataFusion's catalog system with:
    - Semantic type annotations on all columns
    - Automatic join path discovery between views
    - Evidence tier tracking for quality scoring
    - Plan fingerprints for caching
    """

    NAMESPACE: Final[str] = "semantic"

    def __init__(self, ctx: SessionContext):
        """Initialize catalog with DataFusion context.

        Parameters
        ----------
        ctx
            DataFusion session context for view registration.
        """
        self._ctx = ctx
        self._views: dict[str, SemanticViewEntry] = {}
        self._join_cache: dict[tuple[str, str], JoinInferenceResult] = {}

    @property
    def ctx(self) -> SessionContext:
        """Get the underlying DataFusion context."""
        return self._ctx

    def register_view(
        self,
        name: str,
        builder: DataFrameBuilder,
        *,
        evidence_tier: EvidenceTier,
        upstream_deps: frozenset[str] | None = None,
        type_overrides: Mapping[str, SemanticType] | None = None,
    ) -> SemanticViewEntry:
        """Register a view with semantic metadata.

        Parameters
        ----------
        name
            View name (will be prefixed with semantic namespace).
        builder
            DataFrame builder function.
        evidence_tier
            Evidence tier for quality scoring.
        upstream_deps
            Names of upstream views/tables this view depends on.
        type_overrides
            Manual semantic type annotations.

        Returns
        -------
        SemanticViewEntry
            The registered view entry with full metadata.
        """
        from semantics.types.core import SemanticType

        # Build DataFrame to get schema
        df = builder(self._ctx)

        # Create annotated schema
        schema = AnnotatedSchema.from_dataframe(
            df,
            source_name=name,
            type_overrides=type_overrides,
        )

        # Compute plan fingerprint
        from utils.hashing import stable_hash64
        plan_str = str(df.logical_plan())
        fingerprint = stable_hash64(plan_str)

        # Register in DataFusion with namespace prefix
        qualified_name = f"{self.NAMESPACE}.{name}"
        self._ctx.register_table(qualified_name, df)

        # Also register without prefix for convenience
        self._ctx.register_table(name, df)

        # Create entry
        entry = SemanticViewEntry(
            name=name,
            semantic_schema=schema,
            evidence_tier=evidence_tier,
            upstream_deps=upstream_deps or frozenset(),
            plan_fingerprint=fingerprint,
            builder=builder,
        )

        self._views[name] = entry

        # Invalidate join cache for this view
        self._invalidate_join_cache(name)

        return entry

    def get_view(self, name: str) -> SemanticViewEntry | None:
        """Get a view entry by name."""
        return self._views.get(name)

    def __getitem__(self, name: str) -> SemanticViewEntry:
        """Get a view entry, raising KeyError if not found."""
        if name not in self._views:
            raise KeyError(f"View not found in semantic catalog: {name}")
        return self._views[name]

    def __contains__(self, name: str) -> bool:
        """Check if a view is registered."""
        return name in self._views

    def __iter__(self) -> Iterator[str]:
        """Iterate over view names."""
        return iter(self._views)

    def __len__(self) -> int:
        """Get number of registered views."""
        return len(self._views)

    @property
    def view_names(self) -> tuple[str, ...]:
        """Get all registered view names."""
        return tuple(self._views.keys())

    def semantic_schema_for(self, name: str) -> AnnotatedSchema:
        """Get the annotated schema for a view."""
        return self[name].semantic_schema

    def infer_join(
        self,
        left_view: str,
        right_view: str,
        *,
        intent: str = "auto",
    ) -> JoinInferenceResult:
        """Infer join strategy between two views.

        Results are cached for performance.
        """
        cache_key = (left_view, right_view, intent)

        if cache_key not in self._join_cache:
            left_schema = self.semantic_schema_for(left_view)
            right_schema = self.semantic_schema_for(right_view)

            result = infer_join_strategies(
                left_schema,
                right_schema,
                intent=intent,
            )
            self._join_cache[cache_key] = result

        return self._join_cache[cache_key]

    def discover_all_join_paths(self) -> list[JoinablePair]:
        """Discover all valid join paths between registered views.

        Returns
        -------
        list[JoinablePair]
            All pairs of views that can be joined with inferred strategies.
        """
        pairs = []
        view_names = list(self._views.keys())

        for i, left_name in enumerate(view_names):
            for right_name in view_names[i + 1:]:
                result = self.infer_join(left_name, right_name)
                if result.has_valid_path:
                    pairs.append(
                        JoinablePair(
                            left_view=left_name,
                            right_view=right_name,
                            inference_result=result,
                        )
                    )

        return pairs

    def dependency_order(self) -> list[str]:
        """Get view names in dependency order (topological sort).

        Returns
        -------
        list[str]
            View names ordered so dependencies come before dependents.
        """
        from graphlib import TopologicalSorter

        # Build dependency graph
        graph: dict[str, set[str]] = {}
        for name, entry in self._views.items():
            # Filter to only include deps that are in this catalog
            deps = {d for d in entry.upstream_deps if d in self._views}
            graph[name] = deps

        sorter = TopologicalSorter(graph)
        return list(sorter.static_order())

    def _invalidate_join_cache(self, view_name: str) -> None:
        """Invalidate cached join results involving a view."""
        keys_to_remove = [
            k for k in self._join_cache
            if view_name in (k[0], k[1])
        ]
        for key in keys_to_remove:
            del self._join_cache[key]


__all__ = [
    "JoinablePair",
    "SemanticCatalog",
    "SemanticViewEntry",
]
```

**Acceptance Criteria:**
- [ ] View registration with semantic metadata
- [ ] Automatic join path discovery
- [ ] Dependency-ordered view iteration
- [ ] Join inference caching

---

## Part 4: Statistics-Aware Optimization

### 4.1 Statistics Collection

**File:** `src/semantics/stats/collector.py` (NEW)

```python
"""Statistics collection for semantic views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datafusion import DataFrame, SessionContext

    from semantics.types.annotated_schema import AnnotatedSchema


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column."""

    name: str
    null_count: int | None = None
    distinct_count: int | None = None
    min_value: object | None = None
    max_value: object | None = None
    avg_size_bytes: int | None = None  # For variable-length columns


@dataclass(frozen=True)
class TableStatistics:
    """Statistics for a table/view."""

    row_count: int
    total_bytes: int | None = None
    column_stats: Mapping[str, ColumnStatistics] | None = None

    @classmethod
    def from_dataframe(
        cls,
        df: DataFrame,
        *,
        compute_column_stats: bool = False,
    ) -> TableStatistics:
        """Compute statistics from a DataFrame.

        Parameters
        ----------
        df
            DataFrame to analyze.
        compute_column_stats
            Whether to compute per-column statistics (slower).

        Returns
        -------
        TableStatistics
            Computed statistics.
        """
        # Get row count (requires execution)
        row_count = df.count()

        column_stats = None
        if compute_column_stats:
            column_stats = _compute_column_stats(df)

        return cls(
            row_count=row_count,
            column_stats=column_stats,
        )

    @classmethod
    def estimate_from_schema(
        cls,
        schema: AnnotatedSchema,
        *,
        estimated_rows: int,
    ) -> TableStatistics:
        """Create estimated statistics without execution.

        Parameters
        ----------
        schema
            Annotated schema for the table.
        estimated_rows
            Estimated row count.

        Returns
        -------
        TableStatistics
            Estimated statistics.
        """
        # Estimate total bytes based on schema
        bytes_per_row = _estimate_bytes_per_row(schema)

        return cls(
            row_count=estimated_rows,
            total_bytes=estimated_rows * bytes_per_row,
        )


def _compute_column_stats(df: DataFrame) -> dict[str, ColumnStatistics]:
    """Compute per-column statistics."""
    stats = {}

    for field in df.schema():
        col_name = field.name

        # Build aggregation query
        agg_df = df.aggregate(
            [],
            [
                f.count(col(col_name)).alias("cnt"),
                f.count(lit(1)).alias("total"),
                f.approx_distinct(col(col_name)).alias("approx_distinct"),
                f.min(col(col_name)).alias("min_val"),
                f.max(col(col_name)).alias("max_val"),
            ],
        )

        result = agg_df.collect()[0]
        total = result["total"].as_py()
        cnt = result["cnt"].as_py()

        stats[col_name] = ColumnStatistics(
            name=col_name,
            null_count=total - cnt,
            distinct_count=result["approx_distinct"].as_py(),
            min_value=result["min_val"].as_py(),
            max_value=result["max_val"].as_py(),
        )

    return stats


def _estimate_bytes_per_row(schema: AnnotatedSchema) -> int:
    """Estimate bytes per row from schema."""
    import pyarrow as pa

    total = 0
    for col in schema.columns:
        if pa.types.is_int64(col.arrow_type):
            total += 8
        elif pa.types.is_int32(col.arrow_type):
            total += 4
        elif pa.types.is_boolean(col.arrow_type):
            total += 1
        elif pa.types.is_float64(col.arrow_type):
            total += 8
        elif pa.types.is_string(col.arrow_type) or pa.types.is_large_string(col.arrow_type):
            total += 50  # Estimate for variable-length
        elif pa.types.is_binary(col.arrow_type):
            total += 100  # Estimate for binary
        else:
            total += 8  # Default estimate

    return total


__all__ = [
    "ColumnStatistics",
    "TableStatistics",
]
```

### 4.2 Custom Table Provider

**File:** `src/semantics/stats/table_provider.py` (NEW)

```python
"""Custom table provider that exposes statistics to DataFusion."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from semantics.stats.collector import TableStatistics
    from semantics.types.annotated_schema import AnnotatedSchema


class SemanticTableProvider:
    """Table provider that exposes semantic metadata and statistics.

    This provider wraps a PyArrow table and provides:
    - Statistics for DataFusion's cost-based optimizer
    - Filter pushdown hints based on semantic types
    - Projection pushdown

    Note: Full implementation requires DataFusion's Python TableProvider
    interface which is still evolving. This is the target API.
    """

    def __init__(
        self,
        table: pa.Table,
        *,
        semantic_schema: AnnotatedSchema,
        statistics: TableStatistics | None = None,
    ):
        """Initialize table provider.

        Parameters
        ----------
        table
            The underlying PyArrow table.
        semantic_schema
            Annotated schema with semantic types.
        statistics
            Pre-computed statistics (computed if not provided).
        """
        self._table = table
        self._semantic_schema = semantic_schema
        self._statistics = statistics

    @property
    def schema(self) -> pa.Schema:
        """Get the table schema."""
        return self._table.schema

    def statistics(self) -> dict:
        """Get statistics in DataFusion's expected format.

        Returns statistics that DataFusion's optimizer can use for
        cost-based join ordering and predicate pushdown.
        """
        if self._statistics is None:
            return {}

        return {
            "num_rows": self._statistics.row_count,
            "total_byte_size": self._statistics.total_bytes,
            "column_statistics": self._format_column_stats(),
        }

    def _format_column_stats(self) -> list[dict] | None:
        """Format column statistics for DataFusion."""
        if not self._statistics or not self._statistics.column_stats:
            return None

        result = []
        for field in self._table.schema:
            col_stats = self._statistics.column_stats.get(field.name)
            if col_stats:
                result.append({
                    "null_count": col_stats.null_count,
                    "distinct_count": col_stats.distinct_count,
                    "min_value": col_stats.min_value,
                    "max_value": col_stats.max_value,
                })
            else:
                result.append({})

        return result

    def supports_filter_pushdown(self, filter_expr: str) -> str:
        """Determine filter pushdown support.

        Returns
        -------
        str
            One of: "exact", "inexact", "unsupported"
        """
        # Span filters on indexed columns are highly effective
        if self._is_span_filter(filter_expr):
            return "exact"

        # File identity filters are always pushed down
        if self._is_file_identity_filter(filter_expr):
            return "exact"

        return "unsupported"

    def _is_span_filter(self, filter_expr: str) -> bool:
        """Check if filter involves span columns."""
        span_keywords = ("bstart", "bend", "span_start", "span_end")
        return any(kw in filter_expr.lower() for kw in span_keywords)

    def _is_file_identity_filter(self, filter_expr: str) -> bool:
        """Check if filter involves file identity."""
        return "file_id" in filter_expr.lower() or "path" in filter_expr.lower()


__all__ = [
    "SemanticTableProvider",
]
```

**Acceptance Criteria:**
- [ ] Statistics collection from DataFrames
- [ ] Estimated statistics without execution
- [ ] Filter pushdown hints based on semantic types

---

## Part 5: Plan Template Library

### 5.1 Template Definitions

**File:** `src/semantics/plans/templates.py` (NEW)

```python
"""Reusable plan templates for common join patterns."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal, Protocol

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from semantics.joins.strategies import JoinStrategyType
    from semantics.stats.collector import TableStatistics


class PlanTemplate(Protocol):
    """Protocol for plan templates."""

    @property
    def name(self) -> str:
        """Template name for logging."""
        ...

    def applicable(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> bool:
        """Check if template is applicable given statistics."""
        ...

    def estimated_cost(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> float:
        """Estimate execution cost for comparison."""
        ...

    def build_plan(
        self,
        ctx: SessionContext,
        left: DataFrame,
        right: DataFrame,
        *,
        strategy: JoinStrategyType,
    ) -> DataFrame:
        """Build the join plan."""
        ...


@dataclass(frozen=True)
class BroadcastJoinTemplate:
    """Broadcast join for small right tables.

    Broadcasts the smaller table to all partitions for local joins.
    Efficient when one side fits in memory.
    """

    SMALL_TABLE_THRESHOLD: Final[int] = 100_000  # rows

    @property
    def name(self) -> str:
        return "broadcast_join"

    def applicable(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> bool:
        # Applicable when right side is small
        return right_stats.row_count < self.SMALL_TABLE_THRESHOLD

    def estimated_cost(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> float:
        # Cost is dominated by broadcast + local join
        broadcast_cost = right_stats.row_count * 10  # Network transfer
        join_cost = left_stats.row_count * 0.001  # Fast local lookup
        return broadcast_cost + join_cost

    def build_plan(
        self,
        ctx: SessionContext,
        left: DataFrame,
        right: DataFrame,
        *,
        strategy: JoinStrategyType,
    ) -> DataFrame:
        # DataFusion will automatically choose broadcast when appropriate
        # We can hint via configuration
        join_expr = strategy.build_join_expr("l", "r")

        return left.join(
            right,
            join_type="inner",
            left_on=[],
            right_on=[],
        ).filter(join_expr)


@dataclass(frozen=True)
class HashPartitionJoinTemplate:
    """Hash-partitioned join for large tables.

    Partitions both tables by join key, then joins within partitions.
    Efficient for large tables with good key distribution.
    """

    @property
    def name(self) -> str:
        return "hash_partition_join"

    def applicable(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> bool:
        # Always applicable as fallback
        return True

    def estimated_cost(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> float:
        # Cost: shuffle both sides + local join
        shuffle_cost = (left_stats.row_count + right_stats.row_count) * 5
        join_cost = min(left_stats.row_count, right_stats.row_count) * 0.1
        return shuffle_cost + join_cost

    def build_plan(
        self,
        ctx: SessionContext,
        left: DataFrame,
        right: DataFrame,
        *,
        strategy: JoinStrategyType,
    ) -> DataFrame:
        join_expr = strategy.build_join_expr("l", "r")

        # DataFusion handles partitioning automatically
        return left.join(
            right,
            join_type="inner",
            left_on=[],
            right_on=[],
        ).filter(join_expr)


@dataclass(frozen=True)
class SortMergeJoinTemplate:
    """Sort-merge join for pre-sorted data.

    Efficient when data is already sorted on join keys.
    """

    @property
    def name(self) -> str:
        return "sort_merge_join"

    def applicable(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> bool:
        # Only applicable for large datasets where sort-merge wins
        return (
            left_stats.row_count > 1_000_000 and
            right_stats.row_count > 1_000_000
        )

    def estimated_cost(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> float:
        import math

        # Cost: sort both sides + linear merge
        left_sort = left_stats.row_count * math.log2(left_stats.row_count + 1)
        right_sort = right_stats.row_count * math.log2(right_stats.row_count + 1)
        merge_cost = left_stats.row_count + right_stats.row_count
        return left_sort + right_sort + merge_cost

    def build_plan(
        self,
        ctx: SessionContext,
        left: DataFrame,
        right: DataFrame,
        *,
        strategy: JoinStrategyType,
    ) -> DataFrame:
        join_expr = strategy.build_join_expr("l", "r")

        return left.join(
            right,
            join_type="inner",
            left_on=[],
            right_on=[],
        ).filter(join_expr)


@dataclass(frozen=True)
class IntervalIndexJoinTemplate:
    """Interval index join for span overlaps.

    Uses interval tree indexing for efficient span overlap queries.
    """

    @property
    def name(self) -> str:
        return "interval_index_join"

    def applicable(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> bool:
        # Only for span-based joins with large enough data
        return (
            left_stats.row_count > 10_000 or
            right_stats.row_count > 10_000
        )

    def estimated_cost(
        self,
        left_stats: TableStatistics,
        right_stats: TableStatistics,
    ) -> float:
        import math

        # Cost: build index on smaller side + probe with larger
        smaller = min(left_stats.row_count, right_stats.row_count)
        larger = max(left_stats.row_count, right_stats.row_count)

        index_build = smaller * math.log2(smaller + 1)
        probe_cost = larger * math.log2(smaller + 1) * 2  # Each probe may return multiple
        return index_build + probe_cost

    def build_plan(
        self,
        ctx: SessionContext,
        left: DataFrame,
        right: DataFrame,
        *,
        strategy: JoinStrategyType,
    ) -> DataFrame:
        # DataFusion doesn't have native interval index yet
        # Fall back to hash join with filter
        join_expr = strategy.build_join_expr("l", "r")

        return left.join(
            right,
            join_type="inner",
            left_on=[],
            right_on=[],
        ).filter(join_expr)


# All available templates
PLAN_TEMPLATES: Final[tuple[PlanTemplate, ...]] = (
    BroadcastJoinTemplate(),
    IntervalIndexJoinTemplate(),
    SortMergeJoinTemplate(),
    HashPartitionJoinTemplate(),  # Fallback
)


def select_best_template(
    left_stats: TableStatistics,
    right_stats: TableStatistics,
) -> PlanTemplate:
    """Select the best plan template based on statistics.

    Parameters
    ----------
    left_stats
        Statistics for left table.
    right_stats
        Statistics for right table.

    Returns
    -------
    PlanTemplate
        The template with lowest estimated cost.
    """
    applicable = [
        t for t in PLAN_TEMPLATES
        if t.applicable(left_stats, right_stats)
    ]

    if not applicable:
        # Should never happen since HashPartitionJoinTemplate is always applicable
        return HashPartitionJoinTemplate()

    return min(
        applicable,
        key=lambda t: t.estimated_cost(left_stats, right_stats),
    )


__all__ = [
    "BroadcastJoinTemplate",
    "HashPartitionJoinTemplate",
    "IntervalIndexJoinTemplate",
    "PLAN_TEMPLATES",
    "PlanTemplate",
    "SortMergeJoinTemplate",
    "select_best_template",
]
```

**Acceptance Criteria:**
- [ ] Multiple join templates with cost models
- [ ] Statistics-based template selection
- [ ] Fallback to hash-partition join

---

## Part 6: Substrait Plan Caching

### 6.1 Substrait Integration

**File:** `src/semantics/plans/substrait.py` (NEW)

```python
"""Substrait plan serialization for caching and portability."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


@dataclass(frozen=True)
class SubstratePlan:
    """A serialized Substrait plan with metadata."""

    plan_bytes: bytes
    schema_fingerprint: str
    relationship_name: str
    created_at: str


def serialize_to_substrait(
    df: DataFrame,
    *,
    relationship_name: str,
    schema_fingerprint: str,
) -> SubstratePlan:
    """Serialize a DataFrame plan to Substrait.

    Parameters
    ----------
    df
        DataFrame to serialize.
    relationship_name
        Name of the relationship for metadata.
    schema_fingerprint
        Fingerprint of input schemas for cache invalidation.

    Returns
    -------
    SubstratePlan
        Serialized plan with metadata.
    """
    from datetime import datetime, timezone

    # DataFusion's Substrait producer
    try:
        from datafusion.substrait import Serde
        plan_bytes = Serde.serialize_to_plan(df.logical_plan())
    except ImportError:
        # Substrait support may not be available
        # Fall back to logical plan string (not portable but useful for debugging)
        plan_bytes = str(df.logical_plan()).encode("utf-8")

    return SubstratePlan(
        plan_bytes=plan_bytes,
        schema_fingerprint=schema_fingerprint,
        relationship_name=relationship_name,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


def deserialize_from_substrait(
    ctx: SessionContext,
    plan: SubstratePlan,
) -> DataFrame:
    """Deserialize a Substrait plan to a DataFrame.

    Parameters
    ----------
    ctx
        DataFusion session context.
    plan
        Serialized Substrait plan.

    Returns
    -------
    DataFrame
        Executable DataFrame from the plan.
    """
    try:
        from datafusion.substrait import Serde
        logical_plan = Serde.deserialize_to_plan(plan.plan_bytes, ctx)
        return ctx.create_dataframe_from_logical_plan(logical_plan)
    except ImportError:
        # If Substrait not available, we can't deserialize
        msg = "Substrait support not available for plan deserialization"
        raise RuntimeError(msg)


class SubstratePlanCache:
    """Cache for Substrait plans with disk persistence.

    Caches compiled relationship plans to avoid recompilation.
    Cache invalidation is based on schema fingerprints.
    """

    CACHE_VERSION: Final[str] = "v1"

    def __init__(self, cache_dir: Path):
        """Initialize cache with directory.

        Parameters
        ----------
        cache_dir
            Directory for storing cached plans.
        """
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def cache_key(
        self,
        relationship_name: str,
        schema_fingerprint: str,
    ) -> str:
        """Compute cache key from relationship and schema."""
        from utils.hashing import stable_hash64

        return stable_hash64(
            self.CACHE_VERSION,
            relationship_name,
            schema_fingerprint,
        )

    def _cache_path(self, key: str) -> Path:
        """Get file path for a cache key."""
        return self._cache_dir / f"{key}.substrait"

    def get(
        self,
        relationship_name: str,
        schema_fingerprint: str,
    ) -> SubstratePlan | None:
        """Get cached plan if available.

        Parameters
        ----------
        relationship_name
            Name of the relationship.
        schema_fingerprint
            Current schema fingerprint for validation.

        Returns
        -------
        SubstratePlan | None
            Cached plan or None if not found/invalid.
        """
        import json

        key = self.cache_key(relationship_name, schema_fingerprint)
        plan_path = self._cache_path(key)
        meta_path = plan_path.with_suffix(".meta.json")

        if not plan_path.exists() or not meta_path.exists():
            return None

        # Load and validate metadata
        meta = json.loads(meta_path.read_text())
        if meta.get("schema_fingerprint") != schema_fingerprint:
            # Schema changed, invalidate cache
            plan_path.unlink(missing_ok=True)
            meta_path.unlink(missing_ok=True)
            return None

        return SubstratePlan(
            plan_bytes=plan_path.read_bytes(),
            schema_fingerprint=schema_fingerprint,
            relationship_name=relationship_name,
            created_at=meta.get("created_at", ""),
        )

    def put(self, plan: SubstratePlan) -> None:
        """Store a plan in the cache.

        Parameters
        ----------
        plan
            Substrait plan to cache.
        """
        import json

        key = self.cache_key(plan.relationship_name, plan.schema_fingerprint)
        plan_path = self._cache_path(key)
        meta_path = plan_path.with_suffix(".meta.json")

        # Write plan bytes
        plan_path.write_bytes(plan.plan_bytes)

        # Write metadata
        meta = {
            "relationship_name": plan.relationship_name,
            "schema_fingerprint": plan.schema_fingerprint,
            "created_at": plan.created_at,
            "cache_version": self.CACHE_VERSION,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

    def get_or_compile(
        self,
        ctx: SessionContext,
        relationship_name: str,
        schema_fingerprint: str,
        compile_fn: callable,
    ) -> DataFrame:
        """Get cached plan or compile and cache.

        Parameters
        ----------
        ctx
            DataFusion session context.
        relationship_name
            Name of the relationship.
        schema_fingerprint
            Schema fingerprint for cache key.
        compile_fn
            Function to compile DataFrame if not cached.

        Returns
        -------
        DataFrame
            Executable DataFrame (from cache or freshly compiled).
        """
        # Try cache first
        cached = self.get(relationship_name, schema_fingerprint)
        if cached:
            try:
                return deserialize_from_substrait(ctx, cached)
            except RuntimeError:
                # Substrait deserialization failed, recompile
                pass

        # Compile fresh
        df = compile_fn()

        # Cache the result
        plan = serialize_to_substrait(
            df,
            relationship_name=relationship_name,
            schema_fingerprint=schema_fingerprint,
        )
        self.put(plan)

        return df

    def clear(self) -> int:
        """Clear all cached plans.

        Returns
        -------
        int
            Number of plans cleared.
        """
        count = 0
        for path in self._cache_dir.glob("*.substrait"):
            path.unlink()
            path.with_suffix(".meta.json").unlink(missing_ok=True)
            count += 1
        return count


__all__ = [
    "SubstratePlan",
    "SubstratePlanCache",
    "deserialize_from_substrait",
    "serialize_to_substrait",
]
```

**Acceptance Criteria:**
- [ ] Substrait serialization with fallback
- [ ] Schema-fingerprint-based cache invalidation
- [ ] Disk persistence for cache

---

## Part 7: Incremental Computation

### 7.1 CDF-Based Incremental Joins

**File:** `src/semantics/incremental/cdf_joins.py` (NEW)

```python
"""CDF-based incremental relationship computation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from semantics.joins.strategies import JoinStrategyType


@dataclass(frozen=True)
class IncrementalJoinSpec:
    """Specification for incremental join computation."""

    relationship_name: str
    left_view: str
    right_view: str
    strategy: JoinStrategyType

    # Which changes trigger recomputation
    change_sensitivity: Literal["left_only", "right_only", "both"] = "both"

    # Merge strategy for results
    merge_key: tuple[str, ...]
    merge_strategy: Literal["upsert", "append", "replace"] = "upsert"


def compute_incremental_join(
    ctx: SessionContext,
    spec: IncrementalJoinSpec,
    *,
    left_cdf: DataFrame | None,
    right_cdf: DataFrame | None,
    output_schema: pa.Schema,
) -> DataFrame:
    """Compute incremental join from change data feeds.

    Parameters
    ----------
    ctx
        DataFusion session context.
    spec
        Incremental join specification.
    left_cdf
        Change data for left view (or None if unchanged).
    right_cdf
        Change data for right view (or None if unchanged).
    output_schema
        Expected output schema for empty results.

    Returns
    -------
    DataFrame
        Changed relationship edges.
    """
    import pyarrow as pa

    # No changes = empty result
    if left_cdf is None and right_cdf is None:
        empty = pa.table({f.name: [] for f in output_schema}, schema=output_schema)
        return ctx.create_dataframe([[empty]])

    # Determine what to recompute based on sensitivity
    if spec.change_sensitivity == "left_only":
        if left_cdf is None:
            # Left didn't change, nothing to do
            empty = pa.table({f.name: [] for f in output_schema}, schema=output_schema)
            return ctx.create_dataframe([[empty]])

        # Join changed left rows against full right
        right_full = ctx.table(spec.right_view)
        return _execute_join(ctx, left_cdf, right_full, spec.strategy)

    elif spec.change_sensitivity == "right_only":
        if right_cdf is None:
            empty = pa.table({f.name: [] for f in output_schema}, schema=output_schema)
            return ctx.create_dataframe([[empty]])

        left_full = ctx.table(spec.left_view)
        return _execute_join(ctx, left_full, right_cdf, spec.strategy)

    else:  # both
        results = []

        if left_cdf is not None:
            # Changed left against full right
            right_full = ctx.table(spec.right_view)
            results.append(_execute_join(ctx, left_cdf, right_full, spec.strategy))

        if right_cdf is not None:
            # Full left against changed right (but exclude already computed)
            left_full = ctx.table(spec.left_view)
            right_result = _execute_join(ctx, left_full, right_cdf, spec.strategy)

            if left_cdf is not None:
                # Deduplicate: remove rows already covered by left_cdf join
                right_result = _deduplicate_incremental(
                    right_result,
                    left_cdf,
                    spec.merge_key,
                )
            results.append(right_result)

        if not results:
            empty = pa.table({f.name: [] for f in output_schema}, schema=output_schema)
            return ctx.create_dataframe([[empty]])

        # Union all results
        return _union_all(ctx, results)


def _execute_join(
    ctx: SessionContext,
    left: DataFrame,
    right: DataFrame,
    strategy: JoinStrategyType,
) -> DataFrame:
    """Execute a join with the given strategy."""
    join_expr = strategy.build_join_expr("l", "r")

    return left.join(
        right,
        join_type="inner",
        left_on=[],
        right_on=[],
    ).filter(join_expr)


def _deduplicate_incremental(
    result: DataFrame,
    exclude_source: DataFrame,
    merge_key: tuple[str, ...],
) -> DataFrame:
    """Remove rows from result that came from exclude_source."""
    # Anti-join on merge key
    return result.join(
        exclude_source.select(*merge_key),
        join_type="anti",
        left_on=list(merge_key),
        right_on=list(merge_key),
    )


def _union_all(ctx: SessionContext, dfs: list[DataFrame]) -> DataFrame:
    """Union multiple DataFrames."""
    if len(dfs) == 1:
        return dfs[0]

    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)
    return result


@dataclass(frozen=True)
class IncrementalMergeResult:
    """Result of merging incremental changes into base."""

    merged_df: DataFrame
    rows_inserted: int
    rows_updated: int
    rows_deleted: int


def merge_incremental_changes(
    ctx: SessionContext,
    base: DataFrame,
    changes: DataFrame,
    *,
    merge_key: tuple[str, ...],
    strategy: Literal["upsert", "append", "replace"],
) -> IncrementalMergeResult:
    """Merge incremental changes into base DataFrame.

    Parameters
    ----------
    ctx
        DataFusion session context.
    base
        Base DataFrame to merge into.
    changes
        Changed rows to merge.
    merge_key
        Columns that identify unique rows.
    strategy
        Merge strategy:
        - "upsert": Insert new, update existing
        - "append": Only insert new (ignore existing)
        - "replace": Replace base with changes entirely

    Returns
    -------
    IncrementalMergeResult
        Merged DataFrame with statistics.
    """
    if strategy == "replace":
        return IncrementalMergeResult(
            merged_df=changes,
            rows_inserted=0,
            rows_updated=0,
            rows_deleted=0,
        )

    if strategy == "append":
        # Anti-join to get only new rows
        new_rows = changes.join(
            base.select(*merge_key),
            join_type="anti",
            left_on=list(merge_key),
            right_on=list(merge_key),
        )
        merged = base.union(new_rows)
        return IncrementalMergeResult(
            merged_df=merged,
            rows_inserted=new_rows.count(),
            rows_updated=0,
            rows_deleted=0,
        )

    # upsert strategy
    # Remove old versions of changed rows, then add new versions
    unchanged = base.join(
        changes.select(*merge_key),
        join_type="anti",
        left_on=list(merge_key),
        right_on=list(merge_key),
    )
    merged = unchanged.union(changes)

    # Count statistics
    base_count = base.count()
    changes_count = changes.count()
    merged_count = merged.count()

    rows_updated = base_count - (merged_count - changes_count)
    rows_inserted = changes_count - rows_updated

    return IncrementalMergeResult(
        merged_df=merged,
        rows_inserted=max(0, rows_inserted),
        rows_updated=max(0, rows_updated),
        rows_deleted=0,
    )


__all__ = [
    "IncrementalJoinSpec",
    "IncrementalMergeResult",
    "compute_incremental_join",
    "merge_incremental_changes",
]
```

**Acceptance Criteria:**
- [ ] CDF-based incremental join computation
- [ ] Change sensitivity configuration
- [ ] Merge strategies for results

---

## Part 8: Self-Documenting Architecture

### 8.1 Graph Documentation Generator

**File:** `src/semantics/docs/graph_docs.py` (NEW)

```python
"""Auto-generate documentation from semantic graph structure."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Sequence

    from semantics.catalog.catalog import SemanticCatalog
    from semantics.relationships.declaration import RelationshipDeclaration


@dataclass(frozen=True)
class ViewDocumentation:
    """Documentation for a semantic view."""

    name: str
    description: str
    columns: list[dict]
    semantic_types: list[str]
    evidence_tier: str
    upstream_deps: list[str]
    downstream_deps: list[str]
    row_count_estimate: int | None


@dataclass(frozen=True)
class RelationshipDocumentation:
    """Documentation for a relationship."""

    name: str
    description: str
    left_view: str
    right_view: str
    join_strategy: str
    evidence_tier: str
    quality_signals: list[str]
    output_columns: list[str]


@dataclass(frozen=True)
class GraphDocumentation:
    """Complete documentation for the semantic graph."""

    views: list[ViewDocumentation]
    relationships: list[RelationshipDocumentation]
    mermaid_diagram: str
    json_schema: dict


def generate_graph_documentation(
    catalog: SemanticCatalog,
    relationships: Sequence[RelationshipDeclaration],
) -> GraphDocumentation:
    """Generate comprehensive documentation for the semantic graph.

    Parameters
    ----------
    catalog
        Semantic catalog with registered views.
    relationships
        Relationship declarations.

    Returns
    -------
    GraphDocumentation
        Complete documentation with diagrams.
    """
    # Generate view documentation
    view_docs = []
    for name in catalog.view_names:
        entry = catalog[name]
        view_docs.append(
            ViewDocumentation(
                name=name,
                description=f"Semantic view for {name}",
                columns=[
                    {
                        "name": col.name,
                        "type": str(col.arrow_type),
                        "semantic_type": col.semantic_type.name if col.semantic_type else None,
                    }
                    for col in entry.semantic_schema
                ],
                semantic_types=[
                    col.semantic_type.name
                    for col in entry.semantic_schema
                    if col.semantic_type
                ],
                evidence_tier=entry.evidence_tier.name,
                upstream_deps=list(entry.upstream_deps),
                downstream_deps=_find_downstream(name, relationships),
                row_count_estimate=entry.row_count_estimate,
            )
        )

    # Generate relationship documentation
    rel_docs = []
    for rel in relationships:
        compiled = rel.compile(catalog)
        rel_docs.append(
            RelationshipDocumentation(
                name=rel.name,
                description=f"Relationship: {rel.left_view} -> {rel.right_view}",
                left_view=rel.left_view,
                right_view=rel.right_view,
                join_strategy=compiled.strategy.name if compiled.strategy else "unknown",
                evidence_tier=rel.evidence_tier.name,
                quality_signals=[f.name for f in rel.signals.features],
                output_columns=list(rel.required_output_columns),
            )
        )

    # Generate Mermaid diagram
    mermaid = _generate_mermaid_diagram(view_docs, rel_docs)

    # Generate JSON schema
    json_schema = _generate_json_schema(view_docs, rel_docs)

    return GraphDocumentation(
        views=view_docs,
        relationships=rel_docs,
        mermaid_diagram=mermaid,
        json_schema=json_schema,
    )


def _find_downstream(view_name: str, relationships: Sequence[RelationshipDeclaration]) -> list[str]:
    """Find relationships that use this view as input."""
    downstream = []
    for rel in relationships:
        if view_name in (rel.left_view, rel.right_view):
            downstream.append(rel.name)
    return downstream


def _generate_mermaid_diagram(
    views: list[ViewDocumentation],
    relationships: list[RelationshipDocumentation],
) -> str:
    """Generate Mermaid flowchart diagram."""
    lines = ["```mermaid", "flowchart TD"]

    # Add view nodes with styling based on evidence tier
    tier_styles = {
        "SCIP": "fill:#90EE90",
        "SYMTABLE": "fill:#87CEEB",
        "BYTECODE": "fill:#DDA0DD",
        "CST": "fill:#FFD700",
        "HEURISTIC": "fill:#FFA07A",
    }

    for view in views:
        style = tier_styles.get(view.evidence_tier, "fill:#FFFFFF")
        lines.append(f"    {view.name}[{view.name}]")
        lines.append(f"    style {view.name} {style}")

    # Add relationship edges
    for rel in relationships:
        label = rel.join_strategy.replace("_", " ")
        lines.append(f"    {rel.left_view} -->|{label}| {rel.right_view}")

    lines.append("```")
    return "\n".join(lines)


def _generate_json_schema(
    views: list[ViewDocumentation],
    relationships: list[RelationshipDocumentation],
) -> dict:
    """Generate JSON schema representation."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Semantic Graph Schema",
        "type": "object",
        "properties": {
            "views": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "evidence_tier": {"type": "string"},
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "type": {"type": "string"},
                                    "semantic_type": {"type": ["string", "null"]},
                                },
                            },
                        },
                    },
                },
            },
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "left_view": {"type": "string"},
                        "right_view": {"type": "string"},
                        "join_strategy": {"type": "string"},
                    },
                },
            },
        },
    }


def export_documentation_markdown(docs: GraphDocumentation) -> str:
    """Export documentation as Markdown.

    Parameters
    ----------
    docs
        Graph documentation to export.

    Returns
    -------
    str
        Markdown-formatted documentation.
    """
    lines = [
        "# Semantic Graph Documentation",
        "",
        "## Overview Diagram",
        "",
        docs.mermaid_diagram,
        "",
        "## Views",
        "",
    ]

    for view in docs.views:
        lines.extend([
            f"### {view.name}",
            "",
            f"**Evidence Tier:** {view.evidence_tier}",
            "",
            f"**Upstream Dependencies:** {', '.join(view.upstream_deps) or 'None'}",
            "",
            "**Columns:**",
            "",
            "| Name | Type | Semantic Type |",
            "|------|------|---------------|",
        ])

        for col in view.columns:
            sem_type = col.get("semantic_type") or "-"
            lines.append(f"| {col['name']} | {col['type']} | {sem_type} |")

        lines.append("")

    lines.extend([
        "## Relationships",
        "",
    ])

    for rel in docs.relationships:
        lines.extend([
            f"### {rel.name}",
            "",
            f"**Join:** {rel.left_view} -> {rel.right_view}",
            "",
            f"**Strategy:** {rel.join_strategy}",
            "",
            f"**Evidence Tier:** {rel.evidence_tier}",
            "",
            f"**Quality Signals:** {', '.join(rel.quality_signals) or 'None'}",
            "",
        ])

    return "\n".join(lines)


__all__ = [
    "GraphDocumentation",
    "RelationshipDocumentation",
    "ViewDocumentation",
    "export_documentation_markdown",
    "generate_graph_documentation",
]
```

**Acceptance Criteria:**
- [ ] Auto-generated view and relationship documentation
- [ ] Mermaid diagram generation
- [ ] Markdown export

---

## Part 9: Unified View Builder Protocol

### 9.1 Builder Protocol

**File:** `src/semantics/builders/protocol.py` (NEW)

```python
"""Protocol for semantic view builders ensuring consistency."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from semantics.quality import EvidenceTier
    from semantics.types.annotated_schema import AnnotatedSchema


@dataclass(frozen=True)
class BuilderValidationResult:
    """Result of validating builder inputs."""

    valid: bool
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]
    warnings: tuple[str, ...]


@runtime_checkable
class SemanticViewBuilder(Protocol):
    """Protocol for semantic view builders.

    All view builders must implement this protocol to ensure
    consistent behavior and metadata exposure.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """View name (without namespace prefix)."""
        ...

    @property
    @abstractmethod
    def semantic_schema(self) -> AnnotatedSchema:
        """Semantic schema with type annotations.

        Note: This should return the expected output schema,
        not require building the view first.
        """
        ...

    @property
    @abstractmethod
    def evidence_tier(self) -> EvidenceTier:
        """Evidence tier for quality scoring."""
        ...

    @property
    @abstractmethod
    def upstream_dependencies(self) -> frozenset[str]:
        """Names of required upstream tables/views."""
        ...

    @property
    def optional_dependencies(self) -> frozenset[str]:
        """Names of optional upstream tables/views."""
        return frozenset()

    @abstractmethod
    def build(self, ctx: SessionContext) -> DataFrame:
        """Build the view DataFrame.

        Parameters
        ----------
        ctx
            DataFusion session context with dependencies registered.

        Returns
        -------
        DataFrame
            The built view.

        Raises
        ------
        ValueError
            If required dependencies are missing.
        """
        ...

    def validate_inputs(self, ctx: SessionContext) -> BuilderValidationResult:
        """Validate that required inputs are available.

        Default implementation checks table existence.
        Override for custom validation.
        """
        from datafusion_engine.schema.introspection import table_names_snapshot

        available = set(table_names_snapshot(ctx))

        missing_required = tuple(
            dep for dep in self.upstream_dependencies
            if dep not in available
        )

        missing_optional = tuple(
            dep for dep in self.optional_dependencies
            if dep not in available
        )

        return BuilderValidationResult(
            valid=len(missing_required) == 0,
            missing_required=missing_required,
            missing_optional=missing_optional,
            warnings=(),
        )


def validate_builder_protocol(builder: object) -> list[str]:
    """Validate that an object correctly implements SemanticViewBuilder.

    Parameters
    ----------
    builder
        Object to validate.

    Returns
    -------
    list[str]
        List of validation errors (empty if valid).
    """
    errors = []

    if not isinstance(builder, SemanticViewBuilder):
        errors.append("Does not implement SemanticViewBuilder protocol")
        return errors

    # Check required properties
    try:
        _ = builder.name
    except Exception as e:
        errors.append(f"name property failed: {e}")

    try:
        _ = builder.semantic_schema
    except Exception as e:
        errors.append(f"semantic_schema property failed: {e}")

    try:
        _ = builder.evidence_tier
    except Exception as e:
        errors.append(f"evidence_tier property failed: {e}")

    try:
        _ = builder.upstream_dependencies
    except Exception as e:
        errors.append(f"upstream_dependencies property failed: {e}")

    return errors


__all__ = [
    "BuilderValidationResult",
    "SemanticViewBuilder",
    "validate_builder_protocol",
]
```

### 9.2 Builder Registration

**File:** `src/semantics/builders/registry.py` (NEW)

```python
"""Registry for semantic view builders with validation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from graphlib import TopologicalSorter

from semantics.builders.protocol import SemanticViewBuilder, validate_builder_protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from datafusion import SessionContext

    from semantics.catalog.catalog import SemanticCatalog


class BuilderRegistrationError(Exception):
    """Error during builder registration."""

    pass


def register_builders(
    catalog: SemanticCatalog,
    builders: Sequence[SemanticViewBuilder],
    *,
    validate: bool = True,
    fail_fast: bool = True,
) -> list[str]:
    """Register multiple builders with dependency ordering.

    Parameters
    ----------
    catalog
        Semantic catalog to register into.
    builders
        Builders to register.
    validate
        Whether to validate builder protocol compliance.
    fail_fast
        Whether to stop on first error.

    Returns
    -------
    list[str]
        Names of successfully registered builders.

    Raises
    ------
    BuilderRegistrationError
        If registration fails and fail_fast is True.
    """
    # Validate all builders first
    if validate:
        for builder in builders:
            errors = validate_builder_protocol(builder)
            if errors:
                msg = f"Builder {getattr(builder, 'name', '?')} validation failed: {errors}"
                if fail_fast:
                    raise BuilderRegistrationError(msg)

    # Build dependency graph
    graph: dict[str, set[str]] = {}
    builder_by_name: dict[str, SemanticViewBuilder] = {}

    for builder in builders:
        name = builder.name
        builder_by_name[name] = builder
        # Only include deps that are in this batch
        deps = {d for d in builder.upstream_dependencies if d in builder_by_name}
        graph[name] = deps

    # Topological sort
    sorter = TopologicalSorter(graph)
    sorted_names = list(sorter.static_order())

    # Register in order
    registered = []
    for name in sorted_names:
        builder = builder_by_name[name]

        # Validate inputs
        result = builder.validate_inputs(catalog.ctx)
        if not result.valid:
            msg = f"Builder {name} missing required inputs: {result.missing_required}"
            if fail_fast:
                raise BuilderRegistrationError(msg)
            continue

        # Register
        try:
            catalog.register_view(
                name,
                builder.build,
                evidence_tier=builder.evidence_tier,
                upstream_deps=builder.upstream_dependencies,
            )
            registered.append(name)
        except Exception as e:
            msg = f"Builder {name} registration failed: {e}"
            if fail_fast:
                raise BuilderRegistrationError(msg) from e

    return registered


__all__ = [
    "BuilderRegistrationError",
    "register_builders",
]
```

**Acceptance Criteria:**
- [ ] Protocol enforcement at runtime
- [ ] Dependency-ordered registration
- [ ] Validation with clear error messages

---

## Part 10: Integration Summary

### Module Structure

```
src/semantics/
├── __init__.py
├── types/
│   ├── __init__.py
│   ├── core.py              # SemanticType, registry
│   └── annotated_schema.py  # AnnotatedSchema, AnnotatedColumn
├── joins/
│   ├── __init__.py
│   ├── strategies.py        # Join strategy types
│   └── inference.py         # Automatic strategy inference
├── catalog/
│   ├── __init__.py
│   └── catalog.py           # SemanticCatalog
├── stats/
│   ├── __init__.py
│   ├── collector.py         # TableStatistics collection
│   └── table_provider.py    # Custom table provider
├── plans/
│   ├── __init__.py
│   ├── templates.py         # Join plan templates
│   └── substrait.py         # Substrait caching
├── incremental/
│   ├── __init__.py
│   └── cdf_joins.py         # CDF-based incremental computation
├── docs/
│   ├── __init__.py
│   └── graph_docs.py        # Auto-documentation
├── builders/
│   ├── __init__.py
│   ├── protocol.py          # SemanticViewBuilder protocol
│   └── registry.py          # Builder registration
├── quality.py               # From quality plan (SignalsSpec, etc.)
├── schema.py                # Existing (may refactor)
├── specs.py                 # Existing (may refactor)
├── compiler.py              # Existing (enhanced)
└── pipeline.py              # Existing (enhanced)
```

### Dependency Graph

```
types/core.py
    │
    ▼
types/annotated_schema.py
    │
    ├──────────────────┐
    ▼                  ▼
joins/strategies.py    stats/collector.py
    │                      │
    ▼                      ▼
joins/inference.py     stats/table_provider.py
    │                      │
    └──────────┬───────────┘
               ▼
        catalog/catalog.py
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
plans/     incremental/  builders/
templates  cdf_joins     protocol
    │          │          │
    ▼          │          ▼
plans/         │     builders/
substrait      │     registry
    │          │          │
    └──────────┴──────────┘
               │
               ▼
        docs/graph_docs.py
```

### Rollout Phases

**Phase A: Type System (Week 1)**
- [ ] Implement `types/core.py`
- [ ] Implement `types/annotated_schema.py`
- [ ] Unit tests for type inference

**Phase B: Join Infrastructure (Week 2)**
- [ ] Implement `joins/strategies.py`
- [ ] Implement `joins/inference.py`
- [ ] Integration tests for inference

**Phase C: Catalog & Stats (Week 3)**
- [ ] Implement `catalog/catalog.py`
- [ ] Implement `stats/collector.py`
- [ ] Implement `stats/table_provider.py`

**Phase D: Plan Optimization (Week 4)**
- [ ] Implement `plans/templates.py`
- [ ] Implement `plans/substrait.py`
- [ ] Performance benchmarks

**Phase E: Incremental & Docs (Week 5)**
- [ ] Implement `incremental/cdf_joins.py`
- [ ] Implement `docs/graph_docs.py`
- [ ] Implement `builders/protocol.py` and `builders/registry.py`

**Phase F: Integration (Week 6)**
- [ ] Integrate with existing `compiler.py` and `pipeline.py`
- [ ] Update `registry_specs.py` to use new infrastructure
- [ ] End-to-end tests

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Join inference accuracy | > 95% | Manual audit of inferred strategies |
| Plan cache hit rate | > 80% | Cache hits / total compilations |
| Incremental speedup | > 5x | Full rebuild time / incremental time |
| Documentation coverage | 100% | Views/relationships with generated docs |
| Protocol compliance | 100% | Builders passing validation |
