"""Join strategy inference from schema annotations.

Infers the optimal join strategy between two annotated schemas based on
their semantic types and compatibility groups. The inference follows
a priority order:

1. Span-based joins (overlap/contains) when both schemas have spans + file identity
2. Foreign key joins when FK columns reference entity IDs
3. Symbol matching when both schemas have symbol columns
4. File equi-join as fallback when both have file identity

The inference can be guided with optional hints to prefer specific strategies.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from semantics.joins.strategies import (
    JoinStrategy,
    JoinStrategyType,
    make_fk_strategy,
    make_symbol_match_strategy,
)
from semantics.types.core import CompatibilityGroup, SemanticType

if TYPE_CHECKING:
    from semantics.types import AnnotatedSchema

# Confidence scores assigned to inferred join strategies based on the
# strength of the schema-level evidence.  Higher priority strategies
# receive higher confidence.
_SPAN_CONFIDENCE: float = 0.95
_FK_CONFIDENCE: float = 0.85
_SYMBOL_CONFIDENCE: float = 0.75
_FILE_EQUI_CONFIDENCE: float = 0.6


class JoinInferenceError(Exception):
    """Raised when join inference fails with no valid strategy."""


@dataclass(frozen=True)
class JoinCapabilities:
    """Capabilities extracted from a schema for join inference.

    Attributes:
    ----------
    has_file_identity
        Schema has file_id or path column.
    has_spans
        Schema has both bstart and bend columns.
    has_entity_id
        Schema has entity_id column.
    has_symbol
        Schema has symbol or qname column.
    fk_columns
        Column names that look like foreign keys (suffix _id, not entity_id).
    """

    has_file_identity: bool
    has_spans: bool
    has_entity_id: bool
    has_symbol: bool
    fk_columns: tuple[str, ...]

    @classmethod
    def from_schema(cls, schema: AnnotatedSchema) -> JoinCapabilities:
        """Extract join capabilities from an annotated schema.

        Parameters
        ----------
        schema
            Annotated schema to analyze.

        Returns:
        -------
        JoinCapabilities
            Extracted capabilities.
        """
        has_file_identity = schema.has_compatibility_group(CompatibilityGroup.FILE_IDENTITY)
        has_spans = schema.has_semantic_type(SemanticType.SPAN_START) and schema.has_semantic_type(
            SemanticType.SPAN_END
        )
        has_entity_id = schema.has_semantic_type(SemanticType.ENTITY_ID)
        has_symbol = schema.has_compatibility_group(CompatibilityGroup.SYMBOL_IDENTITY)

        # Find FK columns: columns with ENTITY_ID type but not named "entity_id"
        fk_cols = tuple(
            col.name
            for col in schema.columns
            if col.semantic_type == SemanticType.ENTITY_ID and col.name != "entity_id"
        )

        return cls(
            has_file_identity=has_file_identity,
            has_spans=has_spans,
            has_entity_id=has_entity_id,
            has_symbol=has_symbol,
            fk_columns=fk_cols,
        )


def _preferred_column(
    schema: AnnotatedSchema,
    group: CompatibilityGroup,
    *,
    preferred: tuple[str, ...],
) -> str | None:
    cols = schema.columns_by_compatibility_group(group)
    if not cols:
        return None
    names = [col.name for col in cols]
    for name in preferred:
        if name in names:
            return name
    return cols[0].name


def _preferred_span_col(schema: AnnotatedSchema, sem_type: SemanticType) -> str | None:
    cols = schema.columns_by_semantic_type(sem_type)
    if not cols:
        return None
    return cols[0].name


def _span_strategy(
    strategy_type: JoinStrategyType,
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
) -> JoinStrategy:
    left_file = _preferred_column(
        left_schema,
        CompatibilityGroup.FILE_IDENTITY,
        preferred=("file_id", "path"),
    )
    right_file = _preferred_column(
        right_schema,
        CompatibilityGroup.FILE_IDENTITY,
        preferred=("file_id", "path"),
    )
    left_start = _preferred_span_col(left_schema, SemanticType.SPAN_START)
    left_end = _preferred_span_col(left_schema, SemanticType.SPAN_END)
    right_start = _preferred_span_col(right_schema, SemanticType.SPAN_START)
    right_end = _preferred_span_col(right_schema, SemanticType.SPAN_END)
    left_keys = tuple(key for key in (left_file, left_start, left_end) if key)
    right_keys = tuple(key for key in (right_file, right_start, right_end) if key)
    return JoinStrategy(
        strategy_type=strategy_type,
        left_keys=left_keys,
        right_keys=right_keys,
    )


def _can_span_join(left_caps: JoinCapabilities, right_caps: JoinCapabilities) -> bool:
    """Check if span-based join is possible.

    Returns:
    -------
    bool
        True if both schemas have file identity and span columns.
    """
    return (
        left_caps.has_file_identity
        and right_caps.has_file_identity
        and left_caps.has_spans
        and right_caps.has_spans
    )


def _can_file_join(left_caps: JoinCapabilities, right_caps: JoinCapabilities) -> bool:
    """Check if file equi-join is possible.

    Returns:
    -------
    bool
        True if both schemas have file identity columns.
    """
    return left_caps.has_file_identity and right_caps.has_file_identity


def _can_symbol_join(left_caps: JoinCapabilities, right_caps: JoinCapabilities) -> bool:
    """Check if symbol-based join is possible.

    Returns:
    -------
    bool
        True if both schemas have symbol columns.
    """
    return left_caps.has_symbol and right_caps.has_symbol


def _find_fk_match(
    left_caps: JoinCapabilities,
    right_caps: JoinCapabilities,
) -> tuple[str, str] | None:
    """Find a matching FK->PK pair between schemas.

    Parameters
    ----------
    left_caps
        Capabilities of left schema.
    right_caps
        Capabilities of right schema.

    Returns:
    -------
    tuple[str, str] | None
        (left_col, right_col) if match found, None otherwise.
    """
    # Left has FK, right has entity_id
    if left_caps.fk_columns and right_caps.has_entity_id:
        return (left_caps.fk_columns[0], "entity_id")

    # Right has FK, left has entity_id
    if right_caps.fk_columns and left_caps.has_entity_id:
        return ("entity_id", right_caps.fk_columns[0])

    return None


def _resolve_equi_join(
    left_caps: JoinCapabilities,
    right_caps: JoinCapabilities,
    *,
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
) -> JoinStrategy | None:
    if not _can_file_join(left_caps, right_caps):
        return None
    left_key = _preferred_column(
        left_schema,
        CompatibilityGroup.FILE_IDENTITY,
        preferred=("file_id", "path"),
    )
    right_key = _preferred_column(
        right_schema,
        CompatibilityGroup.FILE_IDENTITY,
        preferred=("file_id", "path"),
    )
    if left_key is None or right_key is None:
        return None
    return JoinStrategy(
        strategy_type=JoinStrategyType.EQUI_JOIN,
        left_keys=(left_key,),
        right_keys=(right_key,),
    )


def _resolve_symbol_match(
    left_caps: JoinCapabilities,
    right_caps: JoinCapabilities,
    *,
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
) -> JoinStrategy | None:
    if not _can_symbol_join(left_caps, right_caps):
        return None
    left_key = _preferred_column(
        left_schema,
        CompatibilityGroup.SYMBOL_IDENTITY,
        preferred=("symbol", "qname"),
    )
    right_key = _preferred_column(
        right_schema,
        CompatibilityGroup.SYMBOL_IDENTITY,
        preferred=("symbol", "qname"),
    )
    if left_key is None or right_key is None:
        return None
    return make_symbol_match_strategy(left_key, right_key)


def _infer_with_hint(
    hint: JoinStrategyType,
    left_caps: JoinCapabilities,
    right_caps: JoinCapabilities,
    *,
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
) -> JoinStrategy | None:
    """Attempt to satisfy a specific strategy hint.

    Parameters
    ----------

    Hint:
        Requested strategy type.
    left_caps
        Capabilities of left schema.
    right_caps
        Capabilities of right schema.
    left_schema
        Annotated schema of the left table.
    right_schema
        Annotated schema of the right table.

    Returns:
    -------
    JoinStrategy | None
        Strategy if hint can be satisfied, None otherwise.
    """
    strategy: JoinStrategy | None = None
    if hint in {JoinStrategyType.SPAN_OVERLAP, JoinStrategyType.SPAN_CONTAINS}:
        if _can_span_join(left_caps, right_caps):
            strategy = _span_strategy(hint, left_schema, right_schema)
            strategy = replace(strategy, confidence=_SPAN_CONFIDENCE)
    elif hint == JoinStrategyType.EQUI_JOIN:
        strategy = _resolve_equi_join(
            left_caps,
            right_caps,
            left_schema=left_schema,
            right_schema=right_schema,
        )
        if strategy is not None:
            strategy = replace(strategy, confidence=_FILE_EQUI_CONFIDENCE)
    elif hint == JoinStrategyType.FOREIGN_KEY:
        fk_match = _find_fk_match(left_caps, right_caps)
        if fk_match:
            strategy = make_fk_strategy(fk_match[0], fk_match[1])
            strategy = replace(strategy, confidence=_FK_CONFIDENCE)
    elif hint == JoinStrategyType.SYMBOL_MATCH:
        strategy = _resolve_symbol_match(
            left_caps,
            right_caps,
            left_schema=left_schema,
            right_schema=right_schema,
        )
        if strategy is not None:
            strategy = replace(strategy, confidence=_SYMBOL_CONFIDENCE)
    return strategy


def _infer_default(
    left_caps: JoinCapabilities,
    right_caps: JoinCapabilities,
    *,
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
) -> JoinStrategy | None:
    """Infer strategy using default priority order.

    Priority: span overlap > FK > symbol > file equi-join

    Parameters
    ----------
    left_caps
        Capabilities of left schema.
    right_caps
        Capabilities of right schema.
    left_schema
        Annotated schema of the left table.
    right_schema
        Annotated schema of the right table.

    Returns:
    -------
    JoinStrategy | None
        Best inferred strategy, or None if no valid strategy.
    """
    if _can_span_join(left_caps, right_caps):
        strategy = _span_strategy(JoinStrategyType.SPAN_OVERLAP, left_schema, right_schema)
        return replace(strategy, confidence=_SPAN_CONFIDENCE)

    fk_match = _find_fk_match(left_caps, right_caps)
    if fk_match:
        return replace(make_fk_strategy(fk_match[0], fk_match[1]), confidence=_FK_CONFIDENCE)

    strategy = _resolve_symbol_match(
        left_caps,
        right_caps,
        left_schema=left_schema,
        right_schema=right_schema,
    )
    if strategy is not None:
        return replace(strategy, confidence=_SYMBOL_CONFIDENCE)

    equi = _resolve_equi_join(
        left_caps,
        right_caps,
        left_schema=left_schema,
        right_schema=right_schema,
    )
    if equi is not None:
        return replace(equi, confidence=_FILE_EQUI_CONFIDENCE)
    return None


def infer_join_strategy(
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
    *,
    hint: JoinStrategyType | None = None,
) -> JoinStrategy | None:
    """Infer the best join strategy between two schemas.

    Uses semantic type annotations and compatibility groups to determine
    the optimal join strategy. An optional hint can guide the selection
    toward a specific strategy type.

    Parameters
    ----------
    left_schema
        Annotated schema of the left table.
    right_schema
        Annotated schema of the right table.

    Hint:
        Optional hint for preferred strategy type.

    Returns:
    -------
    JoinStrategy | None
        Inferred join strategy, or None if no valid strategy found.

    Examples:
    --------
    >>> import pyarrow as pa
    >>> from semantics.types import AnnotatedSchema
    >>> from semantics.joins import infer_join_strategy, JoinStrategyType
    >>>
    >>> # Two tables with file_id + spans -> span overlap
    >>> left = AnnotatedSchema.from_arrow_schema(
    ...     pa.schema(
    ...         [
    ...             ("file_id", pa.string()),
    ...             ("bstart", pa.int64()),
    ...             ("bend", pa.int64()),
    ...         ]
    ...     )
    ... )
    >>> right = AnnotatedSchema.from_arrow_schema(
    ...     pa.schema(
    ...         [
    ...             ("file_id", pa.string()),
    ...             ("bstart", pa.int64()),
    ...             ("bend", pa.int64()),
    ...         ]
    ...     )
    ... )
    >>> strategy = infer_join_strategy(left, right)
    >>> strategy.strategy_type
    <JoinStrategyType.SPAN_OVERLAP: 'span_overlap'>
    """
    left_caps = JoinCapabilities.from_schema(left_schema)
    right_caps = JoinCapabilities.from_schema(right_schema)

    if hint is not None:
        return _infer_with_hint(
            hint,
            left_caps,
            right_caps,
            left_schema=left_schema,
            right_schema=right_schema,
        )

    return _infer_default(left_caps, right_caps, left_schema=left_schema, right_schema=right_schema)


def _format_capabilities(name: str, caps: JoinCapabilities) -> str:
    """Format capabilities for diagnostic output.

    Returns:
    -------
    str
        Formatted string describing schema capabilities.
    """
    return (
        f"{name}: file_identity={caps.has_file_identity}, "
        f"spans={caps.has_spans}, entity_id={caps.has_entity_id}, "
        f"symbol={caps.has_symbol}"
    )


def require_join_strategy(
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
    *,
    hint: JoinStrategyType | None = None,
    left_name: str = "left",
    right_name: str = "right",
) -> JoinStrategy:
    """Infer join strategy or raise with clear diagnostics.

    Args:
        left_schema: Left input schema.
        right_schema: Right input schema.
        hint: Optional join strategy hint.
        left_name: Diagnostic label for left input.
        right_name: Diagnostic label for right input.

    Returns:
        JoinStrategy: Result.

    Raises:
        JoinInferenceError: If no valid join strategy can be inferred.
    """
    strategy = infer_join_strategy(left_schema, right_schema, hint=hint)
    if strategy is not None:
        return strategy

    # Build diagnostic message
    left_caps = JoinCapabilities.from_schema(left_schema)
    right_caps = JoinCapabilities.from_schema(right_schema)

    details: list[str] = []
    if hint:
        details.append(f"Hint requested: {hint}")
    details.append(_format_capabilities(left_name, left_caps))
    details.append(_format_capabilities(right_name, right_caps))

    msg = f"Cannot infer join strategy between {left_name!r} and {right_name!r}.\n" + "\n".join(
        f"  {d}" for d in details
    )
    raise JoinInferenceError(msg)


__all__ = [
    "JoinCapabilities",
    "JoinInferenceError",
    "infer_join_strategy",
    "require_join_strategy",
]
