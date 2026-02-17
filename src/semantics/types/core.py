"""Semantic type system for join inference and validation.

Provides semantic types and compatibility groups that enable programmatic
join inference. Unlike column-name-based type inference, this system
uses explicit compatibility groups to determine which columns can be
safely joined together.

The compatibility group model:
- Columns are tagged with semantic types (FILE_ID, SPAN_START, etc.)
- Semantic types belong to compatibility groups (FILE_IDENTITY, SPAN_POSITION)
- Join inference uses compatibility groups to find valid join keys
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class SemanticType(StrEnum):
    """Semantic types for CPG columns.

    Each type represents a semantic role that a column plays in the
    CPG model. Types are used for join inference, validation, and
    schema documentation.
    """

    # Identity columns
    FILE_ID = auto()
    PATH = auto()
    ENTITY_ID = auto()

    # Span columns
    SPAN_START = auto()  # bstart
    SPAN_END = auto()  # bend
    LINE_NO = auto()
    COL_NO = auto()

    # Symbol columns
    SYMBOL = auto()
    QNAME = auto()

    # Evidence columns
    ORIGIN = auto()
    CONFIDENCE = auto()
    EVIDENCE_TIER = auto()
    TEXT = auto()

    # Generic
    UNKNOWN = auto()


class CompatibilityGroup(StrEnum):
    """Groups of types that can be joined together.

    Compatibility groups define sets of columns that are semantically
    compatible for join operations. For example, FILE_IDENTITY columns
    (file_id, path) can be joined with each other.
    """

    FILE_IDENTITY = auto()  # file_id, path
    SPAN_POSITION = auto()  # bstart, bend, line_no, col_no
    ENTITY_IDENTITY = auto()  # entity_id
    SYMBOL_IDENTITY = auto()  # symbol, qname


@dataclass(frozen=True)
class SemanticColumnSpec:
    """Specification for a semantically typed column.

    Attributes:
    ----------
    name
        Canonical column name.
    semantic_type
        Primary semantic type.
    compatibility_groups
        Groups this column can participate in for joins.
    is_join_key
        Whether this column is typically a join key.
    is_partition_key
        Whether this column is typically a partition key.
    """

    name: str
    semantic_type: SemanticType
    compatibility_groups: tuple[CompatibilityGroup, ...] = ()
    is_join_key: bool = False
    is_partition_key: bool = False


# Standard column specifications for common CPG columns
STANDARD_COLUMNS: Final[dict[str, SemanticColumnSpec]] = {
    "file_id": SemanticColumnSpec(
        name="file_id",
        semantic_type=SemanticType.FILE_ID,
        compatibility_groups=(CompatibilityGroup.FILE_IDENTITY,),
        is_join_key=True,
        is_partition_key=True,
    ),
    "path": SemanticColumnSpec(
        name="path",
        semantic_type=SemanticType.PATH,
        compatibility_groups=(CompatibilityGroup.FILE_IDENTITY,),
    ),
    "bstart": SemanticColumnSpec(
        name="bstart",
        semantic_type=SemanticType.SPAN_START,
        compatibility_groups=(CompatibilityGroup.SPAN_POSITION,),
    ),
    "bend": SemanticColumnSpec(
        name="bend",
        semantic_type=SemanticType.SPAN_END,
        compatibility_groups=(CompatibilityGroup.SPAN_POSITION,),
    ),
    "entity_id": SemanticColumnSpec(
        name="entity_id",
        semantic_type=SemanticType.ENTITY_ID,
        compatibility_groups=(CompatibilityGroup.ENTITY_IDENTITY,),
        is_join_key=True,
    ),
    "symbol": SemanticColumnSpec(
        name="symbol",
        semantic_type=SemanticType.SYMBOL,
        compatibility_groups=(CompatibilityGroup.SYMBOL_IDENTITY,),
        is_join_key=True,
    ),
    "qname": SemanticColumnSpec(
        name="qname",
        semantic_type=SemanticType.QNAME,
        compatibility_groups=(CompatibilityGroup.SYMBOL_IDENTITY,),
        is_join_key=True,
    ),
    "line_no": SemanticColumnSpec(
        name="line_no",
        semantic_type=SemanticType.LINE_NO,
        compatibility_groups=(CompatibilityGroup.SPAN_POSITION,),
    ),
    "col_no": SemanticColumnSpec(
        name="col_no",
        semantic_type=SemanticType.COL_NO,
        compatibility_groups=(CompatibilityGroup.SPAN_POSITION,),
    ),
    "origin": SemanticColumnSpec(
        name="origin",
        semantic_type=SemanticType.ORIGIN,
        compatibility_groups=(),
    ),
    "confidence": SemanticColumnSpec(
        name="confidence",
        semantic_type=SemanticType.CONFIDENCE,
        compatibility_groups=(),
    ),
}

SEMANTIC_TYPE_COMPATIBILITY_GROUPS: Final[dict[SemanticType, tuple[CompatibilityGroup, ...]]] = {
    SemanticType.FILE_ID: (CompatibilityGroup.FILE_IDENTITY,),
    SemanticType.PATH: (CompatibilityGroup.FILE_IDENTITY,),
    SemanticType.SPAN_START: (CompatibilityGroup.SPAN_POSITION,),
    SemanticType.SPAN_END: (CompatibilityGroup.SPAN_POSITION,),
    SemanticType.LINE_NO: (CompatibilityGroup.SPAN_POSITION,),
    SemanticType.COL_NO: (CompatibilityGroup.SPAN_POSITION,),
    SemanticType.ENTITY_ID: (CompatibilityGroup.ENTITY_IDENTITY,),
    SemanticType.SYMBOL: (CompatibilityGroup.SYMBOL_IDENTITY,),
    SemanticType.QNAME: (CompatibilityGroup.SYMBOL_IDENTITY,),
}

# Canonical exact-name FILE_IDENTITY join key set used across compiler/IR pipeline.
FILE_IDENTITY_COLUMN_NAMES: Final[frozenset[str]] = frozenset({"file_id", "path"})


def _matches_pattern(lower_name: str, column_name: str, pattern: tuple[str, ...]) -> bool:
    """Check if column name matches a pattern specification.

    Pattern format:
    - "exact:name" - exact match against column_name
    - "suffix:_id" - column_name ends with suffix
    - "contains:word" - lower_name contains word
    - "contains:word1+word2" - lower_name contains both words

    Returns:
    -------
    bool
        True if any pattern matches.
    """
    for spec in pattern:
        if spec.startswith("exact:"):
            if column_name == spec[6:]:
                return True
        elif spec.startswith("suffix:"):
            if column_name.endswith(spec[7:]):
                return True
        elif spec.startswith("contains:"):
            words = spec[9:].split("+")
            if all(w in lower_name for w in words):
                return True
    return False


# Heuristic patterns for semantic type inference
# Order matters - first match wins
_INFERENCE_PATTERNS: tuple[tuple[tuple[str, ...], SemanticType], ...] = (
    (("exact:file_id", "contains:file+id"), SemanticType.FILE_ID),
    (("exact:path", "exact:file_path", "exact:document_path", "contains:path"), SemanticType.PATH),
    (("suffix:_id",), SemanticType.ENTITY_ID),
    (
        (
            "exact:bstart",
            "exact:byte_start",
            "suffix:_bstart",
            "suffix:_byte_start",
            "contains:start",
        ),
        SemanticType.SPAN_START,
    ),
    (
        (
            "exact:bend",
            "exact:byte_end",
            "exact:byte_len",
            "suffix:_bend",
            "suffix:_byte_end",
            "suffix:_byte_len",
            "contains:end",
            "contains:byte_len",
        ),
        SemanticType.SPAN_END,
    ),
    (("contains:symbol",), SemanticType.SYMBOL),
    (("contains:qname", "contains:qualified_name"), SemanticType.QNAME),
    (("exact:name", "suffix:_name", "suffix:_text"), SemanticType.TEXT),
    (("contains:line+no",), SemanticType.LINE_NO),
    (("contains:col+no",), SemanticType.COL_NO),
    (("exact:origin", "exact:resolution_method"), SemanticType.ORIGIN),
    (("contains:confidence", "contains:score"), SemanticType.CONFIDENCE),
)


def infer_semantic_type(column_name: str) -> SemanticType:
    """Infer semantic type from column name.

    Uses exact match against standard columns first, then falls back
    to heuristic pattern matching.

    Parameters
    ----------
    column_name
        Column name to classify.

    Returns:
    -------
    SemanticType
        Inferred semantic type.
    """
    # Exact match first
    if column_name in STANDARD_COLUMNS:
        return STANDARD_COLUMNS[column_name].semantic_type

    # Heuristic inference for non-standard names
    lower_name = column_name.lower()
    for pattern, sem_type in _INFERENCE_PATTERNS:
        if _matches_pattern(lower_name, column_name, pattern):
            return sem_type

    return SemanticType.UNKNOWN


def compatibility_groups_for_semantic_type(
    sem_type: SemanticType,
) -> tuple[CompatibilityGroup, ...]:
    """Return compatibility groups for a semantic type.

    Returns:
    -------
    tuple[CompatibilityGroup, ...]
        Compatibility groups for the semantic type.
    """
    return SEMANTIC_TYPE_COMPATIBILITY_GROUPS.get(sem_type, ())


def get_compatibility_groups(column_name: str) -> tuple[CompatibilityGroup, ...]:
    """Get compatibility groups for a column.

    Parameters
    ----------
    column_name
        Column name to look up.

    Returns:
    -------
    tuple[CompatibilityGroup, ...]
        Compatibility groups the column belongs to.
    """
    if column_name in STANDARD_COLUMNS:
        return STANDARD_COLUMNS[column_name].compatibility_groups
    sem_type = infer_semantic_type(column_name)
    return compatibility_groups_for_semantic_type(sem_type)


def columns_are_joinable(left_col: str, right_col: str) -> bool:
    """Check if two columns can be joined based on compatibility groups.

    Parameters
    ----------
    left_col
        Left column name.
    right_col
        Right column name.

    Returns:
    -------
    bool
        True if columns share at least one compatibility group.
    """
    left_groups = set(get_compatibility_groups(left_col))
    right_groups = set(get_compatibility_groups(right_col))
    return bool(left_groups & right_groups)


def file_identity_equi_join_pairs(
    pairs: Iterable[tuple[str, str]],
    *,
    left_is_file_identity: Callable[[str], bool] | None = None,
    right_is_file_identity: Callable[[str], bool] | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return FILE_IDENTITY-compatible equi-join pairs.

    Applies canonical filtering used across semantic compiler/IR assembly:
    exact-name pairs only, and (optionally) compatibility-group predicates
    provided by the caller.

    Parameters
    ----------
    pairs
        Candidate ``(left_col, right_col)`` pairs.
    left_is_file_identity
        Optional predicate validating the left column compatibility group.
    right_is_file_identity
        Optional predicate validating the right column compatibility group.

    Returns:
    -------
    tuple[tuple[str, str], ...]
        Canonical FILE_IDENTITY equi-join pairs.
    """
    resolved: list[tuple[str, str]] = []
    for left_col, right_col in pairs:
        if left_col != right_col:
            continue
        if left_col not in FILE_IDENTITY_COLUMN_NAMES:
            continue
        if left_is_file_identity is not None and not left_is_file_identity(left_col):
            continue
        if right_is_file_identity is not None and not right_is_file_identity(right_col):
            continue
        resolved.append((left_col, right_col))
    return tuple(resolved)


__all__ = [
    "FILE_IDENTITY_COLUMN_NAMES",
    "STANDARD_COLUMNS",
    "CompatibilityGroup",
    "SemanticColumnSpec",
    "SemanticType",
    "columns_are_joinable",
    "compatibility_groups_for_semantic_type",
    "file_identity_equi_join_pairs",
    "get_compatibility_groups",
    "infer_semantic_type",
]
