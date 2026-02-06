"""Semantic type system for pipeline columns and tables.

The type system derives table capabilities from column presence:
- Column types are inferred from naming patterns
- Table types are derived from column type combinations
- Available operations follow directly from table type
"""

from __future__ import annotations

import re
from enum import StrEnum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


class ColumnType(StrEnum):
    """Semantic column types derived from naming patterns."""

    PATH = auto()  # File path: path, file_path, document_path
    FILE_ID = auto()  # File identity: file_id
    SPAN_START = auto()  # Span start: bstart, *_bstart, byte_start, *_byte_start
    SPAN_END = auto()  # Span end: bend, *_bend, byte_end, *_byte_end
    ENTITY_ID = auto()  # Entity identifier: *_id (excluding file_id)
    SYMBOL = auto()  # Symbol name: symbol, qname, qualified_name
    TEXT = auto()  # Text content: *_text, *_name, name
    EVIDENCE = auto()  # Evidence metadata: confidence, score, origin
    NESTED = auto()  # Nested struct/list types
    OTHER = auto()  # Unclassified


# Pattern-based type inference: (pattern, type)
# Order matters - first match wins
TYPE_PATTERNS: tuple[tuple[re.Pattern[str], ColumnType], ...] = (
    (re.compile(r"^(path|file_path|document_path)$"), ColumnType.PATH),
    (re.compile(r"^file_id$"), ColumnType.FILE_ID),
    (re.compile(r"^bstart$|_bstart$|^byte_start$|_byte_start$"), ColumnType.SPAN_START),
    (re.compile(r"^bend$|_bend$|^byte_end$|_byte_end$|^byte_len$|_byte_len$"), ColumnType.SPAN_END),
    (re.compile(r"_id$"), ColumnType.ENTITY_ID),  # After file_id
    (re.compile(r"^(symbol|qname|qualified_name)$"), ColumnType.SYMBOL),
    (re.compile(r"^name$|_name$|_text$"), ColumnType.TEXT),
    (re.compile(r"^(confidence|score|origin|resolution_method)$"), ColumnType.EVIDENCE),
)


def infer_column_type(
    name: str,
    *,
    patterns: tuple[tuple[re.Pattern[str], ColumnType], ...] = TYPE_PATTERNS,
) -> ColumnType:
    """Infer semantic type from column name.

    Parameters
    ----------
    name
        Column name to classify.
    patterns
        Ordered pattern/type pairs to use for inference.

    Returns:
    -------
    ColumnType
        Inferred semantic type.
    """
    for pattern, col_type in patterns:
        if pattern.search(name):
            return col_type
    return ColumnType.OTHER


class TableType(StrEnum):
    """Table types derived from column type combinations.

    The hierarchy:
    - RAW: No semantic columns
    - EVIDENCE: PATH + SPAN (can locate something in source)
    - ENTITY: EVIDENCE + ENTITY_ID (a first-class entity)
    - SYMBOL_SOURCE: Has SYMBOL (provides symbol information)
    - RELATION: ENTITY_ID + SYMBOL (links entity to symbol)
    """

    RAW = auto()  # No semantic columns recognized
    EVIDENCE = auto()  # Has PATH + SPAN
    ENTITY = auto()  # EVIDENCE + ENTITY_ID
    SYMBOL_SOURCE = auto()  # Has SYMBOL
    RELATION = auto()  # Has ENTITY_ID + SYMBOL


def infer_table_type(column_types: AbstractSet[ColumnType]) -> TableType:
    """Infer table type from column types present.

    Parameters
    ----------
    column_types
        Set of column types present in the table.

    Returns:
    -------
    TableType
        Derived table type.
    """
    has_path = ColumnType.PATH in column_types
    has_span = ColumnType.SPAN_START in column_types and ColumnType.SPAN_END in column_types
    has_entity_id = ColumnType.ENTITY_ID in column_types
    has_symbol = ColumnType.SYMBOL in column_types

    # Relation tables link entities to symbols
    if has_entity_id and has_symbol:
        return TableType.RELATION

    # Entity tables have evidence plus an identity
    if has_entity_id and has_path and has_span:
        return TableType.ENTITY

    # Evidence tables can locate something in source
    if has_path and has_span:
        return TableType.EVIDENCE

    # Symbol source tables provide symbol information
    if has_symbol:
        return TableType.SYMBOL_SOURCE

    return TableType.RAW


__all__ = [
    "TYPE_PATTERNS",
    "ColumnType",
    "TableType",
    "infer_column_type",
    "infer_table_type",
]
