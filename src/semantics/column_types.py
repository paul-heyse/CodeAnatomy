"""Semantic table/column classifications derived from canonical semantic types."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


class ColumnType(StrEnum):
    """Semantic column categories for table-shape inference."""

    PATH = auto()
    FILE_ID = auto()
    SPAN_START = auto()
    SPAN_END = auto()
    ENTITY_ID = auto()
    SYMBOL = auto()
    TEXT = auto()
    EVIDENCE = auto()
    NESTED = auto()
    OTHER = auto()


_SEMANTIC_NAME_TO_COLUMN_TYPE: dict[str, ColumnType] = {
    "PATH": ColumnType.PATH,
    "FILE_ID": ColumnType.FILE_ID,
    "SPAN_START": ColumnType.SPAN_START,
    "SPAN_END": ColumnType.SPAN_END,
    "ENTITY_ID": ColumnType.ENTITY_ID,
    "SYMBOL": ColumnType.SYMBOL,
    "QNAME": ColumnType.SYMBOL,
    "TEXT": ColumnType.TEXT,
    "ORIGIN": ColumnType.EVIDENCE,
    "CONFIDENCE": ColumnType.EVIDENCE,
    "EVIDENCE_TIER": ColumnType.EVIDENCE,
}


def infer_column_type(
    name: str,
    *,
    patterns: object | None = None,
) -> ColumnType:
    """Infer column category from canonical ``SemanticType`` classification.

    The ``patterns`` argument is retained for API compatibility but ignored.

    Returns:
        ColumnType: Semantic column category inferred from canonical semantic type.
    """
    from semantics.types.core import infer_semantic_type

    _ = patterns
    sem_type = infer_semantic_type(name)
    return _SEMANTIC_NAME_TO_COLUMN_TYPE.get(sem_type.name, ColumnType.OTHER)


class TableType(StrEnum):
    """Table types derived from column type combinations."""

    RAW = auto()
    EVIDENCE = auto()
    ENTITY = auto()
    SYMBOL_SOURCE = auto()
    RELATION = auto()


def infer_table_type(column_types: AbstractSet[ColumnType]) -> TableType:
    """Infer table type from column categories present.

    Returns:
        TableType: Best-fit table classification for the supplied column set.
    """
    has_path = ColumnType.PATH in column_types
    has_span = ColumnType.SPAN_START in column_types and ColumnType.SPAN_END in column_types
    has_entity_id = ColumnType.ENTITY_ID in column_types
    has_symbol = ColumnType.SYMBOL in column_types

    if has_entity_id and has_symbol:
        return TableType.RELATION
    if has_entity_id and has_path and has_span:
        return TableType.ENTITY
    if has_path and has_span:
        return TableType.EVIDENCE
    if has_symbol:
        return TableType.SYMBOL_SOURCE

    return TableType.RAW


__all__ = [
    "ColumnType",
    "TableType",
    "infer_column_type",
    "infer_table_type",
]
