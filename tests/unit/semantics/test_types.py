"""Tests for semantics.types module."""

from __future__ import annotations

from semantics.types import (
    ColumnType,
    TableType,
    infer_column_type,
    infer_table_type,
)


def test_path_columns() -> None:
    """Path columns are recognized."""
    assert infer_column_type("path") == ColumnType.PATH
    assert infer_column_type("file_path") == ColumnType.PATH
    assert infer_column_type("document_path") == ColumnType.PATH


def test_file_id_column() -> None:
    """file_id is recognized as FILE_ID, not ENTITY_ID."""
    assert infer_column_type("file_id") == ColumnType.FILE_ID


def test_span_start_columns() -> None:
    """Span start columns are recognized."""
    assert infer_column_type("bstart") == ColumnType.SPAN_START
    assert infer_column_type("byte_start") == ColumnType.SPAN_START
    assert infer_column_type("ref_bstart") == ColumnType.SPAN_START
    assert infer_column_type("node_byte_start") == ColumnType.SPAN_START


def test_span_end_columns() -> None:
    """Span end columns are recognized."""
    assert infer_column_type("bend") == ColumnType.SPAN_END
    assert infer_column_type("byte_end") == ColumnType.SPAN_END
    assert infer_column_type("ref_bend") == ColumnType.SPAN_END


def test_entity_id_columns() -> None:
    """Entity ID columns are recognized."""
    assert infer_column_type("ref_id") == ColumnType.ENTITY_ID
    assert infer_column_type("def_id") == ColumnType.ENTITY_ID
    assert infer_column_type("node_id") == ColumnType.ENTITY_ID


def test_symbol_columns() -> None:
    """Symbol columns are recognized."""
    assert infer_column_type("symbol") == ColumnType.SYMBOL
    assert infer_column_type("qname") == ColumnType.SYMBOL
    assert infer_column_type("qualified_name") == ColumnType.SYMBOL


def test_text_columns() -> None:
    """Text columns are recognized."""
    assert infer_column_type("name") == ColumnType.TEXT
    assert infer_column_type("ref_text") == ColumnType.TEXT
    assert infer_column_type("def_name") == ColumnType.TEXT


def test_evidence_columns() -> None:
    """Evidence columns are recognized."""
    assert infer_column_type("confidence") == ColumnType.EVIDENCE
    assert infer_column_type("score") == ColumnType.EVIDENCE
    assert infer_column_type("origin") == ColumnType.EVIDENCE


def test_unrecognized_columns() -> None:
    """Unrecognized columns return OTHER."""
    assert infer_column_type("random_column") == ColumnType.OTHER
    assert infer_column_type("foo") == ColumnType.OTHER


def test_raw_table() -> None:
    """No semantic columns -> RAW."""
    assert infer_table_type({ColumnType.OTHER}) == TableType.RAW
    assert infer_table_type(set()) == TableType.RAW


def test_evidence_table() -> None:
    """PATH + SPAN -> EVIDENCE."""
    cols = {ColumnType.PATH, ColumnType.SPAN_START, ColumnType.SPAN_END}
    assert infer_table_type(cols) == TableType.EVIDENCE


def test_entity_table() -> None:
    """EVIDENCE + ENTITY_ID -> ENTITY."""
    cols = {
        ColumnType.PATH,
        ColumnType.SPAN_START,
        ColumnType.SPAN_END,
        ColumnType.ENTITY_ID,
    }
    assert infer_table_type(cols) == TableType.ENTITY


def test_symbol_source_table() -> None:
    """SYMBOL alone -> SYMBOL_SOURCE."""
    assert infer_table_type({ColumnType.SYMBOL}) == TableType.SYMBOL_SOURCE


def test_relation_table() -> None:
    """ENTITY_ID + SYMBOL -> RELATION."""
    cols = {ColumnType.ENTITY_ID, ColumnType.SYMBOL}
    assert infer_table_type(cols) == TableType.RELATION


def test_relation_takes_precedence() -> None:
    """RELATION takes precedence over ENTITY."""
    cols = {
        ColumnType.PATH,
        ColumnType.SPAN_START,
        ColumnType.SPAN_END,
        ColumnType.ENTITY_ID,
        ColumnType.SYMBOL,
    }
    assert infer_table_type(cols) == TableType.RELATION
