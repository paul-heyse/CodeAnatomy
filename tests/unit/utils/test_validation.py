"""Tests for validation utilities."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.arrow.coercion import ensure_arrow_table
from utils.validation import (
    ensure_callable,
    ensure_mapping,
    ensure_sequence,
    find_missing,
    validate_required_items,
)


def test_ensure_mapping_accepts_mapping() -> None:
    """Ensure ensure_mapping returns mappings unchanged."""
    payload = {"a": 1}
    assert ensure_mapping(payload, label="payload") is payload


def test_ensure_mapping_rejects_non_mapping() -> None:
    """Ensure ensure_mapping rejects non-mappings."""
    with pytest.raises(TypeError, match="payload must be a Mapping"):
        ensure_mapping(123, label="payload")


def test_ensure_sequence_accepts_sequence() -> None:
    """Ensure ensure_sequence accepts valid sequences."""
    values = [1, 2, 3]
    assert ensure_sequence(values, label="items") is values
    assert ensure_sequence(("a", "b"), label="items", item_type=str) == ("a", "b")


def test_ensure_sequence_rejects_strings() -> None:
    """Ensure ensure_sequence rejects string inputs."""
    with pytest.raises(TypeError, match="items must be a Sequence"):
        ensure_sequence("abc", label="items")


def test_ensure_sequence_item_type_validation() -> None:
    """Ensure ensure_sequence validates item types."""
    with pytest.raises(TypeError, match="items\\[1\\]"):
        ensure_sequence([1, "two"], label="items", item_type=int)


def test_ensure_callable_validation() -> None:
    """Ensure ensure_callable validates callables."""
    assert ensure_callable(lambda: 1, label="callback")
    with pytest.raises(TypeError, match="callback must be callable"):
        ensure_callable(42, label="callback")


def test_find_missing_and_validate_required_items() -> None:
    """Ensure missing-item helpers compute and raise correctly."""
    required = ["a", "b"]
    available = {"a"}
    assert find_missing(required, available) == ["b"]
    with pytest.raises(ValueError, match="Missing required items"):
        validate_required_items(required, available)


def test_ensure_table_accepts_table() -> None:
    """Ensure ensure_table accepts PyArrow tables."""
    table = pa.table({"x": [1, 2]})
    result = ensure_arrow_table(table, label="input")
    assert result.schema == table.schema
    assert result.num_rows == table.num_rows


def test_ensure_table_accepts_reader() -> None:
    """Ensure ensure_table accepts record batch readers."""
    expected_rows = 2
    batch = pa.record_batch([pa.array([1, 2])], names=["x"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    result = ensure_arrow_table(reader, label="input")
    assert result.num_rows == expected_rows


def test_ensure_table_rejects_invalid() -> None:
    """Ensure ensure_table rejects invalid inputs."""
    with pytest.raises(TypeError, match="value must be Table/RecordBatch/RecordBatchReader"):
        ensure_arrow_table(123, label="value")
