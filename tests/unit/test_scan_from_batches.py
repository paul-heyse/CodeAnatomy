"""Unit tests for one-shot scanner construction."""

from __future__ import annotations

import pyarrow as pa
import pytest

from storage.dataset_sources import is_one_shot_dataset, normalize_dataset_source


def test_scan_from_batches_enforces_one_shot_and_projection() -> None:
    """Apply scan policy to streaming sources without materialization."""
    table = pa.table({"a": [1, 2], "b": [3, 4]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    dataset = normalize_dataset_source(reader)
    assert is_one_shot_dataset(dataset)
    scanner = dataset.scanner(columns=("a",))
    result = scanner.to_reader().read_all()
    assert result.column_names == ["a"]
    assert result.num_rows == table.num_rows
    with pytest.raises(ValueError, match="One-shot dataset has already been scanned"):
        _ = dataset.scanner()
