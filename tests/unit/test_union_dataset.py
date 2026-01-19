"""Tests for union dataset normalization and one-shot handling."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from storage.dataset_sources import OneShotDataset, normalize_dataset_source, unwrap_dataset
from tests.utils import values_as_list

EXPECTED_ROW_COUNT = 3


def test_union_dataset_normalizes_sources() -> None:
    """Union multiple datasets into a composite dataset."""
    left = ds.dataset(pa.table({"a": [1]}))
    right = ds.dataset(pa.table({"a": [2, 3]}))
    unioned = unwrap_dataset(normalize_dataset_source([left, right]))
    assert isinstance(unioned, ds.Dataset)
    assert unioned.count_rows() == EXPECTED_ROW_COUNT


def test_union_dataset_aligns_schema_by_name() -> None:
    """Align unioned schemas by field name ordering."""
    left = ds.dataset(pa.table({"a": [1], "b": ["x"]}))
    right = ds.dataset(pa.table({"b": ["y"], "a": [2]}))
    unioned = unwrap_dataset(normalize_dataset_source([left, right]))
    table = unioned.scanner().to_table()
    assert values_as_list(table.column("a")) == [1, 2]
    assert values_as_list(table.column("b")) == ["x", "y"]


def test_one_shot_dataset_blocks_rescan() -> None:
    """Reject multiple scans of one-shot datasets."""
    batch = pa.record_batch([pa.array([1])], names=["a"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    dataset = normalize_dataset_source(reader)
    assert isinstance(dataset, OneShotDataset)
    _ = dataset.scanner()
    with pytest.raises(ValueError, match=r"One-shot dataset has already been scanned\."):
        dataset.scanner()
