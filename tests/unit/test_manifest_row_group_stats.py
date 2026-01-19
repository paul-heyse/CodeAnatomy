"""Unit tests for row group stats capture."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa

from arrowdsl.core.context import OrderingLevel
from arrowdsl.io.parquet import DatasetWriteConfig, ParquetMetadataConfig, write_dataset_parquet
from arrowdsl.schema.metadata import ordering_metadata_spec


def _apply_ordering(table: pa.Table, keys: Sequence[tuple[str, str]]) -> pa.Table:
    spec = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    schema = spec.apply(table.schema)
    return table.cast(schema)


def _capture_row_group_stats(
    tmp_path: Path,
    *,
    table: pa.Table,
    metadata: ParquetMetadataConfig,
) -> Sequence[object] | None:
    reports: list[object] = []

    def _reporter(report: object) -> None:
        reports.append(report)

    config = DatasetWriteConfig(metadata=metadata, reporter=_reporter)
    _ = write_dataset_parquet(table, str(tmp_path), config=config)
    return getattr(reports[-1], "row_group_stats", None)


def test_row_group_stats_emitted_when_allowed(tmp_path: Path) -> None:
    """Capture row group stats when within limits."""
    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    ordered = _apply_ordering(table, keys=(("id", "ascending"),))
    stats = _capture_row_group_stats(
        tmp_path / "allowed",
        table=ordered,
        metadata=ParquetMetadataConfig(max_row_group_stats_files=10, max_row_group_stats=10),
    )
    assert stats is not None
    assert stats


def test_row_group_stats_gated_by_file_limit(tmp_path: Path) -> None:
    """Skip row group stats when file count exceeds limits."""
    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    ordered = _apply_ordering(table, keys=(("id", "ascending"),))
    stats = _capture_row_group_stats(
        tmp_path / "limited",
        table=ordered,
        metadata=ParquetMetadataConfig(max_row_group_stats_files=0, max_row_group_stats=10),
    )
    assert stats is None
