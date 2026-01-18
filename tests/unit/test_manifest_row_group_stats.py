"""Tests for row-group stats capture during Parquet writes."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pyarrow as pa

from arrowdsl.core.context import OrderingLevel
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    DatasetWriteReport,
    ParquetMetadataConfig,
    write_dataset_parquet,
)
from arrowdsl.schema.metadata import ordering_metadata_spec


def test_manifest_row_group_stats(tmp_path: Path) -> None:
    """Capture row-group stats and metadata sidecars for key columns."""
    reports: list[DatasetWriteReport] = []

    def _report(report: DatasetWriteReport) -> None:
        reports.append(report)

    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    schema = ordering_metadata_spec(
        OrderingLevel.EXPLICIT,
        keys=(("id", "ascending"),),
    ).apply(schema)
    table = pa.Table.from_pydict({"id": [1, 2], "value": ["a", "b"]}, schema=schema)
    config = DatasetWriteConfig(
        metadata=ParquetMetadataConfig(),
        reporter=_report,
    )
    write_dataset_parquet(table, tmp_path / "dataset", config=config)
    assert reports
    report = reports[0]
    assert report.metadata_paths is not None
    assert "metadata" in report.metadata_paths
    assert report.row_group_stats is not None
    columns = cast("Mapping[str, object]", report.row_group_stats[0]["columns"])
    assert "id" in columns
