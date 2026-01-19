"""Unit tests for Parquet sorting metadata emission."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import pyarrow as pa

from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.io.parquet import DatasetWriteConfig, write_dataset_parquet
from arrowdsl.schema.metadata import ordering_metadata_spec


def _apply_ordering(table: pa.Table, keys: Sequence[tuple[str, str]]) -> pa.Table:
    spec = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    schema = spec.apply(table.schema)
    return table.cast(schema)


def _capture_report(tmp_path: str, table: pa.Table) -> list[dict[str, object]]:
    reports: list[dict[str, object]] = []

    def _reporter(report: object) -> None:
        reports.append({"sorting_columns": getattr(report, "sorting_columns", None)})

    config = DatasetWriteConfig(reporter=_reporter)
    _ = write_dataset_parquet(table, tmp_path, config=config)
    return reports


def test_sorting_metadata_emitted_for_explicit_ordering(tmp_path: Path) -> None:
    """Emit sorting metadata when ordering metadata is explicit."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    ordered = _apply_ordering(table, keys=(("id", "ascending"),))
    reports = _capture_report(str(tmp_path / "ordered"), ordered)
    assert reports
    sorting = cast("Sequence[Mapping[str, object]]", reports[-1]["sorting_columns"])
    assert sorting is not None
    assert any(entry.get("column") == "id" for entry in sorting)


def test_sorting_metadata_absent_without_ordering(tmp_path: Path) -> None:
    """Skip sorting metadata when ordering metadata is absent."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    reports = _capture_report(str(tmp_path / "unordered"), table)
    assert reports
    assert reports[-1]["sorting_columns"] is None
