"""Engine-side metrics helpers relocated from obs package."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.interop import TableLike
from obs.metrics import (
    TableSummary,
    column_stats_table,
    dataset_stats_table,
    empty_scan_telemetry_table,
    scan_telemetry_table,
    table_summary,
)

__all__ = [
    "TableSummary",
    "column_stats_table",
    "dataset_stats_table",
    "empty_scan_telemetry_table",
    "scan_telemetry_table",
    "schema_to_dict",
    "table_summary",
]


def table_summary_payload(table: TableLike) -> TableSummary:
    """Return canonical table summary payload."""
    return table_summary(table)


def scan_telemetry_rows(rows: Sequence[Mapping[str, object]]) -> TableLike:
    """Build canonical scan telemetry table from rows.

    Returns:
    -------
    TableLike
        Arrow-compatible telemetry table.
    """
    return scan_telemetry_table(rows)
