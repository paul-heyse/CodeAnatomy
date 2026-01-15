"""Shared scan telemetry helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow.dataset as ds

from arrowdsl.core.interop import ComputeExpression
from arrowdsl.plan.metrics import (
    fragment_file_hints,
    list_fragments,
    row_group_count,
    scan_task_count,
)


@dataclass(frozen=True)
class ScanTelemetry:
    """Scan telemetry for dataset fragments."""

    fragment_count: int
    row_group_count: int
    count_rows: int | None
    estimated_rows: int | None
    file_hints: tuple[str, ...] = ()


def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
    scanner: ds.Scanner | None = None,
    hint_limit: int | None = 5,
) -> ScanTelemetry:
    """Return telemetry for dataset fragments.

    Returns
    -------
    ScanTelemetry
        Fragment counts and estimated row totals (when available).
    """
    fragments = list_fragments(dataset, predicate=predicate)
    try:
        count_rows = dataset.count_rows(filter=predicate)
    except (AttributeError, NotImplementedError, TypeError, ValueError):
        count_rows = None
    total_rows = 0
    estimated_rows: int | None = 0
    for fragment in fragments:
        metadata = fragment.metadata
        if metadata is None or metadata.num_rows is None or metadata.num_rows < 0:
            estimated_rows = None
            break
        total_rows += int(metadata.num_rows)
    if estimated_rows is not None:
        estimated_rows = total_rows
    if scanner is None:
        scanner = dataset.scanner(filter=predicate)
    try:
        task_count = scan_task_count(scanner)
    except (AttributeError, TypeError):
        task_count = row_group_count(fragments)
    return ScanTelemetry(
        fragment_count=len(fragments),
        row_group_count=task_count,
        count_rows=int(count_rows) if count_rows is not None else None,
        estimated_rows=estimated_rows,
        file_hints=fragment_file_hints(fragments, limit=hint_limit),
    )


__all__ = ["ScanTelemetry", "fragment_telemetry"]
