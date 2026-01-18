"""Shared scan telemetry helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow.dataset as ds

from arrowdsl.core.interop import ComputeExpression, SchemaLike
from arrowdsl.plan.metrics import (
    fragment_file_hints,
    list_fragments,
    row_group_count,
    scan_task_count,
)
from arrowdsl.schema.serialization import schema_to_dict
from core_types import JsonDict


@dataclass(frozen=True)
class ScanTelemetry:
    """Scan telemetry for dataset fragments."""

    fragment_count: int
    row_group_count: int
    count_rows: int | None
    estimated_rows: int | None
    file_hints: tuple[str, ...] = ()
    fragment_paths: tuple[str, ...] = ()
    partition_expressions: tuple[str, ...] = ()
    dataset_schema: JsonDict | None = None
    projected_schema: JsonDict | None = None
    discovery_policy: JsonDict | None = None
    scan_profile: JsonDict | None = None


@dataclass(frozen=True)
class ScanTelemetryOptions:
    """Options for fragment scan telemetry."""

    hint_limit: int | None = 5
    discovery_policy: JsonDict | None = None
    scan_profile: JsonDict | None = None


def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
    scanner: ds.Scanner | None = None,
    options: ScanTelemetryOptions | None = None,
) -> ScanTelemetry:
    """Return telemetry for dataset fragments.

    Returns
    -------
    ScanTelemetry
        Fragment counts and estimated row totals (when available).
    """
    resolved = options or ScanTelemetryOptions()
    fragments = list_fragments(dataset, predicate=predicate)
    fragment_paths: list[str] = []
    partition_expressions: list[str] = []
    for fragment in fragments:
        path = getattr(fragment, "path", None)
        if path is not None:
            fragment_paths.append(str(path))
        partition_expression = getattr(fragment, "partition_expression", None)
        if partition_expression is not None:
            partition_expressions.append(str(partition_expression))
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
    dataset_schema = _schema_payload(getattr(dataset, "schema", None))
    projected_schema = _schema_payload(getattr(scanner, "schema", None))
    try:
        task_count = scan_task_count(scanner)
    except (AttributeError, TypeError):
        task_count = row_group_count(fragments)
    return ScanTelemetry(
        fragment_count=len(fragments),
        row_group_count=task_count,
        count_rows=int(count_rows) if count_rows is not None else None,
        estimated_rows=estimated_rows,
        file_hints=fragment_file_hints(fragments, limit=resolved.hint_limit),
        fragment_paths=tuple(fragment_paths),
        partition_expressions=tuple(partition_expressions),
        dataset_schema=dataset_schema,
        projected_schema=projected_schema,
        discovery_policy=resolved.discovery_policy,
        scan_profile=resolved.scan_profile,
    )


def _schema_payload(schema: object | None) -> JsonDict | None:
    if schema is None:
        return None
    if isinstance(schema, SchemaLike):
        return schema_to_dict(schema)
    return None


__all__ = ["ScanTelemetry", "ScanTelemetryOptions", "fragment_telemetry"]
