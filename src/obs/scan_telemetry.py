"""Shared scan telemetry helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow.dataset as ds

from core_types import JsonDict
from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.interop import ComputeExpression, SchemaLike
from obs.metrics import fragment_file_hints, list_fragments, row_group_count, scan_task_count
from obs.otel.metrics import set_scan_telemetry
from serde_msgspec import StructBaseCompat


class ScanTelemetry(
    StructBaseCompat,
    array_like=True,
    gc=False,
    cache_hash=True,
    frozen=True,
):
    """Scan telemetry for dataset fragments."""

    fragment_count: int
    row_group_count: int
    count_rows: int | None
    estimated_rows: int | None
    file_hints: tuple[str, ...] = ()
    fragment_paths: tuple[str, ...] = ()
    partition_expressions: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    scan_columns: tuple[str, ...] = ()
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
    required_columns: Sequence[str] | None = None
    scan_columns: Sequence[str] | None = None


def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
    scanner: ds.Scanner | None = None,
    options: ScanTelemetryOptions | None = None,
) -> ScanTelemetry:
    """Return telemetry for dataset fragments.

    Returns:
    -------
    ScanTelemetry
        Fragment counts and estimated row totals (when available).
    """
    resolved = options or ScanTelemetryOptions()
    fragments = list_fragments(dataset, predicate=predicate)
    try:
        count_rows = dataset.count_rows(filter=predicate)
    except (AttributeError, NotImplementedError, TypeError, ValueError):
        count_rows = None
    fragment_paths, partition_expressions = _fragment_paths_and_partitions(fragments)
    estimated_rows = _estimated_rows(fragments)
    if scanner is None:
        scanner = dataset.scanner(filter=predicate)
    dataset_schema = _schema_payload(getattr(dataset, "schema", None))
    projected_schema = _schema_payload(getattr(scanner, "schema", None))
    required_columns, scan_columns = _scan_columns(scanner, resolved)
    try:
        task_count = scan_task_count(scanner)
    except (AttributeError, TypeError):
        task_count = row_group_count(fragments)
    dataset_name = getattr(dataset, "name", None)
    if isinstance(dataset_name, str) and dataset_name:
        set_scan_telemetry(
            dataset_name,
            fragment_count=len(fragments),
            row_group_count=task_count,
        )
    return ScanTelemetry(
        fragment_count=len(fragments),
        row_group_count=task_count,
        count_rows=int(count_rows) if count_rows is not None else None,
        estimated_rows=estimated_rows,
        file_hints=fragment_file_hints(fragments, limit=resolved.hint_limit),
        fragment_paths=fragment_paths,
        partition_expressions=partition_expressions,
        required_columns=required_columns,
        scan_columns=scan_columns,
        dataset_schema=dataset_schema,
        projected_schema=projected_schema,
        discovery_policy=resolved.discovery_policy,
        scan_profile=resolved.scan_profile,
    )


def _fragment_paths_and_partitions(
    fragments: Sequence[ds.Fragment],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    fragment_paths: list[str] = []
    partition_expressions: list[str] = []
    for fragment in fragments:
        path = getattr(fragment, "path", None)
        if path is not None:
            fragment_paths.append(str(path))
        partition_expression = getattr(fragment, "partition_expression", None)
        if partition_expression is not None:
            partition_expressions.append(str(partition_expression))
    return tuple(fragment_paths), tuple(partition_expressions)


def _estimated_rows(fragments: Sequence[ds.Fragment]) -> int | None:
    total_rows = 0
    for fragment in fragments:
        metadata = fragment.metadata
        if metadata is None or metadata.num_rows is None or metadata.num_rows < 0:
            return None
        total_rows += int(metadata.num_rows)
    return total_rows


def _scan_columns(
    scanner: ds.Scanner,
    resolved: ScanTelemetryOptions,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    required_columns = (
        tuple(resolved.required_columns) if resolved.required_columns is not None else ()
    )
    scan_columns = (
        tuple(resolved.scan_columns)
        if resolved.scan_columns is not None
        else tuple(scanner.schema.names)
    )
    return required_columns, scan_columns


def _schema_payload(schema: object | None) -> JsonDict | None:
    if schema is None:
        return None
    if isinstance(schema, SchemaLike):
        return schema_to_dict(schema)
    return None


__all__ = ["ScanTelemetry", "ScanTelemetryOptions", "fragment_telemetry"]
