"""Shared scan telemetry helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.dataset as ds

from core_types import JsonDict
from obs.otel.metrics import set_scan_telemetry
from serde_msgspec import StructBaseCompat


class _SchemaLike(Protocol):
    names: Sequence[str]
    metadata: object | None

    def field(self, index: int) -> pa.Field: ...


def _decoded_metadata(raw_metadata: object | None) -> dict[str, str]:
    if not isinstance(raw_metadata, Mapping):
        return {}
    decoded: dict[str, str] = {}
    for key, value in raw_metadata.items():
        if not isinstance(key, (bytes, bytearray, memoryview)):
            continue
        if not isinstance(value, (bytes, bytearray, memoryview)):
            continue
        decoded[bytes(key).decode("utf-8", errors="replace")] = bytes(value).decode(
            "utf-8",
            errors="replace",
        )
    return decoded


def _field_payload(field: pa.Field) -> JsonDict:
    return {
        "name": field.name,
        "type": str(field.type),
        "nullable": bool(field.nullable),
        "metadata": _decoded_metadata(field.metadata),
    }


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


def list_fragments(
    dataset: ds.Dataset, *, predicate: ds.Expression | None = None
) -> list[ds.Fragment]:
    """Return dataset fragments, optionally filtered by a predicate.

    Returns:
    -------
    list[ds.Fragment]
        Dataset fragments matching the predicate.
    """
    fragments = (
        dataset.get_fragments(filter=predicate)
        if predicate is not None
        else dataset.get_fragments()
    )
    collected: list[ds.Fragment] = []
    for fragment in fragments:
        if predicate is None:
            collected.append(fragment)
            continue
        subset = getattr(fragment, "subset", None)
        if callable(subset):
            try:
                pruned = subset(predicate)
            except (AttributeError, NotImplementedError, TypeError, ValueError):
                pruned = fragment
        else:
            pruned = fragment
        if pruned is None:
            continue
        if isinstance(pruned, ds.Fragment):
            collected.append(pruned)
            continue
        collected.extend(list(cast("Iterable[ds.Fragment]", pruned)))
    return collected


def row_group_count(fragments: Sequence[ds.Fragment]) -> int:
    """Return the total row-group count for supported fragments."""
    total = 0
    for fragment in fragments:
        splitter = getattr(fragment, "split_by_row_group", None)
        if callable(splitter):
            split = splitter()
            total += len(list(cast("Iterable[ds.Fragment]", split)))
    return total


def scan_task_count(scanner: ds.Scanner) -> int:
    """Return the scan task count for a scanner."""
    return sum(1 for _ in scanner.scan_tasks())


def fragment_file_hints(
    fragments: Sequence[ds.Fragment],
    *,
    limit: int | None = 5,
) -> tuple[str, ...]:
    """Return a small set of fragment path hints."""
    hints: list[str] = []
    for fragment in fragments:
        path = getattr(fragment, "path", None)
        if path is None:
            continue
        hints.append(str(path))
        if limit is not None and len(hints) >= limit:
            break
    return tuple(hints)


def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: object | None = None,
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
    predicate_expr = predicate if isinstance(predicate, ds.Expression) else None
    fragments = list_fragments(dataset, predicate=predicate_expr)
    try:
        count_rows = dataset.count_rows(filter=predicate_expr)
    except (AttributeError, NotImplementedError, TypeError, ValueError):
        count_rows = None
    fragment_paths, partition_expressions = _fragment_paths_and_partitions(fragments)
    estimated_rows = _estimated_rows(fragments)
    if scanner is None:
        scanner = dataset.scanner(filter=predicate_expr)
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
    schema_field = getattr(schema, "field", None)
    schema_names = getattr(schema, "names", None)
    if not callable(schema_field) or not isinstance(schema_names, Sequence):
        return None
    schema_obj = cast("_SchemaLike", schema)
    fields = [_field_payload(schema_obj.field(index)) for index in range(len(schema_obj.names))]
    return {
        "fields": fields,
        "metadata": _decoded_metadata(schema_obj.metadata),
    }


__all__ = [
    "ScanTelemetry",
    "ScanTelemetryOptions",
    "fragment_file_hints",
    "fragment_telemetry",
    "list_fragments",
    "row_group_count",
    "scan_task_count",
]
