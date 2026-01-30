"""Dataset statistics, quality, and fragment utilities."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypedDict, cast

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.types as patypes

from arrow_utils.core.array_iter import iter_array_values
from core_types import JsonDict, JsonValue, PathLike, RowStrict, ensure_path
from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.build import (
    const_array,
    empty_table,
    rows_to_table,
    table_from_columns,
)
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    SchemaLike,
    TableLike,
)
from datafusion_engine.encoding import NormalizePolicy
from datafusion_engine.identity import schema_identity_hash
from obs.otel.metrics import set_dataset_stats
from serde_msgspec import MSGPACK_ENCODER

type ValuesLike = ArrayLike | ChunkedArrayLike

DATASET_STATS_ENCODING_POLICY = EncodingPolicy(dictionary_cols=frozenset({"dataset_name"}))

COLUMN_STATS_ENCODING_POLICY = EncodingPolicy(
    dictionary_cols=frozenset({"dataset_name", "column_name", "type"})
)

DATASET_STATS_SCHEMA = pa.schema(
    [
        ("dataset_name", pa.string()),
        ("rows", pa.int64()),
        ("columns", pa.int32()),
        ("schema_identity_hash", pa.string()),
    ]
)

COLUMN_STATS_SCHEMA = pa.schema(
    [
        ("dataset_name", pa.string()),
        ("column_name", pa.string()),
        ("type", pa.string()),
        ("null_count", pa.int64()),
    ]
)

SCAN_TELEMETRY_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("fragment_count", pa.int64()),
        ("row_group_count", pa.int64()),
        ("count_rows", pa.int64()),
        ("estimated_rows", pa.int64()),
        ("file_hints", pa.list_(pa.string())),
        ("fragment_paths", pa.list_(pa.string())),
        ("partition_expressions", pa.list_(pa.string())),
        ("required_columns", pa.list_(pa.string())),
        ("scan_columns", pa.list_(pa.string())),
        ("dataset_schema_msgpack", pa.binary()),
        ("projected_schema_msgpack", pa.binary()),
        ("discovery_policy_msgpack", pa.binary()),
        ("scan_profile_msgpack", pa.binary()),
    ]
)

QUALITY_SCHEMA = pa.schema(
    [
        pa.field("entity_kind", pa.string(), nullable=False),
        pa.field("entity_id", pa.string()),
        pa.field("issue", pa.string(), nullable=False),
        pa.field("source_table", pa.string()),
    ]
)


class TableSummary(TypedDict):
    """Typed summary of a single table for manifest output."""

    rows: int
    columns: int
    schema_identity_hash: str
    schema: list[JsonDict]


def table_summary(table: TableLike) -> TableSummary:
    """Return a compact summary for a table suitable for manifest recording.

    Returns
    -------
    TableSummary
        Summary statistics for the table.
    """
    sch_fp = schema_identity_hash(table.schema)
    schema_fields = cast("list[JsonDict]", schema_to_dict(table.schema).get("fields", []))
    return {
        "rows": int(table.num_rows),
        "columns": len(table.column_names),
        "schema_identity_hash": sch_fp,
        "schema": schema_fields,
    }


def dataset_stats_table(tables: Mapping[str, TableLike | None]) -> TableLike:
    """Build a dataset-level stats table.

    Table columns:
      dataset_name, rows, columns, schema_identity_hash

    Returns
    -------
    TableLike
        Dataset-level statistics table.
    """
    rows: list[RowStrict] = []
    for name, t in tables.items():
        if t is None:
            continue
        sch_fp = schema_identity_hash(t.schema)
        set_dataset_stats(str(name), rows=int(t.num_rows), columns=len(t.column_names))
        rows.append(
            {
                "dataset_name": str(name),
                "rows": int(t.num_rows),
                "columns": len(t.column_names),
                "schema_identity_hash": sch_fp,
            }
        )
    table = rows_to_table(rows, DATASET_STATS_SCHEMA)
    return NormalizePolicy(encoding=DATASET_STATS_ENCODING_POLICY).apply(table)


def column_stats_table(tables: Mapping[str, TableLike | None]) -> TableLike:
    """Build a column-level stats table.

    Table columns:
      dataset_name, column_name, type, null_count

    Returns
    -------
    TableLike
        Column-level statistics table.
    """
    rows: list[RowStrict] = []
    for dname, t in tables.items():
        if t is None:
            continue
        for col_name in t.column_names:
            col = t[col_name]
            rows.append(
                {
                    "dataset_name": str(dname),
                    "column_name": str(col_name),
                    "type": str(col.type),
                    "null_count": int(col.null_count),
                }
            )
    table = rows_to_table(rows, COLUMN_STATS_SCHEMA)
    return NormalizePolicy(encoding=COLUMN_STATS_ENCODING_POLICY).apply(table)


def empty_scan_telemetry_table() -> TableLike:
    """Return an empty scan telemetry table.

    Returns
    -------
    TableLike
        Empty scan telemetry table.
    """
    return empty_table(SCAN_TELEMETRY_SCHEMA)


def scan_telemetry_table(rows: Sequence[Mapping[str, object]]) -> TableLike:
    """Build a scan telemetry table from row mappings.

    Returns
    -------
    TableLike
        Scan telemetry table.
    """
    normalized: list[dict[str, object]] = []
    for row in rows:
        payload = dict(row)
        normalized.append(
            {
                "dataset": payload.get("dataset"),
                "fragment_count": payload.get("fragment_count"),
                "row_group_count": payload.get("row_group_count"),
                "count_rows": payload.get("count_rows"),
                "estimated_rows": payload.get("estimated_rows"),
                "file_hints": payload.get("file_hints"),
                "fragment_paths": payload.get("fragment_paths"),
                "partition_expressions": payload.get("partition_expressions"),
                "required_columns": payload.get("required_columns"),
                "scan_columns": payload.get("scan_columns"),
                "dataset_schema_msgpack": _scan_payload_bytes(
                    payload,
                    primary="dataset_schema_msgpack",
                    fallback="dataset_schema_json",
                ),
                "projected_schema_msgpack": _scan_payload_bytes(
                    payload,
                    primary="projected_schema_msgpack",
                    fallback="projected_schema_json",
                ),
                "discovery_policy_msgpack": _scan_payload_bytes(
                    payload,
                    primary="discovery_policy_msgpack",
                    fallback="discovery_policy_json",
                ),
                "scan_profile_msgpack": _scan_payload_bytes(
                    payload,
                    primary="scan_profile_msgpack",
                    fallback="scan_profile_json",
                ),
            }
        )
    return rows_to_table(normalized, SCAN_TELEMETRY_SCHEMA)


_SCAN_TELEMETRY_ENCODER = MSGPACK_ENCODER


def _scan_payload_bytes(payload: Mapping[str, object], *, primary: str, fallback: str) -> bytes:
    value = payload.get(primary)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    legacy = payload.get(fallback)
    if legacy is not None:
        return MSGPACK_ENCODER.encode(legacy)
    return MSGPACK_ENCODER.encode({})


def encode_scan_telemetry_rows(rows: Sequence[Mapping[str, object]]) -> bytes:
    """Encode scan telemetry rows into MessagePack bytes.

    Returns
    -------
    bytes
        MessagePack payload for scan telemetry rows.
    """
    buf = bytearray()
    _SCAN_TELEMETRY_ENCODER.encode_into(list(rows), buf)
    return bytes(buf)


def list_fragments(
    dataset: ds.Dataset, *, predicate: ComputeExpression | None = None
) -> list[ds.Fragment]:
    """Return dataset fragments, optionally filtered by a predicate.

    Returns
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


def split_fragment_by_row_group(fragment: ds.Fragment) -> list[ds.Fragment]:
    """Split a fragment into row-group fragments when supported.

    Returns
    -------
    list[ds.Fragment]
        Row-group fragments or the original fragment when unsupported.
    """
    splitter = getattr(fragment, "split_by_row_group", None)
    if callable(splitter):
        fragments = splitter()
        return list(cast("Iterable[ds.Fragment]", fragments))
    return [fragment]


def row_group_fragments(
    dataset: ds.Dataset, *, predicate: ComputeExpression | None = None
) -> list[ds.Fragment]:
    """Return row-group fragments for a dataset.

    Returns
    -------
    list[ds.Fragment]
        Row-group fragments or dataset fragments when unsupported.
    """
    fragments = list_fragments(dataset, predicate=predicate)
    expanded: list[ds.Fragment] = []
    for fragment in fragments:
        expanded.extend(split_fragment_by_row_group(fragment))
    return expanded


def row_group_count(fragments: Sequence[ds.Fragment]) -> int:
    """Return the total row-group count for supported fragments.

    Returns
    -------
    int
        Total row-group count.
    """
    total = 0
    for fragment in fragments:
        splitter = getattr(fragment, "split_by_row_group", None)
        if callable(splitter):
            split = splitter()
            total += len(list(cast("Iterable[ds.Fragment]", split)))
    return total


def _stat_value(value: object) -> JsonValue:
    if value is None:
        return None
    if isinstance(value, (int, float, bool, str)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return value.hex()
    return str(value)


def _sorting_columns_payload(
    schema: SchemaLike, sorting_columns: Sequence[pq.SortingColumn] | None
) -> list[JsonDict] | None:
    if not sorting_columns:
        return None
    names = list(schema.names)
    payload: list[JsonDict] = []
    for col in sorting_columns:
        name = names[col.column_index] if col.column_index < len(names) else str(col.column_index)
        payload.append(
            {
                "column": name,
                "order": "descending" if col.descending else "ascending",
                "nulls_first": col.nulls_first,
            }
        )
    return payload or None


def row_group_stats(
    fragments: Sequence[ds.Fragment],
    *,
    schema: SchemaLike,
    columns: Sequence[str],
    max_row_groups: int | None = None,
) -> list[JsonDict]:
    """Return row-group statistics for selected columns.

    Returns
    -------
    list[JsonDict]
        Row group statistics payloads.
    """
    if not columns:
        return []
    indices = {name: idx for idx, name in enumerate(schema.names)}
    rows: list[JsonDict] = []
    count = 0
    for fragment in fragments:
        metadata = fragment.metadata
        if metadata is None:
            continue
        file_path = getattr(fragment, "path", None)
        for idx in range(metadata.num_row_groups):
            if max_row_groups is not None and count >= max_row_groups:
                return rows
            group = metadata.row_group(idx)
            column_stats: dict[str, JsonDict] = {}
            for name in columns:
                col_index = indices.get(name)
                if col_index is None:
                    continue
                column = group.column(col_index)
                stats = column.statistics
                if stats is None:
                    continue
                column_stats[name] = {
                    "null_count": stats.null_count,
                    "distinct_count": stats.distinct_count,
                    "min": _stat_value(stats.min),
                    "max": _stat_value(stats.max),
                }
            rows.append(
                {
                    "file": str(file_path) if file_path is not None else None,
                    "row_group": idx,
                    "num_rows": group.num_rows,
                    "total_byte_size": group.total_byte_size,
                    "columns": column_stats,
                    "sorting_columns": _sorting_columns_payload(
                        schema,
                        getattr(group, "sorting_columns", None),
                    ),
                }
            )
            count += 1
    return rows


def fragment_file_hints(
    fragments: Sequence[ds.Fragment],
    *,
    limit: int | None = 5,
) -> tuple[str, ...]:
    """Return a small set of fragment path hints.

    Returns
    -------
    tuple[str, ...]
        Tuple of path strings for fragment hints.
    """
    hints: list[str] = []
    for fragment in fragments:
        path = getattr(fragment, "path", None)
        if path is None:
            continue
        hints.append(str(path))
        if limit is not None and len(hints) >= limit:
            break
    return tuple(hints)


@dataclass(frozen=True)
class ParquetMetadataSpec:
    """Parquet metadata sidecar configuration."""

    schema: SchemaLike
    file_metadata: tuple[pq.FileMetaData, ...] = ()

    def write_common_metadata(
        self,
        base_dir: PathLike,
        *,
        filename: str = "_common_metadata",
    ) -> str:
        """Write a _common_metadata sidecar file.

        Returns
        -------
        str
            Path to the written metadata file.
        """
        path = ensure_path(base_dir) / filename
        pq.write_metadata(self.schema, str(path))
        return str(path)

    def write_metadata(
        self,
        base_dir: PathLike,
        *,
        filename: str = "_metadata",
    ) -> str | None:
        """Write a _metadata sidecar file with row-group stats when available.

        Returns
        -------
        str | None
            Path to the written metadata file, or ``None`` when unavailable.
        """
        if not self.file_metadata:
            return None
        path = ensure_path(base_dir) / filename
        pq.write_metadata(
            self.schema,
            str(path),
            metadata_collector=list(self.file_metadata),
        )
        return str(path)


def parquet_metadata_collector(
    fragments: Sequence[ds.Fragment],
) -> tuple[pq.FileMetaData, ...]:
    """Collect Parquet file metadata from dataset fragments.

    Returns
    -------
    tuple[pq.FileMetaData, ...]
        File metadata entries for the fragments.
    """
    collector: list[pq.FileMetaData] = []
    for fragment in fragments:
        metadata = fragment.metadata
        if metadata is None:
            continue
        collector.append(cast("pq.FileMetaData", metadata))
    return tuple(collector)


def parquet_metadata_factory(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
) -> ParquetMetadataSpec:
    """Return a ParquetMetadataSpec for a dataset.

    Returns
    -------
    ParquetMetadataSpec
        Metadata spec for sidecar generation.
    """
    fragments = list_fragments(dataset, predicate=predicate)
    file_metadata = parquet_metadata_collector(fragments)
    schema = _metadata_schema(dataset.schema, file_metadata)
    return ParquetMetadataSpec(schema=schema, file_metadata=file_metadata)


def _metadata_schema(
    dataset_schema: pa.Schema,
    file_metadata: Sequence[pq.FileMetaData],
) -> pa.Schema:
    if not file_metadata:
        return dataset_schema
    return file_metadata[0].schema.to_arrow_schema()


def scan_task_count(scanner: ds.Scanner) -> int:
    """Return the scan task count for a scanner.

    Returns
    -------
    int
        Number of scan tasks.
    """
    return sum(1 for _ in scanner.scan_tasks())


def take_row_group_fragments(
    fragments: Sequence[ds.Fragment], *, limit: int | None = None
) -> list[ds.Fragment]:
    """Return a limited subset of row-group fragments.

    Returns
    -------
    list[ds.Fragment]
        Subset of row-group fragments, limited when requested.
    """
    expanded: list[ds.Fragment] = []
    for fragment in fragments:
        expanded.extend(split_fragment_by_row_group(fragment))
        if limit is not None and len(expanded) >= limit:
            return expanded[:limit]
    return expanded


def empty_quality_table() -> TableLike:
    """Return an empty quality table.

    Returns
    -------
    TableLike
        Empty quality table with the canonical schema.
    """
    return empty_table(QUALITY_SCHEMA)


@dataclass(frozen=True)
class QualityPlanSpec:
    """Specification for building quality plans from IDs."""

    id_col: str
    entity_kind: str
    issue: str
    source_table: str | None = None


def _is_zero(values: ValuesLike) -> ValuesLike:
    def is_zero(value: object | None) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value == "0"
        if isinstance(value, (int, float)):
            return value == 0
        return str(value) == "0"

    flags = [is_zero(value) for value in iter_array_values(values)]
    return pa.array(flags, type=pa.bool_())


def _invalid_id_mask(values: ValuesLike) -> ValuesLike:
    flags = [
        value is None or bool(is_zero)
        for value, is_zero in zip(
            iter_array_values(values),
            iter_array_values(_is_zero(values)),
            strict=True,
        )
    ]
    return pa.array(flags, type=pa.bool_())


def _quality_table_from_ids(
    ids: ValuesLike,
    *,
    entity_kind: str,
    issue: str,
    source_table: str | None,
) -> TableLike:
    if ids.null_count != 0 or not patypes.is_string(ids.type):
        cast_fn = getattr(ids, "cast", None)
        if not callable(cast_fn):
            msg = "Values do not support cast()."
            raise TypeError(msg)
        ids = cast("ValuesLike", cast_fn(pa.string(), safe=False))
    n = len(ids)
    kind_arr = const_array(n, entity_kind, dtype=pa.string())
    issue_arr = const_array(n, issue, dtype=pa.string())
    if source_table is None:
        source_arr = pa.nulls(n, type=pa.string())
    else:
        source_arr = const_array(n, source_table, dtype=pa.string())
    columns = {
        "entity_kind": kind_arr,
        "entity_id": ids,
        "issue": issue_arr,
        "source_table": source_arr,
    }
    return table_from_columns(QUALITY_SCHEMA, columns)


def quality_from_ids(
    table: TableLike,
    *,
    id_col: str,
    entity_kind: str,
    issue: str,
    source_table: str | None = None,
) -> TableLike:
    """Return quality rows for invalid IDs in the specified column.

    Returns
    -------
    TableLike
        Quality rows for invalid identifiers.
    """
    if id_col not in table.column_names:
        if table.num_rows == 0:
            return empty_quality_table()
        ids = pa.nulls(table.num_rows, type=pa.string())
        return _quality_table_from_ids(
            ids, entity_kind=entity_kind, issue=issue, source_table=source_table
        )
    values = table[id_col]
    mask = _invalid_id_mask(values)
    invalid = table.filter(mask)
    if invalid.num_rows == 0:
        return empty_quality_table()
    return _quality_table_from_ids(
        invalid[id_col], entity_kind=entity_kind, issue=issue, source_table=source_table
    )


def concat_quality_tables(tables: Sequence[TableLike]) -> TableLike:
    """Concatenate quality tables, returning an empty table when none exist.

    Returns
    -------
    TableLike
        Concatenated quality table.
    """
    parts = [table for table in tables if table.num_rows]
    if not parts:
        return empty_quality_table()
    return pa.concat_tables(parts)


__all__ = [
    "COLUMN_STATS_ENCODING_POLICY",
    "COLUMN_STATS_SCHEMA",
    "DATASET_STATS_ENCODING_POLICY",
    "DATASET_STATS_SCHEMA",
    "QUALITY_SCHEMA",
    "SCAN_TELEMETRY_SCHEMA",
    "ParquetMetadataSpec",
    "QualityPlanSpec",
    "TableSummary",
    "column_stats_table",
    "concat_quality_tables",
    "dataset_stats_table",
    "empty_quality_table",
    "empty_scan_telemetry_table",
    "encode_scan_telemetry_rows",
    "fragment_file_hints",
    "list_fragments",
    "parquet_metadata_collector",
    "parquet_metadata_factory",
    "quality_from_ids",
    "row_group_count",
    "row_group_fragments",
    "row_group_stats",
    "scan_task_count",
    "scan_telemetry_table",
    "split_fragment_by_row_group",
    "table_summary",
    "take_row_group_fragments",
]
