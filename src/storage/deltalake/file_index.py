"""Delta Lake file index from get_add_actions metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from datafusion_engine.arrow.interop import empty_table_for_schema


@dataclass(frozen=True)
class FileIndexEntry:
    """Typed representation of a Delta file index entry.

    Attributes
    ----------
    path : str
        Relative path to the data file.
    size_bytes : int
        Size of the file in bytes.
    modification_time : int
        Modification timestamp in milliseconds since epoch.
    partition_values : dict[str, str]
        Partition column values for this file.
    stats_min : dict[str, Any] | None
        Minimum values per column from file statistics.
    stats_max : dict[str, Any] | None
        Maximum values per column from file statistics.
    num_records : int | None
        Number of records in the file.
    """

    path: str
    size_bytes: int
    modification_time: int
    partition_values: dict[str, str]
    stats_min: dict[str, Any] | None
    stats_max: dict[str, Any] | None
    num_records: int | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> FileIndexEntry:
        """Create FileIndexEntry from a row dictionary.

        Parameters
        ----------
        row : dict[str, Any]
            Row data from the file index table.

        Returns
        -------
        FileIndexEntry
            Typed file index entry.
        """
        return cls(
            path=str(row.get("path", "")),
            size_bytes=int(row.get("size_bytes", 0)),
            modification_time=int(row.get("modification_time", 0)),
            partition_values=dict(row.get("partition_values") or {}),
            stats_min=row.get("stats_min"),
            stats_max=row.get("stats_max"),
            num_records=row.get("num_records"),
        )


def _parse_int_field(value: object, *, field: str) -> int:
    """Coerce optional numeric payloads into integer values.

    Parameters
    ----------
    value
        Raw payload value that should represent an integer.
    field
        Field label used in error messages.

    Returns
    -------
    int
        Integer value with ``None`` mapped to ``0``.

    Raises
    ------
    TypeError
        Raised when the payload is not numeric or string-like.
    ValueError
        Raised when a string payload cannot be parsed as an integer.
    """
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            msg = f"Delta add action field {field} was not an integer: {value!r}"
            raise ValueError(msg) from exc
    msg = f"Delta add action field {field} had unsupported type: {type(value).__name__}"
    raise TypeError(msg)


def build_delta_file_index_from_add_actions(
    add_actions: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a file index table from Rust control-plane add actions.

    Parameters
    ----------
    add_actions
        Add action payloads returned by the Rust Delta control plane.

    Returns
    -------
    pa.Table
        Normalized file index table compatible with pruning utilities.

    Raises
    ------
    ValueError
        Raised when required add action fields are missing.
    """
    if not add_actions:
        return _empty_file_index_table()
    paths: list[str] = []
    sizes: list[int] = []
    modification_times: list[int] = []
    partition_values: list[dict[str, str | None]] = []
    stats_values: list[str | None] = []
    for action in add_actions:
        path_value = action.get("path")
        if path_value is None:
            msg = "Delta add action missing required path field."
            raise ValueError(msg)
        paths.append(str(path_value))
        sizes.append(_parse_int_field(action.get("size"), field="size"))
        modification_times.append(
            _parse_int_field(action.get("modification_time"), field="modification_time")
        )
        partitions_raw = action.get("partition_values")
        partitions: dict[str, str | None] = {}
        if isinstance(partitions_raw, Mapping):
            for key, value in partitions_raw.items():
                name = str(key)
                partitions[name] = None if value is None else str(value)
        partition_values.append(partitions)
        stats_raw = action.get("stats")
        stats_values.append(None if stats_raw is None else str(stats_raw))
    map_type = pa.map_(pa.string(), pa.string())
    raw_table = pa.table(
        {
            "path": pa.array(paths, type=pa.string()),
            "size": pa.array(sizes, type=pa.int64()),
            "modificationTime": pa.array(modification_times, type=pa.int64()),
            "partitionValues": pa.array(partition_values, type=map_type),
            "stats": pa.array(stats_values, type=pa.string()),
        }
    )
    return _normalize_file_index(raw_table)


def _normalize_file_index(raw_table: pa.Table) -> pa.Table:
    """Normalize the file index table to a consistent schema.

    Parameters
    ----------
    raw_table : pa.Table
        Raw table from get_add_actions.

    Returns
    -------
    pa.Table
        Normalized file index table.
    """
    # Extract relevant columns from the raw add actions table
    # The exact column names may vary based on deltalake version
    column_mapping = {
        "path": "path",
        "size": "size_bytes",
        "size_bytes": "size_bytes",
        "modificationTime": "modification_time",
        "modification_time": "modification_time",
        "partitionValues": "partition_values",
        "partition_values": "partition_values",
        "stats": "stats",
        "numRecords": "num_records",
        "num_records": "num_records",
    }

    # Build column extraction map
    available_cols = set(raw_table.column_names)
    columns_to_extract: dict[str, str] = {}
    for source_col, target_col in column_mapping.items():
        if source_col in available_cols and target_col not in columns_to_extract:
            columns_to_extract[target_col] = source_col

    context = ColumnResolveContext(
        raw_table=raw_table,
        columns_to_extract=columns_to_extract,
        available_cols=available_cols,
    )
    result_columns: dict[str, pa.Array] = {
        "path": _resolve_column(
            context,
            target="path",
            default=pa.array([None] * raw_table.num_rows, type=pa.string()),
        ),
        "size_bytes": _resolve_column(
            context,
            target="size_bytes",
            default=pa.array([0] * raw_table.num_rows, type=pa.int64()),
            cast_type=pa.int64(),
        ),
        "modification_time": _resolve_column(
            context,
            target="modification_time",
            default=pa.array([0] * raw_table.num_rows, type=pa.int64()),
            cast_type=pa.int64(),
        ),
        "partition_values": _resolve_column(
            context,
            target="partition_values",
            default=pa.array(
                [{}] * raw_table.num_rows,
                type=pa.map_(pa.string(), pa.string()),
            ),
        ),
        "num_records": _resolve_column(
            context,
            target="num_records",
            default=pa.array([None] * raw_table.num_rows, type=pa.int64()),
            cast_type=pa.int64(),
        ),
    }

    stats_min, stats_max = _extract_stats_column(context)
    result_columns["stats_min"] = stats_min
    result_columns["stats_max"] = stats_max

    return pa.table(result_columns)


def _extract_stats(stats_column: pa.Array) -> tuple[pa.Array, pa.Array]:
    """Extract min and max statistics from stats column.

    Parameters
    ----------
    stats_column : pa.Array
        Stats column from add actions (may be string JSON or struct).

    Returns
    -------
    tuple[pa.Array, pa.Array]
        Min and max statistics as map arrays.
    """
    import json

    num_rows = len(stats_column)
    min_values: list[dict[str, str] | None] = []
    max_values: list[dict[str, str] | None] = []

    for i in range(num_rows):
        stats_value = stats_column[i].as_py()

        if stats_value is None:
            min_values.append(None)
            max_values.append(None)
            continue

        # Parse stats from JSON string if needed
        if isinstance(stats_value, str):
            try:
                stats_dict = json.loads(stats_value)
            except (json.JSONDecodeError, TypeError, ValueError):
                min_values.append(None)
                max_values.append(None)
                continue
        elif isinstance(stats_value, dict):
            stats_dict = stats_value
        else:
            min_values.append(None)
            max_values.append(None)
            continue

        # Extract minValues and maxValues
        min_vals = stats_dict.get("minValues")
        max_vals = stats_dict.get("maxValues")

        # Convert to string maps for consistent storage
        if min_vals and isinstance(min_vals, dict):
            min_values.append({str(k): str(v) for k, v in min_vals.items()})
        else:
            min_values.append(None)

        if max_vals and isinstance(max_vals, dict):
            max_values.append({str(k): str(v) for k, v in max_vals.items()})
        else:
            max_values.append(None)

    map_type = pa.map_(pa.string(), pa.string())
    return pa.array(min_values, type=map_type), pa.array(max_values, type=map_type)


def _resolve_adds_table(adds: object) -> pa.Table:
    if isinstance(adds, pa.Table):
        return adds
    if isinstance(adds, pa.RecordBatch):
        return pa.Table.from_batches([adds])
    to_table = getattr(adds, "to_pyarrow_table", None)
    if callable(to_table):
        result = to_table()
        if isinstance(result, pa.Table):
            return result
    msg = "Delta add actions could not be coerced to a PyArrow table."
    raise TypeError(msg)


@dataclass(frozen=True)
class ColumnResolveContext:
    raw_table: pa.Table
    columns_to_extract: Mapping[str, str]
    available_cols: set[str]


def _resolve_column(
    context: ColumnResolveContext,
    *,
    target: str,
    default: pa.Array,
    cast_type: pa.DataType | None = None,
) -> pa.Array:
    source_col = context.columns_to_extract.get(target)
    if source_col and source_col in context.available_cols:
        column = context.raw_table.column(source_col)
        return column.cast(cast_type) if cast_type is not None else column
    return default


def _extract_stats_column(context: ColumnResolveContext) -> tuple[pa.Array, pa.Array]:
    stats_col = context.columns_to_extract.get("stats")
    if stats_col and stats_col in context.available_cols:
        return _extract_stats(context.raw_table.column(stats_col))
    null_stats = pa.array(
        [None] * context.raw_table.num_rows,
        type=pa.map_(pa.string(), pa.string()),
    )
    return null_stats, null_stats


def _empty_file_index_table() -> pa.Table:
    """Create an empty file index table with the correct schema.

    Returns
    -------
    pa.Table
        Empty file index table.
    """
    schema = pa.schema(
        [
            pa.field("path", pa.string()),
            pa.field("size_bytes", pa.int64()),
            pa.field("modification_time", pa.int64()),
            pa.field("partition_values", pa.map_(pa.string(), pa.string())),
            pa.field("stats_min", pa.map_(pa.string(), pa.string())),
            pa.field("stats_max", pa.map_(pa.string(), pa.string())),
            pa.field("num_records", pa.int64()),
        ]
    )
    return empty_table_for_schema(schema)


__all__ = [
    "FileIndexEntry",
    "build_delta_file_index_from_add_actions",
]
