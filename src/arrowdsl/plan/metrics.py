"""Plan statistics, quality, and dataset fragment utilities."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypedDict, cast

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.types as patypes

from arrowdsl.compute.filters import invalid_id_expr
from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.compute.macros import null_expr, scalar_expr
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.build import (
    const_array,
    empty_table,
    rows_to_table,
    table_from_arrays,
    table_from_schema,
)
from arrowdsl.schema.schema import EncodingSpec, encode_columns, schema_fingerprint, schema_to_dict
from core_types import JsonDict, PathLike, ensure_path

type RowValue = str | int
type Row = dict[str, RowValue]
type ValuesLike = ArrayLike | ChunkedArrayLike

DATASET_STATS_ENCODING_SPECS: tuple[EncodingSpec, ...] = (EncodingSpec(column="dataset_name"),)

COLUMN_STATS_ENCODING_SPECS: tuple[EncodingSpec, ...] = (
    EncodingSpec(column="dataset_name"),
    EncodingSpec(column="column_name"),
    EncodingSpec(column="type"),
)

DATASET_STATS_SCHEMA = pa.schema(
    [
        ("dataset_name", pa.string()),
        ("rows", pa.int64()),
        ("columns", pa.int32()),
        ("schema_fingerprint", pa.string()),
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
    schema_fingerprint: str
    schema: list[JsonDict]


def table_summary(table: TableLike) -> TableSummary:
    """Return a compact summary for a table suitable for manifest recording.

    Returns
    -------
    TableSummary
        Summary statistics for the table.
    """
    sch_fp = schema_fingerprint(table.schema)
    schema_fields = cast("list[JsonDict]", schema_to_dict(table.schema).get("fields", []))
    return {
        "rows": int(table.num_rows),
        "columns": len(table.column_names),
        "schema_fingerprint": sch_fp,
        "schema": schema_fields,
    }


def dataset_stats_table(tables: Mapping[str, TableLike | None]) -> TableLike:
    """Build a dataset-level stats table.

    Table columns:
      dataset_name, rows, columns, schema_fingerprint

    Returns
    -------
    TableLike
        Dataset-level statistics table.
    """
    rows: list[Row] = []
    for name, t in tables.items():
        if t is None:
            continue
        sch_fp = schema_fingerprint(t.schema)
        rows.append(
            {
                "dataset_name": str(name),
                "rows": int(t.num_rows),
                "columns": len(t.column_names),
                "schema_fingerprint": sch_fp,
            }
        )
    table = rows_to_table(rows, DATASET_STATS_SCHEMA)
    encoded = encode_columns(table, specs=DATASET_STATS_ENCODING_SPECS)
    return ChunkPolicy().apply(encoded)


def column_stats_table(tables: Mapping[str, TableLike | None]) -> TableLike:
    """Build a column-level stats table.

    Table columns:
      dataset_name, column_name, type, null_count

    Returns
    -------
    TableLike
        Column-level statistics table.
    """
    rows: list[Row] = []
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
    encoded = encode_columns(table, specs=COLUMN_STATS_ENCODING_SPECS)
    return ChunkPolicy().apply(encoded)


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
    return rows_to_table(list(rows), SCAN_TELEMETRY_SCHEMA)


def list_fragments(
    dataset: ds.Dataset, *, predicate: ComputeExpression | None = None
) -> list[ds.Fragment]:
    """Return dataset fragments, optionally filtered by a predicate.

    Returns
    -------
    list[ds.Fragment]
        Dataset fragments matching the predicate.
    """
    if predicate is None:
        return list(dataset.get_fragments())
    return list(dataset.get_fragments(filter=predicate))


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
    return ParquetMetadataSpec(schema=dataset.schema, file_metadata=file_metadata)


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
    return table_from_schema(QUALITY_SCHEMA, columns={}, num_rows=0)


@dataclass(frozen=True)
class QualityPlanSpec:
    """Specification for building quality plans from IDs."""

    id_col: str
    entity_kind: str
    issue: str
    source_table: str | None = None


def _is_zero(values: ValuesLike) -> ValuesLike:
    dtype = values.type
    if patypes.is_dictionary(dtype):
        values = pc.cast(values, pa.string(), safe=False)
        dtype = values.type
    if patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return pc.equal(values, pa.scalar("0"))
    if patypes.is_integer(dtype):
        return pc.equal(values, pa.scalar(0, type=dtype))
    if patypes.is_floating(dtype):
        return pc.equal(values, pa.scalar(0.0, type=dtype))
    return pc.equal(pc.cast(values, pa.string(), safe=False), pa.scalar("0"))


def _invalid_id_mask(values: ValuesLike) -> ValuesLike:
    return pc.or_(pc.is_null(values), _is_zero(values))


def _quality_table_from_ids(
    ids: ValuesLike,
    *,
    entity_kind: str,
    issue: str,
    source_table: str | None,
) -> TableLike:
    if ids.null_count != 0 or not patypes.is_string(ids.type):
        ids = pc.cast(ids, pa.string(), safe=False)
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
    return table_from_arrays(QUALITY_SCHEMA, columns=columns, num_rows=n)


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


def quality_plan_from_ids(
    plan: Plan,
    *,
    spec: QualityPlanSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Return a plan producing quality rows for invalid IDs.

    Returns
    -------
    Plan
        Plan emitting quality rows for invalid identifiers.
    """
    schema = plan.schema(ctx=ctx)
    available = set(schema.names)
    if spec.id_col in available:
        dtype = schema.field(spec.id_col).type
        id_expr = ensure_expression(pc.field(spec.id_col))
    else:
        dtype = pa.string()
        id_expr = null_expr(dtype)

    id_str = ensure_expression(pc.cast(id_expr, pa.string(), safe=False))
    invalid = invalid_id_expr(id_expr, dtype=dtype)
    issue_expr = scalar_expr(spec.issue, dtype=pa.string())
    kind_expr = scalar_expr(spec.entity_kind, dtype=pa.string())
    if spec.source_table is None:
        source_expr = null_expr(pa.string())
    else:
        source_expr = scalar_expr(spec.source_table, dtype=pa.string())
    filtered = plan.filter(invalid, ctx=ctx)
    exprs = [kind_expr, id_str, issue_expr, source_expr]
    names = ["entity_kind", "entity_id", "issue", "source_table"]
    return filtered.project(exprs, names, ctx=ctx)


__all__ = [
    "COLUMN_STATS_ENCODING_SPECS",
    "COLUMN_STATS_SCHEMA",
    "DATASET_STATS_ENCODING_SPECS",
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
    "fragment_file_hints",
    "list_fragments",
    "parquet_metadata_collector",
    "parquet_metadata_factory",
    "quality_from_ids",
    "quality_plan_from_ids",
    "row_group_count",
    "row_group_fragments",
    "scan_task_count",
    "scan_telemetry_table",
    "split_fragment_by_row_group",
    "table_summary",
    "take_row_group_fragments",
]
