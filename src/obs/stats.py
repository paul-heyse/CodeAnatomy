"""Dataset statistics helpers for manifests and debugging."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypedDict, cast

import pyarrow as pa

from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import EncodingSpec, encode_columns, schema_fingerprint, schema_to_dict
from core_types import JsonDict

type RowValue = str | int
type Row = dict[str, RowValue]

DATASET_STATS_ENCODING_SPECS: tuple[EncodingSpec, ...] = (EncodingSpec(column="dataset_name"),)

COLUMN_STATS_ENCODING_SPECS: tuple[EncodingSpec, ...] = (
    EncodingSpec(column="dataset_name"),
    EncodingSpec(column="column_name"),
    EncodingSpec(column="type"),
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
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("dataset_name", pa.string()),
                ("rows", pa.int64()),
                ("columns", pa.int32()),
                ("schema_fingerprint", pa.string()),
            ]
        ),
    )
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
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("dataset_name", pa.string()),
                ("column_name", pa.string()),
                ("type", pa.string()),
                ("null_count", pa.int64()),
            ]
        ),
    )
    encoded = encode_columns(table, specs=COLUMN_STATS_ENCODING_SPECS)
    return ChunkPolicy().apply(encoded)
