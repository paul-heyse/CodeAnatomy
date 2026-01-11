"""Dataset statistics helpers for manifests and debugging."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import TypedDict

import pyarrow as pa

from core_types import JsonDict

type RowValue = str | int
type Row = dict[str, RowValue]


class TableSummary(TypedDict):
    """Typed summary of a single table for manifest output."""

    rows: int
    columns: int
    schema_fingerprint: str
    schema: list[JsonDict]


def schema_fingerprint(schema: pa.Schema) -> str:
    """Compute a stable fingerprint for an Arrow schema.

    We fingerprint only:
      - field name
      - field type (string form)
      - nullable

    This avoids instability from metadata ordering and is good enough for “debug manifests”.

    Returns
    -------
    str
        SHA-256 fingerprint of the schema.
    """
    fields = [{"name": f.name, "type": str(f.type), "nullable": bool(f.nullable)} for f in schema]
    payload = json.dumps(fields, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def table_summary(table: pa.Table) -> TableSummary:
    """Return a compact summary for a table suitable for manifest recording.

    Returns
    -------
    TableSummary
        Summary statistics for the table.
    """
    sch_fp = schema_fingerprint(table.schema)
    return {
        "rows": int(table.num_rows),
        "columns": len(table.column_names),
        "schema_fingerprint": sch_fp,
        "schema": [
            {"name": f.name, "type": str(f.type), "nullable": bool(f.nullable)}
            for f in table.schema
        ],
    }


def dataset_stats_table(tables: Mapping[str, pa.Table | None]) -> pa.Table:
    """Build a dataset-level stats table.

    Table columns:
      dataset_name, rows, columns, schema_fingerprint

    Returns
    -------
    pa.Table
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
    return pa.Table.from_pylist(
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


def column_stats_table(tables: Mapping[str, pa.Table | None]) -> pa.Table:
    """Build a column-level stats table.

    Table columns:
      dataset_name, column_name, type, null_count

    Returns
    -------
    pa.Table
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
    return pa.Table.from_pylist(
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
