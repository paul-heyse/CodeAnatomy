"""Quality-diagnostics table helpers extracted from ``obs.metrics``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrow_utils.core.array_iter import iter_array_values
from obs.otel.metrics import record_artifact_count

type ArrayLike = pa.Array
type ChunkedArrayLike = pa.ChunkedArray
type TableLike = pa.Table
type ValuesLike = ArrayLike | ChunkedArrayLike

QUALITY_SCHEMA = pa.schema(
    [
        pa.field("entity_kind", pa.string(), nullable=False),
        pa.field("entity_id", pa.string()),
        pa.field("issue", pa.string(), nullable=False),
        pa.field("source_table", pa.string()),
    ]
)


def empty_quality_table() -> TableLike:
    """Return an empty quality table."""
    return pa.Table.from_batches([], schema=QUALITY_SCHEMA)


@dataclass(frozen=True)
class QualityPlanSpec:
    """Specification for building quality plans from IDs."""

    id_col: str
    entity_kind: str
    issue: str
    source_table: str | None = None


def _is_zero(values: ValuesLike) -> ArrayLike:
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


def _invalid_id_mask(values: ValuesLike) -> ArrayLike:
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
        ids = cast("ValuesLike", ids.cast(pa.string(), safe=False))
    n = len(ids)
    kind_arr = pa.array([entity_kind] * n, type=pa.string())
    issue_arr = pa.array([issue] * n, type=pa.string())
    if source_table is None:
        source_arr = pa.nulls(n, type=pa.string())
    else:
        source_arr = pa.array([source_table] * n, type=pa.string())
    return pa.Table.from_arrays(
        [kind_arr, ids, issue_arr, source_arr],
        schema=QUALITY_SCHEMA,
    )


def quality_from_ids(
    table: TableLike,
    *,
    id_col: str,
    entity_kind: str,
    issue: str,
    source_table: str | None = None,
) -> TableLike:
    """Return quality rows for invalid IDs in the specified column."""
    if id_col not in table.column_names:
        if table.num_rows == 0:
            return empty_quality_table()
        ids = pa.nulls(table.num_rows, type=pa.string())
        return _quality_table_from_ids(
            ids,
            entity_kind=entity_kind,
            issue=issue,
            source_table=source_table,
        )
    values = table[id_col]
    mask = _invalid_id_mask(values)
    invalid = table.filter(mask)
    if invalid.num_rows == 0:
        return empty_quality_table()
    return _quality_table_from_ids(
        invalid[id_col],
        entity_kind=entity_kind,
        issue=issue,
        source_table=source_table,
    )


def quality_issue_rows(
    *,
    entity_kind: str,
    rows: Sequence[Mapping[str, object]],
    source_table: str,
) -> list[dict[str, object]]:
    """Normalize issue rows into the quality schema shape.

    Returns:
    -------
    list[dict[str, object]]
        Normalized rows aligned to the canonical quality schema.
    """
    return [
        {
            "entity_kind": entity_kind,
            "entity_id": row.get("entity_id"),
            "issue": row.get("issue"),
            "source_table": source_table,
        }
        for row in rows
    ]


def record_quality_issue_counts(*, issue_kind: str, count: int) -> None:
    """Record low-cardinality issue-count events."""
    if count <= 0:
        return
    for _ in range(count):
        record_artifact_count(issue_kind, status="ok", attributes={"artifact.type": "event"})


def concat_quality_tables(tables: Sequence[TableLike]) -> TableLike:
    """Concatenate quality tables, defaulting to an empty canonical table.

    Returns:
    -------
    TableLike
        Concatenated quality table payload.
    """
    parts = [table for table in tables if table.num_rows]
    if not parts:
        return empty_quality_table()
    return pa.concat_tables(parts)


__all__ = [
    "QUALITY_SCHEMA",
    "QualityPlanSpec",
    "concat_quality_tables",
    "empty_quality_table",
    "quality_from_ids",
    "quality_issue_rows",
    "record_quality_issue_counts",
]
