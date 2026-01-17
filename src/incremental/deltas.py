"""Delta utilities for incremental export changes."""

from __future__ import annotations

from typing import cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import align_table
from incremental.registry_specs import dataset_schema


def compute_changed_exports(
    *,
    backend: BaseBackend,
    prev_exports: str | None,
    curr_exports: TableLike,
    changed_files: TableLike,
) -> pa.Table:
    """Compute export deltas for changed files.

    Returns
    -------
    pa.Table
        Export delta table with added/removed rows.
    """
    prev: ibis.Table
    if prev_exports is None:
        empty = table_from_arrays(dataset_schema("dim_exported_defs_v1"), columns={}, num_rows=0)
        prev = ibis.memtable(empty)
    else:
        prev = backend.read_parquet(prev_exports)
    curr = ibis.memtable(curr_exports)
    changed = ibis.memtable(changed_files)

    prev_f = prev.inner_join(changed, predicates=[prev.file_id == changed.file_id])
    curr_f = curr.inner_join(changed, predicates=[curr.file_id == changed.file_id])

    key_cols = ["file_id", "qname_id"]
    if "symbol" in prev.columns and "symbol" in curr.columns:
        key_cols.append("symbol")

    predicates = [curr_f[col] == prev_f[col] for col in key_cols]
    added: ibis.Table = curr_f.anti_join(prev_f, predicates=predicates).mutate(
        delta_kind=ibis.literal("added")
    )
    removed: ibis.Table = prev_f.anti_join(curr_f, predicates=predicates).mutate(
        delta_kind=ibis.literal("removed")
    )
    out: ibis.Table = ibis.union(added, removed, distinct=False)
    output_cols = ["delta_kind", "file_id", "path", "qname_id", "qname", "symbol"]
    select_exprs = [
        out[col] if col in out.columns else ibis.literal(None).name(col) for col in output_cols
    ]
    result = cast("pa.Table", out.select(*select_exprs).to_pyarrow())
    schema = dataset_schema("inc_changed_exports_v1")
    return align_table(result, schema=schema, safe_cast=True)


__all__ = ["compute_changed_exports"]
