"""Delta utilities for incremental export changes."""

from __future__ import annotations

from typing import TYPE_CHECKING

import ibis
import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import align_table
from incremental.ibis_exec import ibis_expr_to_table
from incremental.ibis_utils import ibis_table_from_arrow
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime

if TYPE_CHECKING:
    from ibis.backends import BaseBackend
    from ibis.expr.types import Value


def compute_changed_exports(
    *,
    runtime: IncrementalRuntime,
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
    backend = runtime.ibis_backend()
    prev = _load_prev_exports(backend, prev_exports)
    curr = ibis_table_from_arrow(backend, curr_exports)
    changed = ibis_table_from_arrow(backend, changed_files)
    out = _export_delta_expr(curr, prev, changed)
    select_exprs = _select_delta_columns(out)
    result = ibis_expr_to_table(
        out.select(*select_exprs),
        runtime=runtime,
        name="changed_exports",
    )
    schema = dataset_schema("inc_changed_exports_v1")
    return align_table(result, schema=schema, safe_cast=True)


def _load_prev_exports(backend: BaseBackend, prev_exports: str | None) -> ibis.Table:
    if prev_exports is None:
        empty = table_from_arrays(dataset_schema("dim_exported_defs_v1"), columns={}, num_rows=0)
        return ibis_table_from_arrow(backend, empty)
    return backend.read_delta(prev_exports)


def _export_delta_expr(
    curr: ibis.Table,
    prev: ibis.Table,
    changed: ibis.Table,
) -> ibis.Table:
    prev_f = prev.inner_join(changed, predicates=[prev.file_id == changed.file_id])
    curr_f = curr.inner_join(changed, predicates=[curr.file_id == changed.file_id])
    key_cols = _export_key_columns(prev, curr)
    predicates = [curr_f[col] == prev_f[col] for col in key_cols]
    added = curr_f.anti_join(prev_f, predicates=predicates).mutate(delta_kind=ibis.literal("added"))
    removed = prev_f.anti_join(curr_f, predicates=predicates).mutate(
        delta_kind=ibis.literal("removed")
    )
    return ibis.union(added, removed, distinct=False)


def _export_key_columns(prev: ibis.Table, curr: ibis.Table) -> list[str]:
    key_cols = ["file_id", "qname_id"]
    if "symbol" in prev.columns and "symbol" in curr.columns:
        key_cols.append("symbol")
    return key_cols


def _select_delta_columns(out: ibis.Table) -> list[Value]:
    output_cols = ["delta_kind", "file_id", "path", "qname_id", "qname", "symbol"]
    return [out[col] if col in out.columns else ibis.literal(None).name(col) for col in output_cols]


__all__ = ["compute_changed_exports"]
