"""CDF runtime helpers for incremental pipelines."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from deltalake import DeltaTable

from datafusion_engine.registry_bridge import DeltaCdfRegistrationOptions, register_delta_cdf_df
from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.ibis_exec import ibis_expr_to_table
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from storage.deltalake import DeltaCdfOptions

if TYPE_CHECKING:
    from ibis.expr.types import BooleanValue


@dataclass(frozen=True)
class CdfReadResult:
    """Result for a Delta CDF read."""

    table: pa.Table
    updated_version: int


def read_cdf_changes(
    runtime: IncrementalRuntime,
    *,
    dataset_path: str,
    dataset_name: str,
    cursor_store: CdfCursorStore,
    filter_policy: CdfFilterPolicy | None = None,
) -> CdfReadResult | None:
    """Read Delta CDF changes using the shared incremental runtime.

    Returns
    -------
    CdfReadResult | None
        CDF read result when changes are available, otherwise ``None``.
    """
    path = Path(dataset_path)
    if not path.exists():
        return None
    if not DeltaTable.is_deltatable(str(path)):
        return None
    cursor = cursor_store.load_cursor(dataset_name)
    dt = DeltaTable(str(path))
    current_version = dt.version()
    if cursor is None:
        cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))
        return None
    if cursor.last_version >= current_version:
        return None
    starting_version = cursor.last_version + 1
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
    )
    ctx = runtime.session_context()
    with TempTableRegistry(ctx) as registry:
        cdf_name = f"__cdf_{uuid.uuid4().hex}"
        try:
            _ = register_delta_cdf_df(
                ctx,
                name=cdf_name,
                path=str(path),
                options=DeltaCdfRegistrationOptions(
                    cdf_options=cdf_options,
                    storage_options=None,
                    runtime_profile=runtime.profile,
                ),
            )
        except ValueError:
            return None
        registry.track(cdf_name)
        backend = runtime.ibis_backend()
        expr = backend.table(cdf_name)
        predicate = (filter_policy or CdfFilterPolicy.include_all()).to_ibis_predicate(expr)
        if predicate is not None:
            expr = expr.filter(cast("BooleanValue", predicate))
        table = ibis_expr_to_table(expr, runtime=runtime, name="cdf_changes")
    cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))
    return CdfReadResult(table=table, updated_version=current_version)


__all__ = ["CdfReadResult", "read_cdf_changes"]
