"""Canonical CDF read orchestration shared across incremental modules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from datafusion_engine.arrow.coercion import coerce_table_to_storage, to_arrow_table
from semantics.incremental.delta_port import DeltaCdfPort
from storage.deltalake import DeltaCdfOptions, StorageOptions


@dataclass(frozen=True)
class CanonicalCdfReadRequest:
    """Canonical input contract for CDF table reads."""

    table_path: str | Path
    start_version: int
    end_version: int | None = None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    columns: list[str] | None = None
    predicate: str | None = None
    allow_out_of_range: bool = False


@dataclass(frozen=True)
class CanonicalCdfReadResult:
    """Canonical CDF read result in pyarrow-table form."""

    table: pa.Table
    start_version: int
    end_version: int
    has_changes: bool


def _resolve_version_range(
    request: CanonicalCdfReadRequest,
    *,
    port: DeltaCdfPort,
) -> tuple[int, int] | None:
    path_str = str(request.table_path)
    if not port.cdf_enabled(
        path_str,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    ):
        return None
    current_version = port.table_version(
        path_str,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    if current_version is None:
        return None
    resolved_end = request.end_version if request.end_version is not None else current_version
    if request.start_version > resolved_end:
        return None
    return request.start_version, resolved_end


def _safe_read_cdf(
    request: CanonicalCdfReadRequest,
    *,
    cdf_options: DeltaCdfOptions,
    port: DeltaCdfPort,
) -> pa.Table | None:
    """Read CDF table with fallback for adapter-specific value-conversion errors.

    Returns:
        pa.Table | None: Canonical Arrow table when read succeeds, else ``None``.
    """
    path_str = str(request.table_path)
    try:
        raw_table = port.read_cdf(
            path_str,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
            cdf_options=cdf_options,
        )
        return coerce_table_to_storage(to_arrow_table(raw_table))
    except ValueError:
        try:
            return port.fallback_read_cdf(
                path_str,
                storage_options=request.storage_options,
                cdf_options=cdf_options,
            )
        except (RuntimeError, TypeError, ValueError):
            return None


def read_cdf_table(
    *,
    request: CanonicalCdfReadRequest,
    port: DeltaCdfPort,
) -> CanonicalCdfReadResult | None:
    """Read CDF rows for a canonical request.

    Returns:
        CanonicalCdfReadResult | None: Resolved CDF payload and version metadata.
    """
    version_range = _resolve_version_range(
        request,
        port=port,
    )
    if version_range is None:
        return None
    start_version, end_version = version_range
    cdf_options = DeltaCdfOptions(
        starting_version=start_version,
        ending_version=end_version,
        columns=request.columns,
        predicate=request.predicate,
        allow_out_of_range=request.allow_out_of_range,
    )
    table = _safe_read_cdf(request, cdf_options=cdf_options, port=port)
    if table is None:
        return None
    return CanonicalCdfReadResult(
        table=table,
        start_version=start_version,
        end_version=end_version,
        has_changes=table.num_rows > 0,
    )


__all__ = [
    "CanonicalCdfReadRequest",
    "CanonicalCdfReadResult",
    "read_cdf_table",
]
