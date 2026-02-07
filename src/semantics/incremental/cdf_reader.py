"""CDF reader for semantic pipeline incremental processing.

This module provides a CDF (Change Data Feed) reader that integrates with
the semantic pipeline's cursor-based version tracking. It wraps the low-level
Delta CDF infrastructure to provide a higher-level interface suitable for
incremental relationship recomputation.

The reader supports both explicit version ranges and cursor-based version
tracking, allowing incremental reads to automatically resume from the last
processed version.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.delta.service import delta_service_for_profile
from semantics.incremental.cdf_cursors import CdfCursorStore
from storage.deltalake import DeltaCdfOptions, StorageOptions

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.delta.service import DeltaService
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

    ArrowToDataFrame = Callable[[SessionContext, TableLike], DataFrame]


DeltaVersionFn = Callable[..., int | None]
DeltaCdfEnabledFn = Callable[..., bool]
CdfTableReader = Callable[..., TableLike]


@dataclass(frozen=True)
class CdfReadResult:
    """Result of reading CDF changes from a Delta table.

    Encapsulates the CDF DataFrame along with version metadata that can be
    used to update cursors after successful processing. The ``has_changes``
    flag indicates whether any changes were found in the version range.

    Attributes:
    ----------
    df
        DataFrame containing the CDF changes. May be empty if no changes
        exist in the version range.
    start_version
        Starting Delta version (inclusive) of the CDF read.
    end_version
        Ending Delta version (inclusive) of the CDF read.
    has_changes
        Whether any changes were found. True if the DataFrame contains
        rows, False if empty.
    """

    df: DataFrame
    start_version: int
    end_version: int
    has_changes: bool


@dataclass(frozen=True)
class CdfReadOptions:
    """Options for reading CDF changes from a Delta table.

    Groups optional parameters for CDF reads to simplify the function
    signature and provide a reusable configuration object.

    Attributes:
    ----------
    start_version
        Optional starting version (inclusive). If not provided and cursor
        tracking is enabled, uses the cursor's ``last_version + 1``.
    end_version
        Optional ending version (inclusive). Defaults to latest.
    cursor_store
        Optional cursor store for version tracking.
    dataset_name
        Dataset name for cursor lookup. Required if ``cursor_store`` is set.
    runtime_profile
        Optional runtime profile used to resolve Delta store policy defaults.
    storage_options
        Optional storage options for Delta table access.
    log_storage_options
        Optional log-store options for Delta table access.
    columns
        Optional list of columns to select from the CDF data.
    predicate
        Optional SQL predicate to filter CDF rows.
    delta_table_version_fn
        Optional override for Delta table version lookup (for testing).
    delta_cdf_enabled_fn
        Optional override for CDF enabled check (for testing).
    read_delta_cdf_fn
        Optional override for Delta CDF read (for testing).
    arrow_table_to_dataframe_fn
        Optional override for Arrow-to-DataFrame conversion (for testing).
    """

    start_version: int | None = None
    end_version: int | None = None
    cursor_store: CdfCursorStore | None = None
    dataset_name: str | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    columns: list[str] | None = None
    predicate: str | None = None
    delta_table_version_fn: DeltaVersionFn | None = None
    delta_cdf_enabled_fn: DeltaCdfEnabledFn | None = None
    read_delta_cdf_fn: CdfTableReader | None = None
    arrow_table_to_dataframe_fn: ArrowToDataFrame | None = None


def _validate_cdf_options(opts: CdfReadOptions) -> None:
    if opts.cursor_store is not None and opts.dataset_name is None:
        msg = "dataset_name is required when cursor_store is provided"
        raise ValueError(msg)


def _resolve_cdf_callbacks(
    opts: CdfReadOptions,
    *,
    service: DeltaService,
) -> tuple[DeltaVersionFn, DeltaCdfEnabledFn, CdfTableReader, ArrowToDataFrame]:
    def _table_version(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> int | None:
        return service.table_version(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    def _cdf_enabled(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        return service.cdf_enabled(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    def _read_cdf(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> TableLike:
        return service.read_cdf_eager(
            table_path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            cdf_options=cdf_options,
        )

    table_version_fn = opts.delta_table_version_fn or _table_version
    cdf_enabled_fn = opts.delta_cdf_enabled_fn or _cdf_enabled
    read_delta_fn = opts.read_delta_cdf_fn or _read_cdf
    arrow_to_df = opts.arrow_table_to_dataframe_fn or _arrow_table_to_dataframe
    return table_version_fn, cdf_enabled_fn, read_delta_fn, arrow_to_df


def _resolve_cdf_version_range(
    path: str,
    opts: CdfReadOptions,
    *,
    table_version_fn: DeltaVersionFn,
    cdf_enabled_fn: DeltaCdfEnabledFn,
) -> tuple[int, int] | None:
    if not _table_exists(
        path,
        opts.storage_options,
        opts.log_storage_options,
        table_version_fn=table_version_fn,
    ):
        return None
    if not cdf_enabled_fn(
        path,
        storage_options=opts.storage_options,
        log_storage_options=opts.log_storage_options,
    ):
        return None
    current_version = table_version_fn(
        path,
        storage_options=opts.storage_options,
        log_storage_options=opts.log_storage_options,
    )
    if current_version is None:
        return None
    resolved_start = _resolve_start_version(
        start_version=opts.start_version,
        cursor_store=opts.cursor_store,
        dataset_name=opts.dataset_name,
    )
    resolved_end = opts.end_version if opts.end_version is not None else current_version
    if resolved_start > resolved_end:
        return None
    return resolved_start, resolved_end


def _safe_read_cdf(
    path: str,
    opts: CdfReadOptions,
    *,
    read_delta_fn: CdfTableReader,
    cdf_options: DeltaCdfOptions,
) -> TableLike | None:
    def _fallback_load_cdf() -> TableLike | None:
        from deltalake import DeltaTable

        from datafusion_engine.arrow.coercion import coerce_table_to_storage, to_arrow_table

        storage_options = dict(opts.storage_options) if opts.storage_options else None
        table = DeltaTable(path, storage_options=storage_options)
        reader = table.load_cdf(
            starting_version=cdf_options.starting_version,
            ending_version=cdf_options.ending_version,
            starting_timestamp=cdf_options.starting_timestamp,
            ending_timestamp=cdf_options.ending_timestamp,
            columns=cdf_options.columns,
            predicate=cdf_options.predicate,
            allow_out_of_range=cdf_options.allow_out_of_range,
        )
        return coerce_table_to_storage(to_arrow_table(reader.read_all()))

    try:
        return read_delta_fn(
            path,
            storage_options=opts.storage_options,
            log_storage_options=opts.log_storage_options,
            cdf_options=cdf_options,
        )
    except ValueError:
        try:
            return _fallback_load_cdf()
        except (RuntimeError, TypeError, ValueError):
            return None


def read_cdf_changes(
    ctx: SessionContext,
    table_path: str | Path,
    options: CdfReadOptions | None = None,
) -> CdfReadResult | None:
    """Read CDF changes from a Delta table.

    Read change data feed entries from a Delta table, optionally using
    cursor-based version tracking for incremental processing. The function
    automatically resolves the version range based on the provided options
    and cursor state.

    Parameters
    ----------
    ctx
        DataFusion session context. Used to read the CDF data into a
        DataFrame for downstream processing.
    table_path
        Path to the Delta table. Can be a string or Path object.
    options
        Optional read options including version range, cursor tracking,
        storage options, and column/predicate filters. If not provided,
        defaults are used (start from version 0, read to latest).
        Validation may raise ``ValueError`` if ``cursor_store`` is set
        without ``dataset_name``.

    Returns:
    -------
    CdfReadResult | None
        Result containing the CDF DataFrame and version info, or None if:

        - The table does not exist or is not a valid Delta table
        - CDF is not enabled on the table
        - The table version could not be determined
        - The start version is greater than the end version (no changes)

    Examples:
    --------
    Read all CDF changes from version 0 to latest:

    >>> result = read_cdf_changes(ctx, "/path/to/delta/table")
    >>> if result and result.has_changes:
    ...     process_changes(result.df)

    Read CDF changes using cursor-based tracking:

    >>> from semantics.incremental.cdf_cursors import CdfCursorStore
    >>> store = CdfCursorStore(cursors_path=Path("/tmp/cursors"))
    >>> options = CdfReadOptions(cursor_store=store, dataset_name="my_dataset")
    >>> result = read_cdf_changes(ctx, "/path/to/delta/table", options)
    >>> if result and result.has_changes:
    ...     process_changes(result.df)

    Read specific version range:

    >>> options = CdfReadOptions(start_version=5, end_version=10)
    >>> result = read_cdf_changes(ctx, "/path/to/delta/table", options)
    """
    opts = options or CdfReadOptions()
    _validate_cdf_options(opts)

    path_str = str(table_path)
    service = delta_service_for_profile(opts.runtime_profile)
    table_version_fn, cdf_enabled_fn, read_delta_fn, arrow_to_df = _resolve_cdf_callbacks(
        opts,
        service=service,
    )
    version_range = _resolve_cdf_version_range(
        path_str,
        opts,
        table_version_fn=table_version_fn,
        cdf_enabled_fn=cdf_enabled_fn,
    )
    if version_range is None:
        return None
    resolved_start, resolved_end = version_range

    # Build CDF options
    cdf_options = DeltaCdfOptions(
        starting_version=resolved_start,
        ending_version=resolved_end,
        columns=opts.columns,
        predicate=opts.predicate,
        allow_out_of_range=False,
    )

    # Read CDF data
    arrow_table = _safe_read_cdf(
        path_str,
        opts,
        read_delta_fn=read_delta_fn,
        cdf_options=cdf_options,
    )
    if arrow_table is None:
        return None

    # Convert Arrow table to DataFrame via session context
    df = arrow_to_df(ctx, arrow_table)

    has_changes = arrow_table.num_rows > 0
    if opts.cursor_store is not None and opts.dataset_name is not None:
        opts.cursor_store.update_version(opts.dataset_name, resolved_end)

    return CdfReadResult(
        df=df,
        start_version=resolved_start,
        end_version=resolved_end,
        has_changes=has_changes,
    )


def _table_exists(
    path: str,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
    *,
    table_version_fn: DeltaVersionFn,
) -> bool:
    """Check if a Delta table exists at the given path.

    Parameters
    ----------
    path
        Path to the Delta table.
    storage_options
        Optional storage options.
    log_storage_options
        Optional log storage options.
    table_version_fn
        Callable that returns the Delta table version if available.

    Returns:
    -------
    bool
        True if the table exists and is a valid Delta table.
    """
    # Use delta_table_version as existence check - returns None if not a table
    version = table_version_fn(
        path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return version is not None


def _resolve_start_version(
    *,
    start_version: int | None,
    cursor_store: CdfCursorStore | None,
    dataset_name: str | None,
) -> int:
    """Resolve the starting version for CDF read.

    Priority order:
    1. Explicit start_version if provided
    2. Cursor's last_version + 1 if cursor exists
    3. Default to 0 (full history)

    Parameters
    ----------
    start_version
        Explicitly provided start version.
    cursor_store
        Optional cursor store for lookup.
    dataset_name
        Dataset name for cursor lookup.

    Returns:
    -------
    int
        Resolved starting version.
    """
    if start_version is not None:
        return start_version

    if cursor_store is not None and dataset_name is not None:
        cursor_start = cursor_store.get_start_version(dataset_name)
        if cursor_start is not None:
            return cursor_start

    return 0


def _arrow_table_to_dataframe(
    ctx: SessionContext,
    table: object,
) -> DataFrame:
    """Convert an Arrow table to a DataFusion DataFrame.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table
        Arrow table to convert.

    Returns:
    -------
    DataFrame
        DataFusion DataFrame containing the table data.
    """
    from datafusion_engine.session.helpers import deregister_table, register_temp_table

    # Register as temporary table and read back as DataFrame
    temp_name = register_temp_table(ctx, table, prefix="__cdf_temp_")
    df = ctx.table(temp_name)

    # Deregister the temporary table after creating the DataFrame
    # The DataFrame holds a reference to the data, so deregistration is safe
    deregister_table(ctx, temp_name)

    return df


__all__ = ["CdfReadOptions", "CdfReadResult", "read_cdf_changes"]
