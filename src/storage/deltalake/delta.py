"""Delta Lake read/write helpers for Arrow tables."""

from __future__ import annotations

import importlib
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake
from deltalake._internal import Transaction
from deltalake.exceptions import CommitFailedError, DeltaError

from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.schema.encoding_policy import EncodingPolicy, apply_encoding
from arrowdsl.schema.schema import SchemaTransform
from datafusion_engine.bridge import DeltaInsertOptions, datafusion_insert_from_dataframe
from datafusion_engine.listing_table_provider import TableProviderCapsule
from storage.deltalake.config import (
    DeltaSchemaPolicy,
    DeltaWritePolicy,
    delta_schema_configuration,
    delta_write_configuration,
)

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.runtime import DataFusionRuntimeProfile

type DeltaWriteMode = Literal["error", "append", "overwrite", "ignore"]
type DeltaSchemaMode = Literal["merge", "overwrite"]
type DeltaWriteInput = TableLike | RecordBatchReaderLike
type StorageOptions = Mapping[str, str]

DEFAULT_DELTA_FEATURE_PROPERTIES: dict[str, str] = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableRowTracking": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableInCommitTimestamps": "true",
}


@dataclass(frozen=True)
class DeltaWriteOptions:
    """Options for Delta Lake writes."""

    mode: DeltaWriteMode = "append"
    schema_mode: DeltaSchemaMode | None = None
    predicate: str | None = None
    partition_by: Sequence[str] | None = None
    configuration: Mapping[str, str | None] | None = None
    commit_metadata: Mapping[str, str] | None = None
    target_file_size: int | None = None
    writer_properties: WriterProperties | None = None
    retry_policy: DeltaWriteRetryPolicy | None = None


@dataclass(frozen=True)
class DeltaWriteRetryPolicy:
    """Retry settings for Delta write commits."""

    max_attempts: int = 3
    backoff_seconds: float = 0.5

    def __post_init__(self) -> None:
        """Validate retry policy values.

        Raises
        ------
        ValueError
            Raised when attempts or backoff are out of bounds.
        """
        if self.max_attempts < 1:
            msg = "DeltaWriteRetryPolicy.max_attempts must be >= 1."
            raise ValueError(msg)
        if self.backoff_seconds < 0:
            msg = "DeltaWriteRetryPolicy.backoff_seconds must be >= 0."
            raise ValueError(msg)


@dataclass(frozen=True)
class IdempotentWriteOptions:
    """Options for idempotent Delta writes.

    Uses Delta Lake's CommitProperties to tag commits with app_id and version,
    enabling safe retries where duplicate commits are ignored.

    Attributes
    ----------
    app_id : str
        Unique application/pipeline identifier (e.g., run_id).
    version : int
        Commit sequence number for this app_id.
    """

    app_id: str
    version: int


@dataclass(frozen=True)
class DeltaWriteResult:
    """Result payload for Delta Lake writes."""

    path: str
    version: int | None


@dataclass(frozen=True)
class DeltaUpsertOptions:
    """Options for Delta partition upserts."""

    base_dir: str
    partition_cols: Sequence[str]
    delete_partitions: Sequence[Mapping[str, str]] = ()
    options: DeltaWriteOptions | None = None
    storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class DeltaCdfOptions:
    """Options for reading Delta change data feeds."""

    starting_version: int = 0
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None
    columns: list[str] | None = None
    predicate: str | None = None
    allow_out_of_range: bool = False


@dataclass(frozen=True)
class DeltaVacuumOptions:
    """Options for Delta vacuum maintenance."""

    retention_hours: int | None = None
    dry_run: bool = True
    enforce_retention_duration: bool = True
    full: bool = False
    keep_versions: Sequence[int] | None = None
    commit_metadata: Mapping[str, str] | None = None


def open_delta_table(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    version: int | None = None,
    timestamp: str | None = None,
) -> DeltaTable:
    """Open a DeltaTable with optional time travel.

    Returns
    -------
    deltalake.DeltaTable
        Delta table instance for the path.

    Raises
    ------
    ValueError
        Raised when both version and timestamp are provided.
    """
    if version is not None and timestamp is not None:
        msg = "Specify either version or timestamp, not both."
        raise ValueError(msg)
    storage = _storage_dict(storage_options)
    table = DeltaTable(path, storage_options=storage)
    if version is not None:
        table.load_as_version(version)
    elif timestamp is not None:
        table.load_as_version(timestamp)
    return table


def write_table_delta(
    table: DeltaWriteInput,
    path: str,
    *,
    options: DeltaWriteOptions,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    """Write a table or reader into a Delta table path.

    Supports streaming writes when passed a RecordBatchReader - the data
    is streamed directly to Delta without materializing in memory.

    Returns
    -------
    DeltaWriteResult
        Delta write result with table version.

    Raises
    ------
    ValueError
        Raised when a predicate is supplied without overwrite mode.
    """
    # Check if DataFusion INSERT path is available (requires materialized table)
    if _supports_datafusion_delta_insert(options):
        existing_version = delta_table_version(path, storage_options=storage_options)
        if existing_version is not None:
            # DataFusion path needs materialized table for registration
            data = _coerce_write_input(table)
            return _write_table_delta_datafusion(
                data,
                path=path,
                options=options,
                storage_options=storage_options,
            )

    # Streaming path - pass input directly to write_deltalake
    arrow_data = cast(
        "ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable]",
        table,
    )
    storage = _storage_dict(storage_options)
    partition_by = list(options.partition_by) if options.partition_by else None
    predicate = None
    if options.mode == "overwrite":
        predicate = options.predicate
    elif options.predicate is not None:
        msg = "Predicate filters require overwrite mode for Delta writes."
        raise ValueError(msg)
    invocation = _DeltaWriteInvocation(
        storage=storage,
        partition_by=partition_by,
        predicate=predicate,
        commit_metadata=options.commit_metadata,
    )
    _write_deltalake_with_retry(
        path,
        arrow_data,
        options=options,
        invocation=invocation,
    )
    return DeltaWriteResult(path=path, version=delta_table_version(path, storage_options=storage))


def _commit_properties_for_idempotent(
    options: DeltaWriteOptions,
    *,
    idempotent: IdempotentWriteOptions | None,
) -> CommitProperties | None:
    if idempotent is not None:
        app_transaction = Transaction(
            app_id=idempotent.app_id,
            version=idempotent.version,
        )
        return CommitProperties(
            app_transactions=[app_transaction],
            custom_metadata=dict(options.commit_metadata) if options.commit_metadata else None,
        )
    if options.commit_metadata:
        return CommitProperties(custom_metadata=dict(options.commit_metadata))
    return None


def _is_duplicate_commit_error(exc: CommitFailedError) -> bool:
    """Check if the error indicates a duplicate idempotent commit.

    Delta Lake returns a specific error when attempting to write
    with an (app_id, version) that already exists.

    Returns
    -------
    bool
        True when the error matches a duplicate commit failure.
    """
    error_msg = str(exc).lower()
    # Delta returns errors containing "transaction" and "already committed"
    return "already committed" in error_msg or "duplicate" in error_msg


@dataclass(frozen=True)
class _IdempotentWriteContext:
    options: DeltaWriteOptions
    partition_by: list[str] | None
    predicate: str | None
    commit_properties: CommitProperties | None
    storage: dict[str, str] | None
    retry: DeltaWriteRetryPolicy
    idempotent: IdempotentWriteOptions | None


def _write_deltalake_idempotent_with_retry(
    path: str,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    context: _IdempotentWriteContext,
) -> None:
    last_error: Exception | None = None
    for attempt in range(context.retry.max_attempts):
        try:
            _write_deltalake_idempotent_once(
                path,
                data,
                context=context,
            )
        except CommitFailedError as exc:
            if context.idempotent is not None and _is_duplicate_commit_error(exc):
                return
            last_error = exc
        except DeltaError as exc:
            last_error = exc
        else:
            return
        if attempt < context.retry.max_attempts - 1:
            time.sleep(context.retry.backoff_seconds * (2**attempt))
    if last_error is not None:
        raise last_error


def write_deltalake_idempotent(
    path: str,
    data: DeltaWriteInput,
    *,
    options: DeltaWriteOptions,
    idempotent: IdempotentWriteOptions | None = None,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    """Write to Delta with idempotent commit properties.

    When idempotent options are provided, the commit is tagged with app_id
    and version. Delta Lake will reject duplicate commits with the same
    app_id + version, making retries safe.

    Supports streaming writes when passed a RecordBatchReader - the data
    is streamed directly to Delta without materializing in memory.

    Parameters
    ----------
    path : str
        Delta table path.
    data : DeltaWriteInput
        Data to write (Table or RecordBatchReader).
    options : DeltaWriteOptions
        Standard Delta write options.
    idempotent : IdempotentWriteOptions | None
        Optional idempotent commit options.
    storage_options : StorageOptions | None
        Storage backend options.

    Returns
    -------
    DeltaWriteResult
        Write result with path and version.

    Raises
    ------
    ValueError
        Raised when a predicate is supplied without overwrite mode.

    Notes
    -----
    When CommitProperties with app_id + version are used:

    - First write with (app_id, version) succeeds normally
    - Retry with same (app_id, version) is silently skipped
    - This enables safe "at-least-once" retry patterns
    """
    # Streaming path - pass input directly to write_deltalake
    arrow_data = cast(
        "ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable]",
        data,
    )
    storage = _storage_dict(storage_options)
    partition_by = list(options.partition_by) if options.partition_by else None
    commit_properties = _commit_properties_for_idempotent(options, idempotent=idempotent)
    predicate = None
    if options.mode == "overwrite":
        predicate = options.predicate
    elif options.predicate is not None:
        msg = "Predicate filters require overwrite mode for Delta writes."
        raise ValueError(msg)
    retry = options.retry_policy or DeltaWriteRetryPolicy()
    context = _IdempotentWriteContext(
        options=options,
        partition_by=partition_by,
        predicate=predicate,
        commit_properties=commit_properties,
        storage=storage,
        retry=retry,
        idempotent=idempotent,
    )

    _write_deltalake_idempotent_with_retry(
        path,
        arrow_data,
        context=context,
    )

    return DeltaWriteResult(
        path=path,
        version=delta_table_version(path, storage_options=storage_options),
    )


def write_table_delta_idempotent(
    table: DeltaWriteInput,
    path: str,
    *,
    options: DeltaWriteOptions,
    idempotent: IdempotentWriteOptions,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    """Write Delta table with idempotent commit properties.

    Parameters
    ----------
    table : DeltaWriteInput
        Data to write.
    path : str
        Delta table path.
    options : DeltaWriteOptions
        Write options.
    idempotent : IdempotentWriteOptions
        Idempotent commit options containing app_id and version.
    storage_options : StorageOptions | None
        Storage backend options.

    Returns
    -------
    DeltaWriteResult
        Write result.
    """
    return write_deltalake_idempotent(
        path,
        table,
        options=options,
        idempotent=idempotent,
        storage_options=storage_options,
    )


def _write_table_delta_datafusion(
    table: TableLike,
    *,
    path: str,
    options: DeltaWriteOptions,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    ctx = DataFusionRuntimeProfile().session_context()
    arrow_data = cast("ArrowArrayExportable | ArrowStreamExportable", table)
    df = ctx.from_arrow(arrow_data)
    return write_datafusion_delta(
        df,
        base_dir=path,
        options=options,
        storage_options=storage_options,
    )


def write_dataset_delta(
    table: DeltaWriteInput,
    base_dir: str,
    *,
    options: DeltaWriteOptions,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    """Write a Delta table into a dataset directory.

    Returns
    -------
    DeltaWriteResult
        Delta write result with table version.
    """
    return write_table_delta(
        table,
        base_dir,
        options=options,
        storage_options=storage_options,
    )


def write_named_datasets_delta(
    datasets: Mapping[str, DeltaWriteInput],
    base_dir: str,
    *,
    options: DeltaWriteOptions,
    storage_options: StorageOptions | None = None,
) -> dict[str, DeltaWriteResult]:
    """Write named datasets into per-dataset Delta tables.

    Returns
    -------
    dict[str, DeltaWriteResult]
        Mapping of dataset names to Delta write results.
    """
    results: dict[str, DeltaWriteResult] = {}
    for name, table in datasets.items():
        path = f"{base_dir.rstrip('/')}/{name}"
        results[name] = write_table_delta(
            table,
            path,
            options=options,
            storage_options=storage_options,
        )
    return results


def write_datafusion_delta(
    df: object,
    *,
    base_dir: str,
    options: DeltaWriteOptions,
    storage_options: StorageOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> DeltaWriteResult:
    """Insert data into an existing Delta table using DataFusion.

    Returns
    -------
    DeltaWriteResult
        Delta write metadata when DataFusion insert succeeds.

    Raises
    ------
    ValueError
        Raised when the insert configuration is unsupported or the table is missing.
    """
    if not _supports_datafusion_delta_insert(options):
        msg = (
            "DataFusion Delta inserts only support append/overwrite without schema or commit "
            "options."
        )
        raise ValueError(msg)
    if delta_table_version(base_dir, storage_options=storage_options) is None:
        msg = "DataFusion Delta insert requires an existing Delta table."
        raise ValueError(msg)
    ctx = _datafusion_ctx_from_df(df)
    provider = _delta_table_provider_from_session(
        ctx,
        path=base_dir,
        storage_options=storage_options,
    )
    name = f"__delta_sink_{uuid.uuid4().hex}"
    insert_mode = cast("Literal['append', 'overwrite']", options.mode)
    try:
        _register_delta_table_provider(ctx, name=name, provider=provider)
        insert_result = datafusion_insert_from_dataframe(
            ctx,
            name,
            cast("DataFrame", df),
            options=DeltaInsertOptions(mode=insert_mode),
        )
    finally:
        _deregister_datafusion_table(ctx, name=name)
    _record_delta_write_artifact(
        runtime_profile,
        artifact=_DeltaWriteArtifact(
            name=name,
            path=base_dir,
            mode=options.mode,
            provider=provider,
            rows_affected=insert_result.rows_affected,
        ),
    )
    version = delta_table_version(base_dir, storage_options=storage_options)
    return DeltaWriteResult(path=base_dir, version=version)


def upsert_dataset_partitions_delta(
    table: DeltaWriteInput,
    *,
    options: DeltaUpsertOptions,
) -> DeltaWriteResult:
    """Upsert partitioned datasets by deleting partitions then appending.

    Returns
    -------
    DeltaWriteResult
        Delta write result with table version.
    """
    resolved = options.options or DeltaWriteOptions(mode="append")
    storage = _storage_dict(options.storage_options)
    if options.delete_partitions:
        predicate = _partition_predicate(options.delete_partitions)
        if predicate:
            table_ref = DeltaTable(options.base_dir, storage_options=storage)
            table_ref.delete(predicate)
    data = _coerce_write_input(table)
    if data.num_rows == 0:
        return DeltaWriteResult(
            path=options.base_dir,
            version=delta_table_version(options.base_dir, storage_options=storage),
        )
    return write_table_delta(
        data,
        options.base_dir,
        options=DeltaWriteOptions(
            mode=resolved.mode,
            schema_mode=resolved.schema_mode,
            predicate=resolved.predicate,
            partition_by=options.partition_cols,
            configuration=resolved.configuration,
            commit_metadata=resolved.commit_metadata,
            target_file_size=resolved.target_file_size,
            writer_properties=resolved.writer_properties,
            retry_policy=resolved.retry_policy,
        ),
        storage_options=storage,
    )


def write_finalize_result_delta(
    result: FinalizeResult,
    base_path: str,
    *,
    options: DeltaWriteOptions,
    storage_options: StorageOptions | None = None,
) -> dict[str, str]:
    """Write a finalized table plus error artifacts to Delta tables.

    Returns
    -------
    dict[str, str]
        Mapping of artifact names to Delta table paths.
    """
    data_result = write_table_delta(
        result.good,
        base_path,
        options=options,
        storage_options=storage_options,
    )
    errors_result = write_table_delta(
        result.errors,
        _artifact_path(base_path, "errors"),
        options=options,
        storage_options=storage_options,
    )
    stats_result = write_table_delta(
        result.stats,
        _artifact_path(base_path, "error_stats"),
        options=options,
        storage_options=storage_options,
    )
    alignment_result = write_table_delta(
        result.alignment,
        _artifact_path(base_path, "alignment"),
        options=options,
        storage_options=storage_options,
    )
    return {
        "data": data_result.path,
        "errors": errors_result.path,
        "stats": stats_result.path,
        "alignment": alignment_result.path,
    }


def delta_table_version(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
) -> int | None:
    """Return the latest Delta table version when the table exists.

    Returns
    -------
    int | None
        Latest Delta table version, or None if not a Delta table.
    """
    storage = _storage_dict(storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    return DeltaTable(path, storage_options=storage).version()


def delta_table_features(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
) -> dict[str, str] | None:
    """Return Delta table feature configuration values when present.

    Returns
    -------
    dict[str, str] | None
        Feature configuration values or ``None`` if no features are set.
    """
    storage = _storage_dict(storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    table = DeltaTable(path, storage_options=storage)
    metadata = table.metadata()
    configuration = metadata.configuration or {}
    features = {key: str(value) for key, value in configuration.items() if key.startswith("delta.")}
    protocol = table.protocol()
    if protocol.reader_features:
        features["reader_features"] = ",".join(protocol.reader_features)
    if protocol.writer_features:
        features["writer_features"] = ",".join(protocol.writer_features)
    return features or None


def delta_commit_metadata(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
) -> dict[str, str] | None:
    """Return custom commit metadata for the latest Delta table version.

    Returns
    -------
    dict[str, str] | None
        Custom commit metadata or ``None`` when not present.
    """
    storage = _storage_dict(storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    history = DeltaTable(path, storage_options=storage).history(1)
    if not history:
        return None
    record = history[0]
    reserved = {
        "timestamp",
        "operation",
        "operationParameters",
        "engineInfo",
        "clientVersion",
        "operationMetrics",
        "version",
    }
    custom = {key: str(value) for key, value in record.items() if key not in reserved}
    return custom or None


def delta_history_snapshot(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    limit: int = 1,
) -> dict[str, object] | None:
    """Return the latest Delta history entry.

    Returns
    -------
    dict[str, object] | None
        History entry payload or ``None`` when unavailable.
    """
    storage = _storage_dict(storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    history = DeltaTable(path, storage_options=storage).history(limit)
    if not history:
        return None
    return _delta_json_value(history[0])


def delta_protocol_snapshot(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
) -> dict[str, object] | None:
    """Return Delta protocol versions and active feature flags.

    Returns
    -------
    dict[str, object] | None
        Protocol payload or ``None`` when unavailable.
    """
    storage = _storage_dict(storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    protocol = DeltaTable(path, storage_options=storage).protocol()
    return {
        "min_reader_version": protocol.min_reader_version,
        "min_writer_version": protocol.min_writer_version,
        "reader_features": list(protocol.reader_features) if protocol.reader_features else None,
        "writer_features": list(protocol.writer_features) if protocol.writer_features else None,
    }


def enable_delta_features(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    features: Mapping[str, str] | None = None,
    commit_metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Enable Delta table features by setting table properties.

    Returns
    -------
    dict[str, str]
        Properties applied to the Delta table.
    """
    storage = _storage_dict(storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return {}
    resolved = features or DEFAULT_DELTA_FEATURE_PROPERTIES
    properties = {key: str(value) for key, value in resolved.items() if value is not None}
    if not properties:
        return {}
    table = DeltaTable(path, storage_options=storage)
    table.alter.set_table_properties(
        properties,
        raise_if_not_exists=False,
        commit_properties=_commit_properties(commit_metadata),
    )
    return properties


def apply_delta_write_policies(
    options: DeltaWriteOptions,
    *,
    write_policy: DeltaWritePolicy | None = None,
    schema_policy: DeltaSchemaPolicy | None = None,
) -> DeltaWriteOptions:
    """Merge Delta write and schema policies into write options.

    Returns
    -------
    DeltaWriteOptions
        Updated write options with policy-derived configuration.
    """
    configs = _merge_delta_configurations(
        delta_write_configuration(write_policy),
        delta_schema_configuration(schema_policy),
        options.configuration,
    )
    schema_mode = options.schema_mode
    if schema_mode is None and schema_policy is not None:
        schema_mode = schema_policy.schema_mode
    target_file_size = options.target_file_size
    if target_file_size is None and write_policy is not None:
        target_file_size = write_policy.target_file_size
    return replace(
        options,
        configuration=configs,
        schema_mode=schema_mode,
        target_file_size=target_file_size,
    )


def _merge_delta_configurations(
    *configs: Mapping[str, str | None] | None,
) -> Mapping[str, str | None] | None:
    merged: dict[str, str | None] = {}
    for config in configs:
        if not config:
            continue
        for key, value in config.items():
            if value is None:
                continue
            merged[key] = value
    return merged or None


def vacuum_delta(
    path: str,
    *,
    options: DeltaVacuumOptions | None = None,
    storage_options: StorageOptions | None = None,
) -> list[str]:
    """Run a Delta vacuum to remove stale files.

    Returns
    -------
    list[str]
        Files eligible for deletion (or removed when ``dry_run`` is False).
    """
    options = options or DeltaVacuumOptions()
    storage = _storage_dict(storage_options)
    table = DeltaTable(path, storage_options=storage)
    return table.vacuum(
        retention_hours=options.retention_hours,
        dry_run=options.dry_run,
        enforce_retention_duration=options.enforce_retention_duration,
        commit_properties=_commit_properties(options.commit_metadata),
        full=options.full,
        keep_versions=list(options.keep_versions) if options.keep_versions is not None else None,
    )


def create_delta_checkpoint(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
) -> None:
    """Create a checkpoint for a Delta table."""
    storage = _storage_dict(storage_options)
    DeltaTable(path, storage_options=storage).create_checkpoint()


def cleanup_delta_log(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
) -> None:
    """Delete expired Delta log files."""
    storage = _storage_dict(storage_options)
    DeltaTable(path, storage_options=storage).cleanup_metadata()


@dataclass(frozen=True)
class _DeltaWriteInvocation:
    storage: dict[str, str] | None
    partition_by: Sequence[str] | None
    predicate: str | None
    commit_metadata: Mapping[str, str] | None


def _commit_properties(metadata: Mapping[str, str] | None) -> CommitProperties | None:
    if metadata is None:
        return None
    return CommitProperties(custom_metadata=dict(metadata))


def _delta_json_value(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): _delta_json_scalar(item) for key, item in value.items()}
    return {"value": _delta_json_scalar(value)}


def _delta_json_scalar(value: object) -> object:
    if value is None or isinstance(value, (bool, float, int, str)):
        return value
    if isinstance(value, list):
        return [_delta_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_delta_json_scalar(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _delta_json_scalar(item) for key, item in value.items()}
    return str(value)


def _write_deltalake_with_retry(
    path: str,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    options: DeltaWriteOptions,
    invocation: _DeltaWriteInvocation,
) -> None:
    retry_policy = options.retry_policy
    if retry_policy is None:
        _write_deltalake_once(
            path,
            data,
            options=options,
            invocation=invocation,
        )
        return
    for attempt in range(1, retry_policy.max_attempts + 1):
        commit_metadata = _retry_commit_metadata(
            invocation.commit_metadata,
            attempt=attempt,
            policy=retry_policy,
        )
        retry_invocation = _DeltaWriteInvocation(
            storage=invocation.storage,
            partition_by=invocation.partition_by,
            predicate=invocation.predicate,
            commit_metadata=commit_metadata,
        )
        try:
            _write_deltalake_once(
                path,
                data,
                options=options,
                invocation=retry_invocation,
            )
        except (CommitFailedError, DeltaError) as exc:
            if not _is_retryable_delta_error(exc):
                raise
            if attempt >= retry_policy.max_attempts:
                raise
            time.sleep(retry_policy.backoff_seconds * attempt)
        else:
            return


def _write_deltalake_once(
    path: str,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    options: DeltaWriteOptions,
    invocation: _DeltaWriteInvocation,
) -> None:
    partition_by = list(invocation.partition_by) if invocation.partition_by else None
    commit_properties = _commit_properties(invocation.commit_metadata)
    if invocation.predicate is not None:
        overwrite_mode = cast("Literal['overwrite']", options.mode)
        if options.writer_properties is None:
            write_deltalake(
                path,
                data,
                mode=overwrite_mode,
                schema_mode=options.schema_mode,
                partition_by=partition_by,
                configuration=options.configuration,
                commit_properties=commit_properties,
                predicate=invocation.predicate,
                target_file_size=options.target_file_size,
                storage_options=invocation.storage,
            )
        else:
            write_deltalake(
                path,
                data,
                mode=overwrite_mode,
                schema_mode=options.schema_mode,
                partition_by=partition_by,
                configuration=options.configuration,
                commit_properties=commit_properties,
                predicate=invocation.predicate,
                target_file_size=options.target_file_size,
                writer_properties=options.writer_properties,
                storage_options=invocation.storage,
            )
        return
    if options.writer_properties is None:
        write_deltalake(
            path,
            data,
            mode=options.mode,
            schema_mode=options.schema_mode,
            partition_by=partition_by,
            configuration=options.configuration,
            commit_properties=commit_properties,
            target_file_size=options.target_file_size,
            storage_options=invocation.storage,
        )
        return
    write_deltalake(
        path,
        data,
        mode=options.mode,
        schema_mode=options.schema_mode,
        partition_by=partition_by,
        configuration=options.configuration,
        commit_properties=commit_properties,
        target_file_size=options.target_file_size,
        writer_properties=options.writer_properties,
        storage_options=invocation.storage,
    )


def _retry_commit_metadata(
    metadata: Mapping[str, str] | None,
    *,
    attempt: int,
    policy: DeltaWriteRetryPolicy,
) -> dict[str, str] | None:
    if metadata is None and policy.max_attempts <= 1:
        return metadata
    merged = dict(metadata) if metadata is not None else {}
    merged["delta_write_retry_attempt"] = str(attempt)
    merged["delta_write_retry_max_attempts"] = str(policy.max_attempts)
    merged["delta_write_retry_backoff_seconds"] = str(policy.backoff_seconds)
    return merged


def _is_retryable_delta_error(exc: Exception) -> bool:
    if isinstance(exc, CommitFailedError):
        return True
    if not isinstance(exc, DeltaError):
        return False
    msg = str(exc).lower()
    hints = ("concurrent", "conflict", "already exists", "transaction", "version")
    return any(hint in msg for hint in hints)


def _write_deltalake_idempotent_once(
    path: str,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    context: _IdempotentWriteContext,
) -> None:
    """Execute a single Delta write with idempotent commit properties."""
    if context.predicate is not None:
        overwrite_mode = cast("Literal['overwrite']", context.options.mode)
        if context.options.writer_properties is None:
            write_deltalake(
                path,
                data,
                mode=overwrite_mode,
                schema_mode=context.options.schema_mode,
                partition_by=context.partition_by,
                configuration=context.options.configuration,
                commit_properties=context.commit_properties,
                predicate=context.predicate,
                target_file_size=context.options.target_file_size,
                storage_options=context.storage,
            )
        else:
            write_deltalake(
                path,
                data,
                mode=overwrite_mode,
                schema_mode=context.options.schema_mode,
                partition_by=context.partition_by,
                configuration=context.options.configuration,
                commit_properties=context.commit_properties,
                predicate=context.predicate,
                target_file_size=context.options.target_file_size,
                writer_properties=context.options.writer_properties,
                storage_options=context.storage,
            )
        return
    if context.options.writer_properties is None:
        write_deltalake(
            path,
            data,
            mode=context.options.mode,
            schema_mode=context.options.schema_mode,
            partition_by=context.partition_by,
            configuration=context.options.configuration,
            commit_properties=context.commit_properties,
            target_file_size=context.options.target_file_size,
            storage_options=context.storage,
        )
        return
    write_deltalake(
        path,
        data,
        mode=context.options.mode,
        schema_mode=context.options.schema_mode,
        partition_by=context.partition_by,
        configuration=context.options.configuration,
        commit_properties=context.commit_properties,
        target_file_size=context.options.target_file_size,
        writer_properties=context.options.writer_properties,
        storage_options=context.storage,
    )


def _coerce_write_input(value: DeltaWriteInput) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def coerce_delta_table(
    value: DeltaWriteInput,
    *,
    schema: SchemaLike | None = None,
    encoding_policy: EncodingPolicy | None = None,
) -> TableLike:
    """Apply schema transforms and encoding policies to Delta inputs.

    Returns
    -------
    TableLike
        Transformed Arrow table ready for Delta writes.
    """
    table = _coerce_write_input(value)
    if schema is not None:
        table = SchemaTransform(schema=schema, safe_cast=True, keep_extra_columns=False).apply(
            table
        )
    if encoding_policy is not None:
        table = apply_encoding(table, policy=encoding_policy)
    return table


def _storage_dict(storage_options: StorageOptions | None) -> dict[str, str] | None:
    if storage_options is None:
        return None
    return dict(storage_options)


def _partition_predicate(
    partitions: Sequence[Mapping[str, str]],
) -> str:
    clauses: list[str] = []
    for partition in partitions:
        if not partition:
            continue
        parts = [f"{key} = '{value}'" for key, value in partition.items()]
        clauses.append(f"({' AND '.join(parts)})")
    return " OR ".join(clauses)


def _artifact_path(base: str, suffix: str) -> str:
    base_path = Path(base)
    name = f"{base_path.name}_{suffix}"
    return str(base_path.with_name(name))


def _supports_datafusion_delta_insert(options: DeltaWriteOptions) -> bool:
    disallowed = (
        options.mode not in {"append", "overwrite"},
        options.schema_mode is not None,
        options.predicate is not None,
        bool(options.partition_by),
        bool(options.configuration),
        bool(options.commit_metadata),
        options.target_file_size is not None,
        options.writer_properties is not None,
        options.retry_policy is not None,
    )
    return not any(disallowed)


def _datafusion_ctx_from_df(df: object) -> SessionContext:
    ctx = None
    for attr in ("_ctx", "_context", "context", "ctx", "session_context"):
        ctx = getattr(df, attr, None)
        if ctx is not None:
            break
    if ctx is None:
        msg = "DataFusion DataFrame missing SessionContext."
        raise TypeError(msg)
    return cast("SessionContext", ctx)


def _delta_table_provider_from_session(
    ctx: SessionContext,
    *,
    path: str,
    storage_options: StorageOptions | None,
) -> object:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "datafusion_ext is required for Delta table providers."
        raise ValueError(msg) from exc
    provider_factory = getattr(module, "delta_table_provider_from_session", None)
    if not callable(provider_factory):
        msg = "datafusion_ext.delta_table_provider_from_session is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options is not None else None
    return provider_factory(
        ctx,
        path,
        storage_payload,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )


def _register_delta_table_provider(
    ctx: SessionContext,
    *,
    name: str,
    provider: object,
) -> None:
    register = getattr(ctx, "register_table", None)
    if not callable(register):
        msg = "DataFusion SessionContext missing register_table."
        raise TypeError(msg)
    register(name, TableProviderCapsule(provider))


def _deregister_datafusion_table(ctx: SessionContext, *, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        deregister(name)


def _provider_pushdown_value(provider: object, *, names: Sequence[str]) -> str | bool | None:
    for name in names:
        attr = getattr(provider, name, None)
        if isinstance(attr, bool):
            return attr
        if callable(attr):
            try:
                value = attr()
            except TypeError:
                continue
            if isinstance(value, bool):
                return value
            if value is None:
                return None
            return str(value)
    return None


def _provider_pushdown_hints(provider: object) -> dict[str, object]:
    return {
        "projection_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_projection_pushdown",),
        ),
        "predicate_pushdown": _provider_pushdown_value(
            provider,
            names=(
                "supports_filter_pushdown",
                "supports_filters_pushdown",
                "supports_predicate_pushdown",
            ),
        ),
        "limit_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_limit_pushdown",),
        ),
    }


@dataclass(frozen=True)
class _DeltaWriteArtifact:
    name: str
    path: str
    mode: DeltaWriteMode
    provider: object
    rows_affected: int | None


def _record_delta_write_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: _DeltaWriteArtifact,
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "name": artifact.name,
        "path": artifact.path,
        "provider": "delta_write_provider",
        "provider_type": type(artifact.provider).__name__,
        "capsule_id": repr(artifact.provider),
        "write_mode": artifact.mode,
        "rows_affected": artifact.rows_affected,
    }
    payload.update(_provider_pushdown_hints(artifact.provider))
    runtime_profile.diagnostics_sink.record_artifact("datafusion_table_providers_v1", payload)


def _delta_cdf_table_provider(
    table_path: str,
    *,
    storage_options: StorageOptions | None,
    options: DeltaCdfOptions | None,
) -> object | None:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        return None
    provider_factory = getattr(module, "delta_cdf_table_provider", None)
    options_type = getattr(module, "DeltaCdfOptions", None)
    if not callable(provider_factory) or options_type is None:
        return None
    resolved = options or DeltaCdfOptions()
    ext_options = options_type()
    if resolved.starting_version is not None:
        ext_options.starting_version = resolved.starting_version
    if resolved.ending_version is not None:
        ext_options.ending_version = resolved.ending_version
    if resolved.starting_timestamp is not None:
        ext_options.starting_timestamp = resolved.starting_timestamp
    if resolved.ending_timestamp is not None:
        ext_options.ending_timestamp = resolved.ending_timestamp
    ext_options.allow_out_of_range = resolved.allow_out_of_range
    storage = list(storage_options.items()) if storage_options else None
    return provider_factory(table_path, storage, ext_options)


def read_delta_cdf(
    table_path: str,
    *,
    storage_options: StorageOptions | None = None,
    cdf_options: DeltaCdfOptions | None = None,
) -> TableLike:
    """Read change data feed from a Delta table.

    Parameters
    ----------
    table_path : str
        Path to the Delta table.
    storage_options : StorageOptions | None
        Storage options for Delta table access.
    cdf_options : DeltaCdfOptions | None
        Options for CDF read (version range, columns, etc.).

    Returns
    -------
    TableLike
        Arrow Table with CDF changes.

    Raises
    ------
    ValueError
        If CDF is not enabled on the Delta table.
    """
    resolved_options = cdf_options or DeltaCdfOptions()
    provider = _delta_cdf_table_provider(
        table_path,
        storage_options=storage_options,
        options=resolved_options,
    )
    if provider is None:
        msg = "Delta CDF provider requires datafusion_ext.delta_cdf_table_provider."
        raise ValueError(msg)
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    ctx = DataFusionRuntimeProfile().session_context()
    name = f"__delta_cdf_{uuid.uuid4().hex}"
    select_columns = "*"
    if resolved_options.columns:
        select_columns = ", ".join(resolved_options.columns)
    predicate = resolved_options.predicate
    try:
        ctx.register_table(name, provider)
        sql = f"SELECT {select_columns} FROM {name}"
        if predicate:
            sql = f"{sql} WHERE {predicate}"
        df = ctx.sql(sql)
        return cast("TableLike", df.to_arrow_table())
    finally:
        ctx.deregister_table(name)


__all__ = [
    "DeltaCdfOptions",
    "DeltaSchemaMode",
    "DeltaUpsertOptions",
    "DeltaVacuumOptions",
    "DeltaWriteInput",
    "DeltaWriteMode",
    "DeltaWriteOptions",
    "DeltaWriteResult",
    "DeltaWriteRetryPolicy",
    "EncodingPolicy",
    "IdempotentWriteOptions",
    "StorageOptions",
    "apply_delta_write_policies",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_commit_metadata",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "enable_delta_features",
    "open_delta_table",
    "read_delta_cdf",
    "upsert_dataset_partitions_delta",
    "vacuum_delta",
    "write_datafusion_delta",
    "write_dataset_delta",
    "write_deltalake_idempotent",
    "write_finalize_result_delta",
    "write_named_datasets_delta",
    "write_table_delta",
    "write_table_delta_idempotent",
]
