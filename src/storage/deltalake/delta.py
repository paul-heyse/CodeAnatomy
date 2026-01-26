"""Delta Lake read/write helpers for Arrow tables."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction

from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.schema.encoding_policy import EncodingPolicy, apply_encoding
from arrowdsl.schema.schema import SchemaTransform
from storage.ipc import ipc_bytes

if TYPE_CHECKING:
    from datafusion import SessionContext

type StorageOptions = Mapping[str, str]

DEFAULT_DELTA_FEATURE_PROPERTIES: dict[str, str] = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableRowTracking": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableInCommitTimestamps": "true",
}


@dataclass(frozen=True)
class DeltaWriteResult:
    """Result payload for Delta Lake writes."""

    path: str
    version: int | None


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


@dataclass(frozen=True)
class DeltaDataCheckRequest:
    """Inputs required for Delta constraint checks."""

    ctx: SessionContext
    table_path: str
    data: TableLike | RecordBatchReaderLike
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    extra_constraints: Sequence[str] | None = None


def open_delta_table(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
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
    storage = _log_storage_dict(storage_options, log_storage_options)
    table = DeltaTable(path, storage_options=storage)
    if version is not None:
        table.load_as_version(version)
    elif timestamp is not None:
        table.load_as_version(timestamp)
    return table


def delta_table_version(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> int | None:
    """Return the latest Delta table version when the table exists.

    Returns
    -------
    int | None
        Latest Delta table version, or None if not a Delta table.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    return DeltaTable(path, storage_options=storage).version()


def delta_table_schema(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    version: int | None = None,
    timestamp: str | None = None,
) -> pa.Schema | None:
    """Return the Delta table schema when the table exists.

    Returns
    -------
    pyarrow.Schema | None
        Arrow schema for the Delta table or ``None`` when the table does not exist.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
    if not DeltaTable.is_deltatable(path, storage_options=storage):
        return None
    table = open_delta_table(
        path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        version=version,
        timestamp=timestamp,
    )
    schema = table.schema()
    return schema.to_arrow()


def delta_table_features(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> dict[str, str] | None:
    """Return Delta table feature configuration values when present.

    Returns
    -------
    dict[str, str] | None
        Feature configuration values or ``None`` if no features are set.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
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


def delta_cdf_enabled(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> bool:
    """Return True when Delta CDF is enabled for the table.

    Returns
    -------
    bool
        True when Change Data Feed is enabled.
    """
    features = delta_table_features(
        path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    if not features:
        return False
    cdf_flag = features.get("delta.enableChangeDataFeed")
    if cdf_flag is not None:
        return str(cdf_flag).lower() == "true"
    for key in ("reader_features", "writer_features"):
        raw = features.get(key)
        if not raw:
            continue
        tokens = [token.strip().lower() for token in str(raw).split(",") if token.strip()]
        if "changedatafeed" in tokens:
            return True
    return False


def delta_commit_metadata(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> dict[str, str] | None:
    """Return custom commit metadata for the latest Delta table version.

    Returns
    -------
    dict[str, str] | None
        Custom commit metadata or ``None`` when not present.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
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
    log_storage_options: StorageOptions | None = None,
    limit: int = 1,
) -> dict[str, object] | None:
    """Return the latest Delta history entry.

    Returns
    -------
    dict[str, object] | None
        History entry payload or ``None`` when unavailable.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
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
    log_storage_options: StorageOptions | None = None,
) -> dict[str, object] | None:
    """Return Delta protocol versions and active feature flags.

    Returns
    -------
    dict[str, object] | None
        Protocol payload or ``None`` when unavailable.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
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
    log_storage_options: StorageOptions | None = None,
    features: Mapping[str, str] | None = None,
    commit_metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Enable Delta table features by setting table properties.

    Returns
    -------
    dict[str, str]
        Properties applied to the Delta table.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
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
        commit_properties=build_commit_properties(commit_metadata=commit_metadata),
    )
    return properties


def vacuum_delta(
    path: str,
    *,
    options: DeltaVacuumOptions | None = None,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> list[str]:
    """Run a Delta vacuum to remove stale files.

    Returns
    -------
    list[str]
        Files eligible for deletion (or removed when ``dry_run`` is False).
    """
    options = options or DeltaVacuumOptions()
    storage = _log_storage_dict(storage_options, log_storage_options)
    table = DeltaTable(path, storage_options=storage)
    return table.vacuum(
        retention_hours=options.retention_hours,
        dry_run=options.dry_run,
        enforce_retention_duration=options.enforce_retention_duration,
        commit_properties=build_commit_properties(commit_metadata=options.commit_metadata),
        full=options.full,
        keep_versions=list(options.keep_versions) if options.keep_versions is not None else None,
    )


def create_delta_checkpoint(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> None:
    """Create a checkpoint for a Delta table."""
    storage = _log_storage_dict(storage_options, log_storage_options)
    DeltaTable(path, storage_options=storage).create_checkpoint()


def cleanup_delta_log(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> None:
    """Delete expired Delta log files."""
    storage = _log_storage_dict(storage_options, log_storage_options)
    DeltaTable(path, storage_options=storage).cleanup_metadata()


def coerce_delta_table(
    value: TableLike | RecordBatchReaderLike,
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
    table = _coerce_table(value)
    if schema is not None:
        table = SchemaTransform(schema=schema, safe_cast=False, keep_extra_columns=False).apply(
            table
        )
    if encoding_policy is not None:
        table = apply_encoding(table, policy=encoding_policy)
    return table


def read_delta_cdf(
    table_path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    cdf_options: DeltaCdfOptions | None = None,
) -> TableLike:
    """Read change data feed from a Delta table.

    Parameters
    ----------
    table_path : str
        Path to the Delta table.
    storage_options : StorageOptions | None
        Storage options for Delta table access.
    log_storage_options : StorageOptions | None
        Log-store options for Delta table access.
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
        log_storage_options=log_storage_options,
        options=resolved_options,
    )
    if provider is None:
        msg = "Delta CDF provider requires datafusion_ext.delta_cdf_table_provider."
        raise ValueError(msg)
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    ctx = DataFusionRuntimeProfile().session_context()
    df = ctx.read_table(provider)
    if resolved_options.columns:
        from datafusion import col

        df = df.select(*(col(name) for name in resolved_options.columns))
    if resolved_options.predicate:
        try:
            predicate_expr = df.parse_sql_expr(resolved_options.predicate)
            df = df.filter(predicate_expr)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Delta CDF predicate parse failed: {exc}"
            raise ValueError(msg) from exc
    return cast("TableLike", df.to_arrow_table())


def delta_data_checker(request: DeltaDataCheckRequest) -> list[str]:
    """Validate incoming data against Delta table constraints.

    Returns
    -------
    list[str]
        Constraint violation messages when present.

    Raises
    ------
    ValueError
        Raised when datafusion_ext is unavailable or Delta inputs are invalid.
    TypeError
        Raised when the delta data checker entrypoint is missing.
    """
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "Delta data checks require datafusion_ext."
        raise ValueError(msg) from exc
    checker = getattr(module, "delta_data_checker", None)
    if not callable(checker):
        msg = "datafusion_ext.delta_data_checker is unavailable."
        raise TypeError(msg)
    table = _coerce_table(request.data)
    if not isinstance(table, pa.Table):
        to_pyarrow = getattr(table, "to_pyarrow", None)
        if callable(to_pyarrow):
            resolved = to_pyarrow()
            if isinstance(resolved, pa.Table):
                table = resolved
            else:
                table = pa.Table.from_batches(table.to_batches())
        else:
            table = pa.Table.from_batches(table.to_batches())
    payload = ipc_bytes(cast("pa.Table", table))
    storage = _log_storage_dict(request.storage_options, request.log_storage_options)
    storage_payload = list(storage.items()) if storage else None
    constraints_payload = (
        [str(item) for item in request.extra_constraints if str(item).strip()]
        if request.extra_constraints
        else None
    )
    result = checker(
        request.ctx,
        request.table_path,
        storage_payload,
        request.version,
        request.timestamp,
        payload,
        constraints_payload,
    )
    if result is None:
        return []
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
        return [str(item) for item in result]
    return [str(result)]


def _coerce_table(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _log_storage_dict(
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> dict[str, str] | None:
    if log_storage_options is not None:
        return dict(log_storage_options)
    return _storage_dict(storage_options)


def _storage_dict(storage_options: StorageOptions | None) -> dict[str, str] | None:
    if storage_options is None:
        return None
    return dict(storage_options)


def build_commit_properties(
    *,
    app_id: str | None = None,
    version: int | None = None,
    commit_metadata: Mapping[str, str] | None = None,
) -> CommitProperties | None:
    """Return CommitProperties for Delta writes when needed.

    Returns
    -------
    CommitProperties | None
        Commit properties with app transaction and custom metadata when provided.
    """
    custom_metadata = dict(commit_metadata) if commit_metadata is not None else None
    app_transactions = None
    if app_id is not None and version is not None:
        app_transactions = [Transaction(app_id=app_id, version=version)]
    if custom_metadata is None and app_transactions is None:
        return None
    return CommitProperties(
        app_transactions=app_transactions,
        custom_metadata=custom_metadata,
    )


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


def _delta_cdf_table_provider(
    table_path: str,
    *,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
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
    ext_options.starting_version = resolved.starting_version
    if resolved.ending_version is not None:
        ext_options.ending_version = resolved.ending_version
    if resolved.starting_timestamp is not None:
        ext_options.starting_timestamp = resolved.starting_timestamp
    if resolved.ending_timestamp is not None:
        ext_options.ending_timestamp = resolved.ending_timestamp
    ext_options.allow_out_of_range = resolved.allow_out_of_range
    storage = _log_storage_dict(storage_options, log_storage_options)
    storage_payload = list(storage.items()) if storage else None
    return provider_factory(table_path, storage_payload, ext_options)


__all__ = [
    "DEFAULT_DELTA_FEATURE_PROPERTIES",
    "DeltaCdfOptions",
    "DeltaDataCheckRequest",
    "DeltaVacuumOptions",
    "DeltaWriteResult",
    "EncodingPolicy",
    "IdempotentWriteOptions",
    "StorageOptions",
    "build_commit_properties",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_cdf_enabled",
    "delta_commit_metadata",
    "delta_data_checker",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "enable_delta_features",
    "open_delta_table",
    "read_delta_cdf",
    "vacuum_delta",
]
