"""Delta Lake read/write helpers for Arrow tables."""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from deltalake import CommitProperties, Transaction, WriterProperties

from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.schema.encoding_policy import EncodingPolicy, apply_encoding
from arrowdsl.schema.schema import SchemaTransform
from storage.ipc import ipc_bytes

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta_control_plane import (
        DeltaAppTransaction,
        DeltaCdfProviderBundle,
        DeltaCommitOptions,
        DeltaFeatureGate,
    )
    from datafusion_engine.runtime import DataFusionRuntimeProfile

type StorageOptions = Mapping[str, str]

DEFAULT_DELTA_FEATURE_PROPERTIES: dict[str, str] = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableRowTracking": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableInCommitTimestamps": "true",
}


def _runtime_ctx(ctx: SessionContext | None) -> SessionContext:
    if ctx is not None:
        return ctx
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    return DataFusionRuntimeProfile().session_context()


@dataclass(frozen=True)
class DeltaSnapshotLookup:
    """Inputs for Delta snapshot retrieval."""

    path: str
    storage_options: StorageOptions | None
    log_storage_options: StorageOptions | None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None


def _snapshot_info(request: DeltaSnapshotLookup) -> Mapping[str, object] | None:
    storage = _log_storage_dict(request.storage_options, request.log_storage_options)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaSnapshotRequest,
            delta_snapshot_info,
        )

        snapshot_request = DeltaSnapshotRequest(
            table_uri=request.path,
            storage_options=storage or None,
            version=request.version,
            timestamp=request.timestamp,
            gate=request.gate,
        )
        return delta_snapshot_info(snapshot_request)
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None


def _register_temp_table(ctx: SessionContext, table: pa.Table) -> str:
    name = f"__delta_temp_{uuid.uuid4().hex}"
    batches = table.to_batches()
    register = getattr(ctx, "register_record_batches", None)
    if not callable(register):
        msg = "SessionContext.register_record_batches is unavailable."
        raise TypeError(msg)
    register(name, batches)
    return name


def _deregister_table(ctx: SessionContext, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if not callable(deregister):
        return
    with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
        deregister(name)


@dataclass(frozen=True)
class DeltaWriteResult:
    """Result payload for Delta Lake writes."""

    path: str
    version: int | None
    report: Mapping[str, object] | None = None


@dataclass(frozen=True)
class DeltaWriteOptions:
    """Options for Delta Lake writes."""

    mode: Literal["error", "append", "overwrite", "ignore"] = "append"
    schema_mode: Literal["merge", "overwrite"] | None = None
    predicate: str | None = None
    partition_by: Sequence[str] | None = None
    configuration: Mapping[str, str | None] | None = None
    commit_metadata: Mapping[str, str] | None = None
    commit_properties: CommitProperties | None = None
    target_file_size: int | None = None
    writer_properties: WriterProperties | None = None
    app_id: str | None = None
    version: int | None = None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    extra_constraints: Sequence[str] | None = None


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
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaSchemaRequest:
    """Inputs required to resolve a Delta table schema."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaDeleteWhereRequest:
    """Inputs required to delete rows from a Delta table."""

    path: str
    predicate: str | None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    commit_properties: CommitProperties | None = None
    commit_metadata: Mapping[str, str] | None = None
    extra_constraints: Sequence[str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset_name: str | None = None


@dataclass(frozen=True)
class DeltaMergeArrowRequest:
    """Inputs required to merge Arrow data into a Delta table."""

    path: str
    source: pa.Table
    predicate: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    source_alias: str | None = "source"
    target_alias: str | None = "target"
    matched_predicate: str | None = None
    matched_updates: Mapping[str, str] | None = None
    not_matched_predicate: str | None = None
    not_matched_inserts: Mapping[str, str] | None = None
    not_matched_by_source_predicate: str | None = None
    delete_not_matched_by_source: bool = False
    update_all: bool = False
    insert_all: bool = False
    commit_properties: CommitProperties | None = None
    commit_metadata: Mapping[str, str] | None = None
    extra_constraints: Sequence[str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset_name: str | None = None


def delta_table_version(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    gate: DeltaFeatureGate | None = None,
) -> int | None:
    """Return the latest Delta table version when the table exists.

    Returns
    -------
    int | None
        Latest Delta table version, or None if not a Delta table.
    """
    snapshot = _snapshot_info(
        DeltaSnapshotLookup(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            gate=gate,
        )
    )
    if snapshot is None:
        return None
    version_value = snapshot.get("version")
    if isinstance(version_value, int):
        return version_value
    if isinstance(version_value, str) and version_value.strip():
        try:
            return int(version_value)
        except ValueError:
            return None
    return None


def delta_table_schema(request: DeltaSchemaRequest) -> pa.Schema | None:
    """Return the Delta table schema when the table exists.

    Parameters
    ----------
    request
        Schema lookup request with optional snapshot pins and feature gate.

    Returns
    -------
    pyarrow.Schema | None
        Arrow schema for the Delta table or ``None`` when the table does not exist.
    """
    storage = _log_storage_dict(request.storage_options, request.log_storage_options)
    ctx = _runtime_ctx(None)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaProviderRequest,
            delta_provider_from_session,
        )

        bundle = delta_provider_from_session(
            ctx,
            request=DeltaProviderRequest(
                table_uri=request.path,
                storage_options=storage or None,
                version=request.version,
                timestamp=request.timestamp,
                delta_scan=None,
                gate=request.gate,
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    df = ctx.read_table(bundle.provider)
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def delta_table_features(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    gate: DeltaFeatureGate | None = None,
) -> dict[str, str] | None:
    """Return Delta table feature configuration values when present.

    Returns
    -------
    dict[str, str] | None
        Feature configuration values or ``None`` if no features are set.
    """
    snapshot = _snapshot_info(
        DeltaSnapshotLookup(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            gate=gate,
        )
    )
    if snapshot is None:
        return None
    features: dict[str, str] = {}
    properties = snapshot.get("table_properties")
    if isinstance(properties, Mapping):
        for key, value in properties.items():
            name = str(key)
            if not name.startswith("delta."):
                continue
            features[name] = str(value)
    reader_features = snapshot.get("reader_features")
    if isinstance(reader_features, Sequence) and not isinstance(
        reader_features, (str, bytes, bytearray)
    ):
        features["reader_features"] = ",".join(str(value) for value in reader_features)
    writer_features = snapshot.get("writer_features")
    if isinstance(writer_features, Sequence) and not isinstance(
        writer_features, (str, bytes, bytearray)
    ):
        features["writer_features"] = ",".join(str(value) for value in writer_features)
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
    gate: DeltaFeatureGate | None = None,
) -> dict[str, str] | None:
    """Return custom commit metadata for the latest Delta table version.

    Returns
    -------
    dict[str, str] | None
        Custom commit metadata or ``None`` when not present.
    """
    snapshot = _snapshot_info(
        DeltaSnapshotLookup(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            gate=gate,
        )
    )
    if snapshot is None:
        return None
    return None


def delta_history_snapshot(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    limit: int = 1,
    gate: DeltaFeatureGate | None = None,
) -> dict[str, object] | None:
    """Return the latest Delta history entry.

    Returns
    -------
    dict[str, object] | None
        History entry payload or ``None`` when unavailable.
    """
    snapshot = _snapshot_info(
        DeltaSnapshotLookup(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            gate=gate,
        )
    )
    if snapshot is None:
        return None
    return {
        "version": snapshot.get("version"),
        "snapshot_timestamp": snapshot.get("snapshot_timestamp"),
        "min_reader_version": snapshot.get("min_reader_version"),
        "min_writer_version": snapshot.get("min_writer_version"),
        "reader_features": snapshot.get("reader_features"),
        "writer_features": snapshot.get("writer_features"),
        "table_properties": snapshot.get("table_properties"),
        "schema_json": snapshot.get("schema_json"),
        "partition_columns": snapshot.get("partition_columns"),
        "limit": limit,
    }


def delta_protocol_snapshot(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    gate: DeltaFeatureGate | None = None,
) -> dict[str, object] | None:
    """Return Delta protocol versions and active feature flags.

    Returns
    -------
    dict[str, object] | None
        Protocol payload or ``None`` when unavailable.
    """
    snapshot = _snapshot_info(
        DeltaSnapshotLookup(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            gate=gate,
        )
    )
    if snapshot is None:
        return None
    return {
        "min_reader_version": snapshot.get("min_reader_version"),
        "min_writer_version": snapshot.get("min_writer_version"),
        "reader_features": snapshot.get("reader_features"),
        "writer_features": snapshot.get("writer_features"),
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

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane property update fails.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
    if (
        delta_table_version(
            path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        is None
    ):
        return {}
    resolved = features or DEFAULT_DELTA_FEATURE_PROPERTIES
    properties = {key: str(value) for key, value in resolved.items() if value is not None}
    if not properties:
        return {}
    ctx = _runtime_ctx(None)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaCommitOptions,
            DeltaSetPropertiesRequest,
            delta_set_properties,
        )

        delta_set_properties(
            ctx,
            request=DeltaSetPropertiesRequest(
                table_uri=path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                properties=properties,
                commit_options=DeltaCommitOptions(metadata=dict(commit_metadata or {})),
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to set Delta table properties via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
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

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane vacuum call fails.
    """
    options = options or DeltaVacuumOptions()
    storage = _log_storage_dict(storage_options, log_storage_options)
    ctx = _runtime_ctx(None)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaCommitOptions,
            DeltaVacuumRequest,
            delta_vacuum,
        )

        report = delta_vacuum(
            ctx,
            request=DeltaVacuumRequest(
                table_uri=path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                retention_hours=options.retention_hours,
                dry_run=options.dry_run,
                enforce_retention_duration=options.enforce_retention_duration,
                commit_options=DeltaCommitOptions(metadata=dict(options.commit_metadata or {})),
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Delta vacuum failed via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
    metrics = report.get("metrics")
    if isinstance(metrics, Mapping):
        for key in ("files", "removed_files", "deleted_files"):
            value = metrics.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return [str(item) for item in value]
    return []


def create_delta_checkpoint(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> None:
    """Create a checkpoint for a Delta table."""
    msg = "create_delta_checkpoint is not supported in the Rust control-plane surface."
    raise NotImplementedError(msg)


def cleanup_delta_log(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> None:
    """Delete expired Delta log files."""
    msg = "cleanup_delta_log is not supported in the Rust control-plane surface."
    raise NotImplementedError(msg)


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
    bundle = _delta_cdf_table_provider(
        table_path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        options=resolved_options,
    )
    if bundle is None:
        msg = "Delta CDF provider requires Rust control-plane support."
        raise ValueError(msg)
    provider = bundle.provider
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


def write_delta_table(
    data: TableLike | RecordBatchReaderLike,
    path: str,
    *,
    options: DeltaWriteOptions | None = None,
    ctx: SessionContext | None = None,
) -> DeltaWriteResult:
    """Write an Arrow table or stream to a Delta table path.

    Parameters
    ----------
    data
        Table-like or record-batch reader to persist.
    path
        Delta table path.
    options
        Optional Delta write options.
    ctx
        Optional DataFusion session context for the write.

    Returns
    -------
    DeltaWriteResult
        Delta write result containing path and version metadata.

    Raises
    ------
    RuntimeError
        Raised when Rust control-plane adapters are unavailable.
    """
    resolved = options or DeltaWriteOptions()
    storage = dict(resolved.storage_options or {})
    if resolved.log_storage_options is not None:
        storage.update(dict(resolved.log_storage_options))
    if isinstance(data, RecordBatchReaderLike):
        table = cast("pa.Table", data.read_all())
    else:
        table = cast("pa.Table", data)
    data_ipc = ipc_bytes(table)
    ctx = _runtime_ctx(ctx)
    try:
        from datafusion_engine.delta_control_plane import DeltaWriteRequest, delta_write_ipc
    except ImportError as exc:
        msg = "Rust Delta control-plane adapters are required for Delta writes."
        raise RuntimeError(msg) from exc
    commit_properties = resolved.commit_properties or build_commit_properties(
        app_id=resolved.app_id,
        version=resolved.version,
        commit_metadata=resolved.commit_metadata,
    )
    commit_options = _delta_commit_options(
        commit_properties=commit_properties,
        commit_metadata=resolved.commit_metadata,
        app_id=resolved.app_id,
        app_version=resolved.version,
    )
    report = delta_write_ipc(
        ctx,
        request=DeltaWriteRequest(
            table_uri=path,
            storage_options=storage or None,
            version=resolved.version,
            timestamp=None,
            data_ipc=data_ipc,
            mode=resolved.mode,
            schema_mode=resolved.schema_mode,
            partition_columns=resolved.partition_by,
            target_file_size=resolved.target_file_size,
            extra_constraints=resolved.extra_constraints,
            commit_options=commit_options,
        ),
    )
    version = _mutation_version(report)
    return DeltaWriteResult(path=path, version=version, report=report)


def delta_delete_where(
    ctx: SessionContext,
    *,
    request: DeltaDeleteWhereRequest,
) -> Mapping[str, object]:
    """Delete rows from a Delta table via the Rust control plane.

    Parameters
    ----------
    ctx
        DataFusion session context for the delete.
    request
        Delete request describing target table and predicate.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.
    """
    storage = _log_storage_dict(request.storage_options, request.log_storage_options)
    commit_options = _delta_commit_options(
        commit_properties=request.commit_properties,
        commit_metadata=request.commit_metadata,
        app_id=None,
        app_version=None,
    )
    from datafusion_engine.delta_control_plane import DeltaDeleteRequest, delta_delete

    report = delta_delete(
        ctx,
        request=DeltaDeleteRequest(
            table_uri=request.path,
            storage_options=storage or None,
            version=None,
            timestamp=None,
            predicate=request.predicate,
            extra_constraints=request.extra_constraints,
            commit_options=commit_options,
        ),
    )
    _record_mutation_artifact(
        _MutationArtifactRequest(
            profile=request.runtime_profile,
            report=report,
            table_uri=request.path,
            operation="delete",
            mode="delete",
            commit_metadata=request.commit_metadata,
            commit_properties=request.commit_properties,
            constraint_status=_constraint_status(request.extra_constraints, checked=False),
            constraint_violations=(),
            storage_options_hash=_storage_options_hash(
                request.storage_options,
                request.log_storage_options,
            ),
            dataset_name=request.dataset_name,
        )
    )
    return report


def delta_merge_arrow(
    ctx: SessionContext,
    *,
    request: DeltaMergeArrowRequest,
) -> Mapping[str, object]:
    """Merge Arrow data into a Delta table via the Rust control plane.

    Parameters
    ----------
    ctx
        DataFusion session context for the merge.
    request
        Merge request describing source data and predicates.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.
    """
    storage = _log_storage_dict(request.storage_options, request.log_storage_options)
    resolved_source_alias = request.source_alias or "source"
    resolved_target_alias = request.target_alias or "target"
    resolved_updates = dict(request.matched_updates or {})
    resolved_inserts = dict(request.not_matched_inserts or {})
    if request.update_all:
        for name in request.source.schema.names:
            resolved_updates.setdefault(name, f"{resolved_source_alias}.{name}")
    if request.insert_all:
        for name in request.source.schema.names:
            resolved_inserts.setdefault(name, f"{resolved_source_alias}.{name}")
    source_table = _register_temp_table(ctx, request.source)
    commit_options = _delta_commit_options(
        commit_properties=request.commit_properties,
        commit_metadata=request.commit_metadata,
        app_id=None,
        app_version=None,
    )
    from datafusion_engine.delta_control_plane import DeltaMergeRequest, delta_merge

    try:
        report = delta_merge(
            ctx,
            request=DeltaMergeRequest(
                table_uri=request.path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                source_table=source_table,
                predicate=request.predicate,
                source_alias=resolved_source_alias,
                target_alias=resolved_target_alias,
                matched_predicate=request.matched_predicate,
                matched_updates=resolved_updates,
                not_matched_predicate=request.not_matched_predicate,
                not_matched_inserts=resolved_inserts,
                not_matched_by_source_predicate=request.not_matched_by_source_predicate,
                delete_not_matched_by_source=request.delete_not_matched_by_source,
                extra_constraints=request.extra_constraints,
                commit_options=commit_options,
            ),
        )
        _record_mutation_artifact(
            _MutationArtifactRequest(
                profile=request.runtime_profile,
                report=report,
                table_uri=request.path,
                operation="merge",
                mode="merge",
                commit_metadata=request.commit_metadata,
                commit_properties=request.commit_properties,
                constraint_status=_constraint_status(request.extra_constraints, checked=True),
                constraint_violations=(),
                storage_options_hash=_storage_options_hash(
                    request.storage_options,
                    request.log_storage_options,
                ),
                dataset_name=request.dataset_name,
            )
        )
        return report
    finally:
        _deregister_table(ctx, source_table)


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
    gate = request.gate
    result = checker(
        request.ctx,
        request.table_path,
        storage_payload,
        request.version,
        request.timestamp,
        payload,
        constraints_payload,
        gate.min_reader_version if gate is not None else None,
        gate.min_writer_version if gate is not None else None,
        list(gate.required_reader_features) if gate is not None else None,
        list(gate.required_writer_features) if gate is not None else None,
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


def _storage_options_hash(
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> str | None:
    storage = _log_storage_dict(storage_options, log_storage_options)
    if not storage:
        return None
    payload = json.dumps(sorted(storage.items()), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _constraint_status(
    extra_constraints: Sequence[str] | None,
    *,
    checked: bool,
) -> str:
    if not extra_constraints:
        return "skipped"
    return "passed" if checked else "not_applicable"


@dataclass(frozen=True)
class _MutationArtifactRequest:
    """Inputs required to record a Delta mutation artifact."""

    profile: DataFusionRuntimeProfile | None
    report: Mapping[str, object]
    table_uri: str
    operation: str
    mode: str | None
    commit_metadata: Mapping[str, str] | None
    commit_properties: CommitProperties | None
    constraint_status: str
    constraint_violations: Sequence[str]
    storage_options_hash: str | None
    dataset_name: str | None


def _commit_metadata_from_properties(commit_properties: CommitProperties) -> dict[str, str]:
    custom_metadata = getattr(commit_properties, "custom_metadata", None)
    if not isinstance(custom_metadata, Mapping):
        return {}
    return {str(key): str(value) for key, value in custom_metadata.items()}


def _record_mutation_artifact(request: _MutationArtifactRequest) -> None:
    if request.profile is None:
        return
    from datafusion_engine.delta_observability import (
        DeltaMutationArtifact,
        record_delta_mutation,
    )

    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None
    if request.commit_properties is not None:
        commit_payload = _commit_metadata_from_properties(request.commit_properties)
        commit_app_id = commit_payload.get("commit_app_id")
        commit_version_value = commit_payload.get("commit_version")
        commit_run_id = commit_payload.get("commit_run_id")
        if isinstance(commit_version_value, str) and commit_version_value.isdigit():
            commit_version = int(commit_version_value)
    commit_metadata = request.commit_metadata
    if commit_metadata is None and request.commit_properties is not None:
        commit_metadata = _commit_metadata_from_properties(request.commit_properties)
    record_delta_mutation(
        request.profile,
        artifact=DeltaMutationArtifact(
            table_uri=request.table_uri,
            operation=request.operation,
            report=request.report,
            dataset_name=request.dataset_name,
            mode=request.mode,
            commit_metadata=commit_metadata,
            commit_app_id=commit_app_id,
            commit_version=commit_version,
            commit_run_id=commit_run_id,
            constraint_status=request.constraint_status,
            constraint_violations=request.constraint_violations,
            storage_options_hash=request.storage_options_hash,
        ),
    )


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


def idempotent_commit_properties(
    *,
    operation: str,
    mode: str,
    idempotent: IdempotentWriteOptions | None = None,
    extra_metadata: Mapping[str, str] | None = None,
) -> CommitProperties:
    """Return standardized commit properties for deterministic Delta writes.

    Parameters
    ----------
    operation
        Operation label for commit metadata.
    mode
        Write mode label for commit metadata.
    idempotent
        Optional idempotent write options for app transactions.
    extra_metadata
        Extra metadata entries merged into the commit metadata payload.

    Returns
    -------
    CommitProperties
        Commit properties with standardized metadata and optional app transaction.

    Raises
    ------
    RuntimeError
        Raised when commit metadata is empty after normalization.
    """
    metadata: dict[str, str] = {
        "operation": str(operation),
        "mode": str(mode),
    }
    if extra_metadata:
        metadata.update({str(key): str(value) for key, value in extra_metadata.items()})
    app_id = idempotent.app_id if idempotent is not None else None
    version = idempotent.version if idempotent is not None else None
    commit_properties = build_commit_properties(
        app_id=app_id,
        version=version,
        commit_metadata=metadata,
    )
    if commit_properties is None:
        msg = "idempotent_commit_properties requires commit metadata."
        raise RuntimeError(msg)
    return commit_properties


def _delta_commit_options(
    *,
    commit_properties: CommitProperties | None,
    commit_metadata: Mapping[str, str] | None,
    app_id: str | None,
    app_version: int | None,
) -> DeltaCommitOptions:
    metadata: dict[str, str] = {}
    app_transaction: DeltaAppTransaction | None = None
    if commit_properties is not None:
        custom_metadata = getattr(commit_properties, "custom_metadata", None)
        if isinstance(custom_metadata, Mapping):
            metadata.update({str(key): str(value) for key, value in custom_metadata.items()})
        transactions = getattr(commit_properties, "app_transactions", None)
        if isinstance(transactions, Sequence) and not isinstance(
            transactions, (str, bytes, bytearray)
        ):
            first = next(iter(transactions), None)
            if first is not None:
                txn_app_id = getattr(first, "app_id", None)
                txn_version = getattr(first, "version", None)
                txn_last_updated = getattr(first, "last_updated", None)
                if isinstance(txn_app_id, str) and isinstance(txn_version, int):
                    from datafusion_engine.delta_control_plane import DeltaAppTransaction

                    app_transaction = DeltaAppTransaction(
                        app_id=txn_app_id,
                        version=txn_version,
                        last_updated=txn_last_updated
                        if isinstance(txn_last_updated, int)
                        else None,
                    )
    if commit_metadata:
        metadata.update({str(key): str(value) for key, value in commit_metadata.items()})
    if app_transaction is None and app_id is not None and app_version is not None:
        from datafusion_engine.delta_control_plane import DeltaAppTransaction

        app_transaction = DeltaAppTransaction(app_id=app_id, version=app_version)
    from datafusion_engine.delta_control_plane import DeltaCommitOptions

    return DeltaCommitOptions(metadata=metadata, app_transaction=app_transaction)


def _mutation_version(report: Mapping[str, object]) -> int | None:
    for key in ("mutation_version", "maintenance_version", "version"):
        value = report.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return int(value)
            except ValueError:
                continue
    return None


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
) -> DeltaCdfProviderBundle | None:
    storage = _log_storage_dict(storage_options, log_storage_options)
    try:
        from datafusion_engine.delta_control_plane import DeltaCdfRequest, delta_cdf_provider

        return delta_cdf_provider(
            request=DeltaCdfRequest(
                table_uri=table_path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                options=options,
            )
        )
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None


__all__ = [
    "DEFAULT_DELTA_FEATURE_PROPERTIES",
    "DeltaCdfOptions",
    "DeltaDataCheckRequest",
    "DeltaDeleteWhereRequest",
    "DeltaMergeArrowRequest",
    "DeltaSchemaRequest",
    "DeltaVacuumOptions",
    "DeltaWriteOptions",
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
    "idempotent_commit_properties",
    "read_delta_cdf",
    "vacuum_delta",
    "write_delta_table",
]
