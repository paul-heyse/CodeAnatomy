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

from datafusion_engine.arrow_interop import RecordBatchReaderLike, SchemaLike, TableLike
from datafusion_engine.arrow_schema.encoding import EncodingPolicy
from datafusion_engine.encoding import apply_encoding
from datafusion_engine.schema_alignment import align_table
from storage.ipc_utils import ipc_bytes

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta_control_plane import (
        DeltaAppTransaction,
        DeltaCdfProviderBundle,
        DeltaCommitOptions,
        DeltaFeatureEnableRequest,
    )
    from datafusion_engine.delta_protocol import DeltaFeatureGate, DeltaProtocolSnapshot
    from datafusion_engine.runtime import DataFusionRuntimeProfile

type StorageOptions = Mapping[str, str]


@dataclass(frozen=True)
class DeltaFeatureMutationOptions:
    """Options for Delta feature control-plane operations."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None
    commit_metadata: Mapping[str, str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset_name: str | None = None


@dataclass(frozen=True)
class _DeltaFeatureMutationRecord:
    runtime_profile: DataFusionRuntimeProfile | None
    report: Mapping[str, object]
    operation: str
    path: str
    storage_options: StorageOptions | None
    log_storage_options: StorageOptions | None
    dataset_name: str | None
    commit_metadata: Mapping[str, str] | None


@dataclass(frozen=True)
class _DeltaMaintenanceRecord:
    runtime_profile: DataFusionRuntimeProfile | None
    report: Mapping[str, object]
    operation: str
    path: str
    storage_options: StorageOptions | None
    log_storage_options: StorageOptions | None
    dataset_name: str | None
    commit_metadata: Mapping[str, str] | None
    retention_hours: int | None
    dry_run: bool | None


def _runtime_ctx(ctx: SessionContext | None) -> SessionContext:
    if ctx is not None:
        return ctx
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    return DataFusionRuntimeProfile().session_context()


def _runtime_profile_ctx(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> SessionContext:
    if runtime_profile is not None:
        return runtime_profile.session_context()
    return _runtime_ctx(None)


def query_delta_sql(
    sql: str,
    tables: Mapping[str, str],
    storage_options: StorageOptions | None = None,
) -> RecordBatchReaderLike:
    """Execute SQL over Delta tables using the embedded QueryBuilder engine.

    Returns
    -------
    RecordBatchReaderLike
        Query results as a record batch reader.
    """
    from storage.deltalake.query_builder import execute_query, open_delta_table

    delta_tables = {
        name: open_delta_table(path, storage_options=storage_options)
        for name, path in tables.items()
    }
    return execute_query(sql, delta_tables)


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
    from_arrow = getattr(ctx, "from_arrow", None)
    if not callable(from_arrow):
        msg = "SessionContext does not support from_arrow ingestion."
        raise NotImplementedError(msg)
    from_arrow(table, name=name)
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
    require_vacuum_protocol_check: bool = False
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
class DeltaReadRequest:
    """Inputs required to read a Delta table with optional time travel."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    columns: Sequence[str] | None = None
    predicate: str | None = None
    gate: DeltaFeatureGate | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


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


def read_delta_table(request: DeltaReadRequest) -> TableLike:
    """Read a Delta table at a specific version or timestamp.

    Returns
    -------
    TableLike
        Arrow Table containing the requested Delta snapshot.

    Raises
    ------
    ValueError
        Raised when the Delta provider is unavailable or the request is invalid.
    """
    if request.version is not None and request.timestamp is not None:
        msg = "Delta read request must set either version or timestamp, not both."
        raise ValueError(msg)
    storage = _log_storage_dict(request.storage_options, request.log_storage_options)
    ctx = _runtime_profile_ctx(request.runtime_profile)
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
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Delta read provider request failed: {exc}"
        raise ValueError(msg) from exc
    df = ctx.read_table(bundle.provider)
    if request.predicate:
        try:
            predicate_expr = df.parse_sql_expr(request.predicate)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Delta read predicate parse failed: {exc}"
            raise ValueError(msg) from exc
        df = df.filter(predicate_expr)
    if request.columns:
        from datafusion import col

        df = df.select(*(col(name) for name in request.columns))
    return cast("TableLike", df.to_arrow_table())


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
) -> DeltaProtocolSnapshot | None:
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
    from datafusion_engine.delta_protocol import DeltaProtocolSnapshot

    payload = DeltaProtocolSnapshot(
        min_reader_version=_coerce_int(snapshot.get("min_reader_version")),
        min_writer_version=_coerce_int(snapshot.get("min_writer_version")),
        reader_features=tuple(_coerce_str_list(snapshot.get("reader_features"))),
        writer_features=tuple(_coerce_str_list(snapshot.get("writer_features"))),
    )
    if (
        payload.min_reader_version is None
        and payload.min_writer_version is None
        and not payload.reader_features
        and not payload.writer_features
    ):
        return None
    return payload


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_str_list(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value if str(item)]
    return []


def enable_delta_features(
    options: DeltaFeatureMutationOptions,
    *,
    features: Mapping[str, str] | None = None,
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
    storage = _log_storage_dict(options.storage_options, options.log_storage_options)
    if (
        delta_table_version(
            options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
        )
        is None
    ):
        return {}
    resolved = features or {}
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

        report = delta_set_properties(
            ctx,
            request=DeltaSetPropertiesRequest(
                table_uri=options.path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                properties=properties,
                gate=options.gate,
                commit_options=DeltaCommitOptions(metadata=dict(options.commit_metadata or {})),
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to set Delta table properties via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="set_properties",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return properties


def _feature_enable_request(
    options: DeltaFeatureMutationOptions,
) -> tuple[SessionContext, DeltaFeatureEnableRequest]:
    storage = _log_storage_dict(options.storage_options, options.log_storage_options)
    ctx = _runtime_profile_ctx(options.runtime_profile)
    from datafusion_engine.delta_control_plane import (
        DeltaCommitOptions,
        DeltaFeatureEnableRequest,
    )

    commit_options = DeltaCommitOptions(metadata=dict(options.commit_metadata or {}))
    request = DeltaFeatureEnableRequest(
        table_uri=options.path,
        storage_options=storage or None,
        version=options.version,
        timestamp=options.timestamp,
        gate=options.gate,
        commit_options=commit_options,
    )
    return ctx, request


def _require_feature_report(
    report: Mapping[str, object] | None,
    *,
    operation: str,
) -> Mapping[str, object]:
    if report is None:
        msg = f"Delta feature operation {operation} returned no report."
        raise RuntimeError(msg)
    return report


def delta_add_constraints(
    options: DeltaFeatureMutationOptions,
    *,
    constraints: Mapping[str, str],
) -> Mapping[str, object]:
    """Add Delta check constraints via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    if not constraints:
        return {}
    storage = _log_storage_dict(options.storage_options, options.log_storage_options)
    ctx = _runtime_profile_ctx(options.runtime_profile)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaAddConstraintsRequest,
            DeltaCommitOptions,
        )
        from datafusion_engine.delta_control_plane import (
            delta_add_constraints as add_constraints,
        )

        report = add_constraints(
            ctx,
            request=DeltaAddConstraintsRequest(
                table_uri=options.path,
                storage_options=storage or None,
                version=options.version,
                timestamp=options.timestamp,
                constraints=dict(constraints),
                gate=options.gate,
                commit_options=DeltaCommitOptions(metadata=dict(options.commit_metadata or {})),
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to add Delta constraints via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="add_constraints",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def delta_drop_constraints(
    options: DeltaFeatureMutationOptions,
    *,
    constraints: Sequence[str],
    raise_if_not_exists: bool = True,
) -> Mapping[str, object]:
    """Drop Delta check constraints via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    if not constraints:
        return {}
    storage = _log_storage_dict(options.storage_options, options.log_storage_options)
    ctx = _runtime_profile_ctx(options.runtime_profile)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaCommitOptions,
            DeltaDropConstraintsRequest,
        )
        from datafusion_engine.delta_control_plane import (
            delta_drop_constraints as drop_constraints,
        )

        report = drop_constraints(
            ctx,
            request=DeltaDropConstraintsRequest(
                table_uri=options.path,
                storage_options=storage or None,
                version=options.version,
                timestamp=options.timestamp,
                constraints=list(constraints),
                raise_if_not_exists=raise_if_not_exists,
                gate=options.gate,
                commit_options=DeltaCommitOptions(metadata=dict(options.commit_metadata or {})),
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to drop Delta constraints via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="drop_constraints",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_column_mapping(
    options: DeltaFeatureMutationOptions,
    *,
    mode: str = "name",
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta column mapping via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_column_mapping

        report = delta_enable_column_mapping(
            ctx,
            request=request,
            mode=mode,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta column mapping: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_column_mapping",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_deletion_vectors(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta deletion vectors via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_deletion_vectors

        report = delta_enable_deletion_vectors(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta deletion vectors: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_deletion_vectors",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_row_tracking(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta row tracking via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_row_tracking

        report = delta_enable_row_tracking(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta row tracking: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_row_tracking",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_change_data_feed(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta Change Data Feed via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_change_data_feed

        report = delta_enable_change_data_feed(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta change data feed: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_change_data_feed",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_generated_columns(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta generated columns via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_generated_columns

        report = delta_enable_generated_columns(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta generated columns: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_generated_columns",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_invariants(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta invariants via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_invariants

        report = delta_enable_invariants(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta invariants: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_invariants",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_check_constraints(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta check constraints via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_check_constraints

        report = delta_enable_check_constraints(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta check constraints: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_check_constraints",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_in_commit_timestamps(
    options: DeltaFeatureMutationOptions,
    *,
    enablement_version: int | None = None,
    enablement_timestamp: str | None = None,
) -> Mapping[str, object]:
    """Enable Delta in-commit timestamps via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_in_commit_timestamps

        report = delta_enable_in_commit_timestamps(
            ctx,
            request=request,
            enablement_version=enablement_version,
            enablement_timestamp=enablement_timestamp,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta in-commit timestamps: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_in_commit_timestamps",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_v2_checkpoints(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta v2 checkpoints via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_v2_checkpoints

        report = delta_enable_v2_checkpoints(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta v2 checkpoints: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_v2_checkpoints",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_vacuum_protocol_check(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Enable Delta vacuum protocol checks via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_vacuum_protocol_check

        report = delta_enable_vacuum_protocol_check(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta vacuum protocol check: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_vacuum_protocol_check",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def enable_delta_checkpoint_protection(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Enable Delta checkpoint protection via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_enable_checkpoint_protection

        report = delta_enable_checkpoint_protection(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta checkpoint protection: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="enable_checkpoint_protection",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_change_data_feed(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta Change Data Feed via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_change_data_feed

        report = delta_disable_change_data_feed(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta change data feed: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_change_data_feed",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_deletion_vectors(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta deletion vectors via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_deletion_vectors

        report = delta_disable_deletion_vectors(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta deletion vectors: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_deletion_vectors",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_row_tracking(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta row tracking via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_row_tracking

        report = delta_disable_row_tracking(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta row tracking: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_row_tracking",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_in_commit_timestamps(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta in-commit timestamps via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_in_commit_timestamps

        report = delta_disable_in_commit_timestamps(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta in-commit timestamps: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_in_commit_timestamps",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_vacuum_protocol_check(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta vacuum protocol checks via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_vacuum_protocol_check

        report = delta_disable_vacuum_protocol_check(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta vacuum protocol check: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_vacuum_protocol_check",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_column_mapping(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta column mapping via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_column_mapping

        report = _require_feature_report(
            delta_disable_column_mapping(ctx, request=request),
            operation="disable_column_mapping",
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta column mapping: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_column_mapping",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_generated_columns(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta generated columns via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_generated_columns

        report = _require_feature_report(
            delta_disable_generated_columns(ctx, request=request),
            operation="disable_generated_columns",
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta generated columns: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_generated_columns",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_invariants(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta invariants via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_invariants

        report = _require_feature_report(
            delta_disable_invariants(ctx, request=request),
            operation="disable_invariants",
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta invariants: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_invariants",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_check_constraints(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta check constraints via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_check_constraints

        report = _require_feature_report(
            delta_disable_check_constraints(ctx, request=request),
            operation="disable_check_constraints",
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta check constraints: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_check_constraints",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_v2_checkpoints(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta v2 checkpoints via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_v2_checkpoints

        report = _require_feature_report(
            delta_disable_v2_checkpoints(ctx, request=request),
            operation="disable_v2_checkpoints",
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta v2 checkpoints: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_v2_checkpoints",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


def disable_delta_checkpoint_protection(
    options: DeltaFeatureMutationOptions,
) -> Mapping[str, object]:
    """Disable Delta checkpoint protection via the Rust control plane.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane call fails.
    """
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta_control_plane import delta_disable_checkpoint_protection

        report = delta_disable_checkpoint_protection(ctx, request=request)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to disable Delta checkpoint protection: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(
        _DeltaFeatureMutationRecord(
            runtime_profile=options.runtime_profile,
            report=report,
            operation="disable_checkpoint_protection",
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
            dataset_name=options.dataset_name,
            commit_metadata=options.commit_metadata,
        )
    )
    return report


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
                require_vacuum_protocol_check=options.require_vacuum_protocol_check,
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
    runtime_profile: DataFusionRuntimeProfile | None = None,
    dataset_name: str | None = None,
) -> Mapping[str, object]:
    """Create a checkpoint for a Delta table.

    Returns
    -------
    Mapping[str, object]
        Control-plane maintenance report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane checkpoint call fails.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
    ctx = _runtime_profile_ctx(runtime_profile)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaCheckpointRequest,
            delta_create_checkpoint,
        )

        report = delta_create_checkpoint(
            ctx,
            request=DeltaCheckpointRequest(
                table_uri=path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Delta checkpoint failed via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_maintenance(
        _DeltaMaintenanceRecord(
            runtime_profile=runtime_profile,
            report=report,
            operation="create_checkpoint",
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            dataset_name=dataset_name,
            commit_metadata=None,
            retention_hours=None,
            dry_run=None,
        )
    )
    return report


def cleanup_delta_log(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    dataset_name: str | None = None,
) -> Mapping[str, object]:
    """Delete expired Delta log files.

    Returns
    -------
    Mapping[str, object]
        Control-plane maintenance report payload.

    Raises
    ------
    RuntimeError
        Raised when the Rust control-plane cleanup call fails.
    """
    storage = _log_storage_dict(storage_options, log_storage_options)
    ctx = _runtime_profile_ctx(runtime_profile)
    try:
        from datafusion_engine.delta_control_plane import (
            DeltaCheckpointRequest,
            delta_cleanup_metadata,
        )

        report = delta_cleanup_metadata(
            ctx,
            request=DeltaCheckpointRequest(
                table_uri=path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Delta metadata cleanup failed via Rust control plane: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_maintenance(
        _DeltaMaintenanceRecord(
            runtime_profile=runtime_profile,
            report=report,
            operation="cleanup_metadata",
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            dataset_name=dataset_name,
            commit_metadata=None,
            retention_hours=None,
            dry_run=None,
        )
    )
    return report


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
        table = align_table(
            table,
            schema=schema,
            safe_cast=False,
            keep_extra_columns=False,
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
    if resolved_options.predicate:
        try:
            predicate_expr = df.parse_sql_expr(resolved_options.predicate)
            df = df.filter(predicate_expr)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Delta CDF predicate parse failed: {exc}"
            raise ValueError(msg) from exc
    if resolved_options.columns:
        from datafusion import col

        df = df.select(*(col(name) for name in resolved_options.columns))
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
    ValueError
        Raised when the write mode is not append or overwrite.
    """
    resolved = options or DeltaWriteOptions()
    if resolved.mode not in {"append", "overwrite"}:
        msg = "Delta control-plane writes support only append or overwrite modes."
        raise ValueError(msg)
    storage = dict(resolved.storage_options or {})
    if resolved.log_storage_options is not None:
        storage.update(dict(resolved.log_storage_options))
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
    if resolved.writer_properties is not None:
        if resolved.extra_constraints:
            msg = "Delta writer_properties are not supported with extra_constraints."
            raise ValueError(msg)
        from deltalake import writer as delta_writer

        if isinstance(data, RecordBatchReaderLike):
            table = cast("pa.Table", data.read_all())
        else:
            table = cast("pa.Table", data)
        partition_by = list(resolved.partition_by or []) or None
        if resolved.mode == "overwrite":
            delta_writer.write_deltalake(
                path,
                table,
                partition_by=partition_by,
                mode="overwrite",
                configuration=resolved.configuration,
                schema_mode=resolved.schema_mode,
                storage_options=storage or None,
                predicate=resolved.predicate,
                target_file_size=resolved.target_file_size,
                writer_properties=resolved.writer_properties,
                commit_properties=commit_properties,
            )
        else:
            delta_writer.write_deltalake(
                path,
                table,
                partition_by=partition_by,
                mode=resolved.mode,
                configuration=resolved.configuration,
                schema_mode=resolved.schema_mode,
                storage_options=storage or None,
                target_file_size=resolved.target_file_size,
                writer_properties=resolved.writer_properties,
                commit_properties=commit_properties,
            )
        version = delta_table_version(
            path,
            storage_options=resolved.storage_options,
            log_storage_options=resolved.log_storage_options,
        )
        return DeltaWriteResult(path=path, version=version, report=None)
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
    merged: dict[str, str] = {}
    if storage_options:
        merged.update({str(key): str(value) for key, value in storage_options.items()})
    if log_storage_options:
        merged.update({str(key): str(value) for key, value in log_storage_options.items()})
    return merged or None


def _storage_options_hash(
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> str | None:
    storage = _log_storage_dict(storage_options, log_storage_options)
    if not storage:
        return None
    payload = json.dumps(sorted(storage.items()), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _record_delta_feature_mutation(request: _DeltaFeatureMutationRecord) -> None:
    if request.runtime_profile is None:
        return
    from datafusion_engine.delta_observability import (
        DeltaMutationArtifact,
        record_delta_mutation,
    )

    record_delta_mutation(
        request.runtime_profile,
        artifact=DeltaMutationArtifact(
            table_uri=request.path,
            operation=request.operation,
            report=request.report,
            dataset_name=request.dataset_name,
            commit_metadata=request.commit_metadata,
            storage_options_hash=_storage_options_hash(
                request.storage_options,
                request.log_storage_options,
            ),
        ),
    )


def _record_delta_maintenance(request: _DeltaMaintenanceRecord) -> None:
    if request.runtime_profile is None:
        return
    metrics_payload: dict[str, object] = {}
    metrics = request.report.get("metrics")
    if isinstance(metrics, Mapping):
        metrics_payload.update({str(key): value for key, value in dict(metrics).items()})
    snapshot = request.report.get("snapshot")
    if isinstance(snapshot, Mapping):
        metrics_payload.setdefault("snapshot", dict(snapshot))
    from datafusion_engine.delta_observability import (
        DeltaMaintenanceArtifact,
        record_delta_maintenance,
    )

    record_delta_maintenance(
        request.runtime_profile,
        artifact=DeltaMaintenanceArtifact(
            table_uri=request.path,
            operation=request.operation,
            report={**dict(request.report), "metrics": metrics_payload},
            dataset_name=request.dataset_name,
            retention_hours=request.retention_hours,
            dry_run=request.dry_run,
            storage_options_hash=_storage_options_hash(
                request.storage_options,
                request.log_storage_options,
            ),
            commit_metadata=request.commit_metadata,
        ),
    )


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
    "DeltaCdfOptions",
    "DeltaDataCheckRequest",
    "DeltaDeleteWhereRequest",
    "DeltaFeatureMutationOptions",
    "DeltaMergeArrowRequest",
    "DeltaReadRequest",
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
    "delta_add_constraints",
    "delta_cdf_enabled",
    "delta_commit_metadata",
    "delta_data_checker",
    "delta_drop_constraints",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "disable_delta_change_data_feed",
    "disable_delta_check_constraints",
    "disable_delta_checkpoint_protection",
    "disable_delta_column_mapping",
    "disable_delta_deletion_vectors",
    "disable_delta_generated_columns",
    "disable_delta_in_commit_timestamps",
    "disable_delta_invariants",
    "disable_delta_row_tracking",
    "disable_delta_v2_checkpoints",
    "disable_delta_vacuum_protocol_check",
    "enable_delta_change_data_feed",
    "enable_delta_check_constraints",
    "enable_delta_checkpoint_protection",
    "enable_delta_column_mapping",
    "enable_delta_deletion_vectors",
    "enable_delta_features",
    "enable_delta_generated_columns",
    "enable_delta_in_commit_timestamps",
    "enable_delta_invariants",
    "enable_delta_row_tracking",
    "enable_delta_v2_checkpoints",
    "enable_delta_vacuum_protocol_check",
    "idempotent_commit_properties",
    "read_delta_cdf",
    "read_delta_table",
    "vacuum_delta",
    "write_delta_table",
]
