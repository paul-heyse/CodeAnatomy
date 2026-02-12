"""Delta Lake read/write helpers for Arrow tables."""

from __future__ import annotations

import importlib
import time
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction
from deltalake.exceptions import TableNotFoundError

from arrow_utils.core.streaming import to_reader
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    as_reader,
    coerce_arrow_schema,
    coerce_table_like,
)
from datafusion_engine.encoding import apply_encoding
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.schema.alignment import align_table
from datafusion_engine.session.helpers import deregister_table, register_temp_table
from obs.otel.run_context import get_query_id, get_run_id
from obs.otel.scopes import SCOPE_STORAGE
from obs.otel.tracing import stage_span
from storage.deltalake.config import DeltaMutationPolicy, DeltaRetryPolicy
from storage.ipc_utils import ipc_bytes
from utils.storage_options import merged_storage_options
from utils.value_coercion import coerce_int, coerce_str_list

if TYPE_CHECKING:
    from datafusion import SessionContext
    from opentelemetry.trace import Span

    from datafusion_engine.delta.control_plane import (
        DeltaAppTransaction,
        DeltaCdfProviderBundle,
        DeltaCommitOptions,
        DeltaFeatureEnableRequest,
        DeltaMergeRequest,
    )
    from datafusion_engine.delta.protocol import DeltaFeatureGate, DeltaProtocolSnapshot
    from datafusion_engine.delta.specs import DeltaCdfOptionsSpec
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

type StorageOptions = Mapping[str, str]

_RESERVED_DELTA_COMMIT_METADATA_KEYS: frozenset[str] = frozenset(
    {
        "operation",
        "operationparameters",
        "operationmetrics",
        "timestamp",
        "readversion",
        "isolationlevel",
        "isblindappend",
        "engineinfo",
        "userdata",
        "usermetadata",
    }
)


def _normalize_commit_metadata_key(key: str) -> str:
    if key.lower() in _RESERVED_DELTA_COMMIT_METADATA_KEYS:
        return f"codeanatomy_{key}"
    return key


def _normalize_commit_metadata(
    commit_metadata: Mapping[str, str] | None,
) -> dict[str, str] | None:
    if commit_metadata is None:
        return None
    normalized = {
        _normalize_commit_metadata_key(str(key)): str(value)
        for key, value in commit_metadata.items()
    }
    return normalized or None


def canonical_table_uri(table_uri: str) -> str:
    """Return a canonical URI form for Delta snapshot identity keys.

    Returns:
    -------
    str
        Canonicalized table URI suitable for snapshot identity keys.
    """
    raw = str(table_uri).strip()
    parsed = urlparse(raw)
    if not parsed.scheme:
        return str(Path(raw).expanduser().resolve())
    scheme = parsed.scheme.lower()
    if scheme in {"s3a", "s3n"}:
        scheme = "s3"
    netloc = parsed.netloc
    if scheme in {"s3", "gs", "az", "abfs", "abfss", "http", "https"}:
        netloc = netloc.lower()
    path = parsed.path or ""
    if netloc and path and not path.startswith("/"):
        path = f"/{path}"
    return parsed._replace(scheme=scheme, netloc=netloc, path=path).geturl()


@dataclass(frozen=True)
class SnapshotKey:
    """Deterministic Delta snapshot identity."""

    canonical_uri: str
    version: int


def snapshot_key_for_table(table_uri: str, version: int) -> SnapshotKey:
    """Return a stable snapshot key for a table URI/version pair.

    Returns:
    -------
    SnapshotKey
        Stable snapshot identity for the URI/version pair.
    """
    return SnapshotKey(canonical_uri=canonical_table_uri(table_uri), version=int(version))


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


def _runtime_profile_for_delta(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRuntimeProfile:
    if runtime_profile is not None:
        return runtime_profile
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

    return DataFusionRuntimeProfile()


def _resolve_delta_mutation_policy(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaMutationPolicy:
    profile = _runtime_profile_for_delta(runtime_profile)
    return profile.policies.delta_mutation_policy or DeltaMutationPolicy()


def _is_s3_uri(table_uri: str) -> bool:
    scheme = urlparse(table_uri).scheme.lower()
    return scheme in {"s3", "s3a", "s3n"}


def _has_locking_provider(
    storage_options: StorageOptions | None,
    *,
    policy: DeltaMutationPolicy,
) -> bool:
    if not storage_options:
        return False
    normalized = {str(key).lower(): str(value) for key, value in storage_options.items()}
    for key in policy.locking_option_keys:
        value = normalized.get(str(key).lower())
        if value:
            return True
    return False


def _enforce_locking_provider(
    table_uri: str,
    storage_options: StorageOptions | None,
    *,
    policy: DeltaMutationPolicy,
) -> None:
    if not policy.require_locking_provider:
        return
    if not _is_s3_uri(table_uri):
        return
    if _has_locking_provider(storage_options, policy=policy):
        return
    msg = (
        "Delta mutations on S3 require a locking provider. "
        f"Set one of {tuple(policy.locking_option_keys)} in storage_options."
    )
    raise ValueError(msg)


def _enforce_append_only_policy(
    *,
    policy: DeltaMutationPolicy,
    operation: str,
    updates_present: bool,
) -> None:
    if not policy.append_only:
        return
    if operation == "delete" or updates_present:
        msg = "Delta mutation rejected: append_only policy forbids deletes or updates."
        raise ValueError(msg)


def _delta_retry_classification(
    exc: BaseException,
    *,
    policy: DeltaRetryPolicy,
) -> str:
    signature_parts = [type(exc).__name__, str(exc)]
    cause = getattr(exc, "__cause__", None)
    if cause is not None:
        signature_parts.append(str(cause))
    signature = " ".join(signature_parts).lower()
    fatal = {item.lower() for item in policy.fatal_errors}
    retryable = {item.lower() for item in policy.retryable_errors}
    if any(token in signature for token in fatal):
        return "fatal"
    if any(token in signature for token in retryable):
        return "retryable"
    return "unknown"


def _delta_retry_delay(attempt: int, *, policy: DeltaRetryPolicy) -> float:
    delay = float(policy.base_delay_s) * (2**attempt)
    return min(delay, float(policy.max_delay_s))


def _resolve_merge_actions(
    request: DeltaMergeArrowRequest,
) -> tuple[str, str, dict[str, str], dict[str, str], bool]:
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
    updates_present = bool(
        request.update_all or resolved_updates or request.delete_not_matched_by_source
    )
    return (
        resolved_source_alias,
        resolved_target_alias,
        resolved_updates,
        resolved_inserts,
        updates_present,
    )


def _merge_rows_affected(metrics: Mapping[str, object] | None) -> int | None:
    if not isinstance(metrics, Mapping):
        return None
    rows = 0
    found = False
    for key in (
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
        "num_inserted",
        "num_updated",
        "num_deleted",
    ):
        value = metrics.get(key)
        if value is None:
            continue
        found = True
        rows += int(coerce_int(value) or 0)
    return rows if found else None


def _execute_delta_merge(
    ctx: SessionContext,
    *,
    request: DeltaMergeRequest,
    retry_policy: DeltaRetryPolicy,
    span: Span,
) -> tuple[Mapping[str, object], int]:
    from datafusion_engine.delta.control_plane import delta_merge

    attempts = 0
    while True:
        try:
            report = delta_merge(ctx, request=request)
        except Exception as exc:  # pragma: no cover - retry paths depend on delta-rs
            classification = _delta_retry_classification(exc, policy=retry_policy)
            if classification != "retryable":
                raise
            attempts += 1
            if attempts >= retry_policy.max_attempts:
                raise
            delay = _delta_retry_delay(attempts - 1, policy=retry_policy)
            span.set_attribute("codeanatomy.retry_attempt", attempts)
            time.sleep(delay)
        else:
            return report, attempts


def _should_fallback_delta_merge(exc: Exception) -> bool:
    message = str(exc)
    if not message:
        return False
    lowered = message.lower()
    return any(
        token in lowered
        for token in (
            "ffi future panicked",
            "there is no reactor running",
            "invalid json in file stats",
            "sessioncontext",
            "cannot be converted",
            "delta control-plane extension is incompatible",
        )
    )


def _execute_delta_merge_fallback(
    fallback: _DeltaMergeFallbackInput,
) -> Mapping[str, object]:
    request = fallback.request
    commit_properties = request.commit_properties
    if commit_properties is None and request.commit_metadata is not None:
        commit_properties = build_commit_properties(commit_metadata=request.commit_metadata)
    storage = dict(fallback.storage_options) if fallback.storage_options is not None else None
    table = DeltaTable(request.path, storage_options=storage)
    source_reader = as_reader(fallback.source)
    merger = table.merge(
        source_reader,
        request.predicate,
        source_alias=fallback.source_alias,
        target_alias=fallback.target_alias,
        commit_properties=commit_properties,
    )
    if request.update_all:
        merger = merger.when_matched_update_all(predicate=request.matched_predicate)
    elif fallback.matched_updates:
        merger = merger.when_matched_update(
            updates=dict(fallback.matched_updates),
            predicate=request.matched_predicate,
        )
    if request.insert_all:
        merger = merger.when_not_matched_insert_all(predicate=request.not_matched_predicate)
    elif fallback.not_matched_inserts:
        merger = merger.when_not_matched_insert(
            updates=dict(fallback.not_matched_inserts),
            predicate=request.not_matched_predicate,
        )
    if request.delete_not_matched_by_source:
        merger = merger.when_not_matched_by_source_delete(
            predicate=request.not_matched_by_source_predicate
        )
    metrics_raw = merger.execute()
    metrics = dict(metrics_raw) if isinstance(metrics_raw, Mapping) else {"value": metrics_raw}
    return {
        "version": table.version(),
        "metrics": metrics,
        "merge_mode": "python_deltalake_fallback",
    }


def _storage_span_attributes(
    *,
    operation: str,
    table_path: str | None = None,
    dataset_name: str | None = None,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    attrs: dict[str, object] = {"codeanatomy.operation": operation}
    if table_path:
        attrs["codeanatomy.table"] = str(table_path)
    run_id = get_run_id()
    if run_id:
        attrs["codeanatomy.run_id"] = run_id
    query_id = get_query_id()
    if query_id:
        attrs["codeanatomy.query_id"] = query_id
    if dataset_name:
        attrs["codeanatomy.dataset_name"] = dataset_name
    if extra:
        attrs.update({key: value for key, value in extra.items() if value is not None})
    return attrs


def _feature_control_span(
    options: DeltaFeatureMutationOptions,
    *,
    operation: str,
) -> AbstractContextManager[Span]:
    attrs = _storage_span_attributes(
        operation="feature_control",
        table_path=options.path,
        dataset_name=options.dataset_name,
        extra={"codeanatomy.feature_name": operation},
    )
    return cast(
        "AbstractContextManager[Span]",
        stage_span(
            "storage.feature_control",
            stage="storage",
            scope_name=SCOPE_STORAGE,
            attributes=attrs,
        ),
    )


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
    storage = merged_storage_options(request.storage_options, request.log_storage_options)
    try:
        from datafusion_engine.delta.control_plane import (
            DeltaSnapshotRequest,
            delta_snapshot_info,
        )
    except ImportError as exc:
        msg = "Delta control-plane support is unavailable."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc

    snapshot_request = DeltaSnapshotRequest(
        table_uri=request.path,
        storage_options=storage or None,
        version=request.version,
        timestamp=request.timestamp,
        gate=request.gate,
    )
    try:
        return delta_snapshot_info(snapshot_request)
    except DataFusionEngineError as exc:
        if exc.kind == ErrorKind.PLUGIN:
            raise
        return None


@dataclass(frozen=True)
class DeltaWriteResult:
    """Result payload for Delta Lake writes."""

    path: str
    version: int | None
    report: Mapping[str, object] | None = None
    snapshot_key: SnapshotKey | None = None


@dataclass(frozen=True)
class IdempotentWriteOptions:
    """Options for idempotent Delta writes.

    Uses Delta Lake's CommitProperties to tag commits with app_id and version,
    enabling safe retries where duplicate commits are ignored.

    Attributes:
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


def cdf_options_to_spec(
    options: DeltaCdfOptions | DeltaCdfOptionsSpec | None,
) -> DeltaCdfOptionsSpec | None:
    """Return a DeltaCdfOptionsSpec payload for control-plane requests.

    Returns:
    -------
    DeltaCdfOptionsSpec | None
        Control-plane CDF options payload.
    """
    if options is None:
        return None
    from datafusion_engine.delta.specs import DeltaCdfOptionsSpec

    if isinstance(options, DeltaCdfOptionsSpec):
        return options

    columns = tuple(options.columns) if options.columns is not None else None
    return DeltaCdfOptionsSpec(
        starting_version=options.starting_version,
        ending_version=options.ending_version,
        starting_timestamp=options.starting_timestamp,
        ending_timestamp=options.ending_timestamp,
        columns=columns,
        predicate=options.predicate,
        allow_out_of_range=options.allow_out_of_range,
    )


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
    source: TableLike | RecordBatchReaderLike
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


@dataclass(frozen=True)
class _DeltaMergeFallbackInput:
    source: TableLike | RecordBatchReaderLike
    request: DeltaMergeArrowRequest
    storage_options: StorageOptions | None
    source_alias: str
    target_alias: str
    matched_updates: Mapping[str, str]
    not_matched_inserts: Mapping[str, str]


@dataclass(frozen=True)
class _DeltaMergeExecutionState:
    ctx: SessionContext
    request: DeltaMergeArrowRequest
    delta_input: DeltaInput
    mutation_policy: DeltaMutationPolicy
    storage: StorageOptions | None
    source_alias: str
    target_alias: str
    matched_updates: dict[str, str]
    not_matched_inserts: dict[str, str]
    source_table: str
    merge_request: DeltaMergeRequest


@dataclass(frozen=True)
class _DeltaMergeExecutionResult:
    report: Mapping[str, object]
    attempts: int


def _open_delta_table(
    *,
    path: str,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
    version: int | None = None,
    timestamp: str | None = None,
) -> DeltaTable | None:
    if version is not None and timestamp is not None:
        msg = "Delta metadata lookup must set either version or timestamp, not both."
        raise ValueError(msg)
    storage = merged_storage_options(storage_options, log_storage_options)
    try:
        table = DeltaTable(
            path,
            version=version,
            storage_options=dict(storage) if storage else None,
        )
        if timestamp is not None:
            table.load_as_version(timestamp)
    except (RuntimeError, TypeError, ValueError, OSError, TableNotFoundError):
        return None
    return table


def _coerce_delta_version(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def delta_table_version(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    gate: DeltaFeatureGate | None = None,
) -> int | None:
    """Return the latest Delta table version when the table exists.

    Args:
        path: Description.
        storage_options: Description.
        log_storage_options: Description.
        gate: Description.

    Raises:
        DataFusionEngineError: If the operation cannot be completed.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=path,
        extra={"codeanatomy.metadata_kind": "version"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        snapshot: Mapping[str, object] | None = None
        try:
            snapshot = _snapshot_info(
                DeltaSnapshotLookup(
                    path=path,
                    storage_options=storage_options,
                    log_storage_options=log_storage_options,
                    gate=gate,
                )
            )
        except DataFusionEngineError as exc:
            if exc.kind != ErrorKind.PLUGIN:
                raise
        if snapshot is not None:
            version = _coerce_delta_version(snapshot.get("version"))
            if version is not None:
                span.set_attribute("codeanatomy.version", version)
                return version
        table = _open_delta_table(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        if table is None:
            return None
        try:
            version = table.version()
        except (RuntimeError, TypeError, ValueError):
            return None
        resolved = _coerce_delta_version(version)
        if resolved is not None:
            span.set_attribute("codeanatomy.version", resolved)
        return resolved


def delta_table_schema(request: DeltaSchemaRequest) -> pa.Schema | None:
    """Return the Delta table schema when the table exists.

    Parameters
    ----------
    request
        Schema lookup request with optional snapshot pins and feature gate.

    Returns:
    -------
    pyarrow.Schema | None
        Arrow schema for the Delta table or ``None`` when the table does not exist.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=request.path,
        extra={"codeanatomy.metadata_kind": "schema"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        table = _open_delta_table(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
            version=request.version,
            timestamp=request.timestamp,
        )
        if table is None:
            return None
        resolved_schema = coerce_arrow_schema(table.schema())
        if resolved_schema is not None:
            span.set_attribute("codeanatomy.schema_columns", len(resolved_schema))
        return resolved_schema


def read_delta_table(request: DeltaReadRequest) -> RecordBatchReaderLike:
    """Read a Delta table at a specific version or timestamp.

    Args:
        request: Description.

    Returns:
        RecordBatchReaderLike: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if request.version is not None and request.timestamp is not None:
        msg = "Delta read request must set either version or timestamp, not both."
        raise ValueError(msg)
    attrs = _storage_span_attributes(
        operation="read",
        table_path=request.path,
        extra={
            "codeanatomy.version": request.version,
            "codeanatomy.timestamp": request.timestamp,
            "codeanatomy.has_filters": bool(request.predicate),
            "codeanatomy.column_count": len(request.columns)
            if request.columns is not None
            else None,
        },
    )
    with stage_span(
        "storage.read_table",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        storage = merged_storage_options(request.storage_options, request.log_storage_options)
        profile = _runtime_profile_for_delta(request.runtime_profile)
        ctx = profile.session_context()
        from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
        from datafusion_engine.dataset.resolution import (
            DatasetResolutionRequest,
            resolve_dataset_provider,
        )

        try:
            overrides = None
            if request.gate is not None:
                from schema_spec.contracts import DeltaPolicyBundle

                overrides = DatasetLocationOverrides(
                    delta=DeltaPolicyBundle(feature_gate=request.gate)
                )
            location = DatasetLocation(
                path=request.path,
                format="delta",
                storage_options=dict(storage or {}),
                delta_log_storage_options=dict(request.log_storage_options or {}),
                delta_version=request.version,
                delta_timestamp=request.timestamp,
                overrides=overrides,
            )
            resolution = resolve_dataset_provider(
                DatasetResolutionRequest(
                    ctx=ctx,
                    location=location,
                    runtime_profile=profile,
                )
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Delta read provider request failed: {exc}"
            raise ValueError(msg) from exc
        from datafusion_engine.tables.metadata import TableProviderCapsule

        df = ctx.read_table(TableProviderCapsule(resolution.provider))
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
        return cast("RecordBatchReaderLike", to_reader(df))


def read_delta_table_eager(request: DeltaReadRequest) -> pa.Table:
    """Read a Delta table and materialize into an Arrow table.

    Returns:
    -------
    pyarrow.Table
        Materialized Arrow table containing the requested Delta snapshot.
    """
    reader = read_delta_table(request)
    return reader.read_all()


def delta_table_features(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    gate: DeltaFeatureGate | None = None,
) -> dict[str, str] | None:
    """Return Delta table feature configuration values when present.

    Returns:
    -------
    dict[str, str] | None
        Feature configuration values or ``None`` if no features are set.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=path,
        extra={"codeanatomy.metadata_kind": "features"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
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

    Returns:
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

    Returns:
    -------
    dict[str, str] | None
        Custom commit metadata or ``None`` when not present.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=path,
        extra={"codeanatomy.metadata_kind": "commit_metadata"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
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

    Returns:
    -------
    dict[str, object] | None
        History entry payload or ``None`` when unavailable.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=path,
        extra={"codeanatomy.metadata_kind": "history"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
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

    Returns:
    -------
    dict[str, object] | None
        Protocol payload or ``None`` when unavailable.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=path,
        extra={"codeanatomy.metadata_kind": "protocol"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
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
        from datafusion_engine.delta.protocol import DeltaProtocolSnapshot

        payload = DeltaProtocolSnapshot(
            min_reader_version=coerce_int(snapshot.get("min_reader_version")),
            min_writer_version=coerce_int(snapshot.get("min_writer_version")),
            reader_features=tuple(coerce_str_list(snapshot.get("reader_features"))),
            writer_features=tuple(coerce_str_list(snapshot.get("writer_features"))),
        )
        if (
            payload.min_reader_version is None
            and payload.min_writer_version is None
            and not payload.reader_features
            and not payload.writer_features
        ):
            return None
        return payload


def enable_delta_features(
    options: DeltaFeatureMutationOptions,
    *,
    features: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Enable Delta table features by setting table properties.

    Args:
        options: Delta feature mutation options.
        features: Table properties to apply.

    Returns:
        dict[str, str]: Result.

    Raises:
        RuntimeError: If setting table properties fails.
    """
    storage = merged_storage_options(options.storage_options, options.log_storage_options)
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
    with _feature_control_span(options, operation="set_properties"):
        profile = _runtime_profile_for_delta(options.runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane import (
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
    storage = merged_storage_options(options.storage_options, options.log_storage_options)
    profile = _runtime_profile_for_delta(options.runtime_profile)
    ctx = profile.delta_ops.delta_runtime_ctx()
    from datafusion_engine.delta.control_plane import (
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

    Args:
        options: Delta feature mutation options.
        constraints: Constraint name-to-expression mapping.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If adding constraints fails.
    """
    if not constraints:
        return {}
    storage = merged_storage_options(options.storage_options, options.log_storage_options)
    with _feature_control_span(options, operation="add_constraints"):
        profile = _runtime_profile_for_delta(options.runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane import (
                DeltaAddConstraintsRequest,
                DeltaCommitOptions,
            )
            from datafusion_engine.delta.control_plane import (
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

    Args:
        options: Delta feature mutation options.
        constraints: Constraint names to drop.
        raise_if_not_exists: Whether missing constraints should raise.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If dropping constraints fails.
    """
    if not constraints:
        return {}
    storage = merged_storage_options(options.storage_options, options.log_storage_options)
    with _feature_control_span(options, operation="drop_constraints"):
        profile = _runtime_profile_for_delta(options.runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane import (
                DeltaCommitOptions,
                DeltaDropConstraintsRequest,
            )
            from datafusion_engine.delta.control_plane import (
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

    Args:
        options: Delta feature mutation options.
        mode: Column mapping mode.
        allow_protocol_versions_increase: Whether protocol upgrade is allowed.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling column mapping fails.
    """
    with _feature_control_span(options, operation="enable_column_mapping"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_column_mapping

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

    Args:
        options: Delta feature mutation options.
        allow_protocol_versions_increase: Whether protocol upgrade is allowed.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling deletion vectors fails.
    """
    with _feature_control_span(options, operation="enable_deletion_vectors"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_deletion_vectors

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

    Args:
        options: Delta feature mutation options.
        allow_protocol_versions_increase: Whether protocol upgrade is allowed.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling row tracking fails.
    """
    with _feature_control_span(options, operation="enable_row_tracking"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_row_tracking

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

    Args:
        options: Delta feature mutation options.
        allow_protocol_versions_increase: Whether protocol upgrade is allowed.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling change data feed fails.
    """
    with _feature_control_span(options, operation="enable_change_data_feed"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_change_data_feed

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


def enable_delta_check_constraints(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta check constraints via the Rust control plane.

    Args:
        options: Delta feature mutation options.
        allow_protocol_versions_increase: Whether protocol upgrade is allowed.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling check constraints fails.
    """
    with _feature_control_span(options, operation="enable_check_constraints"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_check_constraints

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

    Args:
        options: Delta feature mutation options.
        enablement_version: Optional enablement version.
        enablement_timestamp: Optional enablement timestamp.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling in-commit timestamps fails.
    """
    with _feature_control_span(options, operation="enable_in_commit_timestamps"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_in_commit_timestamps

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

    Args:
        options: Delta feature mutation options.
        allow_protocol_versions_increase: Whether protocol upgrade is allowed.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If enabling v2 checkpoints fails.
    """
    with _feature_control_span(options, operation="enable_v2_checkpoints"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane import delta_enable_v2_checkpoints

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


def vacuum_delta(
    path: str,
    *,
    options: DeltaVacuumOptions | None = None,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> list[str]:
    """Run a Delta vacuum to remove stale files.

    Returns:
    -------
    list[str]
        Files eligible for deletion (or removed when ``dry_run`` is False).

    """
    options = options or DeltaVacuumOptions()
    attrs = _storage_span_attributes(
        operation="vacuum",
        table_path=path,
        extra={
            "codeanatomy.retention_hours": options.retention_hours,
            "codeanatomy.dry_run": options.dry_run,
            "codeanatomy.enforce_retention_duration": options.enforce_retention_duration,
        },
    )
    with stage_span(
        "storage.vacuum",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        storage = merged_storage_options(storage_options, log_storage_options)
        profile = _runtime_profile_for_delta(None)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane import (
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
            table = DeltaTable(path, storage_options=dict(storage) if storage else None)
            commit_properties = build_commit_properties(
                commit_metadata=options.commit_metadata or None
            )
            files = table.vacuum(
                retention_hours=options.retention_hours,
                dry_run=options.dry_run,
                enforce_retention_duration=options.enforce_retention_duration,
                commit_properties=commit_properties,
                full=options.full,
                keep_versions=list(options.keep_versions)
                if options.keep_versions is not None
                else None,
            )
            vacuum_fallback = True
            span.set_attribute("codeanatomy.vacuum_fallback", vacuum_fallback)
            span.set_attribute("codeanatomy.vacuum_fallback_error", str(exc))
            if isinstance(files, Sequence) and not isinstance(files, (str, bytes, bytearray)):
                return [str(item) for item in files]
            return []
        metrics = report.get("metrics")
        if isinstance(metrics, Mapping):
            for key in ("files", "removed_files", "deleted_files"):
                value = metrics.get(key)
                if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                    span.set_attribute("codeanatomy.files_removed", len(value))
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

    Returns:
    -------
    Mapping[str, object]
        Control-plane maintenance report payload.

    """
    attrs = _storage_span_attributes(
        operation="checkpoint",
        table_path=path,
        dataset_name=dataset_name,
    )
    with stage_span(
        "storage.checkpoint",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        storage = merged_storage_options(storage_options, log_storage_options)
        profile = _runtime_profile_for_delta(runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane import (
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
            table = DeltaTable(path, storage_options=dict(storage) if storage else None)
            table.create_checkpoint()
            report = {"checkpoint": True, "version": table.version(), "fallback_error": str(exc)}
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

    Args:
        path: Delta table path.
        storage_options: Optional storage options.
        log_storage_options: Optional log-store options.
        runtime_profile: Optional runtime profile.
        dataset_name: Optional dataset name for diagnostics.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If metadata cleanup fails.
    """
    attrs = _storage_span_attributes(
        operation="cleanup_metadata",
        table_path=path,
        dataset_name=dataset_name,
    )
    with stage_span(
        "storage.cleanup_metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        storage = merged_storage_options(storage_options, log_storage_options)
        profile = _runtime_profile_for_delta(runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane import (
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

    Returns:
    -------
    TableLike
        Transformed Arrow table ready for Delta writes.
    """
    delta_input = coerce_delta_input(
        value,
        schema=schema,
        encoding_policy=encoding_policy,
        prefer_reader=False,
    )
    data = delta_input.data
    if isinstance(data, RecordBatchReaderLike):
        return data.read_all()
    return data


@dataclass(frozen=True)
class DeltaInput:
    """Normalized Delta input with optional row counts."""

    data: TableLike | RecordBatchReaderLike
    row_count: int | None
    schema: SchemaLike


def coerce_delta_input(
    value: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike | None = None,
    encoding_policy: EncodingPolicy | None = None,
    prefer_reader: bool = False,
) -> DeltaInput:
    """Normalize Delta inputs with explicit streaming behavior.

    Returns:
    -------
    DeltaInput
        Normalized input wrapper with schema and optional row count.
    """
    resolved = coerce_table_like(value, requested_schema=None)
    if schema is None and encoding_policy is None:
        if isinstance(resolved, RecordBatchReaderLike):
            return DeltaInput(
                data=resolved,
                row_count=None,
                schema=resolved.schema,
            )
        return DeltaInput(
            data=resolved,
            row_count=int(resolved.num_rows),
            schema=resolved.schema,
        )
    table = to_arrow_table(resolved)
    if schema is not None:
        table = align_table(
            table,
            schema=schema,
            safe_cast=False,
            keep_extra_columns=False,
        )
    if encoding_policy is not None:
        table = apply_encoding(table, policy=encoding_policy)
    row_count = int(table.num_rows)
    data: TableLike | RecordBatchReaderLike = table
    if prefer_reader:
        data = to_reader(table)
    return DeltaInput(
        data=data,
        row_count=row_count,
        schema=table.schema,
    )


def read_delta_cdf(
    table_path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    cdf_options: DeltaCdfOptions | None = None,
) -> RecordBatchReaderLike:
    """Read change data feed from a Delta table.

    Args:
        table_path: Delta table path.
        storage_options: Optional storage options.
        log_storage_options: Optional log-store options.
        cdf_options: Optional CDF read options.

    Returns:
        RecordBatchReaderLike: Result.

    Raises:
        ValueError: If CDF read options are invalid.
    """
    resolved_options = cdf_options or DeltaCdfOptions()
    attrs = _storage_span_attributes(
        operation="read_cdf",
        table_path=table_path,
        extra={
            "codeanatomy.starting_version": resolved_options.starting_version,
            "codeanatomy.ending_version": resolved_options.ending_version,
            "codeanatomy.starting_timestamp": resolved_options.starting_timestamp,
            "codeanatomy.ending_timestamp": resolved_options.ending_timestamp,
            "codeanatomy.has_filters": bool(resolved_options.predicate),
            "codeanatomy.column_count": len(resolved_options.columns)
            if resolved_options.columns is not None
            else None,
        },
    )
    with stage_span(
        "storage.read_cdf",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
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
        from datafusion_engine.session.runtime import DataFusionRuntimeProfile

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
        return cast("RecordBatchReaderLike", to_reader(df))


def read_delta_cdf_eager(
    table_path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    cdf_options: DeltaCdfOptions | None = None,
) -> pa.Table:
    """Read change data feed and materialize into an Arrow table.

    Returns:
    -------
    pyarrow.Table
        Materialized Arrow table with CDF changes.
    """
    reader = read_delta_cdf(
        table_path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        cdf_options=cdf_options,
    )
    return reader.read_all()


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

    Returns:
    -------
    Mapping[str, object]
        Control-plane mutation report payload.
    """
    attrs = _storage_span_attributes(
        operation="delete",
        table_path=request.path,
        dataset_name=request.dataset_name,
        extra={"codeanatomy.has_filters": bool(request.predicate)},
    )
    with stage_span(
        "storage.delete",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        mutation_policy = _resolve_delta_mutation_policy(request.runtime_profile)
        storage = merged_storage_options(request.storage_options, request.log_storage_options)
        _enforce_locking_provider(
            request.path,
            storage,
            policy=mutation_policy,
        )
        _enforce_append_only_policy(
            policy=mutation_policy,
            operation="delete",
            updates_present=True,
        )
        commit_options = _delta_commit_options(
            commit_properties=request.commit_properties,
            commit_metadata=request.commit_metadata,
            app_id=None,
            app_version=None,
        )
        from datafusion_engine.delta.control_plane import DeltaDeleteRequest, delta_delete

        attempts = 0
        retry_policy = mutation_policy.retry_policy
        while True:
            try:
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
                break
            except Exception as exc:  # pragma: no cover - retry paths depend on delta-rs
                classification = _delta_retry_classification(exc, policy=retry_policy)
                if classification != "retryable":
                    raise
                attempts += 1
                if attempts >= retry_policy.max_attempts:
                    raise
                delay = _delta_retry_delay(attempts - 1, policy=retry_policy)
                span.set_attribute("codeanatomy.retry_attempt", attempts)
                time.sleep(delay)
        metrics = report.get("metrics") if isinstance(report, Mapping) else None
        if isinstance(metrics, Mapping):
            for key in ("rows_deleted", "deleted_rows", "num_deleted"):
                value = metrics.get(key)
                if value is not None:
                    rows_affected = coerce_int(value)
                    if rows_affected is not None:
                        span.set_attribute("codeanatomy.rows_affected", rows_affected)
                    break
        if attempts:
            span.set_attribute("codeanatomy.retry_attempts", attempts)
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

    Returns:
    -------
    Mapping[str, object]
        Control-plane mutation report payload.
    """
    delta_input = coerce_delta_input(request.source, prefer_reader=True)
    attrs = _storage_span_attributes(
        operation="merge",
        table_path=request.path,
        dataset_name=request.dataset_name,
        extra={
            "codeanatomy.source_rows": delta_input.row_count,
            "codeanatomy.has_filters": bool(request.predicate),
        },
    )
    with stage_span(
        "storage.merge",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        state = _prepare_delta_merge_execution_state(
            ctx,
            request=request,
            delta_input=delta_input,
        )
        try:
            result = _run_delta_merge_control_plane(state, span=span)
            _record_delta_merge_artifacts(request=request, result=result, span=span)
            return result.report
        finally:
            deregister_table(ctx, state.source_table)


def _prepare_delta_merge_execution_state(
    ctx: SessionContext,
    *,
    request: DeltaMergeArrowRequest,
    delta_input: DeltaInput,
) -> _DeltaMergeExecutionState:
    mutation_policy = _resolve_delta_mutation_policy(request.runtime_profile)
    storage = merged_storage_options(request.storage_options, request.log_storage_options)
    _enforce_locking_provider(request.path, storage, policy=mutation_policy)
    (
        source_alias,
        target_alias,
        matched_updates,
        not_matched_inserts,
        updates_present,
    ) = _resolve_merge_actions(request)
    _enforce_append_only_policy(
        policy=mutation_policy,
        operation="merge",
        updates_present=updates_present,
    )
    source_table = register_temp_table(ctx, delta_input.data)
    from datafusion_engine.delta.control_plane import DeltaMergeRequest

    try:
        commit_options = _delta_commit_options(
            commit_properties=request.commit_properties,
            commit_metadata=request.commit_metadata,
            app_id=None,
            app_version=None,
        )
        merge_request = DeltaMergeRequest(
            table_uri=request.path,
            storage_options=storage or None,
            version=None,
            timestamp=None,
            source_table=source_table,
            predicate=request.predicate,
            source_alias=source_alias,
            target_alias=target_alias,
            matched_predicate=request.matched_predicate,
            matched_updates=dict(matched_updates),
            not_matched_predicate=request.not_matched_predicate,
            not_matched_inserts=dict(not_matched_inserts),
            not_matched_by_source_predicate=request.not_matched_by_source_predicate,
            delete_not_matched_by_source=request.delete_not_matched_by_source,
            extra_constraints=request.extra_constraints,
            commit_options=commit_options,
        )
    except (RuntimeError, TypeError, ValueError):
        deregister_table(ctx, source_table)
        raise
    return _DeltaMergeExecutionState(
        ctx=ctx,
        request=request,
        delta_input=delta_input,
        mutation_policy=mutation_policy,
        storage=storage,
        source_alias=source_alias,
        target_alias=target_alias,
        matched_updates=matched_updates,
        not_matched_inserts=not_matched_inserts,
        source_table=source_table,
        merge_request=merge_request,
    )


def _run_delta_merge_control_plane(
    state: _DeltaMergeExecutionState,
    *,
    span: Span,
) -> _DeltaMergeExecutionResult:
    retry_policy = state.mutation_policy.retry_policy
    try:
        report, attempts = _execute_delta_merge(
            ctx=state.ctx,
            request=state.merge_request,
            retry_policy=retry_policy,
            span=span,
        )
    except Exception as exc:
        if not _should_fallback_delta_merge(exc):
            raise
        merge_fallback = True
        span.set_attribute("codeanatomy.merge_fallback", merge_fallback)
        span.set_attribute("codeanatomy.merge_fallback_error", str(exc))
        report = _execute_delta_merge_fallback(
            _DeltaMergeFallbackInput(
                source=state.delta_input.data,
                request=state.request,
                storage_options=state.storage,
                source_alias=state.source_alias,
                target_alias=state.target_alias,
                matched_updates=state.matched_updates,
                not_matched_inserts=state.not_matched_inserts,
            )
        )
        attempts = 0
    return _DeltaMergeExecutionResult(report=report, attempts=attempts)


def _record_delta_merge_artifacts(
    *,
    request: DeltaMergeArrowRequest,
    result: _DeltaMergeExecutionResult,
    span: Span,
) -> None:
    metrics: Mapping[str, object] | None = None
    if isinstance(result.report, Mapping):
        candidate = result.report.get("metrics")
        if isinstance(candidate, Mapping):
            metrics = candidate
    rows = _merge_rows_affected(metrics)
    if rows is not None:
        span.set_attribute("codeanatomy.rows_affected", rows)
    if result.attempts:
        span.set_attribute("codeanatomy.retry_attempts", result.attempts)
    _record_mutation_artifact(
        _MutationArtifactRequest(
            profile=request.runtime_profile,
            report=result.report,
            table_uri=request.path,
            operation="merge",
            mode="merge",
            commit_metadata=request.commit_metadata,
            commit_properties=request.commit_properties,
            constraint_status=_constraint_status(request.extra_constraints, checked=True),
            constraint_violations=(),
            dataset_name=request.dataset_name,
        )
    )


def delta_data_checker(request: DeltaDataCheckRequest) -> list[str]:
    """Validate incoming data against Delta table constraints.

    Args:
        request: Delta data-check request.

    Returns:
        list[str]: Result.

    Raises:
        TypeError: If table value types cannot be validated.
        ValueError: If Delta constraints are violated.
    """
    attrs = _storage_span_attributes(
        operation="data_check",
        table_path=request.table_path,
        extra={
            "codeanatomy.has_constraints": bool(request.extra_constraints),
            "codeanatomy.version": request.version,
            "codeanatomy.timestamp": request.timestamp,
        },
    )
    with stage_span(
        "storage.data_check",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        module = None
        for module_name in ("datafusion._internal", "datafusion_ext"):
            try:
                candidate = importlib.import_module(module_name)
            except ImportError:
                continue
            checker = getattr(candidate, "delta_data_checker", None)
            if callable(checker):
                module = candidate
                break
        if module is None:
            msg = "Delta data checks require datafusion._internal or datafusion_ext."
            raise ValueError(msg)
        checker = getattr(module, "delta_data_checker", None)
        if not callable(checker):
            msg = "Delta data checker is unavailable in the extension module."
            raise TypeError(msg)
        delta_input = coerce_delta_input(request.data, prefer_reader=False)
        data = delta_input.data
        if isinstance(data, RecordBatchReaderLike):
            data = data.read_all()
        payload = ipc_bytes(cast("pa.Table", data))
        storage = merged_storage_options(request.storage_options, request.log_storage_options)
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


def _record_delta_feature_mutation(request: _DeltaFeatureMutationRecord) -> None:
    if request.runtime_profile is None:
        return
    attrs = _storage_span_attributes(
        operation="feature_control",
        table_path=request.path,
        dataset_name=request.dataset_name,
        extra={"codeanatomy.feature_name": request.operation},
    )
    with stage_span(
        "storage.feature_control",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        from datafusion_engine.delta.observability import (
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
    from datafusion_engine.delta.observability import (
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
    dataset_name: str | None


def _commit_metadata_from_properties(commit_properties: CommitProperties) -> dict[str, str]:
    custom_metadata = getattr(commit_properties, "custom_metadata", None)
    if not isinstance(custom_metadata, Mapping):
        return {}
    return {str(key): str(value) for key, value in custom_metadata.items()}


def _record_mutation_artifact(request: _MutationArtifactRequest) -> None:
    if request.profile is None:
        return
    from datafusion_engine.delta.observability import (
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
        ),
    )


def build_commit_properties(
    *,
    app_id: str | None = None,
    version: int | None = None,
    commit_metadata: Mapping[str, str] | None = None,
) -> CommitProperties | None:
    """Return CommitProperties for Delta writes when needed.

    Returns:
    -------
    CommitProperties | None
        Commit properties with app transaction and custom metadata when provided.
    """
    custom_metadata = _normalize_commit_metadata(commit_metadata)
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

    Args:
        operation: Description.
        mode: Description.
        idempotent: Description.
        extra_metadata: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    metadata: dict[str, str] = {
        "codeanatomy_operation": str(operation),
        "codeanatomy_mode": str(mode),
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
                    from datafusion_engine.delta.control_plane import DeltaAppTransaction

                    app_transaction = DeltaAppTransaction(
                        app_id=txn_app_id,
                        version=txn_version,
                        last_updated=txn_last_updated
                        if isinstance(txn_last_updated, int)
                        else None,
                    )
    if commit_metadata:
        metadata.update(
            _normalize_commit_metadata(commit_metadata)
            or {str(key): str(value) for key, value in commit_metadata.items()}
        )
    if app_transaction is None and app_id is not None and app_version is not None:
        from datafusion_engine.delta.control_plane import DeltaAppTransaction

        app_transaction = DeltaAppTransaction(app_id=app_id, version=app_version)
    from datafusion_engine.delta.control_plane import DeltaCommitOptions

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
    storage = merged_storage_options(storage_options, log_storage_options)
    try:
        from datafusion_engine.delta.control_plane import DeltaCdfRequest, delta_cdf_provider

        return delta_cdf_provider(
            request=DeltaCdfRequest(
                table_uri=table_path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                options=cdf_options_to_spec(options),
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
    "DeltaWriteResult",
    "EncodingPolicy",
    "IdempotentWriteOptions",
    "SnapshotKey",
    "StorageOptions",
    "build_commit_properties",
    "canonical_table_uri",
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
    "enable_delta_change_data_feed",
    "enable_delta_check_constraints",
    "enable_delta_column_mapping",
    "enable_delta_deletion_vectors",
    "enable_delta_features",
    "enable_delta_in_commit_timestamps",
    "enable_delta_row_tracking",
    "enable_delta_v2_checkpoints",
    "idempotent_commit_properties",
    "read_delta_cdf",
    "read_delta_cdf_eager",
    "read_delta_table",
    "read_delta_table_eager",
    "snapshot_key_for_table",
    "vacuum_delta",
]
