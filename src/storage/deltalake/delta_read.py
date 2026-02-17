"""Delta Lake read/write helpers for Arrow tables."""
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable
from deltalake.exceptions import TableNotFoundError

from arrow_utils.core.streaming import to_reader
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    as_reader,
    coerce_table_like,
)
from datafusion_engine.encoding import apply_encoding
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.schema.alignment import align_table
from obs.otel import SCOPE_STORAGE, get_query_id, get_run_id, stage_span
from storage.deltalake.config import DeltaMutationPolicy, DeltaRetryPolicy
from utils.storage_options import merged_storage_options
from utils.value_coercion import coerce_int, coerce_str_list

if TYPE_CHECKING:
    from datafusion import SessionContext
    from opentelemetry.trace import Span

    from datafusion_engine.delta.control_plane_core import (
        DeltaAppTransaction,
        DeltaCdfProviderBundle,
        DeltaCommitOptions,
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


@dataclass(frozen=True)
class SnapshotKey:
    """Deterministic Delta snapshot identity."""

    canonical_uri: str
    version: int


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
    from datafusion_engine.delta.control_plane_core import delta_merge

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
    from storage.deltalake.delta_write import build_commit_properties

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
        from datafusion_engine.delta.control_plane_core import (
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
                from schema_spec.dataset_spec import DeltaPolicyBundle

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
                    from datafusion_engine.delta.control_plane_core import DeltaAppTransaction

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
        from datafusion_engine.delta.control_plane_core import DeltaAppTransaction

        app_transaction = DeltaAppTransaction(app_id=app_id, version=app_version)
    from datafusion_engine.delta.control_plane_core import DeltaCommitOptions

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
        from datafusion_engine.delta.control_plane_core import DeltaCdfRequest, delta_cdf_provider

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


from storage.deltalake import delta_write as _delta_write

enable_delta_features = _delta_write.enable_delta_features
delta_add_constraints = _delta_write.delta_add_constraints
delta_drop_constraints = _delta_write.delta_drop_constraints
enable_delta_column_mapping = _delta_write.enable_delta_column_mapping
enable_delta_deletion_vectors = _delta_write.enable_delta_deletion_vectors
enable_delta_row_tracking = _delta_write.enable_delta_row_tracking
enable_delta_change_data_feed = _delta_write.enable_delta_change_data_feed
enable_delta_check_constraints = _delta_write.enable_delta_check_constraints
enable_delta_in_commit_timestamps = _delta_write.enable_delta_in_commit_timestamps
enable_delta_v2_checkpoints = _delta_write.enable_delta_v2_checkpoints
delta_delete_where = _delta_write.delta_delete_where
delta_merge_arrow = _delta_write.delta_merge_arrow


__all__ = [
    "DeltaCdfOptions",
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
    "coerce_delta_table",
    "delta_add_constraints",
    "delta_cdf_enabled",
    "delta_commit_metadata",
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
    "read_delta_cdf",
    "read_delta_cdf_eager",
    "read_delta_table",
    "read_delta_table_eager",
]
