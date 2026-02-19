"""Delta Lake runtime/mutation helpers extracted from delta_read."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

from deltalake import CommitProperties

from arrow_utils.core.streaming import to_reader
from datafusion_engine.arrow.interop import RecordBatchReaderLike
from datafusion_engine.delta.shared_types import DeltaFeatureGate, DeltaProtocolSnapshot
from obs.otel import SCOPE_STORAGE, get_query_id, get_run_id, stage_span
from storage.deltalake.config import DeltaMutationPolicy, DeltaRetryPolicy
from storage.deltalake.delta_read import (
    DeltaCdfOptions,
    DeltaFeatureMutationOptions,
    DeltaMergeArrowRequest,
    DeltaSnapshotLookup,
    _DeltaFeatureMutationRecord,
    _DeltaMaintenanceRecord,
    _normalize_commit_metadata,
    _snapshot_info,
    cdf_options_to_spec,
    read_delta_cdf_eager,
)
from utils.storage_options import merged_storage_options
from utils.value_coercion import coerce_int, coerce_str_list

if TYPE_CHECKING:
    from datafusion import SessionContext
    from opentelemetry.trace import Span

    from datafusion_engine.delta.control_plane_core import (
        DeltaCdfProviderBundle,
        DeltaCommitOptions,
        DeltaMergeRequest,
    )
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

type StorageOptions = Mapping[str, str]


def runtime_profile_for_delta(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRuntimeProfile:
    """Require and return a runtime profile for Delta operations.

    Returns:
    -------
    DataFusionRuntimeProfile
        The provided runtime profile.

    Raises:
        ValueError: If no runtime profile is provided.
    """
    if runtime_profile is not None:
        return runtime_profile
    msg = "Delta operation requires an explicit DataFusionRuntimeProfile."
    raise ValueError(msg)


def resolve_delta_mutation_policy(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaMutationPolicy:
    """Resolve the effective mutation policy from runtime configuration.

    Returns:
    -------
    DeltaMutationPolicy
        Runtime policy when available, otherwise defaults.
    """
    if runtime_profile is None:
        return DeltaMutationPolicy()
    return runtime_profile.policies.delta_mutation_policy or DeltaMutationPolicy()


def is_s3_uri(table_uri: str) -> bool:
    """Return whether the given table URI uses an S3-compatible scheme.

    Returns:
    -------
    bool
        ``True`` when the URI scheme is ``s3``, ``s3a``, or ``s3n``.
    """
    scheme = urlparse(table_uri).scheme.lower()
    return scheme in {"s3", "s3a", "s3n"}


def has_locking_provider(
    storage_options: StorageOptions | None,
    *,
    policy: DeltaMutationPolicy,
) -> bool:
    """Return whether storage options include a configured locking provider.

    Returns:
    -------
    bool
        ``True`` when any configured locking option key has a value.
    """
    if not storage_options:
        return False
    normalized = {str(key).lower(): str(value) for key, value in storage_options.items()}
    for key in policy.locking_option_keys:
        value = normalized.get(str(key).lower())
        if value:
            return True
    return False


def enforce_locking_provider(
    table_uri: str,
    storage_options: StorageOptions | None,
    *,
    policy: DeltaMutationPolicy,
) -> None:
    """Validate locking-provider requirements for S3-backed Delta mutations.

    Raises:
        ValueError: If policy requires locking for S3 and no provider is configured.
    """
    if not policy.require_locking_provider:
        return
    if not is_s3_uri(table_uri):
        return
    if has_locking_provider(storage_options, policy=policy):
        return
    msg = (
        "Delta mutations on S3 require a locking provider. "
        f"Set one of {tuple(policy.locking_option_keys)} in storage_options."
    )
    raise ValueError(msg)


def enforce_append_only_policy(
    *,
    policy: DeltaMutationPolicy,
    operation: str,
    updates_present: bool,
) -> None:
    """Validate append-only mutation constraints.

    Raises:
        ValueError: If the requested operation would mutate existing rows.
    """
    if not policy.append_only:
        return
    if operation == "delete" or updates_present:
        msg = "Delta mutation rejected: append_only policy forbids deletes or updates."
        raise ValueError(msg)


def delta_retry_classification(
    exc: BaseException,
    *,
    policy: DeltaRetryPolicy,
) -> str:
    """Classify an exception for Delta retry behavior.

    Returns:
    -------
    str
        Retry classification: ``"fatal"``, ``"retryable"``, or ``"unknown"``.
    """
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


def delta_retry_delay(attempt: int, *, policy: DeltaRetryPolicy) -> float:
    """Compute exponential backoff delay for a retry attempt.

    Returns:
    -------
    float
        Delay in seconds, bounded by policy maximum.
    """
    delay = float(policy.base_delay_s) * (2**attempt)
    return min(delay, float(policy.max_delay_s))


def retry_with_policy[T](
    operation: Callable[[], T],
    *,
    policy: DeltaRetryPolicy,
    span: Span | None = None,
) -> tuple[T, int]:
    """Run an operation with Delta retry classification/backoff semantics.

    Returns:
    -------
    tuple[T, int]
        Operation result and retry attempt count.
    """
    attempts = 0
    while True:
        try:
            return operation(), attempts
        except Exception as exc:  # pragma: no cover - retry paths depend on delta-rs
            classification = delta_retry_classification(exc, policy=policy)
            if classification != "retryable":
                raise
            attempts += 1
            if attempts >= policy.max_attempts:
                raise
            delay = delta_retry_delay(attempts - 1, policy=policy)
            if span is not None:
                span.set_attribute("codeanatomy.retry_attempt", attempts)
            time.sleep(delay)


def resolve_merge_actions(
    request: DeltaMergeArrowRequest,
) -> tuple[str, str, dict[str, str], dict[str, str], bool]:
    """Resolve effective aliases and merge action maps for an Arrow merge request.

    Returns:
    -------
    tuple[str, str, dict[str, str], dict[str, str], bool]
        Source alias, target alias, matched updates, not-matched inserts, and
        whether update/delete behavior is present.
    """
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


def merge_rows_affected(metrics: Mapping[str, object] | None) -> int | None:
    """Estimate total affected rows from merge metrics.

    Returns:
    -------
    int | None
        Total affected rows when metrics include row counters; otherwise ``None``.
    """
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


def execute_delta_merge(
    ctx: SessionContext,
    *,
    request: DeltaMergeRequest,
    retry_policy: DeltaRetryPolicy,
    span: Span,
) -> tuple[Mapping[str, object], int]:
    """Execute a Delta merge with retry behavior driven by policy.

    Returns:
    -------
    tuple[Mapping[str, object], int]
        Merge report payload and number of retries performed.
    """
    from datafusion_engine.delta.control_plane_core import delta_merge

    return retry_with_policy(
        lambda: delta_merge(ctx, request=request),
        policy=retry_policy,
        span=span,
    )


def storage_span_attributes(
    *,
    operation: str,
    table_path: str | None = None,
    dataset_name: str | None = None,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build OpenTelemetry span attributes for storage operations.

    Returns:
    -------
    dict[str, object]
        Attribute payload for storage operation spans.
    """
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


def feature_control_span(
    options: DeltaFeatureMutationOptions,
    *,
    operation: str,
) -> AbstractContextManager[Span]:
    """Create a feature-control tracing span for Delta feature mutations.

    Returns:
    -------
    AbstractContextManager[Span]
        Context manager that opens and closes the feature-control span.
    """
    attrs = storage_span_attributes(
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
    attrs = storage_span_attributes(
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
    attrs = storage_span_attributes(
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
        properties = snapshot.get("table_properties")
        if not isinstance(properties, Mapping):
            return None
        metadata = {str(key): str(value) for key, value in properties.items()}
        return metadata or None


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
    attrs = storage_span_attributes(
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
    attrs = storage_span_attributes(
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
        from datafusion_engine.delta.shared_types import DeltaProtocolSnapshot

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


def read_delta_cdf(
    table_path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    cdf_options: DeltaCdfOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> RecordBatchReaderLike:
    """Read change data feed from a Delta table.

    Args:
        table_path: Delta table path.
        storage_options: Optional storage options.
        log_storage_options: Optional log-store options.
        cdf_options: Optional CDF read options.
        runtime_profile: Optional runtime profile used for telemetry and diagnostics.
        runtime_profile: Optional runtime profile providing a session context.

    Returns:
        RecordBatchReaderLike: Result.

    Raises:
        ValueError: If CDF read options are invalid.
    """
    resolved_options = cdf_options or DeltaCdfOptions()
    attrs = storage_span_attributes(
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
        if runtime_profile is not None:
            ctx = runtime_profile.session_context()
        else:
            from datafusion import SessionContext

            ctx = SessionContext()
        bundle = delta_cdf_table_provider(
            ctx,
            table_path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            options=resolved_options,
        )
        if bundle is None:
            msg = "Delta CDF provider requires Rust control-plane support."
            raise ValueError(msg)
        provider = bundle.provider
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


def record_delta_feature_mutation(request: _DeltaFeatureMutationRecord) -> None:
    """Record a mutation artifact for Delta feature-control operations."""
    if request.runtime_profile is None:
        return
    attrs = storage_span_attributes(
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


def record_delta_maintenance(request: _DeltaMaintenanceRecord) -> None:
    """Record a maintenance artifact for Delta maintenance operations."""
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


def constraint_status(
    extra_constraints: Sequence[str] | None,
    *,
    checked: bool,
) -> str:
    """Resolve constraint-check status for mutation reporting.

    Returns:
    -------
    str
        ``"skipped"``, ``"passed"``, or ``"not_applicable"``.
    """
    if not extra_constraints:
        return "skipped"
    return "passed" if checked else "not_applicable"


@dataclass(frozen=True)
class MutationArtifactRequest:
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


def commit_metadata_from_properties(commit_properties: CommitProperties) -> dict[str, str]:
    """Extract custom commit metadata from ``CommitProperties``.

    Returns:
    -------
    dict[str, str]
        Stringified custom metadata dictionary.
    """
    custom_metadata = getattr(commit_properties, "custom_metadata", None)
    if not isinstance(custom_metadata, Mapping):
        return {}
    return {str(key): str(value) for key, value in custom_metadata.items()}


def record_mutation_artifact(request: MutationArtifactRequest) -> None:
    """Emit a normalized Delta mutation artifact from a mutation request."""
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
        commit_payload = commit_metadata_from_properties(request.commit_properties)
        commit_app_id = commit_payload.get("commit_app_id")
        commit_version_value = commit_payload.get("commit_version")
        commit_run_id = commit_payload.get("commit_run_id")
        if isinstance(commit_version_value, str) and commit_version_value.isdigit():
            commit_version = int(commit_version_value)
    commit_metadata = request.commit_metadata
    if commit_metadata is None and request.commit_properties is not None:
        commit_metadata = commit_metadata_from_properties(request.commit_properties)
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


def delta_commit_options(
    *,
    commit_properties: CommitProperties | None,
    commit_metadata: Mapping[str, str] | None,
    app_id: str | None,
    app_version: int | None,
) -> DeltaCommitOptions:
    """Build control-plane commit options from Python delta commit inputs.

    Returns:
    -------
    DeltaCommitOptions
        Normalized metadata and optional app transaction payload.
    """
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


def mutation_version(report: Mapping[str, object]) -> int | None:
    """Extract mutation version from a mutation/maintenance report payload.

    Returns:
    -------
    int | None
        Parsed version when present and valid.
    """
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


def delta_json_value(value: object) -> dict[str, object]:
    """Normalize a value into a JSON-object-shaped payload.

    Returns:
    -------
    dict[str, object]
        JSON-serializable mapping for the input value.
    """
    if isinstance(value, dict):
        return {str(key): delta_json_scalar(item) for key, item in value.items()}
    return {"value": delta_json_scalar(value)}


def delta_json_scalar(value: object) -> object:
    """Normalize scalar/container values into JSON-serializable structures.

    Returns:
    -------
    object
        JSON-serializable scalar, list, or mapping representation.
    """
    if value is None or isinstance(value, (bool, float, int, str)):
        return value
    if isinstance(value, list):
        return [delta_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [delta_json_scalar(item) for item in value]
    if isinstance(value, dict):
        return {str(key): delta_json_scalar(item) for key, item in value.items()}
    return str(value)


def delta_cdf_table_provider(
    ctx: SessionContext,
    table_path: str,
    *,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
    options: DeltaCdfOptions | None,
) -> DeltaCdfProviderBundle | None:
    """Build a Rust control-plane provider bundle for Delta CDF reads.

    Returns:
    -------
    DeltaCdfProviderBundle | None
        Provider bundle when extension support is available, else ``None``.
    """
    storage = merged_storage_options(storage_options, log_storage_options)
    try:
        from datafusion_engine.delta.control_plane_core import DeltaCdfRequest, delta_cdf_provider

        return delta_cdf_provider(
            ctx,
            request=DeltaCdfRequest(
                table_uri=table_path,
                storage_options=storage or None,
                version=None,
                timestamp=None,
                options=cdf_options_to_spec(options),
            ),
        )
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None


__all__ = [
    "MutationArtifactRequest",
    "commit_metadata_from_properties",
    "constraint_status",
    "delta_cdf_enabled",
    "delta_cdf_table_provider",
    "delta_commit_metadata",
    "delta_commit_options",
    "delta_history_snapshot",
    "delta_json_scalar",
    "delta_json_value",
    "delta_protocol_snapshot",
    "delta_retry_classification",
    "delta_retry_delay",
    "delta_table_features",
    "enforce_append_only_policy",
    "enforce_locking_provider",
    "execute_delta_merge",
    "feature_control_span",
    "has_locking_provider",
    "is_s3_uri",
    "merge_rows_affected",
    "mutation_version",
    "read_delta_cdf",
    "read_delta_cdf_eager",
    "record_delta_feature_mutation",
    "record_delta_maintenance",
    "record_mutation_artifact",
    "resolve_delta_mutation_policy",
    "resolve_merge_actions",
    "retry_with_policy",
    "runtime_profile_for_delta",
    "storage_span_attributes",
]
