"""Delta Lake write entry points."""
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from deltalake import CommitProperties, Transaction

from datafusion_engine.session.helpers import deregister_table, register_temp_table
from storage.deltalake.delta_read import (
    DeltaDeleteWhereRequest,
    DeltaFeatureMutationOptions,
    DeltaMergeArrowRequest,
    DeltaWriteResult,
    IdempotentWriteOptions,
    _DeltaFeatureMutationRecord,
    _feature_control_span,
    _normalize_commit_metadata,
    _record_delta_feature_mutation,
    _runtime_profile_for_delta,
    coerce_delta_input,
)

if TYPE_CHECKING:
    from datafusion import SessionContext
    from opentelemetry.trace import Span

    from datafusion_engine.delta.control_plane_core import DeltaFeatureEnableRequest
    from storage.deltalake.delta_read import (
        DeltaInput,
        _DeltaMergeExecutionResult,
        _DeltaMergeExecutionState,
    )


def build_commit_properties(
    *,
    app_id: str | None = None,
    version: int | None = None,
    commit_metadata: Mapping[str, str] | None = None,
) -> CommitProperties | None:
    """Build commit metadata properties for Delta writes.

    Returns:
    -------
    CommitProperties | None
        Commit properties payload when metadata/transaction info is present.
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
    """Build idempotent commit metadata properties for Delta writes.

    Returns:
    -------
    CommitProperties
        Commit properties with deterministic operation metadata.

    Raises:
        RuntimeError: If commit metadata cannot be constructed.
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


def enable_delta_features(
    options: DeltaFeatureMutationOptions,
    *,
    features: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Enable Delta table features by setting table properties.

    Returns:
        dict[str, str]: Properties applied to the target table.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    from storage.deltalake.delta_read import delta_table_version
    from utils.storage_options import merged_storage_options

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
            from datafusion_engine.delta.control_plane_core import (
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
    from datafusion_engine.delta.control_plane_core import (
        DeltaCommitOptions,
        DeltaFeatureEnableRequest,
    )
    from utils.storage_options import merged_storage_options

    storage = merged_storage_options(options.storage_options, options.log_storage_options)
    profile = _runtime_profile_for_delta(options.runtime_profile)
    ctx = profile.delta_ops.delta_runtime_ctx()
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


def delta_add_constraints(
    options: DeltaFeatureMutationOptions,
    *,
    constraints: Mapping[str, str],
) -> Mapping[str, object]:
    """Add Delta check constraints via the Rust control plane.

    Returns:
        Mapping[str, object]: Constraint mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    from utils.storage_options import merged_storage_options

    if not constraints:
        return {}
    storage = merged_storage_options(options.storage_options, options.log_storage_options)
    with _feature_control_span(options, operation="add_constraints"):
        profile = _runtime_profile_for_delta(options.runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane_core import (
                DeltaAddConstraintsRequest,
                DeltaCommitOptions,
            )
            from datafusion_engine.delta.control_plane_core import (
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

    Returns:
        Mapping[str, object]: Constraint mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    from utils.storage_options import merged_storage_options

    if not constraints:
        return {}
    storage = merged_storage_options(options.storage_options, options.log_storage_options)
    with _feature_control_span(options, operation="drop_constraints"):
        profile = _runtime_profile_for_delta(options.runtime_profile)
        ctx = profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane_core import (
                DeltaCommitOptions,
                DeltaDropConstraintsRequest,
            )
            from datafusion_engine.delta.control_plane_core import (
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

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_column_mapping"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_column_mapping

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

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_deletion_vectors"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_deletion_vectors

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

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_row_tracking"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_row_tracking

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
    """Enable Delta change data feed via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_change_data_feed"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_change_data_feed

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

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_check_constraints"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_check_constraints

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

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_in_commit_timestamps"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_in_commit_timestamps

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

    Returns:
        Mapping[str, object]: Feature mutation report payload.

    Raises:
        RuntimeError: If the Rust control-plane call fails.
    """
    with _feature_control_span(options, operation="enable_v2_checkpoints"):
        ctx, request = _feature_enable_request(options)
        try:
            from datafusion_engine.delta.control_plane_core import delta_enable_v2_checkpoints

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


def delta_delete_where(
    ctx: SessionContext,
    *,
    request: DeltaDeleteWhereRequest,
) -> Mapping[str, object]:
    """Delete rows from a Delta table via the Rust control plane.

    Returns:
        Mapping[str, object]: Delete mutation report payload.
    """
    import time

    from obs.otel import SCOPE_STORAGE, stage_span
    from storage.deltalake.delta_read import (
        _constraint_status,
        _delta_commit_options,
        _delta_retry_classification,
        _delta_retry_delay,
        _enforce_append_only_policy,
        _enforce_locking_provider,
        _MutationArtifactRequest,
        _record_mutation_artifact,
        _resolve_delta_mutation_policy,
        _storage_span_attributes,
    )
    from utils.storage_options import merged_storage_options
    from utils.value_coercion import coerce_int

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
        from datafusion_engine.delta.control_plane_core import DeltaDeleteRequest, delta_delete

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


def _build_delta_merge_state(
    ctx: SessionContext,
    *,
    request: DeltaMergeArrowRequest,
    delta_input: DeltaInput,
) -> _DeltaMergeExecutionState:
    from datafusion_engine.delta.control_plane_core import DeltaMergeRequest
    from storage.deltalake.delta_read import (
        _delta_commit_options,
        _DeltaMergeExecutionState,
        _enforce_append_only_policy,
        _enforce_locking_provider,
        _resolve_delta_mutation_policy,
        _resolve_merge_actions,
    )
    from utils.storage_options import merged_storage_options

    mutation_policy = _resolve_delta_mutation_policy(request.runtime_profile)
    storage = merged_storage_options(request.storage_options, request.log_storage_options)
    _enforce_locking_provider(request.path, storage, policy=mutation_policy)
    source_alias, target_alias, matched_updates, not_matched_inserts, updates_present = (
        _resolve_merge_actions(request)
    )
    _enforce_append_only_policy(
        policy=mutation_policy,
        operation="merge",
        updates_present=updates_present,
    )
    source_table = register_temp_table(ctx, delta_input.data)
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


def _execute_delta_merge_state(
    *,
    state: _DeltaMergeExecutionState,
    span: Span,
) -> _DeltaMergeExecutionResult:
    from storage.deltalake.delta_read import (
        _DeltaMergeExecutionResult,
        _DeltaMergeFallbackInput,
        _execute_delta_merge,
        _execute_delta_merge_fallback,
        _should_fallback_delta_merge,
    )

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


def delta_merge_arrow(
    ctx: SessionContext,
    *,
    request: DeltaMergeArrowRequest,
) -> Mapping[str, object]:
    """Merge Arrow data into a Delta table via the Rust control plane.

    Returns:
        Mapping[str, object]: Merge mutation report payload.
    """
    from obs.otel import SCOPE_STORAGE, stage_span
    from storage.deltalake.delta_read import (
        _constraint_status,
        _merge_rows_affected,
        _MutationArtifactRequest,
        _record_mutation_artifact,
        _storage_span_attributes,
    )

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
        state = _build_delta_merge_state(ctx, request=request, delta_input=delta_input)
        try:
            result = _execute_delta_merge_state(state=state, span=span)
            metrics: Mapping[str, object] | None = None
            if isinstance(result.report, Mapping):
                candidate = result.report.get("metrics")
                if isinstance(candidate, Mapping):
                    metrics = {str(key): value for key, value in candidate.items()}
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
            return result.report
        finally:
            deregister_table(ctx, state.source_table)


__all__ = [
    "DeltaFeatureMutationOptions",
    "DeltaWriteResult",
    "IdempotentWriteOptions",
    "build_commit_properties",
    "delta_add_constraints",
    "delta_delete_where",
    "delta_drop_constraints",
    "delta_merge_arrow",
    "enable_delta_change_data_feed",
    "enable_delta_check_constraints",
    "enable_delta_column_mapping",
    "enable_delta_deletion_vectors",
    "enable_delta_features",
    "enable_delta_in_commit_timestamps",
    "enable_delta_row_tracking",
    "enable_delta_v2_checkpoints",
    "idempotent_commit_properties",
]
