"""Delta Lake write entry points."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from deltalake import CommitProperties, Transaction

from datafusion_engine.session.helpers import deregister_table, register_temp_table
from storage.deltalake.delta_feature_mutations import (
    delta_add_constraints,
    delta_drop_constraints,
    enable_delta_change_data_feed,
    enable_delta_check_constraints,
    enable_delta_column_mapping,
    enable_delta_deletion_vectors,
    enable_delta_features,
    enable_delta_in_commit_timestamps,
    enable_delta_row_tracking,
    enable_delta_v2_checkpoints,
)
from storage.deltalake.delta_read import (
    DeltaDeleteWhereRequest,
    DeltaFeatureMutationOptions,
    DeltaMergeArrowRequest,
    DeltaWriteResult,
    IdempotentWriteOptions,
    _normalize_commit_metadata,
    coerce_delta_input,
)

if TYPE_CHECKING:
    from datafusion import SessionContext
    from opentelemetry.trace import Span

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


def delta_delete_where(
    ctx: SessionContext,
    *,
    request: DeltaDeleteWhereRequest,
) -> Mapping[str, object]:
    """Delete rows from a Delta table via the Rust control plane.

    Returns:
    -------
    Mapping[str, object]
        Delete mutation report payload.

    Raises:
        ValueError: If the delete predicate is empty or whitespace-only.
    """
    from obs.otel import SCOPE_STORAGE, stage_span
    from storage.deltalake.delta_runtime_ops import (
        MutationArtifactRequest,
        constraint_status,
        delta_commit_options,
        enforce_append_only_policy,
        enforce_locking_provider,
        record_mutation_artifact,
        retry_with_policy,
        resolve_delta_mutation_policy,
        storage_span_attributes,
    )
    from utils.storage_options import merged_storage_options
    from utils.value_coercion import coerce_int

    cleaned_predicate = request.predicate.strip()
    if not cleaned_predicate:
        msg = (
            "delta_delete_where requires a non-empty predicate. "
            "Use an explicit full-table delete operation instead."
        )
        raise ValueError(msg)

    attrs = storage_span_attributes(
        operation="delete",
        table_path=request.path,
        dataset_name=request.dataset_name,
        extra={"codeanatomy.has_filters": True},
    )
    with stage_span(
        "storage.delete",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        mutation_policy = resolve_delta_mutation_policy(request.runtime_profile)
        storage = merged_storage_options(request.storage_options, request.log_storage_options)
        enforce_locking_provider(
            request.path,
            storage,
            policy=mutation_policy,
        )
        enforce_append_only_policy(
            policy=mutation_policy,
            operation="delete",
            updates_present=True,
        )
        commit_options = delta_commit_options(
            commit_properties=request.commit_properties,
            commit_metadata=request.commit_metadata,
            app_id=None,
            app_version=None,
        )
        from datafusion_engine.delta.control_plane_core import DeltaDeleteRequest, delta_delete

        retry_policy = mutation_policy.retry_policy
        report, attempts = retry_with_policy(
            lambda: delta_delete(
                ctx,
                request=DeltaDeleteRequest(
                    table_uri=request.path,
                    storage_options=storage or None,
                    version=None,
                    timestamp=None,
                    predicate=cleaned_predicate,
                    extra_constraints=request.extra_constraints,
                    commit_options=commit_options,
                ),
            ),
            policy=retry_policy,
            span=span,
        )
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
        record_mutation_artifact(
            MutationArtifactRequest(
                profile=request.runtime_profile,
                report=report,
                table_uri=request.path,
                operation="delete",
                mode="delete",
                commit_metadata=request.commit_metadata,
                commit_properties=request.commit_properties,
                constraint_status=constraint_status(request.extra_constraints, checked=False),
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
    from storage.deltalake.delta_read import _DeltaMergeExecutionState
    from storage.deltalake.delta_runtime_ops import (
        delta_commit_options,
        enforce_append_only_policy,
        enforce_locking_provider,
        resolve_delta_mutation_policy,
        resolve_merge_actions,
    )
    from utils.storage_options import merged_storage_options

    mutation_policy = resolve_delta_mutation_policy(request.runtime_profile)
    storage = merged_storage_options(request.storage_options, request.log_storage_options)
    enforce_locking_provider(request.path, storage, policy=mutation_policy)
    source_alias, target_alias, matched_updates, not_matched_inserts, updates_present = (
        resolve_merge_actions(request)
    )
    enforce_append_only_policy(
        policy=mutation_policy,
        operation="merge",
        updates_present=updates_present,
    )
    source_table = register_temp_table(ctx, delta_input.data)
    try:
        commit_options = delta_commit_options(
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
    from storage.deltalake.delta_read import _DeltaMergeExecutionResult
    from storage.deltalake.delta_runtime_ops import execute_delta_merge

    retry_policy = state.mutation_policy.retry_policy
    report, attempts = execute_delta_merge(
        ctx=state.ctx,
        request=state.merge_request,
        retry_policy=retry_policy,
        span=span,
    )
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
    from storage.deltalake.delta_runtime_ops import (
        MutationArtifactRequest,
        constraint_status,
        merge_rows_affected,
        record_mutation_artifact,
        storage_span_attributes,
    )

    delta_input = coerce_delta_input(request.source, prefer_reader=True)
    attrs = storage_span_attributes(
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
            rows = merge_rows_affected(metrics)
            if rows is not None:
                span.set_attribute("codeanatomy.rows_affected", rows)
            if result.attempts:
                span.set_attribute("codeanatomy.retry_attempts", result.attempts)
            record_mutation_artifact(
                MutationArtifactRequest(
                    profile=request.runtime_profile,
                    report=result.report,
                    table_uri=request.path,
                    operation="merge",
                    mode="merge",
                    commit_metadata=request.commit_metadata,
                    commit_properties=request.commit_properties,
                    constraint_status=constraint_status(request.extra_constraints, checked=True),
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
