"""Delta Lake feature-mutation write helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from storage.deltalake.delta_read import DeltaFeatureMutationOptions, delta_table_version
from storage.deltalake.delta_runtime_ops import (
    _DeltaFeatureMutationRecord,
    feature_control_span,
    record_delta_feature_mutation,
    runtime_profile_for_delta,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta.control_plane_core import DeltaFeatureEnableRequest


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
    with feature_control_span(options, operation="set_properties"):
        profile = runtime_profile_for_delta(options.runtime_profile)
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
        record_delta_feature_mutation(
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
    profile = runtime_profile_for_delta(options.runtime_profile)
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


def _enable_delta_feature(
    options: DeltaFeatureMutationOptions,
    *,
    operation: str,
    error_action: str,
    invoke: Callable[[SessionContext, DeltaFeatureEnableRequest], Mapping[str, object]],
) -> Mapping[str, object]:
    with feature_control_span(options, operation=operation):
        ctx, request = _feature_enable_request(options)
        try:
            report = invoke(ctx, request)
        except (ImportError, RuntimeError, TypeError, ValueError) as exc:
            msg = f"Failed to {error_action}: {exc}"
            raise RuntimeError(msg) from exc
        record_delta_feature_mutation(
            _DeltaFeatureMutationRecord(
                runtime_profile=options.runtime_profile,
                report=report,
                operation=operation,
                path=options.path,
                storage_options=options.storage_options,
                log_storage_options=options.log_storage_options,
                dataset_name=options.dataset_name,
                commit_metadata=options.commit_metadata,
            )
        )
    return report


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
    with feature_control_span(options, operation="add_constraints"):
        profile = runtime_profile_for_delta(options.runtime_profile)
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
        record_delta_feature_mutation(
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
    with feature_control_span(options, operation="drop_constraints"):
        profile = runtime_profile_for_delta(options.runtime_profile)
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
        record_delta_feature_mutation(
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
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_column_mapping

    return _enable_delta_feature(
        options,
        operation="enable_column_mapping",
        error_action="enable Delta column mapping",
        invoke=lambda ctx, request: delta_enable_column_mapping(
            ctx,
            request=request,
            mode=mode,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        ),
    )


def enable_delta_deletion_vectors(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta deletion vectors via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_deletion_vectors

    return _enable_delta_feature(
        options,
        operation="enable_deletion_vectors",
        error_action="enable Delta deletion vectors",
        invoke=lambda ctx, request: delta_enable_deletion_vectors(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        ),
    )


def enable_delta_row_tracking(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta row tracking via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_row_tracking

    return _enable_delta_feature(
        options,
        operation="enable_row_tracking",
        error_action="enable Delta row tracking",
        invoke=lambda ctx, request: delta_enable_row_tracking(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        ),
    )


def enable_delta_change_data_feed(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta change data feed via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_change_data_feed

    return _enable_delta_feature(
        options,
        operation="enable_change_data_feed",
        error_action="enable Delta change data feed",
        invoke=lambda ctx, request: delta_enable_change_data_feed(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        ),
    )


def enable_delta_check_constraints(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta check constraints via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_check_constraints

    return _enable_delta_feature(
        options,
        operation="enable_check_constraints",
        error_action="enable Delta check constraints",
        invoke=lambda ctx, request: delta_enable_check_constraints(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        ),
    )


def enable_delta_in_commit_timestamps(
    options: DeltaFeatureMutationOptions,
    *,
    enablement_version: int | None = None,
    enablement_timestamp: str | None = None,
) -> Mapping[str, object]:
    """Enable Delta in-commit timestamps via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_in_commit_timestamps

    return _enable_delta_feature(
        options,
        operation="enable_in_commit_timestamps",
        error_action="enable Delta in-commit timestamps",
        invoke=lambda ctx, request: delta_enable_in_commit_timestamps(
            ctx,
            request=request,
            enablement_version=enablement_version,
            enablement_timestamp=enablement_timestamp,
        ),
    )


def enable_delta_v2_checkpoints(
    options: DeltaFeatureMutationOptions,
    *,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta v2 checkpoints via the Rust control plane.

    Returns:
        Mapping[str, object]: Feature mutation report payload.
    """
    from datafusion_engine.delta.control_plane_core import delta_enable_v2_checkpoints

    return _enable_delta_feature(
        options,
        operation="enable_v2_checkpoints",
        error_action="enable Delta v2 checkpoints",
        invoke=lambda ctx, request: delta_enable_v2_checkpoints(
            ctx,
            request=request,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        ),
    )


__all__ = [
    "delta_add_constraints",
    "delta_drop_constraints",
    "enable_delta_change_data_feed",
    "enable_delta_check_constraints",
    "enable_delta_column_mapping",
    "enable_delta_deletion_vectors",
    "enable_delta_features",
    "enable_delta_in_commit_timestamps",
    "enable_delta_row_tracking",
    "enable_delta_v2_checkpoints",
]
