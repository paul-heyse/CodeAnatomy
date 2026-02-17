"""Delta Lake feature-mutation write helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from storage.deltalake.delta_read import DeltaFeatureMutationOptions, delta_table_version
from storage.deltalake.delta_runtime_ops import (
    _DeltaFeatureMutationRecord,
    _feature_control_span,
    _record_delta_feature_mutation,
    _runtime_profile_for_delta,
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
