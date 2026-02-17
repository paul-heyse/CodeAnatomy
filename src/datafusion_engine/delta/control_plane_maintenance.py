"""Maintenance and feature-mutation operations for the Delta control plane."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion import SessionContext

from datafusion_engine.delta.commit_payload import commit_payload_parts
from datafusion_engine.delta.control_plane_core import (
    DeltaAddConstraintsRequest,
    DeltaAddFeaturesRequest,
    DeltaCheckpointRequest,
    DeltaDropConstraintsRequest,
    DeltaFeatureEnableRequest,
    DeltaOptimizeRequest,
    DeltaRestoreRequest,
    DeltaSetPropertiesRequest,
    DeltaVacuumRequest,
    _internal_ctx,
    _raise_engine_error,
    _require_internal_entrypoint,
)
from datafusion_engine.delta.payload import commit_payload
from datafusion_engine.delta.protocol import delta_feature_gate_rust_payload
from datafusion_engine.errors import ErrorKind
from utils.validation import ensure_mapping


def delta_optimize_compact(
    ctx: SessionContext,
    *,
    request: DeltaOptimizeRequest,
) -> Mapping[str, object]:
    """Run Delta optimize/compact through control-plane authority.

    Returns:
    -------
    Mapping[str, object]
        Control-plane optimize report payload.
    """
    optimize_fn = _require_internal_entrypoint("delta_optimize_compact")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    z_order_payload = list(request.z_order_cols) if request.z_order_cols else None
    response = optimize_fn(
        _internal_ctx(ctx, entrypoint="delta_optimize_compact"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.target_size,
        z_order_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_optimize_compact")


def delta_vacuum(
    ctx: SessionContext,
    *,
    request: DeltaVacuumRequest,
) -> Mapping[str, object]:
    """Run Delta vacuum through control-plane authority.

    Returns:
    -------
    Mapping[str, object]
        Control-plane vacuum report payload.
    """
    vacuum_fn = _require_internal_entrypoint("delta_vacuum")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    response = vacuum_fn(
        _internal_ctx(ctx, entrypoint="delta_vacuum"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.retention_hours,
        request.dry_run,
        request.dry_run,
        request.enforce_retention_duration,
        request.require_vacuum_protocol_check,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_vacuum")


def delta_create_checkpoint(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Create a Delta checkpoint through control-plane authority.

    Returns:
    -------
    Mapping[str, object]
        Control-plane checkpoint report payload.
    """
    checkpoint_fn = _require_internal_entrypoint("delta_create_checkpoint")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    response = checkpoint_fn(
        _internal_ctx(ctx, entrypoint="delta_create_checkpoint"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        *delta_feature_gate_rust_payload(request.gate),
    )
    return ensure_mapping(response, label="delta_create_checkpoint")


def delta_restore(
    ctx: SessionContext,
    *,
    request: DeltaRestoreRequest,
) -> Mapping[str, object]:
    """Run Delta restore through control-plane authority.

    Returns:
        Mapping[str, object]: Control-plane restore report payload.
    """
    restore_fn = _require_internal_entrypoint("delta_restore")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    response = restore_fn(
        _internal_ctx(ctx, entrypoint="delta_restore"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.restore_version,
        request.restore_timestamp,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_restore")


def delta_set_properties(
    ctx: SessionContext,
    *,
    request: DeltaSetPropertiesRequest,
) -> Mapping[str, object]:
    """Run Delta set-properties through control-plane authority.

    Returns:
        Mapping[str, object]: Control-plane property update report payload.
    """
    if not request.properties:
        msg = "Delta property update requires at least one key/value pair."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    set_fn = _require_internal_entrypoint("delta_set_properties")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    properties_payload = sorted((str(key), str(value)) for key, value in request.properties.items())
    response = set_fn(
        _internal_ctx(ctx, entrypoint="delta_set_properties"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        properties_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_set_properties")


def delta_add_features(
    ctx: SessionContext,
    *,
    request: DeltaAddFeaturesRequest,
) -> Mapping[str, object]:
    """Run Delta add-features through control-plane authority.

    Returns:
        Mapping[str, object]: Control-plane feature update report payload.
    """
    if not request.features:
        msg = "Delta add-features requires at least one feature name."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    add_fn = _require_internal_entrypoint("delta_add_features")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    features_payload = [str(feature) for feature in request.features]
    response = add_fn(
        _internal_ctx(ctx, entrypoint="delta_add_features"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        features_payload,
        request.allow_protocol_versions_increase,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_add_features")


def delta_add_constraints(
    ctx: SessionContext,
    *,
    request: DeltaAddConstraintsRequest,
) -> Mapping[str, object]:
    """Run Delta add-constraints through control-plane authority.

    Returns:
        Mapping[str, object]: Control-plane constraint update report payload.
    """
    if not request.constraints:
        msg = "Delta add-constraints requires at least one constraint."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    add_fn = _require_internal_entrypoint("delta_add_constraints")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    constraints_payload = sorted(
        (str(name), str(expr)) for name, expr in request.constraints.items()
    )
    response = add_fn(
        _internal_ctx(ctx, entrypoint="delta_add_constraints"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        constraints_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_add_constraints")


def delta_drop_constraints(
    ctx: SessionContext,
    *,
    request: DeltaDropConstraintsRequest,
) -> Mapping[str, object]:
    """Run Delta drop-constraints through control-plane authority.

    Returns:
        Mapping[str, object]: Control-plane constraint removal report payload.
    """
    if not request.constraints:
        msg = "Delta drop-constraints requires at least one constraint name."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    drop_fn = _require_internal_entrypoint("delta_drop_constraints")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    response = drop_fn(
        _internal_ctx(ctx, entrypoint="delta_drop_constraints"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        [str(name) for name in request.constraints],
        request.raise_if_not_exists,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_drop_constraints")


def delta_cleanup_metadata(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Clean expired Delta log metadata through control-plane authority.

    Returns:
        Mapping[str, object]: Control-plane metadata cleanup report payload.
    """
    cleanup_fn = _require_internal_entrypoint("delta_cleanup_metadata")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    response = cleanup_fn(
        _internal_ctx(ctx, entrypoint="delta_cleanup_metadata"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        *delta_feature_gate_rust_payload(request.gate),
    )
    return ensure_mapping(response, label="delta_cleanup_metadata")


def _feature_reports(
    *,
    properties_report: Mapping[str, object] | None,
    features_report: Mapping[str, object] | None,
) -> Mapping[str, object]:
    payload: dict[str, object] = {}
    if properties_report is not None:
        payload["properties"] = properties_report
    if features_report is not None:
        payload["features"] = features_report
    return payload


def _feature_properties_report(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    properties: Mapping[str, str],
) -> Mapping[str, object]:
    return delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties=properties,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )


def _feature_add_report(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    features: Sequence[str],
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    return delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=features,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )


def _feature_toggle_report(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    properties: Mapping[str, str] | None = None,
    features: Sequence[str] | None = None,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    properties_report = (
        _feature_properties_report(ctx, request=request, properties=properties)
        if properties is not None
        else None
    )
    features_report = (
        _feature_add_report(
            ctx,
            request=request,
            features=features,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )
        if features is not None
        else None
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=features_report,
    )


def delta_enable_column_mapping(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    mode: str = "name",
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta column mapping with the requested mode.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={
            "delta.columnMapping.mode": mode,
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
        features=["columnMapping"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_deletion_vectors(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta deletion vectors (feature + property).

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableDeletionVectors": "true"},
        features=["deletionVectors"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_row_tracking(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta row tracking (feature + property).

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableRowTracking": "true"},
        features=["rowTracking"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_change_data_feed(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta change data feed (feature + property).

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableChangeDataFeed": "true"},
        features=["changeDataFeed"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_generated_columns(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta generated-columns feature.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        features=["generatedColumns"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_invariants(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta invariants feature.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        features=["invariants"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_check_constraints(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta check-constraints feature.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        features=["checkConstraints"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_in_commit_timestamps(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    enablement_version: int | None = None,
    enablement_timestamp: str | None = None,
) -> Mapping[str, object]:
    """Enable Delta in-commit timestamps via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    properties: dict[str, str] = {"delta.enableInCommitTimestamps": "true"}
    if enablement_version is not None:
        properties["delta.inCommitTimestampEnablementVersion"] = str(enablement_version)
    if enablement_timestamp is not None:
        properties["delta.inCommitTimestampEnablementTimestamp"] = enablement_timestamp
    return _feature_toggle_report(ctx, request=request, properties=properties)


def delta_enable_v2_checkpoints(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta v2 checkpoints via feature and checkpoint policy property.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.checkpointPolicy": "v2"},
        features=["v2Checkpoint"],
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_vacuum_protocol_check(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Enable Delta vacuum protocol check via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.vacuumProtocolCheck": "true"},
    )


def delta_enable_checkpoint_protection(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Enable Delta checkpoint protection via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.checkpointProtection": "true"},
    )


def delta_disable_change_data_feed(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta change data feed via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableChangeDataFeed": "false"},
    )


def delta_disable_deletion_vectors(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta deletion vectors via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableDeletionVectors": "false"},
    )


def delta_disable_row_tracking(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta row tracking via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableRowTracking": "false"},
    )


def delta_disable_in_commit_timestamps(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta in-commit timestamps via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.enableInCommitTimestamps": "false"},
    )


def delta_disable_vacuum_protocol_check(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta vacuum protocol check via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.vacuumProtocolCheck": "false"},
    )


def delta_disable_checkpoint_protection(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta checkpoint protection via table properties.

    Returns:
        Mapping[str, object]: Delta feature/property mutation report payload.
    """
    return _feature_toggle_report(
        ctx,
        request=request,
        properties={"delta.checkpointProtection": "false"},
    )


def delta_disable_column_mapping(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta column mapping cannot be disabled safely."""
    msg = "Delta column mapping cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_generated_columns(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta generated columns cannot be disabled safely."""
    msg = "Delta generated columns cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_invariants(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta invariants cannot be disabled safely."""
    msg = "Delta invariants cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_check_constraints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta check constraints must be dropped individually."""
    msg = "Delta check constraints must be dropped individually."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_v2_checkpoints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta v2 checkpoints cannot be disabled safely."""
    msg = "Delta v2 checkpoints cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


__all__ = [
    "DeltaAddConstraintsRequest",
    "DeltaAddFeaturesRequest",
    "DeltaCheckpointRequest",
    "DeltaDropConstraintsRequest",
    "DeltaFeatureEnableRequest",
    "DeltaOptimizeRequest",
    "DeltaRestoreRequest",
    "DeltaSetPropertiesRequest",
    "DeltaVacuumRequest",
    "delta_add_constraints",
    "delta_add_features",
    "delta_cleanup_metadata",
    "delta_create_checkpoint",
    "delta_disable_change_data_feed",
    "delta_disable_check_constraints",
    "delta_disable_checkpoint_protection",
    "delta_disable_column_mapping",
    "delta_disable_deletion_vectors",
    "delta_disable_generated_columns",
    "delta_disable_in_commit_timestamps",
    "delta_disable_invariants",
    "delta_disable_row_tracking",
    "delta_disable_v2_checkpoints",
    "delta_disable_vacuum_protocol_check",
    "delta_drop_constraints",
    "delta_enable_change_data_feed",
    "delta_enable_check_constraints",
    "delta_enable_checkpoint_protection",
    "delta_enable_column_mapping",
    "delta_enable_deletion_vectors",
    "delta_enable_generated_columns",
    "delta_enable_in_commit_timestamps",
    "delta_enable_invariants",
    "delta_enable_row_tracking",
    "delta_enable_v2_checkpoints",
    "delta_enable_vacuum_protocol_check",
    "delta_optimize_compact",
    "delta_restore",
    "delta_set_properties",
    "delta_vacuum",
]
