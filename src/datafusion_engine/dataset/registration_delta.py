"""Delta-focused helpers for dataset registration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datafusion_engine.dataset.registration_core import (
    _cache_prefix_for_registration,
    _delta_provider_artifact_payload,
    _delta_pruning_predicate,
    _DeltaProviderArtifactContext,
    _DeltaProviderRegistration,
    _DeltaRegistrationResult,
    _DeltaRegistrationState,
    _provider_for_registration,
    _record_delta_log_health,
    _record_delta_snapshot_if_applicable,
    _record_table_provider_artifact,
    _table_provider_capsule,
    _TableProviderArtifact,
    _update_table_provider_scan_config,
)
from datafusion_engine.dataset.resolution import (
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from datafusion_engine.io.adapter import DataFusionIOAdapter

if TYPE_CHECKING:
    from datafusion_engine.dataset.registration_core import DataFusionRegistrationContext


def _build_delta_provider_registration(
    context: DataFusionRegistrationContext,
) -> _DeltaProviderRegistration:
    """Resolve Delta provider registration payload.

    Returns:
    -------
    _DeltaProviderRegistration
        Provider-resolution result and predicate metadata.
    """
    location = context.location
    predicate_sql, predicate_error = _delta_pruning_predicate(context)
    resolution = resolve_dataset_provider(
        DatasetResolutionRequest(
            ctx=context.ctx,
            location=location,
            runtime_profile=context.runtime_profile,
            name=context.name,
            predicate=predicate_sql,
        )
    )
    combined_error = predicate_error or resolution.predicate_error
    return _DeltaProviderRegistration(
        resolution=resolution,
        predicate_sql=predicate_sql,
        predicate_error=combined_error,
    )


def _resolve_delta_registration_state(
    context: DataFusionRegistrationContext,
) -> _DeltaRegistrationState:
    """Resolve normalized registration state for Delta providers.

    Returns:
    -------
    _DeltaRegistrationState
        Prepared registration state for downstream registration flow.
    """
    registration = _build_delta_provider_registration(context)
    resolution = registration.resolution
    provider = resolution.provider
    provider_to_register = _provider_for_registration(provider)
    return _DeltaRegistrationState(
        registration=registration,
        resolution=resolution,
        adapter=DataFusionIOAdapter(ctx=context.ctx, profile=context.runtime_profile),
        provider=provider,
        provider_to_register=provider_to_register,
        provider_is_native=_table_provider_capsule(provider_to_register) is not None,
        format_name=context.location.format or "delta",
        cache_prefix=_cache_prefix_for_registration(context, resolution=resolution),
    )


def _delta_registration_mode(state: _DeltaRegistrationState) -> str:
    """Resolve provider-mode label for Delta registration artifacts.

    Returns:
    -------
    str
        Canonical provider-mode label.
    """
    if state.resolution.provider_kind == "delta_cdf":
        return "cdf_table_provider"
    return "delta_table_provider"


def _record_delta_table_registration_artifacts(
    state: _DeltaRegistrationState,
    context: DataFusionRegistrationContext,
    *,
    fingerprint_details: Mapping[str, object] | None,
    schema_identity_hash_value: str | None,
    ddl_fingerprint: str | None,
) -> None:
    """Record Delta registration diagnostics artifacts."""
    location = context.location
    resolution = state.resolution
    provider_mode = _delta_registration_mode(state)
    strict_enabled = (
        context.runtime_profile.features.enforce_delta_ffi_provider
        if context.runtime_profile is not None
        else None
    )
    strict_violation = bool(strict_enabled) and not state.provider_is_native
    details = _delta_provider_artifact_payload(
        context.ctx,
        location,
        context=_DeltaProviderArtifactContext(
            dataset_name=context.name,
            provider_mode=provider_mode,
            ffi_table_provider=state.provider_is_native,
            strict_native_provider_enabled=strict_enabled,
            strict_native_provider_violation=strict_violation,
            delta_scan=resolution.delta_scan_options,
            delta_scan_effective=resolution.delta_scan_effective,
            delta_scan_snapshot=resolution.delta_scan_snapshot,
            delta_scan_identity_hash=resolution.delta_scan_identity_hash,
            snapshot=resolution.delta_snapshot,
            registration_path="provider",
            predicate=state.registration.predicate_sql,
            predicate_error=state.registration.predicate_error,
            add_actions=resolution.add_actions,
        ),
    )
    if fingerprint_details:
        details.update(fingerprint_details)
    _record_table_provider_artifact(
        context.runtime_profile,
        artifact=_TableProviderArtifact(
            name=context.name,
            provider=state.provider,
            provider_kind=provider_mode,
            source=None,
            details=details,
        ),
    )
    _record_delta_log_health(
        context.runtime_profile,
        name=context.name,
        location=location,
        resolution=resolution,
    )
    _update_table_provider_scan_config(
        context.ctx,
        name=context.name,
        delta_scan_snapshot=resolution.delta_scan_snapshot,
        delta_scan_identity_hash=resolution.delta_scan_identity_hash,
        delta_scan_effective=resolution.delta_scan_effective,
    )
    _record_delta_snapshot_if_applicable(
        context,
        resolution=resolution,
        schema_identity_hash_value=schema_identity_hash_value,
        ddl_fingerprint=ddl_fingerprint,
    )


__all__ = [
    "_DeltaRegistrationResult",
    "_build_delta_provider_registration",
    "_delta_registration_mode",
    "_record_delta_table_registration_artifacts",
    "_resolve_delta_registration_state",
]
