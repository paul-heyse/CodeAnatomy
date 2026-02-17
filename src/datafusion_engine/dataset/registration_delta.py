"""Delta-focused helpers for dataset registration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registration_cache import _maybe_cache
from datafusion_engine.dataset.registration_core import DeltaCdfArtifact
from datafusion_engine.dataset.registration_delta_helpers import (
    _cache_prefix_for_registration,
    _delta_cdf_artifact_payload,
    _delta_provider_artifact_payload,
    _delta_pruning_predicate,
    _DeltaProviderArtifactContext,
    _DeltaProviderRegistration,
    _DeltaRegistrationResult,
    _DeltaRegistrationState,
    _enforce_delta_native_provider_policy,
    _provider_for_registration,
    _record_delta_cdf_artifact,
    _record_delta_log_health,
    _record_delta_snapshot_if_applicable,
)
from datafusion_engine.dataset.registration_provider import (
    TableProviderArtifact as _TableProviderArtifact,
)
from datafusion_engine.dataset.registration_provider import (
    record_table_provider_artifact as _record_table_provider_artifact,
)
from datafusion_engine.dataset.registration_provider import (
    table_provider_capsule as _table_provider_capsule,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_capabilities as _update_table_provider_capabilities,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_fingerprints as _update_table_provider_fingerprints,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_scan_config as _update_table_provider_scan_config,
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


def _record_delta_cdf_registration_artifacts(
    state: _DeltaRegistrationState,
    context: DataFusionRegistrationContext,
    *,
    fingerprint_details: Mapping[str, object] | None,
) -> None:
    location = context.location
    resolution = state.resolution
    details = _delta_cdf_artifact_payload(location, resolution=resolution)
    details["ffi_table_provider"] = state.provider_is_native
    provider_mode = _delta_registration_mode(state)
    details["provider_mode"] = provider_mode
    strict_enabled = (
        context.runtime_profile.features.enforce_delta_ffi_provider
        if context.runtime_profile is not None
        else None
    )
    details["strict_native_provider_enabled"] = strict_enabled
    details["strict_native_provider_violation"] = (
        bool(strict_enabled) and not state.provider_is_native
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
    _record_delta_cdf_artifact(
        context.runtime_profile,
        artifact=DeltaCdfArtifact(
            name=context.name,
            path=str(location.path),
            provider="table_provider",
            options=location.delta_cdf_options,
            log_storage_options=location.delta_log_storage_options,
            snapshot=resolution.delta_snapshot,
        ),
    )


def _register_delta_provider_with_adapter(
    state: _DeltaRegistrationState,
    context: DataFusionRegistrationContext,
) -> _DeltaRegistrationResult:
    state.adapter.register_table(context.name, state.provider_to_register)
    return _DeltaRegistrationResult(
        df=context.ctx.table(context.name),
        cache_prefix=state.cache_prefix,
    )


def _register_delta_provider(
    context: DataFusionRegistrationContext,
) -> tuple[DataFrame, str | None]:
    state = _resolve_delta_registration_state(context)
    _enforce_delta_native_provider_policy(state, context)
    result = _register_delta_provider_with_adapter(state, context)
    schema_identity_hash_value, ddl_fingerprint, fingerprint_details = (
        _update_table_provider_fingerprints(
            context.ctx,
            name=context.name,
            schema=result.df.schema(),
        )
    )
    if state.resolution.provider_kind == "delta_cdf":
        _record_delta_cdf_registration_artifacts(
            state,
            context,
            fingerprint_details=fingerprint_details,
        )
    else:
        _record_delta_table_registration_artifacts(
            state,
            context,
            fingerprint_details=fingerprint_details,
            schema_identity_hash_value=schema_identity_hash_value,
            ddl_fingerprint=ddl_fingerprint,
        )
    _update_table_provider_capabilities(
        context.ctx,
        name=context.name,
        supports_insert=state.resolution.provider_kind != "delta_cdf",
        supports_cdf=(
            state.resolution.provider_kind == "delta_cdf"
            or context.location.delta_cdf_options is not None
        ),
    )
    return _maybe_cache(context, result.df), result.cache_prefix


__all__: list[str] = []
