"""Dataset resolution pipeline for provider construction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from datafusion import SessionContext

from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_datafusion_provider,
    resolve_delta_feature_gate,
    resolve_delta_log_storage_options,
)
from datafusion_engine.delta_control_plane import (
    DeltaCdfRequest,
    DeltaProviderRequest,
    delta_cdf_provider,
    delta_provider_from_session,
    delta_provider_with_files,
)
from datafusion_engine.delta_protocol import validate_delta_gate
from datafusion_engine.delta_scan_config import (
    delta_scan_config_snapshot_from_options,
    delta_scan_identity_hash,
    resolve_delta_scan_options,
)
from utils.storage_options import merged_storage_options

if TYPE_CHECKING:
    from datafusion_engine.delta_control_plane import (
        DeltaProviderBundle,
    )
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DeltaScanOptions

DatasetProviderKind = Literal["delta", "delta_cdf"]


@dataclass(frozen=True)
class DatasetResolutionRequest:
    """Inputs required to resolve a dataset provider."""

    ctx: SessionContext
    location: DatasetLocation
    runtime_profile: DataFusionRuntimeProfile | None
    name: str | None = None
    predicate: str | None = None
    scan_files: Sequence[str] | None = None


@dataclass(frozen=True)
class DatasetResolution:
    """Resolved dataset provider plus canonical scan metadata."""

    name: str | None
    location: DatasetLocation
    provider: object
    provider_kind: DatasetProviderKind
    delta_snapshot: Mapping[str, object] | None
    delta_scan_config: Mapping[str, object] | None
    delta_scan_effective: Mapping[str, object] | None
    delta_scan_snapshot: object | None
    delta_scan_identity_hash: str | None
    delta_scan_options: DeltaScanOptions | None
    add_actions: Sequence[Mapping[str, object]] | None = None
    predicate_error: str | None = None
    cdf_options: Mapping[str, object] | None = None


def resolve_dataset_provider(request: DatasetResolutionRequest) -> DatasetResolution:
    """Resolve a dataset into a concrete provider and metadata bundle.

    Parameters
    ----------
    request : DatasetResolutionRequest
        Dataset resolution request payload.

    Returns
    -------
    DatasetResolution
        Resolved dataset provider bundle.
    """
    provider_kind = _provider_kind(request.location)
    if provider_kind == "delta_cdf":
        return _resolve_delta_cdf(
            location=request.location,
            name=request.name,
        )
    return _resolve_delta_table(request)


def _provider_kind(location: DatasetLocation) -> DatasetProviderKind:
    provider = resolve_datafusion_provider(location)
    if provider == "delta_cdf" or location.delta_cdf_options is not None:
        return "delta_cdf"
    return "delta"


def _resolve_delta_table(request: DatasetResolutionRequest) -> DatasetResolution:
    delta_scan = _resolve_delta_scan(request.location, request.runtime_profile)
    storage_options = merged_storage_options(
        request.location.storage_options,
        resolve_delta_log_storage_options(request.location),
    )
    gate = resolve_delta_feature_gate(request.location)
    provider_request = DeltaProviderRequest(
        table_uri=str(request.location.path),
        storage_options=storage_options,
        version=request.location.delta_version,
        timestamp=request.location.delta_timestamp,
        delta_scan=delta_scan,
        predicate=request.predicate,
        gate=gate,
    )
    bundle = _delta_provider_bundle(
        request.ctx,
        request=provider_request,
        scan_files=request.scan_files,
    )
    if gate is not None and bundle.snapshot is not None:
        validate_delta_gate(bundle.snapshot, gate)
    scan_snapshot = delta_scan_config_snapshot_from_options(delta_scan)
    return DatasetResolution(
        name=request.name,
        location=request.location,
        provider=bundle.provider,
        provider_kind="delta",
        delta_snapshot=bundle.snapshot,
        delta_scan_config=bundle.scan_config,
        delta_scan_effective=bundle.scan_effective,
        delta_scan_snapshot=scan_snapshot,
        delta_scan_identity_hash=delta_scan_identity_hash(scan_snapshot),
        delta_scan_options=delta_scan,
        add_actions=bundle.add_actions,
        predicate_error=bundle.predicate_error,
    )


def _delta_provider_bundle(
    ctx: SessionContext,
    *,
    request: DeltaProviderRequest,
    scan_files: Sequence[str] | None,
) -> DeltaProviderBundle:
    if scan_files:
        return delta_provider_with_files(ctx, files=scan_files, request=request)
    return delta_provider_from_session(ctx, request=request)


def _resolve_delta_cdf(*, location: DatasetLocation, name: str | None) -> DatasetResolution:
    storage_options = merged_storage_options(
        location.storage_options,
        resolve_delta_log_storage_options(location),
    )
    request = DeltaCdfRequest(
        table_uri=str(location.path),
        storage_options=storage_options,
        version=location.delta_version,
        timestamp=location.delta_timestamp,
        options=location.delta_cdf_options,
        gate=resolve_delta_feature_gate(location),
    )
    bundle = delta_cdf_provider(request=request)
    return DatasetResolution(
        name=name,
        location=location,
        provider=bundle.provider,
        provider_kind="delta_cdf",
        delta_snapshot=bundle.snapshot,
        delta_scan_config=None,
        delta_scan_effective=None,
        delta_scan_snapshot=None,
        delta_scan_identity_hash=None,
        delta_scan_options=None,
        cdf_options=bundle.cdf_options,
    )


def _resolve_delta_scan(
    location: DatasetLocation,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaScanOptions | None:
    delta_scan = resolve_delta_scan_options(location)
    if delta_scan is None:
        return None
    if delta_scan.schema_force_view_types is not None:
        return delta_scan
    enable_view_types = _schema_hardening_view_types(runtime_profile)
    return replace(delta_scan, schema_force_view_types=enable_view_types)


def _schema_hardening_view_types(runtime_profile: DataFusionRuntimeProfile | None) -> bool:
    if runtime_profile is None:
        return False
    hardening = runtime_profile.schema_hardening
    if hardening is not None:
        return hardening.enable_view_types
    return runtime_profile.schema_hardening_name == "arrow_performance"


__all__ = [
    "DatasetProviderKind",
    "DatasetResolution",
    "DatasetResolutionRequest",
    "resolve_dataset_provider",
]
