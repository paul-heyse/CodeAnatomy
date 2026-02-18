"""Canonical Delta provider artifact payloads."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility
from datafusion_engine.delta.scan_config import delta_scan_config_snapshot_from_options
from serde_msgspec import StructBaseStrict, to_builtins
from storage.deltalake.delta_metadata import canonical_table_uri
from utils.hashing import hash_msgpack_canonical
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from schema_spec.scan_options import DeltaScanOptions


class DeltaProviderBuildResult(StructBaseStrict, frozen=True):
    """Canonical payload describing Delta provider construction."""

    event_time_unix_ms: int | None = None
    run_id: str | None = None
    dataset_name: str | None = None
    path: str | None = None
    table_uri: str | None = None
    format: str | None = None
    provider_kind: str | None = None
    provider_mode: str | None = None
    strict_native_provider_enabled: bool | None = None
    strict_native_provider_violation: bool | None = None
    ffi_table_provider: bool | None = None
    scan_files_requested: bool | None = None
    scan_files_count: int | None = None
    predicate: str | None = None
    module: str | None = None
    entrypoint: str | None = None
    ctx_kind: str | None = None
    probe_result: str | None = None
    compatible: bool | None = None
    available: bool | None = None
    error: str | None = None
    registration_path: str | None = None
    delta_version: int | None = None
    delta_timestamp: str | None = None
    delta_log_storage_options: Mapping[str, str] | None = None
    delta_storage_options: Mapping[str, str] | None = None
    delta_scan: Mapping[str, object] | None = None
    delta_scan_effective: Mapping[str, object] | None = None
    delta_scan_snapshot: Mapping[str, object] | None = None
    delta_scan_identity_hash: str | None = None
    delta_snapshot: Mapping[str, object] | None = None
    delta_scan_ignored: bool | None = None
    delta_pruning_predicate: str | None = None
    delta_pruning_error: str | None = None
    delta_pruning_applied: bool | None = None
    delta_pruned_files: int | None = None
    snapshot_key: Mapping[str, object] | None = None
    storage_profile_fingerprint: str | None = None

    def as_payload(self, *, drop_none: bool = True) -> dict[str, object]:
        """Return a dict payload suitable for diagnostics artifacts."""
        payload = to_builtins(self, str_keys=True)
        if not isinstance(payload, Mapping):
            return {}
        normalized = {str(key): value for key, value in payload.items()}
        if not drop_none:
            return normalized
        return {key: value for key, value in normalized.items() if value is not None}


@dataclass(frozen=True)
class DeltaProviderBuildRequest:
    """Input request for canonical Delta provider artifact construction."""

    table_uri: str | None
    dataset_format: str | None
    provider_kind: str | None
    dataset_name: str | None = None
    provider_mode: str | None = None
    strict_native_provider_enabled: bool | None = None
    strict_native_provider_violation: bool | None = None
    ffi_table_provider: bool | None = None
    scan_files_requested: bool | None = None
    scan_files_count: int | None = None
    predicate: str | None = None
    compatibility: DeltaExtensionCompatibility | None = None
    registration_path: str | None = None
    delta_version: int | None = None
    delta_timestamp: str | None = None
    delta_log_storage_options: Mapping[str, str] | None = None
    delta_storage_options: Mapping[str, str] | None = None
    delta_scan_options: DeltaScanOptions | None = None
    delta_scan_effective: Mapping[str, object] | None = None
    delta_scan_snapshot: object | None = None
    delta_scan_identity_hash: str | None = None
    delta_snapshot: Mapping[str, object] | None = None
    delta_scan_ignored: bool | None = None
    delta_pruning_predicate: str | None = None
    delta_pruning_error: str | None = None
    delta_pruning_applied: bool | None = None
    delta_pruned_files: int | None = None
    include_event_metadata: bool = False
    run_id: str | None = None


@dataclass(frozen=True)
class RegistrationProviderArtifactInput:
    """Input adapter for dataset-registration provider artifact derivation."""

    table_uri: str
    dataset_format: str
    provider_kind: str
    compatibility: DeltaExtensionCompatibility | None
    context: object


@dataclass(frozen=True)
class ServiceProviderArtifactInput:
    """Input adapter for Delta-service provider artifact derivation."""

    request: object
    compatibility: DeltaExtensionCompatibility | None
    provider_mode: str
    strict_native_provider_enabled: bool
    strict_native_provider_violation: bool
    include_event_metadata: bool
    run_id: str | None


def provider_build_request_from_registration_context(
    source: RegistrationProviderArtifactInput,
) -> DeltaProviderBuildRequest:
    """Build a provider artifact request from dataset-registration context.

    Returns:
    -------
    DeltaProviderBuildRequest
        Normalized provider-build request.
    """
    context = source.context
    add_actions = getattr(context, "add_actions", None)
    delta_scan = getattr(context, "delta_scan", None)
    registration_path = _read_attr(context, "registration_path")
    registration_is_ddl = registration_path == "ddl"
    pruned_files_count = len(add_actions) if isinstance(add_actions, Sequence) else None
    pruning_applied = add_actions is not None
    delta_scan_ignored = registration_is_ddl and delta_scan is not None
    return DeltaProviderBuildRequest(
        table_uri=source.table_uri,
        dataset_format=source.dataset_format,
        provider_kind=source.provider_kind,
        dataset_name=_as_optional_str(_read_attr(context, "dataset_name")),
        provider_mode=_as_optional_str(_read_attr(context, "provider_mode")),
        strict_native_provider_enabled=_as_optional_bool(
            _read_attr(context, "strict_native_provider_enabled")
        ),
        strict_native_provider_violation=_as_optional_bool(
            _read_attr(context, "strict_native_provider_violation")
        ),
        ffi_table_provider=_as_optional_bool(_read_attr(context, "ffi_table_provider")),
        predicate=_as_optional_str(_read_attr(context, "predicate")),
        compatibility=source.compatibility,
        registration_path=_as_optional_str(registration_path),
        delta_scan_options=cast("DeltaScanOptions | None", delta_scan),
        delta_scan_effective=_as_mapping(_read_attr(context, "delta_scan_effective")),
        delta_scan_snapshot=_read_attr(context, "delta_scan_snapshot"),
        delta_scan_identity_hash=_as_optional_str(_read_attr(context, "delta_scan_identity_hash")),
        delta_snapshot=_as_mapping(_read_attr(context, "snapshot")),
        delta_scan_ignored=delta_scan_ignored,
        delta_pruning_predicate=_as_optional_str(_read_attr(context, "predicate")),
        delta_pruning_error=_as_optional_str(_read_attr(context, "predicate_error")),
        delta_pruning_applied=pruning_applied,
        delta_pruned_files=pruned_files_count,
    )


def provider_build_request_from_service_context(
    source: ServiceProviderArtifactInput,
) -> DeltaProviderBuildRequest:
    """Build a provider artifact request from Delta service context.

    Returns:
    -------
    DeltaProviderBuildRequest
        Normalized provider-build request.
    """
    request = source.request
    location = _read_attr(request, "location")
    resolution = _read_attr(request, "resolution")
    scan_files = _read_attr(request, "scan_files")
    add_actions = _read_attr(resolution, "add_actions")
    location_path = _as_optional_str(_read_attr(location, "path"))
    location_format = _as_optional_str(_read_attr(location, "format"))
    provider_kind = _as_optional_str(_read_attr(resolution, "provider_kind"))
    if location_path is None:
        location_path = ""
    return DeltaProviderBuildRequest(
        table_uri=location_path,
        dataset_format=location_format,
        provider_kind=provider_kind,
        dataset_name=_as_optional_str(_read_attr(request, "name")),
        provider_mode=source.provider_mode,
        strict_native_provider_enabled=source.strict_native_provider_enabled,
        strict_native_provider_violation=source.strict_native_provider_violation,
        scan_files_requested=bool(scan_files),
        scan_files_count=len(scan_files) if isinstance(scan_files, Sequence) else 0,
        predicate=_as_optional_str(_read_attr(request, "predicate")),
        compatibility=source.compatibility,
        delta_version=coerce_int(_read_attr(location, "delta_version")),
        delta_timestamp=_as_optional_str(_read_attr(location, "delta_timestamp")),
        delta_log_storage_options=(
            dict(log_options)
            if isinstance(log_options := _read_attr(location, "delta_log_storage_options"), Mapping)
            else None
        ),
        delta_storage_options=(
            dict(storage_options)
            if isinstance(storage_options := _read_attr(location, "storage_options"), Mapping)
            else None
        ),
        delta_scan_options=cast(
            "DeltaScanOptions | None", _read_attr(resolution, "delta_scan_options")
        ),
        delta_scan_effective=_as_mapping(_read_attr(resolution, "delta_scan_effective")),
        delta_scan_snapshot=_read_attr(resolution, "delta_scan_snapshot"),
        delta_scan_identity_hash=_as_optional_str(
            _read_attr(resolution, "delta_scan_identity_hash")
        ),
        delta_snapshot=_as_mapping(_read_attr(resolution, "delta_snapshot")),
        delta_pruning_predicate=_as_optional_str(_read_attr(request, "predicate")),
        delta_pruning_error=_as_optional_str(_read_attr(resolution, "predicate_error")),
        delta_pruning_applied=add_actions is not None,
        delta_pruned_files=len(add_actions) if isinstance(add_actions, Sequence) else None,
        include_event_metadata=source.include_event_metadata,
        run_id=source.run_id,
    )


def _as_optional_str(value: object) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _as_optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _as_mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    return None


def _read_attr(value: object, name: str) -> object:
    return getattr(value, name, None)


def build_delta_provider_build_result(
    request: DeltaProviderBuildRequest,
) -> DeltaProviderBuildResult:
    """Build a canonical Delta provider artifact payload object.

    Returns:
    -------
    DeltaProviderBuildResult
        Canonical Delta provider artifact payload.
    """
    normalized_scan = delta_scan_payload(request.delta_scan_options)
    normalized_scan_snapshot = delta_scan_snapshot_payload(request.delta_scan_snapshot)
    snapshot_key = delta_snapshot_key_payload(
        table_uri=request.table_uri,
        snapshot=request.delta_snapshot,
    )
    storage_profile = storage_profile_fingerprint(
        table_uri=request.table_uri,
        storage_options=request.delta_storage_options,
        log_storage_options=request.delta_log_storage_options,
    )
    compatibility = request.compatibility
    return DeltaProviderBuildResult(
        event_time_unix_ms=int(time.time() * 1000) if request.include_event_metadata else None,
        run_id=request.run_id if request.include_event_metadata else None,
        dataset_name=request.dataset_name,
        path=request.table_uri,
        table_uri=request.table_uri,
        format=request.dataset_format,
        provider_kind=request.provider_kind,
        provider_mode=request.provider_mode,
        strict_native_provider_enabled=request.strict_native_provider_enabled,
        strict_native_provider_violation=request.strict_native_provider_violation,
        ffi_table_provider=request.ffi_table_provider,
        scan_files_requested=request.scan_files_requested,
        scan_files_count=request.scan_files_count,
        predicate=request.predicate,
        module=compatibility.module if compatibility is not None else None,
        entrypoint=compatibility.entrypoint if compatibility is not None else None,
        ctx_kind=compatibility.ctx_kind if compatibility is not None else None,
        probe_result=compatibility.probe_result if compatibility is not None else None,
        compatible=compatibility.compatible if compatibility is not None else None,
        available=compatibility.available if compatibility is not None else None,
        error=compatibility.error if compatibility is not None else None,
        registration_path=request.registration_path,
        delta_version=request.delta_version,
        delta_timestamp=request.delta_timestamp,
        delta_log_storage_options=request.delta_log_storage_options,
        delta_storage_options=request.delta_storage_options,
        delta_scan=normalized_scan,
        delta_scan_effective=request.delta_scan_effective,
        delta_scan_snapshot=normalized_scan_snapshot,
        delta_scan_identity_hash=request.delta_scan_identity_hash,
        delta_snapshot=request.delta_snapshot,
        delta_scan_ignored=request.delta_scan_ignored,
        delta_pruning_predicate=request.delta_pruning_predicate,
        delta_pruning_error=request.delta_pruning_error,
        delta_pruning_applied=request.delta_pruning_applied,
        delta_pruned_files=request.delta_pruned_files,
        snapshot_key=snapshot_key,
        storage_profile_fingerprint=storage_profile,
    )


def delta_scan_payload(options: DeltaScanOptions | None) -> dict[str, object] | None:
    """Return a normalized Delta scan-config payload."""
    if options is None:
        return None
    snapshot = delta_scan_config_snapshot_from_options(options)
    if snapshot is None:
        return None
    payload = to_builtins(snapshot, str_keys=True)
    if not isinstance(payload, Mapping):
        return None
    return {str(key): value for key, value in payload.items()}


def delta_scan_snapshot_payload(snapshot: object | None) -> dict[str, object] | None:
    """Return a normalized Delta scan snapshot payload."""
    if snapshot is None:
        return None
    if isinstance(snapshot, Mapping):
        return {str(key): value for key, value in snapshot.items()}
    payload = to_builtins(snapshot, str_keys=True)
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return None


def delta_snapshot_key_payload(
    *,
    table_uri: str | None,
    snapshot: Mapping[str, object] | None,
) -> dict[str, object] | None:
    """Return a canonical snapshot-key payload when version is known."""
    if table_uri is None or snapshot is None:
        return None
    version = coerce_int(snapshot.get("version"))
    if version is None:
        return None
    return {
        "canonical_uri": canonical_table_uri(table_uri),
        "resolved_version": version,
    }


def storage_profile_fingerprint(
    *,
    table_uri: str | None,
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
) -> str | None:
    """Return a stable fingerprint for effective storage-profile inputs."""
    if table_uri is None:
        return None
    payload: dict[str, object] = {
        "canonical_uri": canonical_table_uri(table_uri),
        "storage_options": sorted((str(k), str(v)) for k, v in (storage_options or {}).items()),
        "log_storage_options": sorted(
            (str(k), str(v)) for k, v in (log_storage_options or {}).items()
        ),
    }
    return hash_msgpack_canonical(payload)[:16]


__all__ = [
    "DeltaProviderBuildRequest",
    "DeltaProviderBuildResult",
    "RegistrationProviderArtifactInput",
    "ServiceProviderArtifactInput",
    "build_delta_provider_build_result",
    "delta_scan_payload",
    "delta_scan_snapshot_payload",
    "delta_snapshot_key_payload",
    "provider_build_request_from_registration_context",
    "provider_build_request_from_service_context",
    "storage_profile_fingerprint",
]
