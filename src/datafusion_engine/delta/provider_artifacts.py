"""Canonical Delta provider artifact payloads."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from datafusion_engine.delta.scan_config import delta_scan_config_snapshot_from_options
from serde_msgspec import StructBaseStrict, to_builtins
from storage.deltalake.delta import canonical_table_uri
from utils.hashing import hash_msgpack_canonical
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility
    from schema_spec.system import DeltaScanOptions


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


def build_delta_provider_build_result(  # noqa: PLR0913
    *,
    table_uri: str | None,
    dataset_format: str | None,
    provider_kind: str | None,
    dataset_name: str | None = None,
    provider_mode: str | None = None,
    strict_native_provider_enabled: bool | None = None,
    strict_native_provider_violation: bool | None = None,
    ffi_table_provider: bool | None = None,
    scan_files_requested: bool | None = None,
    scan_files_count: int | None = None,
    predicate: str | None = None,
    compatibility: DeltaExtensionCompatibility | None = None,
    registration_path: str | None = None,
    delta_version: int | None = None,
    delta_timestamp: str | None = None,
    delta_log_storage_options: Mapping[str, str] | None = None,
    delta_storage_options: Mapping[str, str] | None = None,
    delta_scan_options: DeltaScanOptions | None = None,
    delta_scan_effective: Mapping[str, object] | None = None,
    delta_scan_snapshot: object | None = None,
    delta_scan_identity_hash: str | None = None,
    delta_snapshot: Mapping[str, object] | None = None,
    delta_scan_ignored: bool | None = None,
    delta_pruning_predicate: str | None = None,
    delta_pruning_error: str | None = None,
    delta_pruning_applied: bool | None = None,
    delta_pruned_files: int | None = None,
    include_event_metadata: bool = False,
    run_id: str | None = None,
) -> DeltaProviderBuildResult:
    """Build a canonical Delta provider artifact payload object.

    Returns:
    -------
    DeltaProviderBuildResult
        Canonical Delta provider artifact payload.
    """
    normalized_scan = delta_scan_payload(delta_scan_options)
    normalized_scan_snapshot = delta_scan_snapshot_payload(delta_scan_snapshot)
    snapshot_key = delta_snapshot_key_payload(
        table_uri=table_uri,
        snapshot=delta_snapshot,
    )
    storage_profile = storage_profile_fingerprint(
        table_uri=table_uri,
        storage_options=delta_storage_options,
        log_storage_options=delta_log_storage_options,
    )
    return DeltaProviderBuildResult(
        event_time_unix_ms=int(time.time() * 1000) if include_event_metadata else None,
        run_id=run_id if include_event_metadata else None,
        dataset_name=dataset_name,
        path=table_uri,
        table_uri=table_uri,
        format=dataset_format,
        provider_kind=provider_kind,
        provider_mode=provider_mode,
        strict_native_provider_enabled=strict_native_provider_enabled,
        strict_native_provider_violation=strict_native_provider_violation,
        ffi_table_provider=ffi_table_provider,
        scan_files_requested=scan_files_requested,
        scan_files_count=scan_files_count,
        predicate=predicate,
        module=getattr(compatibility, "module", None),
        entrypoint=getattr(compatibility, "entrypoint", None),
        ctx_kind=getattr(compatibility, "ctx_kind", None),
        probe_result=getattr(compatibility, "probe_result", None),
        compatible=getattr(compatibility, "compatible", None),
        available=getattr(compatibility, "available", None),
        error=getattr(compatibility, "error", None),
        registration_path=registration_path,
        delta_version=delta_version,
        delta_timestamp=delta_timestamp,
        delta_log_storage_options=delta_log_storage_options,
        delta_storage_options=delta_storage_options,
        delta_scan=normalized_scan,
        delta_scan_effective=delta_scan_effective,
        delta_scan_snapshot=normalized_scan_snapshot,
        delta_scan_identity_hash=delta_scan_identity_hash,
        delta_snapshot=delta_snapshot,
        delta_scan_ignored=delta_scan_ignored,
        delta_pruning_predicate=delta_pruning_predicate,
        delta_pruning_error=delta_pruning_error,
        delta_pruning_applied=delta_pruning_applied,
        delta_pruned_files=delta_pruned_files,
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
    return cast("str", hash_msgpack_canonical(payload)[:16])


__all__ = [
    "DeltaProviderBuildResult",
    "build_delta_provider_build_result",
    "delta_scan_payload",
    "delta_scan_snapshot_payload",
    "delta_snapshot_key_payload",
    "storage_profile_fingerprint",
]
