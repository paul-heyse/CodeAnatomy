"""Scan-unit aware dataset overrides for deterministic execution."""

from __future__ import annotations

import hashlib
import importlib
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_delta_feature_gate,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from datafusion_engine.diagnostics import record_artifact
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.table_provider_capsule import TableProviderCapsule
from serde_msgspec import dumps_msgpack
from storage.deltalake.delta import delta_table_version

_MIN_QUALIFIED_PARTS = 2


def _validate_delta_provider_available() -> bool:
    """Validate that Delta provider APIs are available at import time.

    Returns
    -------
    bool
        True if the provider is available, False otherwise.
    """
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        return False
    provider_fn = getattr(module, "delta_table_provider_with_files", None)
    return callable(provider_fn)


_DELTA_PROVIDER_AVAILABLE = _validate_delta_provider_available()


def apply_scan_unit_overrides(
    ctx: SessionContext,
    *,
    scan_units: Sequence[ScanUnit],
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    """Apply scan-unit derived overrides to registered Delta datasets."""
    if runtime_profile is None or not scan_units:
        return
    units_by_dataset = _scan_units_by_dataset(scan_units)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    for dataset_name in sorted(units_by_dataset):
        location = _resolve_dataset_location(runtime_profile, dataset_name)
        if location is None or location.format != "delta":
            continue
        gate = resolve_delta_feature_gate(location)
        units = units_by_dataset[dataset_name]
        pinned_version = _pinned_version_for_units(location, units)
        pinned_timestamp = _pinned_timestamp_for_units(location, units)
        scan_files = _scan_files_for_units(location, units)
        if pinned_version is None and not scan_files:
            continue
        updated_location = location
        if pinned_version is not None:
            updated_location = replace(
                location,
                delta_version=pinned_version,
                delta_timestamp=None,
            )
        _register_delta_override(
            ctx,
            adapter=adapter,
            spec=_DeltaOverrideSpec(
                name=dataset_name,
                location=updated_location,
                scan_files=scan_files,
                runtime_profile=runtime_profile,
            ),
        )
        _record_override_artifact(
            runtime_profile,
            request=_ScanOverrideArtifactRequest(
                dataset_name=dataset_name,
                pinned_version=pinned_version,
                pinned_timestamp=pinned_timestamp,
                gate=gate,
                scan_files=scan_files,
            ),
        )


def scan_units_hash(scan_units: Sequence[ScanUnit]) -> str:
    """Return a stable identity hash for a collection of scan units.

    Returns
    -------
    str
        Stable hash for the scan unit collection.
    """
    payload = tuple(
        sorted(
            (
                unit.key,
                unit.delta_version,
                unit.delta_timestamp,
                unit.snapshot_timestamp,
            )
            for unit in scan_units
        )
    )
    return _hash_payload(payload)


def _scan_units_by_dataset(scan_units: Sequence[ScanUnit]) -> dict[str, list[ScanUnit]]:
    units_by_dataset: dict[str, list[ScanUnit]] = {}
    for unit in scan_units:
        units_by_dataset.setdefault(unit.dataset_name, []).append(unit)
    return units_by_dataset


def _resolve_dataset_location(
    runtime_profile: DataFusionRuntimeProfile,
    dataset_name: str,
) -> DatasetLocation | None:
    location = runtime_profile.dataset_location(dataset_name)
    if location is not None:
        return location
    parts = dataset_name.split(".")
    candidates = [dataset_name]
    if parts:
        candidates.append(parts[-1])
    if len(parts) >= _MIN_QUALIFIED_PARTS:
        candidates.append(".".join(parts[-2:]))
    for candidate in candidates:
        resolved = runtime_profile.dataset_location(candidate)
        if resolved is not None:
            return resolved
    return None


def _pinned_version_for_units(
    location: DatasetLocation,
    units: Sequence[ScanUnit],
) -> int | None:
    versions = {unit.delta_version for unit in units if unit.delta_version is not None}
    if len(versions) > 1:
        msg = f"Scan units for a dataset resolved to multiple Delta versions: {sorted(versions)}."
        raise ValueError(msg)
    if versions:
        return versions.pop()
    if location.delta_version is not None:
        return location.delta_version
    if location.format != "delta":
        return None
    storage_options = dict(location.storage_options)
    log_storage_options = dict(resolve_delta_log_storage_options(location) or {})
    return delta_table_version(
        str(location.path),
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )


def _pinned_timestamp_for_units(
    location: DatasetLocation,
    units: Sequence[ScanUnit],
) -> str | None:
    timestamps = {unit.snapshot_timestamp for unit in units if unit.snapshot_timestamp is not None}
    if len(timestamps) > 1:
        msg = "Scan units for a dataset resolved to multiple snapshot timestamps."
        raise ValueError(msg)
    if timestamps:
        return str(timestamps.pop())
    if location.delta_timestamp:
        return location.delta_timestamp
    return None


def _scan_files_for_units(
    location: DatasetLocation,
    units: Sequence[ScanUnit],
) -> tuple[str, ...]:
    files: list[str] = []
    for unit in units:
        for candidate in unit.candidate_files:
            relative = _relative_delta_path(location, str(candidate))
            if relative:
                files.append(relative)
    return tuple(dict.fromkeys(files))


def _relative_delta_path(location: DatasetLocation, candidate: str) -> str:
    root = str(location.path).rstrip("/")
    prefix = f"{root}/"
    return candidate[len(prefix) :] if candidate.startswith(prefix) else candidate.lstrip("/")


@dataclass(frozen=True)
class _DeltaOverrideSpec:
    name: str
    location: DatasetLocation
    scan_files: Sequence[str]
    runtime_profile: DataFusionRuntimeProfile


def _register_delta_override(
    ctx: SessionContext,
    *,
    adapter: DataFusionIOAdapter,
    spec: _DeltaOverrideSpec,
) -> None:
    if spec.scan_files:
        provider = _delta_table_provider_with_files(
            ctx,
            location=spec.location,
            scan_files=spec.scan_files,
            runtime_profile=spec.runtime_profile,
        )
        adapter.register_delta_table_provider(
            spec.name,
            TableProviderCapsule(provider),
            overwrite=True,
        )
        invalidate_introspection_cache(ctx)
        return
    from datafusion_engine.registry_bridge import register_dataset_df

    register_dataset_df(
        ctx,
        name=spec.name,
        location=spec.location,
        runtime_profile=spec.runtime_profile,
    )


def _delta_table_provider_with_files(
    ctx: SessionContext,
    *,
    location: DatasetLocation,
    scan_files: Sequence[str],
    runtime_profile: DataFusionRuntimeProfile,
) -> object:
    if not _DELTA_PROVIDER_AVAILABLE:
        msg = "datafusion_ext.delta_table_provider_with_files is unavailable."
        raise TypeError(msg)
    from datafusion_engine.delta_control_plane import (
        DeltaProviderRequest,
        delta_provider_with_files,
    )

    delta_scan = resolve_delta_scan_options(location)
    if delta_scan is not None and delta_scan.schema_force_view_types is None:
        enable_view_types = _schema_hardening_view_types(runtime_profile)
        delta_scan = replace(delta_scan, schema_force_view_types=enable_view_types)
    storage_options = _delta_storage_options(location)
    bundle = delta_provider_with_files(
        ctx,
        files=scan_files,
        request=DeltaProviderRequest(
            table_uri=str(location.path),
            storage_options=storage_options,
            version=location.delta_version,
            timestamp=location.delta_timestamp,
            delta_scan=delta_scan,
            gate=resolve_delta_feature_gate(location),
        ),
    )
    return bundle.provider


def _delta_storage_options(location: DatasetLocation) -> dict[str, str] | None:
    storage: dict[str, str] = {
        key: str(value) for key, value in dict(location.storage_options).items()
    }
    log_storage = resolve_delta_log_storage_options(location)
    if log_storage:
        storage.update({key: str(value) for key, value in log_storage.items()})
    if not storage:
        return None
    return storage


def _schema_hardening_view_types(runtime_profile: DataFusionRuntimeProfile) -> bool:
    hardening = runtime_profile.schema_hardening
    if hardening is not None:
        return hardening.enable_view_types
    return runtime_profile.schema_hardening_name == "arrow_performance"


@dataclass(frozen=True)
class _ScanOverrideArtifactRequest:
    """Inputs required to record scan override artifacts."""

    dataset_name: str
    pinned_version: int | None
    pinned_timestamp: str | None
    gate: object | None
    scan_files: Sequence[str]


def _record_override_artifact(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    request: _ScanOverrideArtifactRequest,
) -> None:
    scan_files_hash = _hash_payload(request.scan_files)
    payload = {
        "dataset_name": request.dataset_name,
        "pinned_version": request.pinned_version,
        "pinned_timestamp": request.pinned_timestamp,
        "delta_feature_gate": _gate_payload(request.gate),
        "scan_file_count": len(request.scan_files),
        "scan_files_hash": scan_files_hash,
    }
    record_artifact(runtime_profile, "scan_unit_overrides_v1", payload)


def _gate_payload(gate: object | None) -> dict[str, object] | None:
    if gate is None:
        return None
    min_reader_version = getattr(gate, "min_reader_version", None)
    min_writer_version = getattr(gate, "min_writer_version", None)
    required_reader_features = getattr(gate, "required_reader_features", ())
    required_writer_features = getattr(gate, "required_writer_features", ())
    return {
        "min_reader_version": min_reader_version,
        "min_writer_version": min_writer_version,
        "required_reader_features": list(required_reader_features),
        "required_writer_features": list(required_writer_features),
    }


def _hash_payload(payload: object) -> str:
    digest = hashlib.sha256(dumps_msgpack(payload)).hexdigest()
    return digest[:16]


if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.scan_planner import ScanUnit


__all__ = ["apply_scan_unit_overrides", "scan_units_hash"]
