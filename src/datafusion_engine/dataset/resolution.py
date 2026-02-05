"""Dataset resolution pipeline for provider construction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from datafusion import SessionContext

from datafusion_engine.catalog.introspection import invalidate_introspection_cache
from datafusion_engine.dataset.registry import (
    DatasetLocation,
    resolve_datafusion_provider,
)
from datafusion_engine.delta.contracts import (
    build_delta_cdf_contract,
    build_delta_provider_contract,
)
from datafusion_engine.delta.control_plane import (
    DeltaProviderRequest,
    delta_cdf_provider,
    delta_provider_from_session,
    delta_provider_with_files,
)
from datafusion_engine.delta.protocol import validate_delta_gate
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.tables.metadata import TableProviderCapsule
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.delta.control_plane import (
        DeltaProviderBundle,
    )
    from datafusion_engine.delta.specs import DeltaCdfOptionsSpec
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
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
    cdf_options: DeltaCdfOptionsSpec | None = None


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
    contract = build_delta_provider_contract(
        request.location,
        runtime_profile=request.runtime_profile,
    )
    gate = request.location.resolved.delta_feature_gate
    provider_request = contract.to_request(
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
    return DatasetResolution(
        name=request.name,
        location=request.location,
        provider=bundle.provider,
        provider_kind="delta",
        delta_snapshot=bundle.snapshot,
        delta_scan_config=bundle.scan_config,
        delta_scan_effective=bundle.scan_effective,
        delta_scan_snapshot=contract.scan_snapshot,
        delta_scan_identity_hash=contract.scan_identity_hash,
        delta_scan_options=contract.scan_options,
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
    contract = build_delta_cdf_contract(location)
    bundle = delta_cdf_provider(request=contract.to_request())
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


_MIN_QUALIFIED_PARTS = 2


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
        if location.datafusion_provider == "delta_cdf" or location.delta_cdf_options is not None:
            continue
        units = units_by_dataset[dataset_name]
        scan_files = _scan_files_for_units(location, units)
        if not scan_files:
            continue
        _register_delta_override(
            ctx,
            adapter=adapter,
            spec=_DeltaOverrideSpec(
                name=dataset_name,
                location=location,
                scan_files=scan_files,
                runtime_profile=runtime_profile,
            ),
        )
        _record_override_artifact(
            runtime_profile,
            request=_ScanOverrideArtifactRequest(
                dataset_name=dataset_name,
                scan_files=scan_files,
            ),
        )


def scan_units_hash(scan_units: Sequence[ScanUnit]) -> str:
    """Return a stable identity hash for a collection of scan units.

    Returns
    -------
    str
        Stable identity hash for the scan units.
    """
    payload = tuple(
        sorted(
            (
                unit.dataset_name,
                tuple(sorted(str(path) for path in unit.candidate_files)),
            )
            for unit in scan_units
        )
    )
    return hash_msgpack_canonical(payload)[:16]


def _scan_units_by_dataset(scan_units: Sequence[ScanUnit]) -> dict[str, list[ScanUnit]]:
    units_by_dataset: dict[str, list[ScanUnit]] = {}
    for unit in scan_units:
        units_by_dataset.setdefault(unit.dataset_name, []).append(unit)
    return units_by_dataset


def _resolve_dataset_location(
    runtime_profile: DataFusionRuntimeProfile,
    dataset_name: str,
) -> DatasetLocation | None:
    location = runtime_profile.catalog_ops.dataset_location(dataset_name)
    if location is not None:
        return location
    parts = dataset_name.split(".")
    candidates = [dataset_name]
    if parts:
        candidates.append(parts[-1])
    if len(parts) >= _MIN_QUALIFIED_PARTS:
        candidates.append(".".join(parts[-2:]))
    for candidate in candidates:
        resolved = runtime_profile.catalog_ops.dataset_location(candidate)
        if resolved is not None:
            return resolved
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
    resolution = resolve_dataset_provider(
        DatasetResolutionRequest(
            ctx=ctx,
            location=spec.location,
            runtime_profile=spec.runtime_profile,
            name=spec.name,
            scan_files=spec.scan_files,
        )
    )
    adapter.register_delta_table_provider(
        spec.name,
        TableProviderCapsule(resolution.provider),
        overwrite=True,
    )
    invalidate_introspection_cache(ctx)


@dataclass(frozen=True)
class _ScanOverrideArtifactRequest:
    """Inputs required to record scan override artifacts."""

    dataset_name: str
    scan_files: Sequence[str]


def _record_override_artifact(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    request: _ScanOverrideArtifactRequest,
) -> None:
    scan_files_hash = hash_msgpack_canonical(request.scan_files)[:16]
    payload = {
        "dataset_name": request.dataset_name,
        "scan_file_count": len(request.scan_files),
        "scan_files_hash": scan_files_hash,
    }
    record_artifact(runtime_profile, "scan_unit_overrides_v1", payload)


__all__ = [
    "DatasetProviderKind",
    "DatasetResolution",
    "DatasetResolutionRequest",
    "apply_scan_unit_overrides",
    "resolve_dataset_provider",
    "scan_units_hash",
]
