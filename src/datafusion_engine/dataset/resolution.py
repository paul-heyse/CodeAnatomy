"""Dataset resolution pipeline for provider construction."""

from __future__ import annotations

import logging
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
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.tables.metadata import TableProviderCapsule
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.delta.contracts import DeltaCdfContract
    from datafusion_engine.delta.control_plane import (
        DeltaCdfProviderBundle,
        DeltaProviderBundle,
    )
    from datafusion_engine.delta.specs import DeltaCdfOptionsSpec
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DeltaScanOptions

DatasetProviderKind = Literal["delta", "delta_cdf"]
_LOGGER = logging.getLogger(__name__)


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
            runtime_profile=request.runtime_profile,
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
        runtime_profile=request.runtime_profile,
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
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaProviderBundle:
    try:
        if scan_files:
            return delta_provider_with_files(ctx, files=scan_files, request=request)
        return delta_provider_from_session(ctx, request=request)
    except (DataFusionEngineError, RuntimeError, TypeError, ValueError) as exc:
        if _strict_native_provider_enabled(runtime_profile):
            msg = (
                "Delta provider control-plane failed in strict native-provider mode; "
                "degraded fallback is disabled."
            )
            raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc
        _LOGGER.warning(
            "Delta provider control-plane failed with strict provider enforcement disabled; "
            "using degraded Python dataset provider: %s",
            exc,
        )
        return _degraded_delta_provider_bundle(
            request=request,
            scan_files=scan_files,
            error=exc,
        )


def _strict_native_provider_enabled(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> bool:
    if runtime_profile is None:
        return True
    return runtime_profile.features.enforce_delta_ffi_provider


def _degraded_delta_provider_bundle(
    *,
    request: DeltaProviderRequest,
    scan_files: Sequence[str] | None,
    error: Exception,
) -> DeltaProviderBundle:
    from deltalake import DeltaTable

    from datafusion_engine.delta.control_plane import DeltaProviderBundle

    storage_options = dict(request.storage_options) if request.storage_options else None
    try:
        table = DeltaTable(
            request.table_uri,
            version=request.version,
            storage_options=storage_options,
        )
        if request.timestamp is not None:
            table.load_as_version(request.timestamp)
        provider = table.to_pyarrow_dataset()
    except Exception as exc:
        msg = "Delta provider degraded mode failed after control-plane failure."
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA) from exc
    snapshot: dict[str, object] = {
        "table_uri": request.table_uri,
        "version": table.version(),
        "provider_mode": "pyarrow_dataset_degraded",
        "fallback_error": str(error),
    }
    if scan_files:
        snapshot["scan_files_requested"] = list(scan_files)
        snapshot["scan_files_applied"] = False
    return DeltaProviderBundle(
        provider=provider,
        snapshot=snapshot,
        scan_config={},
        scan_effective={
            "fallback": True,
            "scan_files_requested": bool(scan_files),
        },
        add_actions=None,
        predicate_error=None,
    )


def _resolve_delta_cdf(
    *,
    location: DatasetLocation,
    name: str | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DatasetResolution:
    contract = build_delta_cdf_contract(location)
    try:
        bundle = delta_cdf_provider(request=contract.to_request())
    except (DataFusionEngineError, RuntimeError, TypeError, ValueError) as exc:
        if _strict_native_provider_enabled(runtime_profile):
            msg = (
                "Delta CDF control-plane failed in strict native-provider mode; "
                "degraded fallback is disabled."
            )
            raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc
        _LOGGER.warning(
            "Delta CDF control-plane failed with strict provider enforcement disabled; "
            "using degraded Python CDF reader: %s",
            exc,
        )
        bundle = _degraded_delta_cdf_bundle(contract=contract, error=exc)
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


def _degraded_delta_cdf_bundle(
    *,
    contract: DeltaCdfContract,
    error: Exception,
) -> DeltaCdfProviderBundle:
    from deltalake import DeltaTable

    from datafusion_engine.delta.control_plane import DeltaCdfProviderBundle

    storage_options = dict(contract.storage_options) if contract.storage_options else None
    try:
        table = DeltaTable(
            contract.table_uri,
            version=contract.version,
            storage_options=storage_options,
        )
        if contract.timestamp is not None:
            table.load_as_version(contract.timestamp)
        reader = table.load_cdf(
            starting_version=contract.options.starting_version
            if contract.options is not None and contract.options.starting_version is not None
            else 0,
            ending_version=contract.options.ending_version
            if contract.options is not None
            else None,
            starting_timestamp=contract.options.starting_timestamp
            if contract.options is not None
            else None,
            ending_timestamp=contract.options.ending_timestamp
            if contract.options is not None
            else None,
            columns=list(contract.options.columns)
            if contract.options is not None and contract.options.columns is not None
            else None,
            predicate=contract.options.predicate if contract.options is not None else None,
            allow_out_of_range=contract.options.allow_out_of_range
            if contract.options is not None
            else False,
        )
        provider = reader.read_all()
    except Exception as exc:
        msg = "Delta CDF degraded mode failed after control-plane failure."
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA) from exc
    snapshot: dict[str, object] = {
        "table_uri": contract.table_uri,
        "version": table.version(),
        "provider_mode": "pyarrow_cdf_degraded",
        "fallback_error": str(error),
    }
    return DeltaCdfProviderBundle(
        provider=provider,
        snapshot=snapshot,
        cdf_options=contract.to_request().options,
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
