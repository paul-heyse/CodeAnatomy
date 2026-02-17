"""Scan-unit and Delta-input snapshot helpers for plan bundles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from datafusion import SessionContext

from serde_artifacts import DeltaInputPin

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.session.runtime_session import SessionRuntime
    from semantics.program_manifest import ManifestDatasetResolver


def scan_units_for_bundle(
    ctx: SessionContext,
    *,
    plan: object,
    session_runtime: SessionRuntime | None,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[ScanUnit, ...]:
    """Derive scan units from lineage for bundle artifact metadata.

    Returns:
        tuple[ScanUnit, ...]: Planned scan units for bundle metadata capture.
    """
    scan_units: tuple[ScanUnit, ...] = ()
    if session_runtime is None or plan is None:
        return scan_units
    try:
        from datafusion_engine.lineage.reporting import extract_lineage
        from datafusion_engine.lineage.scheduling import plan_scan_units
    except ImportError:
        return scan_units
    lineage = extract_lineage(plan)
    lineage_scans = getattr(lineage, "scans", ())
    if lineage_scans:
        locations = manifest_dataset_locations(
            dataset_resolver=dataset_resolver,
            session_runtime=session_runtime,
        )
        if locations:
            try:
                scan_units, _ = plan_scan_units(
                    ctx,
                    dataset_locations=locations,
                    scans_by_task={"plan_bundle": lineage_scans},
                    runtime_profile=session_runtime.profile,
                )
            except (RuntimeError, TypeError, ValueError):
                scan_units = ()
    return scan_units


def manifest_dataset_locations(
    *,
    dataset_resolver: ManifestDatasetResolver | None,
    session_runtime: SessionRuntime | None,
) -> dict[str, DatasetLocation]:
    """Resolve dataset-name to location mapping for manifest-aware inputs.

    Returns:
        dict[str, DatasetLocation]: Resolved dataset locations keyed by dataset name.
    """
    if dataset_resolver is None:
        if session_runtime is None:
            return {}
        locations = dict(session_runtime.profile.data_sources.dataset_templates)
        locations.update(session_runtime.profile.data_sources.extract_output.dataset_locations)
        return locations
    locations: dict[str, DatasetLocation] = {}
    for name in dataset_resolver.names():
        location = dataset_resolver.location(name)
        if location is None:
            continue
        locations[name] = location
    return locations


def cdf_window_snapshot(
    session_runtime: SessionRuntime | None,
    *,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[dict[str, object], ...]:
    """Capture active CDF window settings for configured datasets.

    Returns:
        tuple[dict[str, object], ...]: Deterministic CDF window payload rows.
    """
    if session_runtime is None:
        return ()
    locations = manifest_dataset_locations(
        dataset_resolver=dataset_resolver,
        session_runtime=session_runtime,
    )
    payloads: list[dict[str, object]] = []
    for name, location in sorted(locations.items(), key=lambda item: item[0]):
        options = location.delta_cdf_options
        if options is None:
            continue
        payloads.append(
            {
                "dataset_name": name,
                "table_uri": str(location.path),
                "starting_version": options.starting_version,
                "ending_version": options.ending_version,
                "starting_timestamp": options.starting_timestamp,
                "ending_timestamp": options.ending_timestamp,
                "allow_out_of_range": options.allow_out_of_range,
            }
        )
    return tuple(payloads)


def canonical_table_uri_for_manifest(table_uri: str) -> str:
    """Normalize manifest URIs across filesystem/cloud schemes.

    Returns:
        str: Canonicalized table URI.
    """
    raw = str(table_uri).strip()
    parsed = urlparse(raw)
    if not parsed.scheme:
        return str(Path(raw).expanduser().resolve())
    scheme = parsed.scheme.lower()
    if scheme in {"s3a", "s3n"}:
        scheme = "s3"
    netloc = parsed.netloc
    if scheme in {"s3", "gs", "az", "abfs", "abfss", "http", "https"}:
        netloc = netloc.lower()
    path = parsed.path or ""
    if netloc and path and not path.startswith("/"):
        path = f"/{path}"
    return parsed._replace(scheme=scheme, netloc=netloc, path=path).geturl()


def snapshot_keys_for_manifest(
    *,
    delta_inputs: Sequence[DeltaInputPin],
    session_runtime: SessionRuntime | None,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[dict[str, object], ...]:
    """Build deterministic manifest snapshot keys from resolved Delta inputs.

    Returns:
        tuple[dict[str, object], ...]: Sorted manifest snapshot-key payload rows.
    """
    if session_runtime is None:
        return ()
    locations = manifest_dataset_locations(
        dataset_resolver=dataset_resolver,
        session_runtime=session_runtime,
    )
    payloads: list[dict[str, object]] = []
    seen: set[tuple[str, str, int]] = set()
    for pin in delta_inputs:
        if pin.version is None:
            continue
        location = locations.get(pin.dataset_name)
        if location is None:
            continue
        canonical_uri = canonical_table_uri_for_manifest(str(location.path))
        resolved_version = int(pin.version)
        key = (pin.dataset_name, canonical_uri, resolved_version)
        if key in seen:
            continue
        seen.add(key)
        payloads.append(
            {
                "dataset_name": pin.dataset_name,
                "canonical_uri": canonical_uri,
                "resolved_version": resolved_version,
            }
        )
    payloads.sort(
        key=lambda row: (
            str(row["dataset_name"]),
            str(row["canonical_uri"]),
            manifest_resolved_version(row),
        )
    )
    return tuple(payloads)


def manifest_resolved_version(row: Mapping[str, object]) -> int:
    """Safely coerce manifest snapshot row version to integer.

    Returns:
        int: Coerced resolved version, or `0` when unavailable.
    """
    value = row.get("resolved_version")
    if isinstance(value, int):
        return value
    return 0


__all__ = [
    "cdf_window_snapshot",
    "manifest_dataset_locations",
    "scan_units_for_bundle",
    "snapshot_keys_for_manifest",
]
