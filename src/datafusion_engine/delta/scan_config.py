"""Canonical Delta scan configuration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.arrow.abi import schema_to_dict
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from schema_spec.system import DeltaScanOptions, ScanPolicyConfig
    from serde_artifacts import DeltaScanConfigSnapshot


@dataclass(frozen=True)
class DeltaScanConfigIdentity:
    """Canonical Delta scan config payload used for identity hashing."""

    options: Mapping[str, object]
    schema_payload: Mapping[str, object] | None


def resolve_delta_scan_options(
    location: DatasetLocation,
    *,
    scan_policy: ScanPolicyConfig | None = None,
) -> DeltaScanOptions | None:
    """Return Delta scan options for a dataset location.

    Returns:
    -------
    DeltaScanOptions | None
        Delta scan options derived from the dataset location, when present.
    """
    options = location.resolved.delta_scan
    if scan_policy is None:
        return options
    from schema_spec.system import apply_delta_scan_policy

    return apply_delta_scan_policy(options, policy=scan_policy)


def delta_scan_config_snapshot(
    location: DatasetLocation | None,
) -> DeltaScanConfigSnapshot | None:
    """Return a snapshot payload for Delta scan configuration.

    Returns:
    -------
    DeltaScanConfigSnapshot | None
        Snapshot payload for the scan configuration, when available.
    """
    if location is None:
        return None
    options = resolve_delta_scan_options(location)
    return delta_scan_config_snapshot_from_options(options)


def delta_scan_config_snapshot_from_options(
    options: DeltaScanOptions | None,
) -> DeltaScanConfigSnapshot | None:
    """Return a snapshot payload for Delta scan configuration options.

    Returns:
    -------
    DeltaScanConfigSnapshot | None
        Snapshot payload for the scan configuration, when available.
    """
    if options is None:
        return None
    from serde_artifacts import DeltaScanConfigSnapshot

    schema_payload = schema_to_dict(options.schema) if options.schema is not None else None
    return DeltaScanConfigSnapshot(
        file_column_name=options.file_column_name,
        enable_parquet_pushdown=options.enable_parquet_pushdown,
        schema_force_view_types=options.schema_force_view_types,
        wrap_partition_values=options.wrap_partition_values,
        schema=dict(schema_payload) if schema_payload is not None else None,
    )


def delta_scan_config_identity_payload(
    snapshot: DeltaScanConfigSnapshot | None,
) -> DeltaScanConfigIdentity | None:
    """Return a canonical identity payload for a Delta scan config snapshot.

    Returns:
    -------
    DeltaScanConfigIdentity | None
        Canonical identity payload, when available.
    """
    if snapshot is None:
        return None
    return DeltaScanConfigIdentity(
        options={
            "file_column_name": snapshot.file_column_name,
            "enable_parquet_pushdown": snapshot.enable_parquet_pushdown,
            "schema_force_view_types": snapshot.schema_force_view_types,
            "wrap_partition_values": snapshot.wrap_partition_values,
        },
        schema_payload=dict(snapshot.schema) if snapshot.schema is not None else None,
    )


def delta_scan_identity_hash(snapshot: DeltaScanConfigSnapshot | None) -> str | None:
    """Return a deterministic identity hash for a Delta scan config snapshot.

    Returns:
    -------
    str | None
        Identity hash for the snapshot payload, when available.
    """
    if snapshot is None:
        return None
    return hash_msgpack_canonical(snapshot)


__all__ = [
    "DeltaScanConfigIdentity",
    "delta_scan_config_identity_payload",
    "delta_scan_config_snapshot",
    "delta_scan_config_snapshot_from_options",
    "delta_scan_identity_hash",
    "resolve_delta_scan_options",
]
