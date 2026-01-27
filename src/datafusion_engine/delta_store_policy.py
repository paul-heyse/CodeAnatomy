"""Delta object-store and log-store policy helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field

from datafusion_engine.dataset_registry import DatasetLocation


@dataclass(frozen=True)
class DeltaStorePolicy:
    """Runtime-level object store and log store configuration."""

    storage_options: Mapping[str, str] = field(default_factory=dict)
    log_storage_options: Mapping[str, str] = field(default_factory=dict)
    require_local_paths: bool = False


def resolve_delta_store_policy(
    *,
    table_uri: str,
    policy: DeltaStorePolicy | None,
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve effective storage and log-store options.

    Returns
    -------
    tuple[dict[str, str], dict[str, str]]
        Effective storage options and log-store options.

    Raises
    ------
    ValueError
        Raised when remote Delta tables are disallowed by policy.
    """
    resolved_storage = dict(policy.storage_options) if policy is not None else {}
    resolved_log = dict(policy.log_storage_options) if policy is not None else {}
    if storage_options:
        resolved_storage.update({str(key): str(value) for key, value in storage_options.items()})
    if log_storage_options:
        resolved_log.update({str(key): str(value) for key, value in log_storage_options.items()})
    if policy is not None and policy.require_local_paths and "://" in table_uri:
        msg = "Remote Delta tables are disallowed by policy."
        raise ValueError(msg)
    return resolved_storage, resolved_log


def apply_delta_store_policy(
    location: DatasetLocation,
    *,
    policy: DeltaStorePolicy | None,
) -> DatasetLocation:
    """Return a DatasetLocation with store policy applied.

    Returns
    -------
    DatasetLocation
        Dataset location with storage policies applied.
    """
    if policy is None:
        return location
    storage, log_storage = resolve_delta_store_policy(
        table_uri=str(location.path),
        policy=policy,
        storage_options=location.storage_options,
        log_storage_options=location.delta_log_storage_options,
    )
    return DatasetLocation(
        path=location.path,
        format=location.format,
        partitioning=location.partitioning,
        read_options=location.read_options,
        storage_options=storage,
        delta_log_storage_options=log_storage,
        delta_scan=location.delta_scan,
        delta_cdf_options=location.delta_cdf_options,
        delta_cdf_policy=location.delta_cdf_policy,
        delta_write_policy=location.delta_write_policy,
        delta_schema_policy=location.delta_schema_policy,
        delta_feature_gate=location.delta_feature_gate,
        delta_constraints=location.delta_constraints,
        filesystem=location.filesystem,
        files=location.files,
        table_spec=location.table_spec,
        dataset_spec=location.dataset_spec,
        datafusion_scan=location.datafusion_scan,
        datafusion_provider=location.datafusion_provider,
        delta_version=location.delta_version,
        delta_timestamp=location.delta_timestamp,
    )


def delta_store_policy_hash(policy: DeltaStorePolicy | None) -> str | None:
    """Return a stable hash of the store policy.

    Returns
    -------
    str | None
        Stable hash string when policy is set, otherwise ``None``.
    """
    if policy is None:
        return None
    payload = {
        "storage_options": sorted((str(k), str(v)) for k, v in policy.storage_options.items()),
        "log_storage_options": sorted(
            (str(k), str(v)) for k, v in policy.log_storage_options.items()
        ),
        "require_local_paths": policy.require_local_paths,
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "DeltaStorePolicy",
    "apply_delta_store_policy",
    "delta_store_policy_hash",
    "resolve_delta_store_policy",
]
