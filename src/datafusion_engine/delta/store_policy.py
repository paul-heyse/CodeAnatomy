"""Delta object-store and log-store policy helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from core.config_base import FingerprintableConfig, config_fingerprint

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation


@dataclass(frozen=True)
class DeltaStorePolicy(FingerprintableConfig):
    """Runtime-level object store and log store configuration."""

    storage_options: Mapping[str, str] = field(default_factory=dict)
    log_storage_options: Mapping[str, str] = field(default_factory=dict)
    require_local_paths: bool = False

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns
        -------
        Mapping[str, object]
            Payload used for policy fingerprinting.
        """
        return {
            "storage_options": sorted((str(k), str(v)) for k, v in self.storage_options.items()),
            "log_storage_options": sorted(
                (str(k), str(v)) for k, v in self.log_storage_options.items()
            ),
            "require_local_paths": self.require_local_paths,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the policy.

        Returns
        -------
        str
            Fingerprint string for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


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
    parsed = urlparse(table_uri) if "://" in table_uri else None
    if parsed is not None and parsed.scheme:
        scheme = parsed.scheme.lower()
        if scheme in {"s3a", "s3n"}:
            msg = "Delta table URIs must use canonical s3:// scheme (not s3a:// or s3n://)."
            raise ValueError(msg)
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


def resolve_store_options_for_location(
    location: DatasetLocation,
    *,
    policy: DeltaStorePolicy | None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve store options for a dataset location.

    Returns
    -------
    tuple[dict[str, str], dict[str, str]]
        Effective storage and log-store options.
    """
    return resolve_delta_store_policy(
        table_uri=str(location.path),
        policy=policy,
        storage_options=location.storage_options,
        log_storage_options=location.delta_log_storage_options,
    )


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
    return replace(
        location,
        storage_options=storage,
        delta_log_storage_options=log_storage,
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
    return policy.fingerprint()


__all__ = [
    "DeltaStorePolicy",
    "apply_delta_store_policy",
    "delta_store_policy_hash",
    "resolve_delta_store_policy",
    "resolve_store_options_for_location",
]
