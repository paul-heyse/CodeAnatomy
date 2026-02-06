"""Delta object-store and log-store policy helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
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


def _canonical_table_uri(table_uri: str) -> str:
    """Return canonical URI for Delta table identity.

    Returns
    -------
    str
        Canonical URI string for stable table identity comparisons.
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


@dataclass(frozen=True)
class StorageProfile:
    """Canonical Delta storage profile for DataFusion and delta-rs surfaces."""

    table_uri: str
    canonical_uri: str
    scheme: str | None
    storage_options: Mapping[str, str] = field(default_factory=dict)
    log_storage_options: Mapping[str, str] = field(default_factory=dict)

    def to_datafusion_object_store_options(self) -> dict[str, str]:
        """Return DataFusion object-store registration options.

        Returns
        -------
        dict[str, str]
            Object-store options for DataFusion registration.
        """
        return {str(key): str(value) for key, value in self.storage_options.items()}

    def to_deltalake_options(self) -> dict[str, str]:
        """Return delta-rs storage options payload.

        Returns
        -------
        dict[str, str]
            Storage options for delta-rs APIs.
        """
        return {str(key): str(value) for key, value in self.storage_options.items()}

    def to_log_store_options(self) -> dict[str, str]:
        """Return delta-rs log-store options payload.

        Returns
        -------
        dict[str, str]
            Log-store options for delta-rs APIs.
        """
        return {str(key): str(value) for key, value in self.log_storage_options.items()}


def resolve_storage_profile(
    *,
    table_uri: str,
    policy: DeltaStorePolicy | None,
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
) -> StorageProfile:
    """Resolve the canonical storage profile for Delta table access.

    Returns
    -------
    StorageProfile
        Canonical storage profile for DataFusion and delta-rs interactions.

    Raises
    ------
    ValueError
        Raised when URI scheme or endpoint policy constraints are violated.
    """
    canonical_uri = _canonical_table_uri(table_uri)
    parsed = urlparse(canonical_uri) if "://" in canonical_uri else None
    scheme = parsed.scheme.lower() if parsed is not None and parsed.scheme else None
    if scheme in {"s3a", "s3n"}:
        msg = "Delta table URIs must use canonical s3:// scheme (not s3a:// or s3n://)."
        raise ValueError(msg)
    resolved_storage = dict(policy.storage_options) if policy is not None else {}
    resolved_log = dict(policy.log_storage_options) if policy is not None else {}
    if storage_options:
        resolved_storage.update({str(key): str(value) for key, value in storage_options.items()})
    if log_storage_options:
        resolved_log.update({str(key): str(value) for key, value in log_storage_options.items()})
    if policy is not None and policy.require_local_paths and scheme is not None:
        msg = "Remote Delta tables are disallowed by policy."
        raise ValueError(msg)
    endpoint_keys = ("endpoint", "aws_endpoint", "aws_s3_endpoint")
    storage_endpoint = next(
        (resolved_storage.get(key) for key in endpoint_keys if key in resolved_storage), None
    )
    log_endpoint = next(
        (resolved_log.get(key) for key in endpoint_keys if key in resolved_log), None
    )
    if storage_endpoint and log_endpoint and storage_endpoint != log_endpoint:
        msg = "Delta storage endpoint mismatch between object store and log store options."
        raise ValueError(msg)
    return StorageProfile(
        table_uri=table_uri,
        canonical_uri=canonical_uri,
        scheme=scheme,
        storage_options=resolved_storage,
        log_storage_options=resolved_log,
    )


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

    """
    profile = resolve_storage_profile(
        table_uri=table_uri,
        policy=policy,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return profile.to_deltalake_options(), profile.to_log_store_options()


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
    profile = resolve_storage_profile(
        table_uri=str(location.path),
        policy=policy,
        storage_options=location.storage_options,
        log_storage_options=location.delta_log_storage_options,
    )
    return replace(
        location,
        path=profile.canonical_uri,
        storage_options=profile.to_deltalake_options(),
        delta_log_storage_options=profile.to_log_store_options(),
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
    "StorageProfile",
    "apply_delta_store_policy",
    "delta_store_policy_hash",
    "resolve_delta_store_policy",
    "resolve_storage_profile",
    "resolve_store_options_for_location",
]
