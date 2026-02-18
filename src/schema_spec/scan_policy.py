"""Canonical scan-policy types and policy-application helpers."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from core.config_base import config_fingerprint
from datafusion_engine.extensions.schema_runtime import load_schema_runtime
from schema_spec.scan_options import DataFusionScanOptions, DeltaScanOptions
from serde_msgspec import StructBaseStrict


class ScanPolicyDefaults(StructBaseStrict, frozen=True):
    """Policy defaults for DataFusion listing scans."""

    collect_statistics: bool | None = None
    list_files_cache_ttl: str | None = None
    list_files_cache_limit: str | None = None
    meta_fetch_concurrency: int | None = None

    def has_any(self) -> bool:
        """Return ``True`` when any default is configured."""
        return any(
            value is not None
            for value in (
                self.collect_statistics,
                self.list_files_cache_ttl,
                self.list_files_cache_limit,
                self.meta_fetch_concurrency,
            )
        )

    def apply(self, options: DataFusionScanOptions) -> DataFusionScanOptions:
        """Apply defaults to listing scan options.

        Returns:
        -------
        DataFusionScanOptions
            Scan options with defaults applied.
        """
        return msgspec.structs.replace(
            options,
            collect_statistics=(
                options.collect_statistics
                if options.collect_statistics is not None
                else self.collect_statistics
            ),
            list_files_cache_ttl=(
                options.list_files_cache_ttl
                if options.list_files_cache_ttl is not None
                else self.list_files_cache_ttl
            ),
            list_files_cache_limit=(
                options.list_files_cache_limit
                if options.list_files_cache_limit is not None
                else self.list_files_cache_limit
            ),
            meta_fetch_concurrency=(
                options.meta_fetch_concurrency
                if options.meta_fetch_concurrency is not None
                else self.meta_fetch_concurrency
            ),
        )

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return deterministic payload used for policy hashing.

        Returns:
        -------
        Mapping[str, object]
            Serializable payload for fingerprint computation.
        """
        return {
            "collect_statistics": self.collect_statistics,
            "list_files_cache_ttl": self.list_files_cache_ttl,
            "list_files_cache_limit": self.list_files_cache_limit,
            "meta_fetch_concurrency": self.meta_fetch_concurrency,
        }


class DeltaScanPolicyDefaults(StructBaseStrict, frozen=True):
    """Policy defaults for Delta scan options."""

    enable_parquet_pushdown: bool | None = None
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool | None = None
    file_column_name: str | None = None

    def has_any(self) -> bool:
        """Return ``True`` when any default is configured."""
        return any(
            value is not None
            for value in (
                self.enable_parquet_pushdown,
                self.schema_force_view_types,
                self.wrap_partition_values,
                self.file_column_name,
            )
        )

    def apply(self, options: DeltaScanOptions) -> DeltaScanOptions:
        """Apply defaults to Delta scan options.

        Returns:
        -------
        DeltaScanOptions
            Delta scan options with defaults applied.
        """
        return msgspec.structs.replace(
            options,
            enable_parquet_pushdown=(
                self.enable_parquet_pushdown
                if self.enable_parquet_pushdown is not None
                else options.enable_parquet_pushdown
            ),
            schema_force_view_types=(
                self.schema_force_view_types
                if self.schema_force_view_types is not None
                else options.schema_force_view_types
            ),
            wrap_partition_values=(
                self.wrap_partition_values
                if self.wrap_partition_values is not None
                else options.wrap_partition_values
            ),
            file_column_name=(
                self.file_column_name
                if self.file_column_name is not None
                else options.file_column_name
            ),
        )

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return deterministic payload used for policy hashing.

        Returns:
        -------
        Mapping[str, object]
            Serializable payload for fingerprint computation.
        """
        return {
            "enable_parquet_pushdown": self.enable_parquet_pushdown,
            "schema_force_view_types": self.schema_force_view_types,
            "wrap_partition_values": self.wrap_partition_values,
            "file_column_name": self.file_column_name,
        }


class ScanPolicyConfig(StructBaseStrict, frozen=True):
    """Policy defaults for scan options by dataset format."""

    listing: ScanPolicyDefaults = msgspec.field(default_factory=ScanPolicyDefaults)
    delta_listing: ScanPolicyDefaults = msgspec.field(default_factory=ScanPolicyDefaults)
    delta_scan: DeltaScanPolicyDefaults = msgspec.field(default_factory=DeltaScanPolicyDefaults)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return deterministic payload used for policy hashing.

        Returns:
        -------
        Mapping[str, object]
            Serializable payload for fingerprint computation.
        """
        return {
            "listing": self.listing.fingerprint_payload(),
            "delta_listing": self.delta_listing.fingerprint_payload(),
            "delta_scan": self.delta_scan.fingerprint_payload(),
        }

    def fingerprint(self) -> str:
        """Return deterministic fingerprint hash.

        Returns:
        -------
        str
            Policy fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())


def apply_scan_policy(
    options: DataFusionScanOptions | None,
    *,
    policy: ScanPolicyConfig | None,
    dataset_format: str | None,
) -> DataFusionScanOptions | None:
    """Apply scan-policy defaults through the runtime bridge.

    Returns:
    -------
    DataFusionScanOptions | None
        Effective scan options or ``None`` when unchanged.

    Raises:
        RuntimeError: If the runtime bridge returns invalid payloads.
    """
    if policy is None:
        return options
    defaults = policy.delta_listing if dataset_format == "delta" else policy.listing
    if options is None and not defaults.has_any():
        return None
    base = options or DataFusionScanOptions()
    runtime = load_schema_runtime()
    defaults_payload = defaults.apply(DataFusionScanOptions())
    merged_json = runtime.apply_scan_policy_json(
        msgspec.json.encode(base).decode("utf-8"),
        msgspec.json.encode(defaults_payload).decode("utf-8"),
    )
    try:
        return msgspec.json.decode(merged_json, type=DataFusionScanOptions)
    except msgspec.DecodeError as exc:
        msg = "SchemaRuntime.apply_scan_policy_json returned invalid DataFusionScanOptions JSON."
        raise RuntimeError(msg) from exc


def apply_delta_scan_policy(
    options: DeltaScanOptions | None,
    *,
    policy: ScanPolicyConfig | None,
) -> DeltaScanOptions | None:
    """Apply Delta scan-policy defaults through the runtime bridge.

    Returns:
    -------
    DeltaScanOptions | None
        Effective Delta scan options or ``None`` when unchanged.

    Raises:
        RuntimeError: If the runtime bridge returns invalid payloads.
    """
    if policy is None:
        return options
    defaults = policy.delta_scan
    if options is None and not defaults.has_any():
        return None
    base = options or DeltaScanOptions()
    runtime = load_schema_runtime()
    defaults_payload = defaults.apply(DeltaScanOptions())
    merged_json = runtime.apply_delta_scan_policy_json(
        msgspec.json.encode(base).decode("utf-8"),
        msgspec.json.encode(defaults_payload).decode("utf-8"),
    )
    try:
        return msgspec.json.decode(merged_json, type=DeltaScanOptions)
    except msgspec.DecodeError as exc:
        msg = "SchemaRuntime.apply_delta_scan_policy_json returned invalid DeltaScanOptions JSON."
        raise RuntimeError(msg) from exc


__all__ = [
    "DeltaScanPolicyDefaults",
    "ScanPolicyConfig",
    "ScanPolicyDefaults",
    "apply_delta_scan_policy",
    "apply_scan_policy",
]
