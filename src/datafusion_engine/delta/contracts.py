"""Delta provider contracts for scan configuration and snapshot pinning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgspec

from datafusion_engine.delta.control_plane import DeltaCdfRequest, DeltaProviderRequest
from datafusion_engine.delta.payload import settings_bool
from datafusion_engine.delta.scan_config import (
    delta_scan_config_snapshot_from_options,
    delta_scan_identity_hash,
    resolve_delta_scan_options,
)
from datafusion_engine.identity import schema_identity_hash
from storage.deltalake import DeltaCdfOptions, DeltaSchemaRequest
from utils.storage_options import merged_storage_options

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.protocol import DeltaFeatureGate
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.contracts import DeltaScanOptions


@dataclass(frozen=True)
class DeltaProviderContract:
    """Resolved contract for Delta provider construction."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    scan_options: DeltaScanOptions | None
    scan_snapshot: object | None
    scan_identity_hash: str | None

    def to_request(
        self,
        *,
        predicate: str | None,
        gate: DeltaFeatureGate | None,
    ) -> DeltaProviderRequest:
        """Return a provider request derived from this contract.

        Parameters
        ----------
        predicate
            Optional predicate to apply during provider resolution.
        gate
            Optional Delta feature gate to enforce.

        Returns:
        -------
        DeltaProviderRequest
            Provider request populated from the contract.
        """
        return DeltaProviderRequest(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
            delta_scan=self.scan_options,
            predicate=predicate,
            gate=gate,
        )


@dataclass(frozen=True)
class DeltaCdfContract:
    """Resolved contract for Delta CDF provider construction."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    options: DeltaCdfOptions | None
    gate: DeltaFeatureGate | None

    def to_request(self) -> DeltaCdfRequest:
        """Return a CDF provider request derived from this contract.

        Returns:
        -------
        DeltaCdfRequest
            Provider request populated from the contract.
        """
        from datafusion_engine.delta.specs import DeltaCdfOptionsSpec

        options = self.options
        if options is None:
            resolved = None
        else:
            columns = tuple(options.columns) if options.columns is not None else None
            resolved = DeltaCdfOptionsSpec(
                starting_version=options.starting_version,
                ending_version=options.ending_version,
                starting_timestamp=options.starting_timestamp,
                ending_timestamp=options.ending_timestamp,
                columns=columns,
                predicate=options.predicate,
                allow_out_of_range=options.allow_out_of_range,
            )
        return DeltaCdfRequest(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
            options=resolved,
            gate=self.gate,
        )


def build_delta_provider_contract(
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaProviderContract:
    """Build the Delta provider contract for a dataset location.

    Parameters
    ----------
    location
        Dataset location describing the Delta table.
    runtime_profile
        Optional runtime profile used to resolve scan defaults.

    Returns:
    -------
    DeltaProviderContract
        Contract containing scan configuration and snapshot metadata.
    """
    scan_options = _resolve_delta_scan(location, runtime_profile=runtime_profile)
    scan_snapshot = delta_scan_config_snapshot_from_options(scan_options)
    return DeltaProviderContract(
        table_uri=str(location.path),
        storage_options=merged_storage_options(
            location.storage_options,
            location.resolved.delta_log_storage_options,
        ),
        version=location.delta_version,
        timestamp=location.delta_timestamp,
        scan_options=scan_options,
        scan_snapshot=scan_snapshot,
        scan_identity_hash=delta_scan_identity_hash(scan_snapshot),
    )


def build_delta_cdf_contract(location: DatasetLocation) -> DeltaCdfContract:
    """Build the Delta CDF provider contract for a dataset location.

    Parameters
    ----------
    location
        Dataset location describing the Delta table.

    Returns:
    -------
    DeltaCdfContract
        Contract containing CDF scan configuration and snapshot metadata.
    """
    return DeltaCdfContract(
        table_uri=str(location.path),
        storage_options=merged_storage_options(
            location.storage_options,
            location.resolved.delta_log_storage_options,
        ),
        version=location.delta_version,
        timestamp=location.delta_timestamp,
        options=location.delta_cdf_options,
        gate=location.resolved.delta_feature_gate,
    )


def _resolve_delta_scan(
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DeltaScanOptions | None:
    delta_scan = resolve_delta_scan_options(
        location,
        scan_policy=runtime_profile.policies.scan_policy if runtime_profile is not None else None,
    )
    if delta_scan is None:
        return None
    settings = _profile_settings(runtime_profile)
    pushdown_setting = settings_bool(
        settings,
        "datafusion.execution.parquet.pushdown_filters",
    )
    if pushdown_setting is not None:
        delta_scan = msgspec.structs.replace(delta_scan, enable_parquet_pushdown=pushdown_setting)
    if delta_scan.schema_force_view_types is not None:
        return delta_scan
    schema_force_setting = settings_bool(
        settings,
        "datafusion.execution.parquet.schema_force_view_types",
    )
    if schema_force_setting is None:
        schema_force_setting = _schema_hardening_view_types(runtime_profile)
    return msgspec.structs.replace(delta_scan, schema_force_view_types=schema_force_setting)


def _schema_hardening_view_types(runtime_profile: DataFusionRuntimeProfile | None) -> bool:
    if runtime_profile is None:
        return False
    hardening = runtime_profile.policies.schema_hardening
    if hardening is not None:
        return hardening.enable_view_types
    return runtime_profile.policies.schema_hardening_name == "arrow_performance"


def _profile_settings(runtime_profile: DataFusionRuntimeProfile | None) -> Mapping[str, str]:
    if runtime_profile is None:
        return {}
    return runtime_profile.settings_payload()


class DeltaSchemaMismatchError(ValueError):
    """Raised when a Delta table schema mismatches expectations."""

    def __init__(
        self,
        *,
        table_uri: str,
        expected_hash: str | None,
        actual_hash: str | None,
    ) -> None:
        """Initialize the instance.

        Args:
            table_uri: Description.
            expected_hash: Description.
            actual_hash: Description.
        """
        msg = (
            "Delta schema mismatch for "
            f"{table_uri!r}: expected={expected_hash!r}, actual={actual_hash!r}."
        )
        super().__init__(msg)


def delta_schema_identity_hash(request: DeltaSchemaRequest) -> str | None:
    """Return the schema identity hash for a Delta table when available.

    Parameters
    ----------
    request
        Delta schema request describing the table and snapshot.

    Returns:
    -------
    str | None
        Schema identity hash when available.
    """
    from datafusion_engine.delta.service import delta_service_for_profile

    schema = delta_service_for_profile(None).table_schema(request)
    if schema is None:
        return None
    return schema_identity_hash(schema)


def enforce_schema_evolution(
    *,
    request: DeltaSchemaRequest,
    expected_schema_hash: str | None,
    allow_evolution: bool,
) -> str | None:
    """Enforce schema evolution gating for Delta tables.

    Args:
        request: Delta schema request.
        expected_schema_hash: Expected schema identity hash.
        allow_evolution: Whether schema drift is allowed.

    Returns:
        str | None: Result.

    Raises:
        DeltaSchemaMismatchError: If schema hashes differ and evolution is disallowed.
    """
    actual_hash = delta_schema_identity_hash(request)
    if expected_schema_hash is None or actual_hash is None:
        return actual_hash
    if actual_hash != expected_schema_hash and not allow_evolution:
        raise DeltaSchemaMismatchError(
            table_uri=request.path,
            expected_hash=expected_schema_hash,
            actual_hash=actual_hash,
        )
    return actual_hash


__all__ = [
    "DeltaCdfContract",
    "DeltaProviderContract",
    "DeltaSchemaMismatchError",
    "build_delta_cdf_contract",
    "build_delta_provider_contract",
    "delta_schema_identity_hash",
    "enforce_schema_evolution",
]
