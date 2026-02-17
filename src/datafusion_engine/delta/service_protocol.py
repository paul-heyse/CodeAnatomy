"""Protocol contract for Delta service dependency injection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from storage.deltalake import DeltaSchemaRequest
from storage.deltalake.delta_read import (
    DeltaCdfOptions,
    DeltaReadRequest,
    DeltaVacuumOptions,
    StorageOptions,
)
from storage.deltalake.delta_write import DeltaFeatureMutationOptions

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import RecordBatchReaderLike
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility
    from datafusion_engine.delta.protocol import DeltaFeatureGate, DeltaProtocolSnapshot
    from datafusion_engine.delta.service import (
        DeltaFeatureMutationRequest,
        _ProviderArtifactRecordRequest,
    )


class DeltaFeatureOpsPort(Protocol):
    """Structural interface for Delta feature mutation helpers."""

    def feature_mutation_options(
        self,
        request: DeltaFeatureMutationRequest,
    ) -> DeltaFeatureMutationOptions:
        """Build resolved feature mutation options."""
        ...

    def enable_features(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        features: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Enable Delta table feature properties."""
        ...

    def enable_change_data_feed(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable change-data-feed features."""
        ...

    def enable_deletion_vectors(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable deletion vectors."""
        ...

    def enable_row_tracking(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable row tracking."""
        ...

    def enable_in_commit_timestamps(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        enablement_version: int | None = None,
        enablement_timestamp: str | None = None,
    ) -> Mapping[str, object]:
        """Enable in-commit timestamp metadata."""
        ...

    def enable_column_mapping(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        mode: str = "name",
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable column mapping."""
        ...

    def enable_v2_checkpoints(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable v2 checkpoints."""
        ...

    def enable_check_constraints(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable check constraints."""
        ...

    def add_constraints(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        constraints: Mapping[str, str],
    ) -> Mapping[str, object]:
        """Add check constraints to an existing table."""
        ...


class DeltaServicePort(Protocol):
    """Structural interface for injected Delta service usage."""

    @property
    def features(self) -> DeltaFeatureOpsPort:
        """Feature-mutation helpers bound to the service."""
        ...

    def provider(
        self,
        *,
        location: DatasetLocation,
        name: str | None = None,
        predicate: str | None = None,
        scan_files: Sequence[str] | None = None,
    ) -> object:
        """Build a table provider for a dataset location."""
        ...

    def table_schema(self, request: DeltaSchemaRequest) -> pa.Schema | None:
        """Return the schema for a Delta table, if available."""
        ...

    def table_version(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        gate: DeltaFeatureGate | None = None,
    ) -> int | None:
        """Return the latest Delta version for a table path, if available."""
        ...

    def history_snapshot(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        limit: int = 1,
        gate: DeltaFeatureGate | None = None,
    ) -> Mapping[str, object] | None:
        """Return the latest history snapshot payload."""
        ...

    def protocol_snapshot(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        gate: DeltaFeatureGate | None = None,
    ) -> DeltaProtocolSnapshot | None:
        """Return protocol snapshot metadata."""
        ...

    def cdf_enabled(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        """Return whether CDF is enabled for a Delta table path."""
        ...

    def read_table_eager(self, request: DeltaReadRequest) -> pa.Table:
        """Read a Delta snapshot and return a materialized table."""
        ...

    def read_cdf(
        self,
        *,
        table_path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> RecordBatchReaderLike:
        """Read CDF rows as a streaming reader."""
        ...

    def read_cdf_eager(
        self,
        *,
        table_path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        """Read CDF rows as an eager Arrow table."""
        ...

    def vacuum(
        self,
        *,
        path: str,
        options: DeltaVacuumOptions,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> list[str]:
        """Run vacuum for a table and return removed files."""
        ...

    def create_checkpoint(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        dataset_name: str | None = None,
    ) -> Mapping[str, object]:
        """Create a checkpoint for the table and return report payload."""
        ...

    def cleanup_log(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        dataset_name: str | None = None,
    ) -> Mapping[str, object]:
        """Cleanup log metadata for the table and return report payload."""
        ...

    def provider_artifact_payload(
        self,
        *,
        request: _ProviderArtifactRecordRequest,
        compatibility: DeltaExtensionCompatibility,
    ) -> Mapping[str, object]:
        """Build provider artifact payload for diagnostics."""
        ...


__all__ = ["DeltaFeatureOpsPort", "DeltaServicePort"]
