"""Protocol contract for Delta service dependency injection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from storage.deltalake import DeltaSchemaRequest
from storage.deltalake.delta_read import DeltaVacuumOptions, StorageOptions

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.protocol import DeltaFeatureGate, DeltaProtocolSnapshot


class DeltaFeatureOpsPort(Protocol):
    """Structural interface for Delta feature mutation helpers."""

    def enable_change_data_feed(
        self,
        options: object,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable change-data-feed features."""
        ...

    def enable_deletion_vectors(
        self,
        options: object,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable deletion vectors."""
        ...

    def enable_row_tracking(
        self,
        options: object,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable row tracking."""
        ...

    def enable_in_commit_timestamps(
        self,
        options: object,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable in-commit timestamp metadata."""
        ...

    def enable_column_mapping(
        self,
        options: object,
        *,
        mode: str = "name",
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable column mapping."""
        ...

    def enable_v2_checkpoints(
        self,
        options: object,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable v2 checkpoints."""
        ...

    def enable_check_constraints(
        self,
        options: object,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable check constraints."""
        ...

    def add_constraints(
        self,
        options: object,
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

    def vacuum(
        self,
        *,
        path: str,
        options: DeltaVacuumOptions,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> tuple[str, ...]:
        """Run vacuum for a table and return removed files."""
        ...


__all__ = ["DeltaFeatureOpsPort", "DeltaServicePort"]
