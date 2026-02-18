"""Shared control-plane request/response types."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec

from datafusion_engine.delta.protocols import DeltaProviderHandle
from datafusion_engine.delta.specs import (
    DeltaCdfOptionsSpec as DeltaCdfOptions,
)
from datafusion_engine.delta.specs import (
    DeltaCommitOptionsSpec as DeltaCommitOptions,
)
from datafusion_engine.generated.delta_types import DeltaFeatureGate
from schema_spec.scan_options import DeltaScanOptions
from serde_msgspec import StructBaseStrict, ensure_raw


class DeltaTableRef(StructBaseStrict, frozen=True):
    """Reference to a Delta table with versioning context."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None


class DeltaProviderBundle(StructBaseStrict, frozen=True):
    """Provider response with canonical control-plane metadata."""

    provider: DeltaProviderHandle
    snapshot: Mapping[str, object]
    scan_config: Mapping[str, object]
    scan_effective: dict[str, object]
    add_actions: Sequence[Mapping[str, object]] | None = None
    predicate_error: str | None = None


class DeltaCdfProviderBundle(StructBaseStrict, frozen=True):
    """CDF provider response with canonical control-plane metadata."""

    provider: DeltaProviderHandle
    snapshot: Mapping[str, object]
    cdf_options: DeltaCdfOptions | None


class DeltaSnapshotRequest(StructBaseStrict, frozen=True):
    """Inputs required to resolve a Delta snapshot."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaProviderRequest(StructBaseStrict, frozen=True):
    """Inputs required to construct a Delta table provider."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    delta_scan: DeltaScanOptions | None
    predicate: str | None = None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaCdfRequest(StructBaseStrict, frozen=True):
    """Inputs required to construct a Delta CDF provider."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    options: DeltaCdfOptions | None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaWriteRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta write."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    data_ipc: msgspec.Raw
    mode: str
    schema_mode: str | None
    partition_columns: Sequence[str] | None
    target_file_size: int | None
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    def __post_init__(self) -> None:
        """Normalize the IPC payload into msgspec.Raw."""
        object.__setattr__(self, "data_ipc", ensure_raw(self.data_ipc))

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaDeleteRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta delete."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    predicate: str | None
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaUpdateRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta update."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    predicate: str | None
    updates: Mapping[str, str]
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaMergeRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta merge."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    source_table: str
    predicate: str
    source_alias: str | None
    target_alias: str | None
    matched_predicate: str | None
    matched_updates: Mapping[str, str]
    not_matched_predicate: str | None
    not_matched_inserts: Mapping[str, str]
    not_matched_by_source_predicate: str | None
    delete_not_matched_by_source: bool
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaOptimizeRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native optimize/compact."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    target_size: int | None
    z_order_cols: Sequence[str] | None = None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaVacuumRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native vacuum."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    retention_hours: int | None
    dry_run: bool
    enforce_retention_duration: bool
    require_vacuum_protocol_check: bool = False
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaRestoreRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native restore."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    restore_version: int | None
    restore_timestamp: str | None
    allow_unsafe_restore: bool = False
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaSetPropertiesRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native property update."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    properties: Mapping[str, str]
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaAddFeaturesRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native feature enablement."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    features: Sequence[str]
    allow_protocol_versions_increase: bool
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaFeatureEnableRequest(StructBaseStrict, frozen=True):
    """Inputs required to enable a Delta protocol feature set."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaAddConstraintsRequest(StructBaseStrict, frozen=True):
    """Inputs required to add Delta check constraints."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    constraints: Mapping[str, str]
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaDropConstraintsRequest(StructBaseStrict, frozen=True):
    """Inputs required to drop Delta check constraints."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    constraints: Sequence[str]
    raise_if_not_exists: bool = True
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaCheckpointRequest(StructBaseStrict, frozen=True):
    """Inputs required to create a Delta checkpoint or cleanup metadata."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


__all__ = [
    "DeltaAddConstraintsRequest",
    "DeltaAddFeaturesRequest",
    "DeltaCdfProviderBundle",
    "DeltaCdfRequest",
    "DeltaCheckpointRequest",
    "DeltaDeleteRequest",
    "DeltaDropConstraintsRequest",
    "DeltaFeatureEnableRequest",
    "DeltaMergeRequest",
    "DeltaOptimizeRequest",
    "DeltaProviderBundle",
    "DeltaProviderRequest",
    "DeltaRestoreRequest",
    "DeltaSetPropertiesRequest",
    "DeltaSnapshotRequest",
    "DeltaTableRef",
    "DeltaUpdateRequest",
    "DeltaVacuumRequest",
    "DeltaWriteRequest",
]
