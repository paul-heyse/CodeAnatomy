"""Unified provider registry for DataFusion table management.

This module provides a consolidated interface for registering tables,
managing UDF dependencies, and tracking registration metadata.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from utils.hashing import hash_json_canonical
from utils.registry_protocol import MutableRegistry

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.dataset.registration import DataFusionCachePolicy
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.tables.spec import TableSpec


@dataclass(frozen=True)
class RegistrationMetadata:
    """Metadata for a table registration event.

    Parameters
    ----------
    table_name
        Name of the registered table.
    registration_time_ms
        Unix timestamp in milliseconds when registered.
    table_spec_hash
        Hash of the TableSpec used for registration.
    udf_snapshot_hash
        Hash of UDF registry at registration time.
    delta_version
        Delta version used for registration (if applicable).
    """

    table_name: str
    registration_time_ms: int
    table_spec_hash: str
    udf_snapshot_hash: str | None
    delta_version: int | None

    def payload(self) -> dict[str, object]:
        """Return a serializable diagnostics payload.

        Returns
        -------
        dict[str, object]
            Payload suitable for logging and persistence.
        """
        return {
            "table_name": self.table_name,
            "registration_time_ms": self.registration_time_ms,
            "table_spec_hash": self.table_spec_hash,
            "udf_snapshot_hash": self.udf_snapshot_hash,
            "delta_version": self.delta_version,
        }


@dataclass
class ProviderRegistry:
    """Unified registry for DataFusion table providers.

    Centralizes table registration, UDF tracking, and metadata collection.
    Replaces fragmented registration patterns across the codebase.

    Parameters
    ----------
    ctx
        DataFusion SessionContext for table registration.
    runtime_profile
        Runtime profile for configuration and diagnostics.
    """

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None = None
    _registrations: MutableRegistry[str, RegistrationMetadata] = field(
        default_factory=MutableRegistry
    )
    _udf_snapshot_hash: str | None = field(default=None)

    def register(
        self,
        spec: TableSpec,
        *,
        overwrite: bool = False,
        cache_policy: DataFusionCachePolicy | None = None,
    ) -> RegistrationMetadata:
        """Register a table from a TableSpec.

        Parameters
        ----------
        spec
            Table specification with schema, location, and format.
        overwrite
            Whether to overwrite an existing registration.
        cache_policy
            Optional cache policy to apply during registration.

        Returns
        -------
        RegistrationMetadata
            Metadata about the registration event.

        Raises
        ------
        ValueError
            When table already registered and overwrite is False.
        """
        if spec.name in self._registrations and not overwrite:
            msg = f"Table {spec.name!r} already registered. Use overwrite=True."
            raise ValueError(msg)

        _, metadata = self._do_registration(spec, cache_policy=cache_policy)
        self._registrations.register(spec.name, metadata, overwrite=overwrite)
        self._emit_registration_diagnostic(metadata)
        return metadata

    def register_delta(
        self,
        spec: TableSpec,
        *,
        overwrite: bool = False,
        cache_policy: DataFusionCachePolicy | None = None,
    ) -> RegistrationMetadata:
        """Register a Delta table with version pinning support.

        Parameters
        ----------
        spec
            Table specification for a Delta table.
        overwrite
            Whether to overwrite an existing registration.
        cache_policy
            Optional cache policy to apply during registration.

        Returns
        -------
        RegistrationMetadata
            Metadata about the registration event.

        Raises
        ------
        ValueError
            When spec.format is not 'delta' or when table already registered.
        """
        if spec.format != "delta":
            msg = f"Expected delta format, got {spec.format!r}"
            raise ValueError(msg)
        return self.register(spec, overwrite=overwrite, cache_policy=cache_policy)

    def register_df(
        self,
        spec: TableSpec,
        *,
        overwrite: bool = False,
        cache_policy: DataFusionCachePolicy | None = None,
    ) -> DataFrame:
        """Register a table and return the DataFrame.

        Parameters
        ----------
        spec
            Table specification with schema, location, and format.
        overwrite
            Whether to overwrite an existing registration.
        cache_policy
            Optional cache policy to apply during registration.

        Returns
        -------
        DataFrame
            DataFrame for the registered table.

        Raises
        ------
        ValueError
            When table already registered and overwrite is False.
        """
        if spec.name in self._registrations and not overwrite:
            msg = f"Table {spec.name!r} already registered. Use overwrite=True."
            raise ValueError(msg)

        df, metadata = self._do_registration(spec, cache_policy=cache_policy)
        self._registrations.register(spec.name, metadata, overwrite=overwrite)
        self._emit_registration_diagnostic(metadata)
        return df

    def udf_registry_hash(self) -> str | None:
        """Return the current UDF registry snapshot hash.

        Returns
        -------
        str | None
            Hash of the UDF registry, or None if unavailable.
        """
        if self._udf_snapshot_hash is not None:
            return self._udf_snapshot_hash
        from datafusion_engine.udf.runtime import rust_udf_snapshot

        try:
            snapshot = rust_udf_snapshot(self.ctx)
            self._udf_snapshot_hash = _compute_snapshot_hash(snapshot)
        except (RuntimeError, TypeError, ValueError):
            return None
        else:
            return self._udf_snapshot_hash

    def registrations(self) -> Mapping[str, RegistrationMetadata]:
        """Return all current registrations.

        Returns
        -------
        Mapping[str, RegistrationMetadata]
            Read-only view of registered tables.
        """
        return self._registrations.snapshot()

    def is_registered(self, name: str) -> bool:
        """Check if a table is registered.

        Parameters
        ----------
        name
            Table name to check.

        Returns
        -------
        bool
            True if table is registered.
        """
        return name in self._registrations

    def _do_registration(
        self,
        spec: TableSpec,
        *,
        cache_policy: DataFusionCachePolicy | None,
    ) -> tuple[DataFrame, RegistrationMetadata]:
        """Perform the actual table registration.

        This method handles the DataFusion-specific registration logic.

        Returns
        -------
        tuple[DataFrame, RegistrationMetadata]
            DataFrame and metadata for the registration event.

        Raises
        ------
        ValueError
            Raised when the runtime profile is missing.
        """
        from datafusion_engine.dataset.registration import (
            _build_registration_context,
            _register_dataset_with_context,
        )
        from datafusion_engine.dataset.registry import DatasetLocation

        if self.runtime_profile is None:
            msg = "ProviderRegistry requires a runtime profile for registration."
            raise ValueError(msg)

        location = DatasetLocation(
            path=spec.storage_location,
            format=spec.format,
            delta_version=spec.delta_version,
            delta_timestamp=spec.delta_timestamp,
            storage_options=spec.storage_options,
            dataset_spec=spec.dataset_spec,
        )

        context = _build_registration_context(
            self.ctx,
            name=spec.name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=self.runtime_profile,
        )
        df = _register_dataset_with_context(context)
        metadata = RegistrationMetadata(
            table_name=spec.name,
            registration_time_ms=int(time.time() * 1000),
            table_spec_hash=spec.cache_key(),
            udf_snapshot_hash=self.udf_registry_hash(),
            delta_version=spec.delta_version,
        )
        return df, metadata

    def _emit_registration_diagnostic(
        self,
        metadata: RegistrationMetadata,
    ) -> None:
        """Emit diagnostics for registration events."""
        if self.runtime_profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact

        record_artifact(
            self.runtime_profile,
            "table_provider_registered_v1",
            metadata.payload(),
        )


def _compute_snapshot_hash(snapshot: Mapping[str, object]) -> str:
    """Compute a stable hash for a UDF snapshot.

    Returns
    -------
    str
        Short hash for the snapshot payload.
    """
    return hash_json_canonical(snapshot, str_keys=True)[:16]


__all__ = [
    "ProviderRegistry",
    "RegistrationMetadata",
]
