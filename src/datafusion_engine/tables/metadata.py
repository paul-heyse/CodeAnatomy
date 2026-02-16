"""TableProvider metadata tracking for DDL provenance and constraints.

This module tracks metadata associated with DataFusion TableProvider registrations.
DDL statements, constraints, schema fingerprints, and storage locations are recorded
at registration time and retrieved for schema validation and introspection.

All metadata flows through DataFusion registration surfaces:
- register_listing_table(): Multi-file datasets with partition columns
- register_object_store(): Object store routing for remote storage
- CREATE EXTERNAL TABLE: DDL-based registration with options
- register_table(): TableProvider registration with custom metadata
"""

from __future__ import annotations

from dataclasses import dataclass, field
from weakref import WeakKeyDictionary

from datafusion import SessionContext


@dataclass(frozen=True)
class TableProviderCapsule:
    """Expose a PyCapsule as a DataFusion table provider."""

    capsule: object

    def datafusion_table_provider(self) -> object:
        """Return the wrapped provider capsule.

        Returns:
        -------
        object
            PyCapsule provider used by DataFusion.
        """
        return self.capsule


_TABLE_PROVIDER_ATTR = "__datafusion_table_provider__"
setattr(
    TableProviderCapsule,
    _TABLE_PROVIDER_ATTR,
    TableProviderCapsule.datafusion_table_provider,
)


@dataclass(frozen=True)
class TableProviderMetadata:
    """Metadata associated with a registered TableProvider.

    This dataclass tracks DDL provenance, schema constraints, and other
    metadata that should be preserved alongside table registrations.

    Attributes:
    ----------
    table_name : str
        Name of the registered table.
    ddl : str | None
        CREATE EXTERNAL TABLE DDL statement used for registration, if available.
    constraints : tuple[str, ...]
        Constraint expressions or constraint names for the table.
    default_values : dict[str, object]
        Column default values by column name.
    schema_identity_hash : str | None
        Stable fingerprint for the table schema, if available.
    storage_location : str | None
        Storage location URI for external tables.
    file_format : str | None
        File format (e.g., 'delta', 'csv') for external tables.
    partition_columns : tuple[str, ...]
        Partition column names for partitioned tables.
    metadata : dict[str, str]
        Additional key-value metadata for the table.
    unbounded : bool
        True if this table represents a streaming/unbounded source.
    ddl_fingerprint : str | None
        Stable fingerprint for the DDL statement, if available.
    supports_insert : bool | None
        Whether the provider supports INSERT operations.
    supports_cdf : bool | None
        Whether the provider exposes change data feed support.
    schema_adapter_enabled : bool
        Whether schema evolution adapters are attached at scan-time.
        When True, schema drift resolution happens at the TableProvider
        boundary via DataFusion-native physical expression adapters,
        eliminating the need for downstream cast/projection transforms.
    delta_scan_config : dict[str, object] | None
        Captured Delta scan configuration snapshot when applicable.
    delta_scan_identity_hash : str | None
        Stable identity hash for the Delta scan configuration.
    delta_scan_effective : dict[str, object] | None
        Effective Delta scan configuration payload resolved by the provider.
    """

    table_name: str
    ddl: str | None = None
    constraints: tuple[str, ...] = field(default_factory=tuple)
    default_values: dict[str, object] = field(default_factory=dict)
    schema_identity_hash: str | None = None
    storage_location: str | None = None
    file_format: str | None = None
    partition_columns: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, str] = field(default_factory=dict)
    unbounded: bool = False
    ddl_fingerprint: str | None = None
    supports_insert: bool | None = None
    supports_cdf: bool | None = None
    schema_adapter_enabled: bool = False
    delta_scan_config: dict[str, object] | None = None
    delta_scan_identity_hash: str | None = None
    delta_scan_effective: dict[str, object] | None = None

    def with_ddl(self, ddl: str) -> TableProviderMetadata:
        """Return a copy with updated DDL.

        Parameters
        ----------
        ddl : str
            CREATE EXTERNAL TABLE DDL statement.

        Returns:
        -------
        TableProviderMetadata
            New metadata instance with updated DDL.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=ddl,
            constraints=self.constraints,
            default_values=self.default_values,
            schema_identity_hash=self.schema_identity_hash,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
            unbounded=self.unbounded,
            ddl_fingerprint=self.ddl_fingerprint,
            supports_insert=self.supports_insert,
            supports_cdf=self.supports_cdf,
            schema_adapter_enabled=self.schema_adapter_enabled,
            delta_scan_config=self.delta_scan_config,
            delta_scan_identity_hash=self.delta_scan_identity_hash,
            delta_scan_effective=self.delta_scan_effective,
        )

    def with_constraints(self, constraints: tuple[str, ...]) -> TableProviderMetadata:
        """Return a copy with updated constraints.

        Parameters
        ----------
        constraints : tuple[str, ...]
            Constraint expressions or constraint names.

        Returns:
        -------
        TableProviderMetadata
            New metadata instance with updated constraints.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=self.ddl,
            constraints=constraints,
            default_values=self.default_values,
            schema_identity_hash=self.schema_identity_hash,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
            unbounded=self.unbounded,
            ddl_fingerprint=self.ddl_fingerprint,
            supports_insert=self.supports_insert,
            supports_cdf=self.supports_cdf,
            schema_adapter_enabled=self.schema_adapter_enabled,
            delta_scan_config=self.delta_scan_config,
            delta_scan_identity_hash=self.delta_scan_identity_hash,
            delta_scan_effective=self.delta_scan_effective,
        )

    def with_schema_identity_hash(self, fingerprint: str) -> TableProviderMetadata:
        """Return a copy with updated schema fingerprint.

        Parameters
        ----------
        fingerprint : str
            Schema fingerprint string.

        Returns:
        -------
        TableProviderMetadata
            New metadata instance with updated fingerprint.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=self.ddl,
            constraints=self.constraints,
            default_values=self.default_values,
            schema_identity_hash=fingerprint,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
            unbounded=self.unbounded,
            ddl_fingerprint=self.ddl_fingerprint,
            supports_insert=self.supports_insert,
            supports_cdf=self.supports_cdf,
            schema_adapter_enabled=self.schema_adapter_enabled,
            delta_scan_config=self.delta_scan_config,
            delta_scan_identity_hash=self.delta_scan_identity_hash,
            delta_scan_effective=self.delta_scan_effective,
        )

    def with_schema_adapter(self, *, enabled: bool = True) -> TableProviderMetadata:
        """Return a copy with schema adapter flag updated.

        Parameters
        ----------
        enabled : bool
            Whether schema evolution adapters are enabled at scan-time.

        Returns:
        -------
        TableProviderMetadata
            New metadata instance with updated schema adapter flag.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=self.ddl,
            constraints=self.constraints,
            default_values=self.default_values,
            schema_identity_hash=self.schema_identity_hash,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
            unbounded=self.unbounded,
            ddl_fingerprint=self.ddl_fingerprint,
            supports_insert=self.supports_insert,
            supports_cdf=self.supports_cdf,
            schema_adapter_enabled=enabled,
            delta_scan_config=self.delta_scan_config,
            delta_scan_identity_hash=self.delta_scan_identity_hash,
            delta_scan_effective=self.delta_scan_effective,
        )


_TABLE_PROVIDER_METADATA_BY_CONTEXT: WeakKeyDictionary[
    SessionContext,
    dict[str, TableProviderMetadata],
] = WeakKeyDictionary()


def record_table_provider_metadata(
    ctx: SessionContext,
    *,
    metadata: TableProviderMetadata,
) -> None:
    """Record TableProvider metadata for a session context.

    Parameters
    ----------
    ctx : SessionContext
        Session context instance.
    metadata : TableProviderMetadata
        Metadata to record for the table.
    """
    context_metadata = _TABLE_PROVIDER_METADATA_BY_CONTEXT.get(ctx)
    if context_metadata is None:
        context_metadata = {}
        _TABLE_PROVIDER_METADATA_BY_CONTEXT[ctx] = context_metadata
    context_metadata[metadata.table_name] = metadata


def table_provider_metadata(
    ctx: SessionContext,
    *,
    table_name: str,
) -> TableProviderMetadata | None:
    """Return TableProvider metadata for a table when available.

    Parameters
    ----------
    ctx : SessionContext
        Session context instance.
    table_name : str
        Table name to look up.

    Returns:
    -------
    TableProviderMetadata | None
        Metadata instance when available.
    """
    return _TABLE_PROVIDER_METADATA_BY_CONTEXT.get(ctx, {}).get(table_name)


def all_table_provider_metadata(ctx: SessionContext) -> dict[str, TableProviderMetadata]:
    """Return all TableProvider metadata for a session context.

    Parameters
    ----------
    ctx : SessionContext
        Session context instance.

    Returns:
    -------
    dict[str, TableProviderMetadata]
        Mapping of table names to metadata.
    """
    return dict(_TABLE_PROVIDER_METADATA_BY_CONTEXT.get(ctx, {}))


def clear_table_provider_metadata(ctx: SessionContext) -> None:
    """Clear all TableProvider metadata for a session context.

    Parameters
    ----------
    ctx : SessionContext
        Session context instance.
    """
    _TABLE_PROVIDER_METADATA_BY_CONTEXT.pop(ctx, None)


__all__ = [
    "TableProviderCapsule",
    "TableProviderMetadata",
    "all_table_provider_metadata",
    "clear_table_provider_metadata",
    "record_table_provider_metadata",
    "table_provider_metadata",
]
