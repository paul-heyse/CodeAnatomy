"""TableProvider metadata tracking for DDL provenance and constraints."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableProviderMetadata:
    """Metadata associated with a registered TableProvider.

    This dataclass tracks DDL provenance, schema constraints, and other
    metadata that should be preserved alongside table registrations.

    Attributes
    ----------
    table_name : str
        Name of the registered table.
    ddl : str | None
        CREATE EXTERNAL TABLE DDL statement used for registration, if available.
    constraints : tuple[str, ...]
        Constraint expressions or constraint names for the table.
    default_values : dict[str, object]
        Column default values by column name.
    schema_fingerprint : str | None
        Stable fingerprint for the table schema, if available.
    storage_location : str | None
        Storage location URI for external tables.
    file_format : str | None
        File format (e.g., 'parquet', 'delta', 'csv') for external tables.
    partition_columns : tuple[str, ...]
        Partition column names for partitioned tables.
    metadata : dict[str, str]
        Additional key-value metadata for the table.
    """

    table_name: str
    ddl: str | None = None
    constraints: tuple[str, ...] = field(default_factory=tuple)
    default_values: dict[str, object] = field(default_factory=dict)
    schema_fingerprint: str | None = None
    storage_location: str | None = None
    file_format: str | None = None
    partition_columns: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, str] = field(default_factory=dict)

    def with_ddl(self, ddl: str) -> TableProviderMetadata:
        """Return a copy with updated DDL.

        Parameters
        ----------
        ddl : str
            CREATE EXTERNAL TABLE DDL statement.

        Returns
        -------
        TableProviderMetadata
            New metadata instance with updated DDL.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=ddl,
            constraints=self.constraints,
            default_values=self.default_values,
            schema_fingerprint=self.schema_fingerprint,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
        )

    def with_constraints(self, constraints: tuple[str, ...]) -> TableProviderMetadata:
        """Return a copy with updated constraints.

        Parameters
        ----------
        constraints : tuple[str, ...]
            Constraint expressions or constraint names.

        Returns
        -------
        TableProviderMetadata
            New metadata instance with updated constraints.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=self.ddl,
            constraints=constraints,
            default_values=self.default_values,
            schema_fingerprint=self.schema_fingerprint,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
        )

    def with_schema_fingerprint(self, fingerprint: str) -> TableProviderMetadata:
        """Return a copy with updated schema fingerprint.

        Parameters
        ----------
        fingerprint : str
            Schema fingerprint string.

        Returns
        -------
        TableProviderMetadata
            New metadata instance with updated fingerprint.
        """
        return TableProviderMetadata(
            table_name=self.table_name,
            ddl=self.ddl,
            constraints=self.constraints,
            default_values=self.default_values,
            schema_fingerprint=fingerprint,
            storage_location=self.storage_location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            metadata=self.metadata,
        )


_TABLE_PROVIDER_METADATA: dict[int, dict[str, TableProviderMetadata]] = {}


def record_table_provider_metadata(
    ctx_id: int,
    *,
    metadata: TableProviderMetadata,
) -> None:
    """Record TableProvider metadata for a session context.

    Parameters
    ----------
    ctx_id : int
        Session context ID (from id(ctx)).
    metadata : TableProviderMetadata
        Metadata to record for the table.
    """
    context_metadata = _TABLE_PROVIDER_METADATA.setdefault(ctx_id, {})
    context_metadata[metadata.table_name] = metadata


def table_provider_metadata(
    ctx_id: int,
    *,
    table_name: str,
) -> TableProviderMetadata | None:
    """Return TableProvider metadata for a table when available.

    Parameters
    ----------
    ctx_id : int
        Session context ID (from id(ctx)).
    table_name : str
        Table name to look up.

    Returns
    -------
    TableProviderMetadata | None
        Metadata instance when available.
    """
    return _TABLE_PROVIDER_METADATA.get(ctx_id, {}).get(table_name)


def all_table_provider_metadata(ctx_id: int) -> dict[str, TableProviderMetadata]:
    """Return all TableProvider metadata for a session context.

    Parameters
    ----------
    ctx_id : int
        Session context ID (from id(ctx)).

    Returns
    -------
    dict[str, TableProviderMetadata]
        Mapping of table names to metadata.
    """
    return dict(_TABLE_PROVIDER_METADATA.get(ctx_id, {}))


def clear_table_provider_metadata(ctx_id: int) -> None:
    """Clear all TableProvider metadata for a session context.

    Parameters
    ----------
    ctx_id : int
        Session context ID (from id(ctx)).
    """
    _TABLE_PROVIDER_METADATA.pop(ctx_id, None)


__all__ = [
    "TableProviderMetadata",
    "all_table_provider_metadata",
    "clear_table_provider_metadata",
    "record_table_provider_metadata",
    "table_provider_metadata",
]
