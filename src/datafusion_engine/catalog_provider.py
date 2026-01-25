"""Registry-backed catalog and schema providers for DataFusion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import pyarrow.dataset as ds
from datafusion import SessionContext
from datafusion.catalog import (
    CatalogProvider,
    Schema,
    SchemaProvider,
    SchemaProviderExportable,
    Table,
)
from datafusion.dataframe import DataFrame
from deltalake import DeltaTable

from datafusion_engine.table_provider_metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)
from ibis_engine.registry import (
    DatasetCatalog,
    DatasetLocation,
    IbisDatasetRegistry,
    resolve_dataset_schema,
)

DATASET_HANDLE_PREFIXES: tuple[str, ...] = ("dataset://", "repo://")


def _normalize_dataset_name(name: str) -> str:
    for prefix in DATASET_HANDLE_PREFIXES:
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def _table_from_dataset(dataset: object) -> Table:
    if isinstance(dataset, Table):
        return dataset
    if isinstance(dataset, DeltaTable):
        return Table(dataset)
    if isinstance(dataset, ds.Dataset):
        return Table(dataset)
    if isinstance(dataset, DataFrame):
        return Table(dataset)
    return Table(ds.dataset(dataset))


def _dataset_from_location(location: DatasetLocation) -> object:
    schema = resolve_dataset_schema(location)
    if location.format == "delta":
        storage_options = dict(location.storage_options)
        if location.delta_version is not None and location.delta_timestamp is not None:
            msg = "Delta dataset open requires either delta_version or delta_timestamp."
            raise ValueError(msg)
        table = DeltaTable(
            str(location.path),
            storage_options=storage_options or None,
            version=location.delta_version,
        )
        if location.delta_timestamp is not None:
            table.load_as_version(location.delta_timestamp)
        return table
    return ds.dataset(
        location.path,
        format=location.format,
        filesystem=location.filesystem,
        partitioning=location.partitioning,
        schema=schema,
    )


@dataclass
class RegistrySchemaProvider(SchemaProvider):
    """Resolve tables from a dataset registry snapshot."""

    catalog: DatasetCatalog
    schema_name: str = "public"
    ctx: SessionContext | None = None

    def __post_init__(self) -> None:
        """Initialize the schema provider cache."""
        self._tables: dict[str, Table] = {}

    def owner_name(self) -> str | None:
        """Return the owner name for the schema provider.

        Returns
        -------
        str | None
            Owner name for diagnostics, when available.
        """
        return self.schema_name

    def register_table(self, name: str, table: object) -> None:
        """Register a table in the schema provider.

        Parameters
        ----------
        name:
            Table name to register.
        table:
            Table-like object to register.
        """
        key = _normalize_dataset_name(name)
        self._tables[key] = _table_from_dataset(table)
        if self.ctx is not None and self.catalog.has(key):
            location = self.catalog.get(key)
            metadata = TableProviderMetadata(
                table_name=key,
                storage_location=str(location.path),
                file_format=location.format or "unknown",
            )
            record_table_provider_metadata(id(self.ctx), metadata=metadata)

    def deregister_table(self, name: str, cascade: object) -> None:
        """Remove a table from the schema provider.

        Parameters
        ----------
        name:
            Table name to deregister.
        cascade:
            Cascade behavior flag from the caller.
        """
        _ = cascade
        key = _normalize_dataset_name(name)
        self._tables.pop(key, None)

    def table(self, name: str) -> Table | None:
        """Return a table for the given name.

        Parameters
        ----------
        name:
            Table name to resolve.

        Returns
        -------
        datafusion.catalog.Table | None
            Resolved table when available.
        """
        key = _normalize_dataset_name(name)
        cached = self._tables.get(key)
        if cached is not None:
            return cached
        if not self.catalog.has(key):
            return None
        location = self.catalog.get(key)
        table = _table_from_dataset(_dataset_from_location(location))
        self._tables[key] = table
        if self.ctx is not None:
            metadata = TableProviderMetadata(
                table_name=key,
                storage_location=str(location.path),
                file_format=location.format or "unknown",
            )
            record_table_provider_metadata(id(self.ctx), metadata=metadata)
        return table

    def table_metadata(self, name: str) -> TableProviderMetadata | None:
        """Return metadata for a registered table.

        Parameters
        ----------
        name:
            Table name to look up.

        Returns
        -------
        TableProviderMetadata | None
            Metadata when available.
        """
        if self.ctx is None:
            return None
        key = _normalize_dataset_name(name)
        return table_provider_metadata(id(self.ctx), table_name=key)

    def table_exist(self, name: str) -> bool:
        """Return whether a table exists in the schema provider.

        Parameters
        ----------
        name:
            Table name to check.

        Returns
        -------
        bool
            True when the table exists.
        """
        key = _normalize_dataset_name(name)
        return key in self._tables or self.catalog.has(key)

    def table_names(self) -> set[str]:
        """Return available table names.

        Returns
        -------
        set[str]
            Registered table names.
        """
        return set(self._tables) | set(self.catalog.names())


@dataclass
class RegistryCatalogProvider(CatalogProvider):
    """Registry-backed catalog provider."""

    catalog: DatasetCatalog
    schema_name: str = "public"
    ctx: SessionContext | None = None

    def __post_init__(self) -> None:
        """Initialize the catalog provider state."""
        self._schema_provider: SchemaProvider | SchemaProviderExportable | Schema | None = (
            RegistrySchemaProvider(
                self.catalog,
                schema_name=self.schema_name,
                ctx=self.ctx,
            )
        )

    def register_schema(
        self,
        name: str,
        schema: SchemaProvider | SchemaProviderExportable | Schema,
    ) -> None:
        """Register a schema provider on the catalog.

        Parameters
        ----------
        name:
            Schema name to register.
        schema:
            Schema provider or schema instance.
        """
        if name != self.schema_name:
            return
        self._schema_provider = schema

    def deregister_schema(self, name: str, cascade: object) -> None:
        """Deregister the schema provider from the catalog.

        Parameters
        ----------
        name:
            Schema name to deregister.
        cascade:
            Cascade behavior flag from the caller.
        """
        _ = cascade
        if name == self.schema_name:
            self._schema_provider = None

    def schema(self, name: str) -> Schema | None:
        """Return the schema provider for the catalog.

        Parameters
        ----------
        name:
            Schema name to resolve.

        Returns
        -------
        datafusion.catalog.Schema | None
            Resolved schema when available.
        """
        if name != self.schema_name:
            return None
        if self._schema_provider is None:
            return None
        return cast("Schema", self._schema_provider)

    def schema_names(self) -> set[str]:
        """Return available schema names.

        Returns
        -------
        set[str]
            Available schema names.
        """
        return {self.schema_name} if self._schema_provider is not None else set()


@dataclass
class MultiRegistryCatalogProvider(CatalogProvider):
    """Catalog provider that exposes multiple registry-backed schemas."""

    catalogs: Mapping[str, DatasetCatalog]
    default_schema: str = "public"
    ctx: SessionContext | None = None

    def __post_init__(self) -> None:
        """Initialize schema providers for registered catalogs."""
        self._schema_providers: dict[str, SchemaProvider] = {
            name: RegistrySchemaProvider(catalog, schema_name=name, ctx=self.ctx)
            for name, catalog in self.catalogs.items()
        }
        self._external_schemas: dict[str, SchemaProvider | SchemaProviderExportable | Schema] = {}

    def register_schema(
        self,
        name: str,
        schema: SchemaProvider | SchemaProviderExportable | Schema,
    ) -> None:
        """Register a schema provider on the catalog.

        Parameters
        ----------
        name:
            Schema name to register.
        schema:
            Schema provider or schema instance to expose.
        """
        self._external_schemas[name] = schema

    def deregister_schema(self, name: str, cascade: object) -> None:
        """Deregister a schema provider from the catalog.

        Parameters
        ----------
        name:
            Schema name to deregister.
        cascade:
            Cascade behavior flag from the caller.
        """
        _ = cascade
        self._external_schemas.pop(name, None)

    def schema(self, name: str) -> Schema | None:
        """Return the schema provider for the catalog.

        Parameters
        ----------
        name:
            Schema name to resolve.

        Returns
        -------
        datafusion.catalog.Schema | None
            Resolved schema provider when available.
        """
        registered = self._external_schemas.get(name)
        if registered is not None:
            return cast("Schema", registered)
        provider = self._schema_providers.get(name)
        if provider is None:
            return None
        return cast("Schema", provider)

    def schema_names(self) -> set[str]:
        """Return available schema names.

        Returns
        -------
        set[str]
            Schema names exposed by the provider.
        """
        return set(self._schema_providers) | set(self._external_schemas)


def register_registry_catalog(
    ctx: SessionContext,
    *,
    registry: IbisDatasetRegistry | DatasetCatalog,
    catalog_name: str = "registry",
    schema_name: str = "public",
) -> RegistryCatalogProvider:
    """Register the registry-backed catalog provider on a SessionContext.

    Parameters
    ----------
    ctx:
        DataFusion session context used for catalog registration.
    registry:
        Dataset registry or catalog used for table resolution.
    catalog_name:
        Catalog name to register in DataFusion.
    schema_name:
        Schema name to expose from the catalog provider.

    Returns
    -------
    RegistryCatalogProvider
        Registered catalog provider.
    """
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    dataset_catalog = registry.catalog if isinstance(registry, IbisDatasetRegistry) else registry
    provider = RegistryCatalogProvider(dataset_catalog, schema_name=schema_name, ctx=ctx)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_catalog_provider(catalog_name, provider)
    return provider


def register_registry_catalogs(
    ctx: SessionContext,
    *,
    catalogs: Mapping[str, DatasetCatalog],
    catalog_name: str = "codeintel",
    default_schema: str = "public",
) -> MultiRegistryCatalogProvider:
    """Register multiple registry catalogs on a SessionContext.

    Parameters
    ----------
    ctx:
        DataFusion session context used for catalog registration.
    catalogs:
        Mapping of schema names to dataset catalogs.
    catalog_name:
        Catalog name to register in DataFusion.
    default_schema:
        Default schema name for diagnostics and policy defaults.

    Returns
    -------
    MultiRegistryCatalogProvider
        Registered catalog provider.
    """
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    provider = MultiRegistryCatalogProvider(
        catalogs=catalogs, default_schema=default_schema, ctx=ctx
    )
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_catalog_provider(catalog_name, provider)
    return provider


__all__ = [
    "DATASET_HANDLE_PREFIXES",
    "MultiRegistryCatalogProvider",
    "RegistryCatalogProvider",
    "RegistrySchemaProvider",
    "register_registry_catalog",
    "register_registry_catalogs",
]
