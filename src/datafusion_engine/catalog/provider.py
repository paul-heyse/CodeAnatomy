"""Registry-backed catalog and schema providers for DataFusion."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

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

from datafusion_engine.dataset.registry import (
    DatasetCatalog,
    DatasetLocation,
    resolve_dataset_schema,
)
from datafusion_engine.dataset.resolution import (
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from datafusion_engine.tables.metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)

if TYPE_CHECKING:
    from datafusion.context import TableProviderExportable

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

DATASET_HANDLE_PREFIXES: tuple[str, ...] = ("dataset://", "repo://")


def _filter_delta_catalog(catalog: DatasetCatalog) -> DatasetCatalog:
    filtered = DatasetCatalog()
    for name in catalog.names():
        location = catalog.get(name)
        if location.format != "delta":
            continue
        filtered.register(name, location, overwrite=True)
    return filtered


def _normalize_dataset_name(name: str) -> str:
    for prefix in DATASET_HANDLE_PREFIXES:
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def _table_from_dataset(dataset: object) -> Table:
    if isinstance(dataset, Table):
        return dataset
    try:
        df_internal = importlib.import_module("datafusion._internal")
    except ImportError:
        df_internal = None
    if df_internal is not None:
        try:
            raw_table_cls = df_internal.catalog.RawTable
        except AttributeError:
            raw_table_cls = None
        if raw_table_cls is not None and isinstance(dataset, raw_table_cls):
            return Table(dataset)
    provider = getattr(dataset, "__datafusion_table_provider__", None)
    if provider is not None:
        return Table(cast("TableProviderExportable", dataset))
    if isinstance(dataset, ds.Dataset):
        return Table(dataset)
    if isinstance(dataset, DataFrame):
        return Table(dataset)
    return Table(ds.dataset(dataset))


def _dataset_from_location(
    _ctx: SessionContext,
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> object:
    schema = resolve_dataset_schema(location)
    if location.format == "delta":
        if location.delta_version is not None and location.delta_timestamp is not None:
            msg = "Delta dataset open requires either delta_version or delta_timestamp."
            raise ValueError(msg)
        resolution = resolve_dataset_provider(
            DatasetResolutionRequest(
                ctx=_ctx,
                location=location,
                runtime_profile=runtime_profile,
            )
        )
        from datafusion_engine.tables.metadata import TableProviderCapsule

        return TableProviderCapsule(resolution.provider)
    return ds.dataset(
        location.path,
        format=location.format,
        filesystem=location.filesystem,
        partitioning=location.partitioning,
        schema=schema,
    )


def _delta_log_missing(location: DatasetLocation) -> bool:
    if location.format != "delta":
        return False
    path_text = str(location.path)
    if "://" in path_text:
        return False
    delta_log = Path(path_text) / "_delta_log"
    if not delta_log.exists():
        return True
    try:
        return not any(delta_log.iterdir())
    except OSError:
        return True


def _record_missing_dataset(
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    name: str,
    location: DatasetLocation,
    reason: str,
) -> None:
    if runtime_profile is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact

    record_artifact(
        runtime_profile,
        "missing_dataset_location_v1",
        {
            "name": name,
            "path": str(location.path),
            "format": location.format,
            "reason": reason,
        },
    )


@dataclass
class RegistrySchemaProvider(SchemaProvider):
    """Resolve tables from a dataset registry snapshot."""

    catalog: DatasetCatalog
    schema_name: str = "public"
    ctx: SessionContext | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None

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

    def deregister_table(self, name: str, cascade: object | None = None) -> None:
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

        Raises
        ------
        ValueError
            Raised when the registry lacks a SessionContext.
        """
        key = _normalize_dataset_name(name)
        cached = self._tables.get(key)
        if cached is not None:
            return cached
        if not self.catalog.has(key):
            return None
        location = self.catalog.get(key)
        if _delta_log_missing(location):
            _record_missing_dataset(
                runtime_profile=self.runtime_profile,
                name=key,
                location=location,
                reason="delta_log_missing",
            )
            return None
        ctx = self.ctx
        if ctx is None:
            msg = "RegistrySchemaProvider requires a SessionContext to resolve tables."
            raise ValueError(msg)
        table = _table_from_dataset(
            _dataset_from_location(ctx, location, runtime_profile=self.runtime_profile)
        )
        self._tables[key] = table
        if ctx is not None:
            metadata = TableProviderMetadata(
                table_name=key,
                storage_location=str(location.path),
                file_format=location.format or "unknown",
            )
            record_table_provider_metadata(id(ctx), metadata=metadata)
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
    runtime_profile: DataFusionRuntimeProfile | None = None

    def __post_init__(self) -> None:
        """Initialize the catalog provider state."""
        self._schema_provider: SchemaProvider | SchemaProviderExportable | Schema | None = (
            RegistrySchemaProvider(
                self.catalog,
                schema_name=self.schema_name,
                ctx=self.ctx,
                runtime_profile=self.runtime_profile,
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
    runtime_profile: DataFusionRuntimeProfile | None = None

    def __post_init__(self) -> None:
        """Initialize schema providers for registered catalogs."""
        self._schema_providers: dict[str, SchemaProvider] = {
            name: RegistrySchemaProvider(
                catalog,
                schema_name=name,
                ctx=self.ctx,
                runtime_profile=self.runtime_profile,
            )
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
    registry: DatasetCatalog,
    catalog_name: str = "datafusion",
    schema_name: str = "public",
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> RegistryCatalogProvider:
    """Register the registry-backed catalog provider on a SessionContext.

    Parameters
    ----------
    ctx:
        DataFusion session context used for catalog registration.
    registry:
        Dataset catalog used for table resolution.
    catalog_name:
        Catalog name to register in DataFusion.
    schema_name:
        Schema name to expose from the catalog provider.
    runtime_profile:
        Optional runtime profile for provider resolution defaults.

    Returns
    -------
    RegistryCatalogProvider
        Registered catalog provider.
    """
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    provider = RegistryCatalogProvider(
        _filter_delta_catalog(registry),
        schema_name=schema_name,
        ctx=ctx,
        runtime_profile=runtime_profile,
    )
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_catalog_provider(catalog_name, provider)
    return provider


def register_registry_catalogs(
    ctx: SessionContext,
    *,
    catalogs: Mapping[str, DatasetCatalog],
    catalog_name: str = "datafusion",
    default_schema: str = "public",
    runtime_profile: DataFusionRuntimeProfile | None = None,
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
    runtime_profile:
        Optional runtime profile for provider resolution defaults.

    Returns
    -------
    MultiRegistryCatalogProvider
        Registered catalog provider.
    """
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    delta_catalogs = {name: _filter_delta_catalog(catalog) for name, catalog in catalogs.items()}
    provider = MultiRegistryCatalogProvider(
        catalogs=delta_catalogs,
        default_schema=default_schema,
        ctx=ctx,
        runtime_profile=runtime_profile,
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
