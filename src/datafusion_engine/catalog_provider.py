"""Registry-backed catalog and schema providers for DataFusion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
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

from datafusion_engine.dataset_registry import (
    DatasetCatalog,
    DatasetLocation,
    resolve_dataset_schema,
    resolve_delta_cdf_policy,
    resolve_delta_feature_gate,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from datafusion_engine.table_provider_metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)

if TYPE_CHECKING:
    from datafusion.context import TableProviderExportable

    from datafusion_engine.plugin_manager import DataFusionPluginManager
    from storage.deltalake.delta import DeltaCdfOptions

DATASET_HANDLE_PREFIXES: tuple[str, ...] = ("dataset://", "repo://")


def _normalize_dataset_name(name: str) -> str:
    for prefix in DATASET_HANDLE_PREFIXES:
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def _table_from_dataset(dataset: object) -> Table:
    if isinstance(dataset, Table):
        return dataset
    if isinstance(dataset, ds.Dataset):
        return Table(dataset)
    if isinstance(dataset, DataFrame):
        return Table(dataset)
    provider = getattr(dataset, "__datafusion_table_provider__", None)
    if callable(provider):
        return Table(cast("TableProviderExportable", dataset))
    return Table(ds.dataset(dataset))


def _delta_storage_options(location: DatasetLocation) -> dict[str, str] | None:
    storage: dict[str, str] = {
        str(key): str(value) for key, value in dict(location.storage_options).items()
    }
    log_storage_options = resolve_delta_log_storage_options(location)
    if log_storage_options:
        storage.update({str(key): str(value) for key, value in log_storage_options.items()})
    return storage or None


def _delta_gate_payload(gate: object | None) -> dict[str, object] | None:
    if gate is None:
        return None
    return {
        "min_reader_version": getattr(gate, "min_reader_version", None),
        "min_writer_version": getattr(gate, "min_writer_version", None),
        "required_reader_features": list(getattr(gate, "required_reader_features", ())),
        "required_writer_features": list(getattr(gate, "required_writer_features", ())),
    }


def _cdf_options_payload(options: DeltaCdfOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "columns": list(options.columns) if options.columns is not None else None,
        "predicate": options.predicate,
        "allow_out_of_range": options.allow_out_of_range,
    }


def _requires_delta_cdf(location: DatasetLocation) -> bool:
    if location.datafusion_provider == "delta_cdf":
        return True
    if location.delta_cdf_options is not None:
        return True
    cdf_policy = resolve_delta_cdf_policy(location)
    if cdf_policy is not None and cdf_policy.required:
        return True
    return bool(
        location.dataset_spec is not None
        and location.dataset_spec.dataset_kind == "delta_cdf"
    )


def _delta_provider_options(
    location: DatasetLocation,
    *,
    storage_options: Mapping[str, str] | None,
    delta_scan: object | None,
    gate_payload: Mapping[str, object] | None,
) -> dict[str, object]:
    options: dict[str, object] = {
        "table_uri": str(location.path),
        "storage_options": dict(storage_options) if storage_options else None,
        "version": location.delta_version,
        "timestamp": location.delta_timestamp,
        "file_column_name": getattr(delta_scan, "file_column_name", None),
        "enable_parquet_pushdown": getattr(delta_scan, "enable_parquet_pushdown", None),
        "schema_force_view_types": getattr(delta_scan, "schema_force_view_types", None),
        "wrap_partition_values": getattr(delta_scan, "wrap_partition_values", None),
    }
    if location.files:
        options["files"] = list(location.files)
    if gate_payload is not None:
        options.update(gate_payload)
    return options


def _delta_cdf_provider_options(
    location: DatasetLocation,
    *,
    storage_options: Mapping[str, str] | None,
    gate_payload: Mapping[str, object] | None,
) -> dict[str, object]:
    cdf_payload = _cdf_options_payload(location.delta_cdf_options) or {}
    options: dict[str, object] = {
        "table_uri": str(location.path),
        "storage_options": dict(storage_options) if storage_options else None,
        "version": location.delta_version,
        "timestamp": location.delta_timestamp,
        "starting_version": cdf_payload.get("starting_version"),
        "ending_version": cdf_payload.get("ending_version"),
        "starting_timestamp": cdf_payload.get("starting_timestamp"),
        "ending_timestamp": cdf_payload.get("ending_timestamp"),
        "allow_out_of_range": cdf_payload.get("allow_out_of_range"),
    }
    if gate_payload is not None:
        options.update(gate_payload)
    return options


def _dataset_from_location(
    _ctx: SessionContext,
    location: DatasetLocation,
    *,
    plugin_manager: DataFusionPluginManager | None,
) -> object:
    schema = resolve_dataset_schema(location)
    if location.format == "delta":
        if plugin_manager is None:
            msg = "Delta registry catalogs require plugin-based providers."
            raise ValueError(msg)
        if location.delta_version is not None and location.delta_timestamp is not None:
            msg = "Delta dataset open requires either delta_version or delta_timestamp."
            raise ValueError(msg)
        storage_options = _delta_storage_options(location)
        gate_payload = _delta_gate_payload(resolve_delta_feature_gate(location))
        delta_scan = resolve_delta_scan_options(location)
        if _requires_delta_cdf(location):
            options = _delta_cdf_provider_options(
                location,
                storage_options=storage_options,
                gate_payload=gate_payload,
            )
            provider = plugin_manager.create_table_provider(
                provider_name="delta_cdf",
                options=options,
            )
        else:
            options = _delta_provider_options(
                location,
                storage_options=storage_options,
                delta_scan=delta_scan,
                gate_payload=gate_payload,
            )
            provider = plugin_manager.create_table_provider(
                provider_name="delta",
                options=options,
            )
        from datafusion_engine.table_provider_capsule import TableProviderCapsule

        return TableProviderCapsule(provider)
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
    plugin_manager: DataFusionPluginManager | None = None

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
        ctx = self.ctx
        if ctx is None:
            msg = "RegistrySchemaProvider requires a SessionContext to resolve tables."
            raise ValueError(msg)
        table = _table_from_dataset(
            _dataset_from_location(ctx, location, plugin_manager=self.plugin_manager)
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
    plugin_manager: DataFusionPluginManager | None = None

    def __post_init__(self) -> None:
        """Initialize the catalog provider state."""
        self._schema_provider: SchemaProvider | SchemaProviderExportable | Schema | None = (
            RegistrySchemaProvider(
                self.catalog,
                schema_name=self.schema_name,
                ctx=self.ctx,
                plugin_manager=self.plugin_manager,
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
    plugin_manager: DataFusionPluginManager | None = None

    def __post_init__(self) -> None:
        """Initialize schema providers for registered catalogs."""
        self._schema_providers: dict[str, SchemaProvider] = {
            name: RegistrySchemaProvider(
                catalog,
                schema_name=name,
                ctx=self.ctx,
                plugin_manager=self.plugin_manager,
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
    plugin_manager: DataFusionPluginManager | None = None,
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
    plugin_manager:
        Optional plugin manager for Delta provider creation.

    Returns
    -------
    RegistryCatalogProvider
        Registered catalog provider.
    """
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    provider = RegistryCatalogProvider(
        registry,
        schema_name=schema_name,
        ctx=ctx,
        plugin_manager=plugin_manager,
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
    plugin_manager: DataFusionPluginManager | None = None,
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
    plugin_manager:
        Optional plugin manager for Delta provider creation.

    Returns
    -------
    MultiRegistryCatalogProvider
        Registered catalog provider.
    """
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    provider = MultiRegistryCatalogProvider(
        catalogs=catalogs,
        default_schema=default_schema,
        ctx=ctx,
        plugin_manager=plugin_manager,
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
