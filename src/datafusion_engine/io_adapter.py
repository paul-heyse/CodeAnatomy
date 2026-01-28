"""Unified IO adapter for all DataFusion access paths.

This module consolidates object store registration, dataset registration,
and dataset metadata capture into a single IO adapter used by all DataFusion
execution paths.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.diagnostics import record_artifact, recorder_for_profile
from datafusion_engine.introspection import invalidate_introspection_cache

if TYPE_CHECKING:
    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.runtime import DataFusionRuntimeProfile

_REGISTERED_OBJECT_STORES: dict[int, set[tuple[str, str | None]]] = {}


def _storage_type(data_type: pa.DataType) -> pa.DataType:
    if isinstance(data_type, pa.ExtensionType):
        return _storage_type(data_type.storage_type)
    if pa.types.is_struct(data_type):
        fields = [_storage_field(field) for field in data_type]
        return pa.struct(fields)
    if pa.types.is_list(data_type):
        value_field = data_type.value_field
        value = _storage_type(value_field.type)
        return pa.list_(pa.field(value_field.name, value, nullable=value_field.nullable))
    if pa.types.is_large_list(data_type):
        value_field = data_type.value_field
        value = _storage_type(value_field.type)
        return pa.large_list(pa.field(value_field.name, value, nullable=value_field.nullable))
    if pa.types.is_map(data_type):
        key_type = _storage_type(data_type.key_type)
        item_type = _storage_type(data_type.item_type)
        return pa.map_(key_type, item_type, keys_sorted=data_type.keys_sorted)
    return data_type


def _storage_field(field: pa.Field) -> pa.Field:
    return pa.field(field.name, _storage_type(field.type), nullable=field.nullable)


def _storage_schema(schema: pa.Schema) -> pa.Schema:
    fields = [_storage_field(field) for field in schema]
    return pa.schema(fields)


def _convert_struct_array(array: pa.StructArray, target_type: pa.StructType) -> pa.StructArray:
    arrays = [
        _convert_array_to_storage(array.field(index), field.type)
        for index, field in enumerate(target_type)
    ]
    fields = list(target_type)
    return pa.StructArray.from_arrays(arrays, fields=fields)


def _convert_list_array(array: pa.Array, target_type: pa.DataType) -> pa.Array:
    values = _convert_array_to_storage(array.values, target_type.value_type)
    offsets = array.offsets
    if pa.types.is_list(target_type):
        return pa.ListArray.from_arrays(offsets, values, type=cast("pa.ListType", target_type))
    return pa.LargeListArray.from_arrays(
        offsets,
        values,
        type=cast("pa.LargeListType", target_type),
    )


def _convert_map_array(array: pa.MapArray, target_type: pa.MapType) -> pa.MapArray:
    keys = _convert_array_to_storage(array.keys, target_type.key_type)
    items = _convert_array_to_storage(array.items, target_type.item_type)
    return pa.MapArray.from_arrays(array.offsets, keys, items, type=target_type)


def _convert_array_to_storage(array: pa.Array, target_type: pa.DataType) -> pa.Array:
    if isinstance(array, pa.ExtensionArray):
        return _convert_array_to_storage(array.storage, target_type)
    if pa.types.is_struct(target_type):
        return _convert_struct_array(
            cast("pa.StructArray", array), cast("pa.StructType", target_type)
        )
    if pa.types.is_list(target_type) or pa.types.is_large_list(target_type):
        return _convert_list_array(array, target_type)
    if pa.types.is_map(target_type):
        return _convert_map_array(cast("pa.MapArray", array), cast("pa.MapType", target_type))
    if array.type != target_type:
        return array.cast(target_type)
    return array


def _convert_chunked_array(column: pa.ChunkedArray, target_type: pa.DataType) -> pa.ChunkedArray:
    chunks = [_convert_array_to_storage(chunk, target_type) for chunk in column.chunks]
    return pa.chunked_array(chunks, type=target_type)


def _datafusion_compatible_table(table: pa.Table) -> pa.Table:
    target_schema = _storage_schema(table.schema)
    arrays = [
        _convert_chunked_array(table.column(field.name), field.type) for field in target_schema
    ]
    return pa.Table.from_arrays(arrays, schema=target_schema)


@dataclass(frozen=True)
class DataFusionIOAdapter:
    """Unified IO adapter for all DataFusion access paths.

    Consolidates object store registration, in-memory Arrow table registration,
    and PyArrow Dataset registration into a single adapter. All registration
    operations flow through this adapter for consistency and diagnostics capture.

    Parameters
    ----------
    ctx
        DataFusion session context for registration operations.
    profile
        Runtime profile containing execution policy and diagnostics sink.
        If None, uses default execution policy.

    Examples
    --------
    >>> from datafusion_engine.runtime import DataFusionRuntimeProfile
    >>> profile = DataFusionRuntimeProfile()
    >>> ctx = profile.session_context()
    >>> adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    >>> adapter.register_arrow_table("my_table", pa.table({"col": [1, 2, 3]}))
    """

    ctx: SessionContext
    profile: DataFusionRuntimeProfile | None

    def register_object_store(
        self,
        *,
        scheme: str,
        store: object,
        host: str | None = None,
    ) -> None:
        """Register object store with diagnostics capture.

        Registers an object store with the DataFusion session context,
        enabling access to external data sources (S3, GCS, Azure, etc.).

        Parameters
        ----------
        scheme
            URI scheme for the object store (e.g., "s3", "gs", "az").
        store
            Object store instance to register.
        host
            Optional host identifier for the object store.

        Notes
        -----
        This method records a diagnostics artifact when a diagnostics
        sink is configured in the runtime profile.
        """
        ctx_key = id(self.ctx)
        registry = _REGISTERED_OBJECT_STORES.setdefault(ctx_key, set())
        key = (scheme, host)
        if key in registry:
            return
        self.ctx.register_object_store(scheme, store, host)
        registry.add(key)
        self._record_registration(
            name=scheme,
            registration_type="object_store",
            location=host,
        )
        self._record_artifact(
            "object_store_registered",
            {
                "scheme": scheme,
                "host": host,
            },
        )

    def register_arrow_table(
        self,
        name: str,
        table: pa.Table,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register in-memory Arrow table.

        Registers a PyArrow Table as a named table in the DataFusion
        catalog for use in SQL queries.

        Parameters
        ----------
        name
            Table name for registration.
        table
            PyArrow Table to register.
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.

        Notes
        -----
        If a table with the same name already exists and overwrite is
        False, DataFusion will raise an error during registration.
        """
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        compatible_table = _datafusion_compatible_table(table)
        table_to_register: pa.Table | ds.Dataset = ds.dataset(compatible_table)
        self.ctx.register_table(name, table_to_register)
        invalidate_introspection_cache(self.ctx)

    def register_record_batches(
        self,
        name: str,
        batches: list[list[pa.RecordBatch]],
        *,
        overwrite: bool = False,
    ) -> None:
        """Register record batches as a DataFusion table.

        Parameters
        ----------
        name
            Table name for registration.
        batches
            Nested list of record batches (partitioned batches).
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.
        """
        register = getattr(self.ctx, "register_record_batches", None)
        if not callable(register):
            msg = "SessionContext does not support register_record_batches."
            raise NotImplementedError(msg)
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        register(name, batches)
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=name,
            registration_type="table",
        )

    def register_table_provider(
        self,
        name: str,
        provider: object,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a table provider with diagnostics capture.

        Parameters
        ----------
        name
            Table name for registration.
        provider
            Table provider instance to register.
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.
        """
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        self.ctx.register_table(name, provider)
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=name,
            registration_type="table",
        )
        self._record_artifact(
            "table_provider_registered",
            {"name": name, "provider_type": type(provider).__name__},
        )

    def register_delta_table_provider(
        self,
        name: str,
        provider: object,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a Delta table provider with diagnostics capture."""
        self.register_table_provider(name, provider, overwrite=overwrite)
        self._record_artifact(
            "delta_table_provider_registered",
            {"name": name, "provider_type": type(provider).__name__},
        )

    def register_delta_cdf_provider(
        self,
        name: str,
        provider: object,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a Delta CDF provider with diagnostics capture."""
        self.register_table_provider(name, provider, overwrite=overwrite)
        self._record_artifact(
            "delta_cdf_provider_registered",
            {"name": name, "provider_type": type(provider).__name__},
        )

    def register_catalog_provider(
        self,
        name: str,
        provider: object,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a catalog provider with diagnostics capture.

        Parameters
        ----------
        name
            Catalog name for registration.
        provider
            Catalog provider instance to register.
        overwrite
            If True, allow replacing an existing catalog registration.
        """
        if overwrite:
            deregister = getattr(self.ctx, "deregister_catalog", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister(name)
        from typing import cast

        from datafusion.catalog import Catalog, CatalogProvider

        typed_provider = cast(
            "Catalog | CatalogProvider",
            provider,
        )
        self.ctx.register_catalog_provider(name, typed_provider)
        self._record_registration(
            name=name,
            registration_type="catalog",
        )
        self._record_artifact(
            "catalog_provider_registered",
            {"name": name, "provider_type": type(provider).__name__},
        )

    def register_view(
        self,
        name: str,
        df: DataFrame,
        *,
        overwrite: bool = False,
        temporary: bool = False,
    ) -> None:
        """Register a DataFusion view from a DataFrame.

        Parameters
        ----------
        name
            View name for registration.
        df
            DataFrame to convert into a view.
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.
        temporary
            If True, register as a temporary view.
        """
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        view = df.into_view(temporary=temporary)
        self.ctx.register_table(name, view)
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=name,
            registration_type="view",
        )
        self._record_artifact(
            "view_registered",
            {"name": name, "temporary": temporary},
        )

    def register_dataset(
        self,
        name: str,
        dataset: ds.Dataset,
    ) -> None:
        """Register PyArrow Dataset as table provider.

        Registers a PyArrow Dataset as a named table in the DataFusion
        catalog. Datasets support partitioned data and predicate pushdown.

        Parameters
        ----------
        name
            Table name for registration.
        dataset
            PyArrow Dataset to register.

        Notes
        -----
        PyArrow Datasets provide efficient access to partitioned data
        with predicate and projection pushdown capabilities.
        """
        self.ctx.register_dataset(name, dataset)
        invalidate_introspection_cache(self.ctx)

    def deregister_table(self, name: str) -> None:
        """Deregister a table from the DataFusion catalog.

        Parameters
        ----------
        name
            Table name to deregister.
        """
        self._deregister_table(name)

    def _deregister_table(self, name: str) -> None:
        """Centralized table deregistration.

        Removes a table from the DataFusion catalog schema.

        Parameters
        ----------
        name
            Table name to deregister.

        Notes
        -----
        This is an internal helper method used by overwrite operations.
        """
        catalog = self.ctx.catalog()
        schema = catalog.schema()
        schema.deregister_table(name)
        invalidate_introspection_cache(self.ctx)

    def _record_registration(
        self,
        *,
        name: str,
        registration_type: str,
        location: str | None = None,
    ) -> None:
        """Record a standardized registration diagnostic."""
        if self.profile is None:
            return
        recorder = recorder_for_profile(self.profile, operation_id=f"register_{name}")
        if recorder is None:
            return
        recorder.record_registration(
            name=name,
            registration_type=registration_type,
            location=location,
        )

    def _record_artifact(self, name: str, payload: dict[str, object]) -> None:
        """Record diagnostics artifact.

        Records a diagnostics artifact if a diagnostics sink is
        configured in the runtime profile.

        Parameters
        ----------
        name
            Artifact name for diagnostics recording.
        payload
            Artifact payload containing metadata to record.

        Notes
        -----
        This method is a no-op if no diagnostics sink is configured.
        """
        record_artifact(self.profile, name, payload)


def _location_payload(location: DatasetLocation | str) -> str:
    if isinstance(location, str):
        return location
    return str(location.path)
