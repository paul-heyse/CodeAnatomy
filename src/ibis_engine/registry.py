"""Ibis dataset registration helpers."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

import ibis
from datafusion import SessionContext
from ibis.expr.types import Table, Value

from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.schema.abi import schema_fingerprint, schema_to_dict
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import (
    DataFusionScanOptions,
    DatasetSpec,
    DeltaScanOptions,
    DeltaSchemaPolicy,
    DeltaWritePolicy,
)
from storage.deltalake import DeltaCdfOptions, delta_table_schema
from storage.deltalake.scan_profile import build_delta_scan_config

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

type PathLike = str | Path
type DatasetFormat = str
type DataFusionProvider = Literal["listing", "delta_cdf"]


class FilesystemBackend(Protocol):
    """Protocol for backends supporting filesystem registration."""

    def register_filesystem(self, filesystem: object) -> None:
        """Register a filesystem for backend reads."""
        ...


@dataclass(frozen=True)
class DatasetLocation:
    """Location metadata for a dataset."""

    path: PathLike
    format: DatasetFormat = "delta"
    partitioning: str | None = "hive"
    read_options: Mapping[str, object] = field(default_factory=dict)
    storage_options: Mapping[str, str] = field(default_factory=dict)
    delta_log_storage_options: Mapping[str, str] = field(default_factory=dict)
    delta_scan: DeltaScanOptions | None = None
    delta_cdf_options: DeltaCdfOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_constraints: tuple[str, ...] = ()
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    table_spec: TableSchemaSpec | None = None
    dataset_spec: DatasetSpec | None = None
    datafusion_scan: DataFusionScanOptions | None = None
    datafusion_provider: DataFusionProvider | None = None
    delta_version: int | None = None
    delta_timestamp: str | None = None


@dataclass(frozen=True)
class ReadDatasetParams:
    """Dataset read parameters for Ibis backends."""

    path: PathLike
    dataset_format: DatasetFormat
    read_options: Mapping[str, object] | None = None
    storage_options: Mapping[str, str] | None = None
    delta_log_storage_options: Mapping[str, str] | None = None
    filesystem: object | None = None
    partitioning: str | None = None
    table_name: str | None = None
    dataset_spec: DatasetSpec | None = None
    datafusion_scan: DataFusionScanOptions | None = None
    datafusion_provider: DataFusionProvider | None = None


class DatasetCatalog:
    """Map dataset names to locations for Ibis registration."""

    def __init__(self) -> None:
        self._locs: dict[str, DatasetLocation] = {}

    def register(self, name: str, location: DatasetLocation) -> None:
        """Register a dataset location.

        Parameters
        ----------
        name:
            Dataset name.
        location:
            Location metadata.

        Raises
        ------
        ValueError
            Raised when the dataset name is empty.
        """
        if not name:
            msg = "DatasetCatalog.register: name must be non-empty."
            raise ValueError(msg)
        self._locs[name] = location

    def get(self, name: str) -> DatasetLocation:
        """Return a registered dataset location.

        Parameters
        ----------
        name:
            Dataset name.

        Returns
        -------
        DatasetLocation
            Location metadata for the dataset.

        Raises
        ------
        KeyError
            Raised when the dataset name is not registered.
        """
        if name not in self._locs:
            msg = f"DatasetCatalog: unknown dataset {name!r}."
            raise KeyError(msg)
        return self._locs[name]

    def has(self, name: str) -> bool:
        """Return whether a dataset name is registered.

        Parameters
        ----------
        name:
            Dataset name.

        Returns
        -------
        bool
            ``True`` when the dataset is registered.
        """
        return name in self._locs

    def names(self) -> list[str]:
        """Return registered dataset names in sorted order.

        Returns
        -------
        list[str]
            Sorted dataset names.
        """
        return sorted(self._locs)


def registry_snapshot(catalog: DatasetCatalog) -> list[dict[str, object]]:
    """Return a JSON-ready snapshot of registry locations.

    Returns
    -------
    list[dict[str, object]]
        Registry snapshot payloads.
    """
    snapshot: list[dict[str, object]] = []
    for name in catalog.names():
        loc = catalog.get(name)
        schema = resolve_dataset_schema(loc)
        scan = None
        if loc.datafusion_scan is not None:
            scan = {
                "partition_cols": [
                    (col, str(dtype)) for col, dtype in loc.datafusion_scan.partition_cols
                ],
                "file_sort_order": [list(key) for key in loc.datafusion_scan.file_sort_order],
                "parquet_pruning": loc.datafusion_scan.parquet_pruning,
                "skip_metadata": loc.datafusion_scan.skip_metadata,
                "skip_arrow_metadata": loc.datafusion_scan.skip_arrow_metadata,
                "binary_as_string": loc.datafusion_scan.binary_as_string,
                "schema_force_view_types": loc.datafusion_scan.schema_force_view_types,
                "listing_table_factory_infer_partitions": (
                    loc.datafusion_scan.listing_table_factory_infer_partitions
                ),
                "listing_table_ignore_subdirectory": (
                    loc.datafusion_scan.listing_table_ignore_subdirectory
                ),
                "file_extension": loc.datafusion_scan.file_extension,
                "cache": loc.datafusion_scan.cache,
                "collect_statistics": loc.datafusion_scan.collect_statistics,
                "meta_fetch_concurrency": loc.datafusion_scan.meta_fetch_concurrency,
                "list_files_cache_ttl": loc.datafusion_scan.list_files_cache_ttl,
                "list_files_cache_limit": loc.datafusion_scan.list_files_cache_limit,
                "projection_exprs": list(loc.datafusion_scan.projection_exprs),
                "unbounded": loc.datafusion_scan.unbounded,
            }
        delta_scan = None
        if loc.delta_scan is not None:
            delta_scan = {
                "file_column_name": loc.delta_scan.file_column_name,
                "enable_parquet_pushdown": loc.delta_scan.enable_parquet_pushdown,
                "schema_force_view_types": loc.delta_scan.schema_force_view_types,
                "schema": (
                    schema_to_dict(loc.delta_scan.schema)
                    if loc.delta_scan.schema is not None
                    else None
                ),
            }
        delta_write_policy = None
        if loc.delta_write_policy is not None:
            delta_write_policy = {
                "target_file_size": loc.delta_write_policy.target_file_size,
                "stats_columns": (
                    list(loc.delta_write_policy.stats_columns)
                    if loc.delta_write_policy.stats_columns is not None
                    else None
                ),
            }
        delta_schema_policy = None
        if loc.delta_schema_policy is not None:
            delta_schema_policy = {
                "schema_mode": loc.delta_schema_policy.schema_mode,
                "column_mapping_mode": loc.delta_schema_policy.column_mapping_mode,
            }
        provider = resolve_datafusion_provider(loc)
        snapshot.append(
            {
                "name": name,
                "path": str(loc.path),
                "format": loc.format,
                "partitioning": loc.partitioning,
                "datafusion_provider": provider,
                "storage_options": dict(loc.storage_options) if loc.storage_options else None,
                "delta_log_storage_options": (
                    dict(loc.delta_log_storage_options) if loc.delta_log_storage_options else None
                ),
                "delta_scan": delta_scan,
                "delta_write_policy": delta_write_policy,
                "delta_schema_policy": delta_schema_policy,
                "delta_constraints": list(loc.delta_constraints) if loc.delta_constraints else None,
                "delta_version": loc.delta_version,
                "delta_timestamp": loc.delta_timestamp,
                "scan": scan,
                "ddl_fingerprint": None,
                "schema_fingerprint": schema_fingerprint(schema) if schema is not None else None,
                "schema": schema_to_dict(schema) if schema is not None else None,
            }
        )
    return snapshot


def ibis_array(values: Sequence[Value]) -> Value:
    """Return an Ibis array literal from values.

    Returns
    -------
    ibis.expr.types.Value
        Array expression constructed from the provided values.
    """
    return ibis.array(values)


def ibis_struct(fields: Mapping[str, Value]) -> Value:
    """Return an Ibis struct literal from named fields.

    Returns
    -------
    ibis.expr.types.Value
        Struct expression constructed from the provided fields.
    """
    return ibis.struct(fields)


def ibis_map(keys: Value, values: Value) -> Value:
    """Return an Ibis map literal from key and value expressions.

    Returns
    -------
    ibis.expr.types.Value
        Map expression constructed from the provided keys and values.
    """
    return ibis.map(keys, values)


def resolve_datafusion_scan_options(location: DatasetLocation) -> DataFusionScanOptions | None:
    """Return DataFusion scan options for a dataset location.

    Precedence:
      1) Explicit ``DatasetLocation.datafusion_scan`` overrides everything.
      2) ``DatasetSpec.datafusion_scan`` provides defaults when location overrides are absent.

    Returns
    -------
    DataFusionScanOptions | None
        Scan options derived from the dataset location, when present.
    """
    scan = location.datafusion_scan
    if scan is None and location.dataset_spec is not None:
        scan = location.dataset_spec.datafusion_scan
    if scan is None:
        return None
    if location.dataset_spec is None:
        return scan
    if scan.file_sort_order:
        return scan
    ordering = location.dataset_spec.ordering()
    file_sort_order: tuple[tuple[str, str], ...] = ()
    if ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        file_sort_order = tuple(ordering.keys)
    elif location.dataset_spec.table_spec.key_fields:
        file_sort_order = tuple(
            (name, "ascending") for name in location.dataset_spec.table_spec.key_fields
        )
    if not file_sort_order:
        return scan
    return replace(scan, file_sort_order=file_sort_order)


def resolve_datafusion_provider(location: DatasetLocation) -> DataFusionProvider | None:
    """Return the effective DataFusion provider for a dataset location.

    Returns
    -------
    DataFusionProvider | None
        Resolved provider for the dataset location, when available.
    """
    if location.datafusion_provider is not None:
        return location.datafusion_provider
    if location.dataset_spec is not None and location.dataset_spec.dataset_kind == "delta_cdf":
        return "delta_cdf"
    return None


def resolve_delta_scan_options(location: DatasetLocation) -> DeltaScanOptions | None:
    """Return Delta scan options for a dataset location.

    Returns
    -------
    DeltaScanOptions | None
        Delta scan options derived from the dataset location, when present.
    """
    return build_delta_scan_config(location)


def resolve_delta_log_storage_options(location: DatasetLocation) -> Mapping[str, str] | None:
    """Return Delta log-store options for a dataset location.

    Returns
    -------
    Mapping[str, str] | None
        Log-store options for Delta table access.
    """
    if location.delta_log_storage_options:
        return location.delta_log_storage_options
    if location.storage_options:
        return location.storage_options
    return None


def resolve_delta_write_policy(location: DatasetLocation) -> DeltaWritePolicy | None:
    """Return Delta write policy for a dataset location.

    Returns
    -------
    DeltaWritePolicy | None
        Delta write policy derived from the dataset location, when present.
    """
    if location.delta_write_policy is not None:
        return location.delta_write_policy
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_write_policy
    return None


def resolve_delta_schema_policy(location: DatasetLocation) -> DeltaSchemaPolicy | None:
    """Return Delta schema policy for a dataset location.

    Returns
    -------
    DeltaSchemaPolicy | None
        Delta schema policy derived from the dataset location, when present.
    """
    if location.delta_schema_policy is not None:
        return location.delta_schema_policy
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_schema_policy
    return None


def resolve_delta_constraints(location: DatasetLocation) -> tuple[str, ...]:
    """Return Delta constraint expressions for a dataset location.

    Returns
    -------
    tuple[str, ...]
        Constraint expressions for the dataset.
    """
    if location.delta_constraints:
        return location.delta_constraints
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_constraints
    return ()


def resolve_dataset_schema(location: DatasetLocation) -> SchemaLike | None:
    """Return the resolved schema for a dataset location.

    Returns
    -------
    SchemaLike | None
        Resolved schema when available, otherwise ``None``.

    Raises
    ------
    ValueError
        Raised when Delta schema resolution fails for a Delta dataset.
    """
    scan = resolve_datafusion_scan_options(location)
    if scan is not None and scan.table_schema_contract is not None:
        return scan.table_schema_contract.file_schema
    if location.table_spec is not None:
        return location.table_spec.to_arrow_schema()
    if location.dataset_spec is not None:
        return location.dataset_spec.schema()
    if location.format == "delta":
        schema = delta_table_schema(
            str(location.path),
            storage_options=location.storage_options or None,
            log_storage_options=location.delta_log_storage_options or None,
            version=location.delta_version,
            timestamp=location.delta_timestamp,
        )
        if schema is None:
            msg = f"Delta schema unavailable for dataset at {location.path!r}."
            raise ValueError(msg)
        return schema
    return None


def read_dataset(
    backend: ibis.backends.BaseBackend,
    *,
    params: ReadDatasetParams,
) -> Table:
    """Read a dataset into an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table expression for the dataset.

    Raises
    ------
    ValueError
        Raised when Delta datasets cannot be registered via DataFusion.
    """
    if params.filesystem is not None and hasattr(backend, "register_filesystem"):
        backend_fs = cast("FilesystemBackend", backend)
        backend_fs.register_filesystem(params.filesystem)
    if params.dataset_format == "delta":
        from ibis_engine.sources import IbisDeltaReadOptions, read_delta_ibis

        location = DatasetLocation(
            path=params.path,
            format=params.dataset_format,
            partitioning=params.partitioning,
            read_options=dict(params.read_options or {}),
            storage_options=dict(params.storage_options or {}),
            delta_log_storage_options=dict(params.delta_log_storage_options or {}),
            filesystem=params.filesystem,
            dataset_spec=params.dataset_spec,
            datafusion_scan=params.datafusion_scan,
            datafusion_provider=params.datafusion_provider,
        )
        provider = resolve_datafusion_provider(location)
        if provider == "delta_cdf":
            datafusion_table = _read_via_datafusion_registry(
                backend,
                params=params,
                force=True,
            )
            if datafusion_table is not None:
                return datafusion_table
            msg = "Delta CDF datasets require DataFusion registry-based reads."
            raise ValueError(msg)
        delta_scan = resolve_delta_scan_options(location)
        delta_log_storage = resolve_delta_log_storage_options(location)
        return read_delta_ibis(
            backend,
            str(params.path),
            options=IbisDeltaReadOptions(
                table_name=params.table_name,
                storage_options=dict(location.storage_options)
                if location.storage_options
                else None,
                log_storage_options=dict(delta_log_storage)
                if delta_log_storage is not None
                else None,
                delta_scan=delta_scan,
            ),
        )
    datafusion_table = _read_via_datafusion_registry(
        backend,
        params=params,
        force=False,
    )
    if datafusion_table is not None:
        return datafusion_table
    options = dict(params.read_options or {})
    if params.table_name is not None:
        options.setdefault("table_name", params.table_name)
    if params.storage_options:
        options.setdefault("storage_options", dict(params.storage_options))
    if params.partitioning and params.partitioning.lower() == "hive":
        options.setdefault("hive_partitioning", True)
    reader = _resolve_reader(backend, params.dataset_format)
    return reader(params.path, **options)


def _read_via_datafusion_registry(
    backend: ibis.backends.BaseBackend,
    *,
    params: ReadDatasetParams,
    force: bool = False,
) -> Table | None:
    if not force and (
        params.datafusion_scan is None
        and params.datafusion_provider is None
        and params.dataset_spec is None
    ):
        return None
    try:
        ctx = datafusion_context(backend)
    except ValueError:
        if force:
            raise
        return None
    table_name = params.table_name or f"__ibis_{uuid.uuid4().hex}"
    scan = params.datafusion_scan
    location = DatasetLocation(
        path=params.path,
        format=params.dataset_format,
        partitioning=params.partitioning,
        read_options=dict(params.read_options or {}),
        storage_options=dict(params.storage_options or {}),
        delta_log_storage_options=dict(params.delta_log_storage_options or {}),
        filesystem=params.filesystem,
        dataset_spec=params.dataset_spec,
        datafusion_scan=scan,
        datafusion_provider=params.datafusion_provider,
    )
    scan = resolve_datafusion_scan_options(location)
    if scan is not None and scan is not location.datafusion_scan:
        location = replace(location, datafusion_scan=scan)
    provider = resolve_datafusion_provider(location)
    if provider is not None and provider != location.datafusion_provider:
        location = replace(location, datafusion_provider=provider)
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(table_name)
    try:
        _register_datafusion_dataset(
            ctx,
            name=table_name,
            location=location,
            runtime_profile=None,
        )
    except ValueError:
        if force:
            raise
        return None
    return backend.table(table_name)


def _resolve_reader(
    backend: ibis.backends.BaseBackend, dataset_format: DatasetFormat
) -> Callable[..., Table]:
    reader_map = {
        "csv": "read_csv",
        "json": "read_json",
        "delta": "read_delta",
        "geo": "read_geo",
        "xlsx": "read_xlsx",
    }
    attr = reader_map.get(dataset_format)
    if attr is None:
        msg = f"Unsupported dataset format: {dataset_format!r}."
        raise ValueError(msg)
    reader = getattr(backend, attr, None)
    if not callable(reader):
        msg = f"Ibis backend does not support {dataset_format!r} datasets."
        raise TypeError(msg)
    return cast("Callable[..., Table]", reader)


def datafusion_context(backend: object) -> SessionContext:
    """Return a DataFusion SessionContext from an Ibis backend.

    Raises
    ------
    ValueError
        Raised when the backend does not expose a DataFusion SessionContext.

    Returns
    -------
    datafusion.SessionContext
        DataFusion SessionContext detected on the backend.
    """
    for attr in ("con", "_context", "_ctx", "ctx", "session_context"):
        ctx = getattr(backend, attr, None)
        if isinstance(ctx, SessionContext):
            return ctx
    msg = "Ibis backend does not expose a DataFusion SessionContext."
    raise ValueError(msg)


def _register_datafusion_dataset(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    if not isinstance(ctx, SessionContext):
        msg = "DataFusion SessionContext is required for dataset registration."
        raise TypeError(msg)
    try:
        facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
        facade.register_dataset(name=name, location=location)
    except ValueError as exc:
        if location.format == "delta":
            msg = f"Delta provider required for {name!r}."
            raise ValueError(msg) from exc
        raise


def _ensure_backend_catalog_schema(
    backend: ibis.backends.BaseBackend,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    if runtime_profile is None:
        return
    catalog = runtime_profile.default_catalog
    schema = runtime_profile.default_schema
    create_catalog = getattr(backend, "create_catalog", None)
    if callable(create_catalog):
        create_catalog(catalog, force=True)
    create_database = getattr(backend, "create_database", None)
    if callable(create_database):
        create_database(schema, catalog=catalog, force=True)


def _ensure_registry_catalog_provider(
    ctx: SessionContext,
    *,
    registry: IbisDatasetRegistry,
    catalog_name: str,
    schema_name: str,
) -> None:
    """Register a registry-backed catalog provider once per SessionContext.

    Parameters
    ----------
    ctx:
        DataFusion session context used for catalog registration.
    registry:
        Dataset registry that backs the catalog provider.
    catalog_name:
        Catalog name to register.
    schema_name:
        Schema name to expose from the catalog provider.

    Raises
    ------
    TypeError
        Raised when the session context is not a DataFusion SessionContext.
    """
    if not isinstance(ctx, SessionContext):
        msg = "DataFusion SessionContext is required for registry catalogs."
        raise TypeError(msg)
    from datafusion_engine.catalog_provider import register_registry_catalog

    register_registry_catalog(
        ctx,
        registry=registry,
        catalog_name=catalog_name,
        schema_name=schema_name,
    )


class IbisDatasetRegistry:
    """Register dataset locations as Ibis tables."""

    def __init__(
        self,
        backend: ibis.backends.BaseBackend,
        *,
        catalog: DatasetCatalog | None = None,
        runtime_profile: DataFusionRuntimeProfile | None = None,
    ) -> None:
        self.backend = backend
        self.catalog = catalog or DatasetCatalog()
        self._tables: dict[str, Table] = {}
        self._runtime_profile = runtime_profile

    def register_location(self, name: str, location: DatasetLocation) -> Table:
        """Register a dataset location as a stable Ibis view.

        Returns
        -------
        ibis.expr.types.Table
            Registered table expression.
        """
        if name in self._tables:
            return self._tables[name]
        _ensure_backend_catalog_schema(self.backend, runtime_profile=self._runtime_profile)
        ctx = datafusion_context(self.backend)
        _ensure_registry_catalog_provider(
            ctx,
            registry=self,
            catalog_name="registry",
            schema_name="public",
        )
        _register_datafusion_dataset(
            ctx,
            name=name,
            location=location,
            runtime_profile=self._runtime_profile,
        )
        registered = self.backend.table(name)
        self._tables[name] = registered
        return registered

    def table(self, name: str) -> Table:
        """Return a registered Ibis table.

        Returns
        -------
        ibis.expr.types.Table
            Registered table expression.
        """
        if name in self._tables:
            return self._tables[name]
        return self.register_location(name, self.catalog.get(name))


__all__ = [
    "DataFusionProvider",
    "DataFusionScanOptions",
    "DatasetCatalog",
    "DatasetFormat",
    "DatasetLocation",
    "DeltaScanOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "IbisDatasetRegistry",
    "PathLike",
    "datafusion_context",
    "ibis_array",
    "ibis_map",
    "ibis_struct",
    "read_dataset",
    "registry_snapshot",
    "resolve_datafusion_provider",
    "resolve_datafusion_scan_options",
    "resolve_dataset_schema",
    "resolve_delta_constraints",
    "resolve_delta_scan_options",
    "resolve_delta_schema_policy",
    "resolve_delta_write_policy",
]
