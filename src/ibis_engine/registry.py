"""Ibis dataset registration helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

import ibis
from ibis.expr.types import Table

try:
    from datafusion import SessionContext
except ImportError:  # pragma: no cover - optional dependency
    SessionContext = None

try:
    from datafusion_engine.registry_bridge import register_dataset_df
except ImportError:  # pragma: no cover - optional dependency
    register_dataset_df = None

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.serialization import schema_fingerprint, schema_to_dict
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import DataFusionScanOptions, DatasetSpec

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

type PathLike = str | Path
type DatasetFormat = str
type DataFusionProvider = Literal["dataset", "listing", "parquet"]

_REGISTERED_CATALOGS: dict[int, set[str]] = {}
_REGISTERED_SCHEMAS: dict[int, set[tuple[str, str]]] = {}


class FilesystemBackend(Protocol):
    """Protocol for backends supporting filesystem registration."""

    def register_filesystem(self, filesystem: object) -> None:
        """Register a filesystem for backend reads."""
        ...


@dataclass(frozen=True)
class DatasetLocation:
    """Location metadata for a dataset."""

    path: PathLike
    format: DatasetFormat = "parquet"
    partitioning: str | None = "hive"
    read_options: Mapping[str, object] = field(default_factory=dict)
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    table_spec: TableSchemaSpec | None = None
    dataset_spec: DatasetSpec | None = None
    datafusion_scan: DataFusionScanOptions | None = None
    datafusion_provider: DataFusionProvider | None = None


@dataclass(frozen=True)
class ReadDatasetParams:
    """Dataset read parameters for Ibis backends."""

    path: PathLike
    dataset_format: DatasetFormat
    read_options: Mapping[str, object] | None = None
    filesystem: object | None = None
    partitioning: str | None = None
    table_name: str | None = None


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
                "file_sort_order": list(loc.datafusion_scan.file_sort_order),
                "parquet_pruning": loc.datafusion_scan.parquet_pruning,
                "skip_metadata": loc.datafusion_scan.skip_metadata,
                "file_extension": loc.datafusion_scan.file_extension,
                "cache": loc.datafusion_scan.cache,
            }
        snapshot.append(
            {
                "name": name,
                "path": str(loc.path),
                "format": loc.format,
                "partitioning": loc.partitioning,
                "datafusion_provider": loc.datafusion_provider,
                "scan": scan,
                "schema_fingerprint": schema_fingerprint(schema) if schema is not None else None,
                "schema": schema_to_dict(schema) if schema is not None else None,
            }
        )
    return snapshot


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
    if location.datafusion_scan is not None:
        return location.datafusion_scan
    if location.dataset_spec is not None:
        return location.dataset_spec.datafusion_scan
    return None


def resolve_dataset_schema(location: DatasetLocation) -> SchemaLike | None:
    """Return the resolved schema for a dataset location.

    Returns
    -------
    SchemaLike | None
        Resolved schema when available, otherwise ``None``.
    """
    if location.table_spec is not None:
        return location.table_spec.to_arrow_schema()
    if location.dataset_spec is not None:
        return location.dataset_spec.schema()
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
    """
    options = dict(params.read_options or {})
    if params.table_name is not None:
        options.setdefault("table_name", params.table_name)
    if (
        params.dataset_format == "parquet"
        and params.partitioning
        and params.partitioning.lower() == "hive"
    ):
        options.setdefault("hive_partitioning", True)
    if params.filesystem is not None and hasattr(backend, "register_filesystem"):
        backend_fs = cast("FilesystemBackend", backend)
        backend_fs.register_filesystem(params.filesystem)
    reader = _resolve_reader(backend, params.dataset_format)
    return reader(params.path, **options)


def _resolve_reader(
    backend: ibis.backends.BaseBackend, dataset_format: DatasetFormat
) -> Callable[..., Table]:
    reader_map = {
        "parquet": "read_parquet",
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


def datafusion_context(backend: ibis.backends.BaseBackend) -> object | None:
    """Return a DataFusion SessionContext from an Ibis backend when available.

    Returns
    -------
    object | None
        DataFusion SessionContext when detected, otherwise ``None``.
    """
    if SessionContext is None:
        return None
    for attr in ("con", "_context", "_ctx", "ctx", "session_context"):
        ctx = getattr(backend, attr, None)
        if isinstance(ctx, SessionContext):
            return ctx
    return None


def _datafusion_context(backend: ibis.backends.BaseBackend) -> object | None:
    """Return the DataFusion SessionContext when available.

    Deprecated: use ``datafusion_context`` instead.

    Returns
    -------
    object | None
        SessionContext when detected, otherwise ``None``.
    """
    return datafusion_context(backend)


def _register_datafusion_dataset(
    ctx: object,
    *,
    name: str,
    location: DatasetLocation,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> bool:
    if register_dataset_df is None or SessionContext is None:
        return False
    if not isinstance(ctx, SessionContext):
        return False
    try:
        register_dataset_df(
            ctx,
            name=name,
            location=location,
            runtime_profile=runtime_profile,
        )
    except ValueError:
        return False
    return True


def _ensure_backend_catalog_schema(
    backend: ibis.backends.BaseBackend,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    if runtime_profile is None:
        return
    backend_id = id(backend)
    catalogs = _REGISTERED_CATALOGS.setdefault(backend_id, set())
    schemas = _REGISTERED_SCHEMAS.setdefault(backend_id, set())
    catalog = runtime_profile.default_catalog
    schema = runtime_profile.default_schema
    if catalog not in catalogs:
        create_catalog = getattr(backend, "create_catalog", None)
        if callable(create_catalog):
            create_catalog(catalog, force=True)
        catalogs.add(catalog)
    if (catalog, schema) not in schemas:
        create_database = getattr(backend, "create_database", None)
        if callable(create_database):
            create_database(schema, catalog=catalog, force=True)
        schemas.add((catalog, schema))


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
        if ctx is not None and not _register_datafusion_dataset(
            ctx,
            name=name,
            location=location,
            runtime_profile=self._runtime_profile,
        ):
            ctx = None
        if ctx is None:
            table = read_dataset(
                self.backend,
                params=ReadDatasetParams(
                    path=location.path,
                    dataset_format=location.format,
                    read_options=location.read_options,
                    filesystem=location.filesystem,
                    partitioning=location.partitioning,
                    table_name=name,
                ),
            )
            self.backend.create_view(name, table, overwrite=True)
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
    "IbisDatasetRegistry",
    "PathLike",
    "datafusion_context",
    "read_dataset",
    "registry_snapshot",
    "resolve_datafusion_scan_options",
    "resolve_dataset_schema",
]
