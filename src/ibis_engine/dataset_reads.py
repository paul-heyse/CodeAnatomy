"""Ibis dataset read helpers backed by DataFusion registration."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, cast

import ibis
from ibis.expr.types import Table

from datafusion_engine.dataset_registry import (
    DataFusionProvider,
    DataFusionScanOptions,
    DatasetFormat,
    DatasetLocation,
    DatasetSpec,
    resolve_datafusion_provider,
    resolve_datafusion_scan_options,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from ibis_engine.datafusion_context import datafusion_context
from storage.deltalake import DeltaCdfOptions

type PathLike = str | Path


class FilesystemBackend(Protocol):
    """Protocol for backends supporting filesystem registration."""

    def register_filesystem(self, filesystem: object) -> None:
        """Register a filesystem for backend reads."""


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
    delta_cdf_options: DeltaCdfOptions | None = None


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
            delta_cdf_options=params.delta_cdf_options,
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
        delta_cdf_options=params.delta_cdf_options,
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


def _register_datafusion_dataset(
    ctx: object,
    *,
    name: str,
    location: DatasetLocation,
) -> None:
    from datafusion import SessionContext

    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    if not isinstance(ctx, SessionContext):
        msg = "DataFusion SessionContext is required for dataset registration."
        raise TypeError(msg)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    facade.register_dataset(name=name, location=location)


__all__ = ["ReadDatasetParams", "read_dataset"]
