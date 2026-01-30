"""Register Delta-backed registry tables in a DataFusion SessionContext."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from core_types import PathLike, ensure_path
from datafusion_engine.dataset_registration import DataFusionCachePolicy
from datafusion_engine.dataset_registry import DatasetLocation, resolve_delta_log_storage_options
from datafusion_engine.delta_scan_config import resolve_delta_scan_options
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from schema_spec.system import DataFusionScanOptions, DeltaScanOptions

RegistryTarget = Literal["cpg", "relspec"]


@dataclass(frozen=True)
class RegistryLoadOptions:
    """Options for registry table registration."""

    datafusion_scan: DataFusionScanOptions | None = None
    delta_scan: DeltaScanOptions | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    storage_options: Mapping[str, str] | None = None
    cache: bool = True


def registry_output_dir(output_dir: PathLike, *, target: RegistryTarget) -> Path:
    """Return the registry output directory for a target.

    Returns
    -------
    pathlib.Path
        Resolved registry output directory.
    """
    resolved = ensure_path(output_dir)
    return resolved / "registry" / target


def registry_delta_table_paths(
    output_dir: PathLike,
    *,
    target: RegistryTarget,
) -> Mapping[str, Path]:
    """Return Delta table directories for a registry target.

    Returns
    -------
    Mapping[str, pathlib.Path]
        Mapping of registry table names to Delta table paths.
    """
    root = registry_output_dir(output_dir, target=target)
    if not root.exists():
        return {}
    tables: dict[str, Path] = {}
    for entry in sorted(root.iterdir()):
        if _is_delta_table_dir(entry):
            tables[entry.name] = entry
    return tables


def register_registry_delta_tables(
    ctx: SessionContext,
    output_dir: PathLike,
    *,
    target: RegistryTarget,
    options: RegistryLoadOptions | None = None,
) -> Mapping[str, DataFrame]:
    """Register registry Delta tables for a single target.

    Returns
    -------
    Mapping[str, datafusion.dataframe.DataFrame]
        Registered DataFrames keyed by table name.
    """
    resolved = options or RegistryLoadOptions()
    resolved_scan = _resolve_scan_options(resolved.datafusion_scan, cache=resolved.cache)
    cache_policy = DataFusionCachePolicy(enabled=resolved.cache, max_columns=None)
    locations = registry_delta_table_paths(output_dir, target=target)
    registered: dict[str, DataFrame] = {}
    adapter = DataFusionIOAdapter(ctx=ctx, profile=resolved.runtime_profile)
    for table_name, path in locations.items():
        name = _registry_table_name(target, table_name)
        with suppress(KeyError, ValueError):
            adapter.deregister_table(name)
        location = DatasetLocation(
            path=str(path),
            format="delta",
            storage_options=dict(resolved.storage_options or {}),
            datafusion_scan=resolved_scan,
            delta_scan=resolved.delta_scan,
            delta_log_storage_options=dict(resolved.storage_options or {}),
        )
        resolved_delta_scan = resolve_delta_scan_options(location)
        resolved_log_storage = resolve_delta_log_storage_options(location)
        location = replace(
            location,
            delta_scan=resolved_delta_scan,
            delta_log_storage_options=dict(resolved_log_storage or {}),
        )
        registered[name] = _register_location(
            ctx,
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=resolved.runtime_profile,
        )
    return registered


def register_registry_exports(
    ctx: SessionContext,
    output_dir: PathLike,
    *,
    targets: Sequence[RegistryTarget] | None = None,
    options: RegistryLoadOptions | None = None,
) -> Mapping[str, DataFrame]:
    """Register registry Delta tables for one or more targets.

    Returns
    -------
    Mapping[str, datafusion.dataframe.DataFrame]
        Registered DataFrames keyed by table name.
    """
    resolved_targets = targets or ("cpg", "relspec")
    resolved = options or RegistryLoadOptions()
    registered: dict[str, DataFrame] = {}
    for target in resolved_targets:
        tables = register_registry_delta_tables(
            ctx,
            output_dir,
            target=target,
            options=resolved,
        )
        registered.update(tables)
    return registered


def _registry_table_name(target: RegistryTarget, table_name: str) -> str:
    prefix = f"{target}_"
    if table_name.startswith(prefix):
        return table_name
    return f"{prefix}{table_name}"


def _is_delta_table_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    return (path / "_delta_log").exists()


def _resolve_scan_options(
    scan: DataFusionScanOptions | None,
    *,
    cache: bool,
) -> DataFusionScanOptions:
    if scan is None:
        return DataFusionScanOptions(cache=cache)
    if not cache:
        return scan
    if scan.cache:
        return scan
    return replace(scan, cache=True)


def _register_location(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    return facade.register_dataset(
        name=name,
        location=location,
        cache_policy=cache_policy,
    )


__all__ = [
    "RegistryTarget",
    "register_registry_delta_tables",
    "register_registry_exports",
    "registry_delta_table_paths",
    "registry_output_dir",
]
