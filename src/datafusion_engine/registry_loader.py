"""Register Delta-backed registry tables in a DataFusion SessionContext."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from deltalake import DeltaTable

from arrowdsl.schema.union_codec import decode_union_table, schema_has_union_encoding
from core_types import PathLike, ensure_path
from datafusion_engine.registry_bridge import DataFusionCachePolicy, register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation
from schema_spec.system import DataFusionScanOptions, DeltaScanOptions
from storage.deltalake.delta import read_table_delta

RegistryTarget = Literal["cpg", "relspec"]


@dataclass(frozen=True)
class RegistryLoadOptions:
    """Options for registry table registration."""

    datafusion_scan: DataFusionScanOptions | None = None
    delta_scan: DeltaScanOptions | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    storage_options: Mapping[str, str] | None = None
    cache: bool = True
    decode_unions: bool = True


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
    for table_name, path in locations.items():
        name = _registry_table_name(target, table_name)
        if resolved.decode_unions and _delta_schema_has_union_encoding(
            path,
            resolved.storage_options,
        ):
            decoded = _read_union_encoded_table(path, resolved.storage_options)
            ctx.register_record_batches(name, [decoded.to_batches()])
            registered[name] = ctx.table(name)
            continue
        location = DatasetLocation(
            path=str(path),
            format="delta",
            storage_options=dict(resolved.storage_options or {}),
            datafusion_scan=resolved_scan,
            delta_scan=resolved.delta_scan,
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


def _delta_schema_has_union_encoding(
    path: Path,
    storage_options: Mapping[str, str] | None,
) -> bool:
    storage = dict(storage_options or {})
    options = storage if storage else None
    table = DeltaTable(str(path), storage_options=options)
    schema = table.schema().to_arrow()
    if isinstance(schema, pa.Schema):
        return schema_has_union_encoding(schema)
    return "arrowdsl.union_encoding" in table.schema().to_json()


def _read_union_encoded_table(
    path: Path,
    storage_options: Mapping[str, str] | None,
) -> pa.Table:
    table = read_table_delta(str(path), storage_options=storage_options)
    arrow_table = _ensure_pyarrow_table(table)
    return decode_union_table(arrow_table)


def _ensure_pyarrow_table(value: object) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    msg = f"Delta read expected pyarrow.Table, got {type(value)}"
    raise TypeError(msg)


def _register_location(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    try:
        return _register_with_cache_fallback(
            ctx,
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=runtime_profile,
        )
    except ValueError as exc:
        if not _should_fallback_to_dataset(exc):
            raise
    fallback = replace(location, datafusion_provider="dataset")
    return _register_with_cache_fallback(
        ctx,
        name=name,
        location=fallback,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )


def _should_fallback_to_dataset(exc: Exception) -> bool:
    message = str(exc)
    return "pyarrow.dataset.Dataset" in message


def _register_with_cache_fallback(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    try:
        return register_dataset_df(
            ctx,
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=runtime_profile,
        )
    except Exception as exc:
        if not _should_skip_cache(exc):
            raise
    return ctx.table(name)


def _should_skip_cache(exc: Exception) -> bool:
    message = str(exc)
    return "No partitions provided" in message


__all__ = [
    "RegistryTarget",
    "register_registry_delta_tables",
    "register_registry_exports",
    "registry_delta_table_paths",
    "registry_output_dir",
]
