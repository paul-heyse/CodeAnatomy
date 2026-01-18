"""Dataset source normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

import pyarrow.dataset as ds
import pyarrow.fs as pafs

from arrowdsl.core.interop import SchemaLike
from core_types import JsonDict

type PathLike = str | Path


@runtime_checkable
class _DatasetFactory(Protocol):
    def build(self, *, schema: SchemaLike | None = None) -> ds.Dataset: ...


@dataclass(frozen=True)
class DatasetDiscoveryOptions:
    """Options for dataset discovery with FileSystem factories."""

    strict: bool = True
    use_metadata: bool = True
    exclude_invalid_files: bool = True
    selector_ignore_prefixes: tuple[str, ...] = (".", "_")

    def promote_options(self) -> str:
        """Return the promote_options value for factory inspection.

        Returns
        -------
        str
            Promotion policy for inspection.
        """
        return "default" if self.strict else "permissive"

    def payload(self) -> JsonDict:
        """Return a JSON-ready payload for the discovery policy.

        Returns
        -------
        JsonDict
            JSON-ready discovery policy payload.
        """
        return {
            "strict": self.strict,
            "use_metadata": self.use_metadata,
            "exclude_invalid_files": self.exclude_invalid_files,
            "selector_ignore_prefixes": list(self.selector_ignore_prefixes),
        }


@dataclass(frozen=True)
class DatasetSourceOptions:
    """Options for dataset source normalization."""

    dataset_format: str = "parquet"
    filesystem: object | None = None
    partitioning: str | None = "hive"
    schema: SchemaLike | None = None
    discovery: DatasetDiscoveryOptions | None = field(default_factory=DatasetDiscoveryOptions)


def normalize_dataset_source(
    source: PathLike | ds.Dataset | _DatasetFactory,
    *,
    options: DatasetSourceOptions | None = None,
) -> ds.Dataset:
    """Normalize dataset sources into a pyarrow Dataset.

    Returns
    -------
    ds.Dataset
        Normalized dataset instance.
    """
    resolved = options or DatasetSourceOptions()
    dataset_format = resolved.dataset_format
    filesystem = resolved.filesystem
    partitioning = resolved.partitioning
    schema = resolved.schema
    discovery = resolved.discovery
    if isinstance(source, ds.Dataset):
        return source
    if isinstance(source, _DatasetFactory):
        builder = source
        return builder.build(schema=schema)
    if discovery is None or dataset_format != "parquet":
        return ds.dataset(
            source,
            format=dataset_format,
            filesystem=filesystem,
            partitioning=partitioning,
            schema=schema,
        )
    resolved_fs, resolved_path = _resolve_filesystem(source, filesystem)
    if not _is_dir(resolved_fs, resolved_path):
        return ds.dataset(
            resolved_path,
            format=dataset_format,
            filesystem=resolved_fs,
            partitioning=partitioning,
            schema=schema,
        )
    if discovery.use_metadata:
        metadata_dataset = _metadata_sidecar_dataset(
            resolved_fs,
            resolved_path,
            schema=schema,
            partitioning=partitioning,
        )
        if metadata_dataset is not None:
            return metadata_dataset
    selector = pafs.FileSelector(resolved_path, recursive=True)
    options = ds.FileSystemFactoryOptions(
        partition_base_dir=resolved_path,
        exclude_invalid_files=discovery.exclude_invalid_files,
        selector_ignore_prefixes=list(discovery.selector_ignore_prefixes),
    )
    factory = ds.FileSystemDatasetFactory(resolved_fs, selector, ds.ParquetFileFormat(), options)
    inspected_schema = factory.inspect(promote_options=discovery.promote_options())
    return factory.finish(schema=schema or inspected_schema)


def _normalize_filesystem(filesystem: object | None) -> pafs.FileSystem | None:
    if filesystem is None:
        return None
    if isinstance(filesystem, pafs.FileSystem):
        return filesystem
    handler = getattr(pafs, "FSSpecHandler", None)
    if handler is not None:
        try:
            return pafs.PyFileSystem(handler(filesystem))
        except (TypeError, ValueError):
            pass
    if isinstance(filesystem, pafs.FileSystemHandler):
        return pafs.PyFileSystem(filesystem)
    msg = "Unsupported filesystem type; provide a pyarrow or fsspec filesystem."
    raise TypeError(msg)


def _resolve_filesystem(source: PathLike, filesystem: object | None) -> tuple[pafs.FileSystem, str]:
    resolved = _normalize_filesystem(filesystem)
    if resolved is not None:
        return resolved, str(source)
    if isinstance(source, str) and "://" in source:
        return pafs.FileSystem.from_uri(source)
    return pafs.LocalFileSystem(), str(source)


def _metadata_sidecar_dataset(
    filesystem: pafs.FileSystem,
    base_path: str,
    *,
    schema: SchemaLike | None,
    partitioning: str | None,
) -> ds.Dataset | None:
    if not _is_dir(filesystem, base_path):
        return None
    metadata_path = f"{base_path}/_metadata"
    common_metadata_path = f"{base_path}/_common_metadata"
    if _path_exists(filesystem, metadata_path):
        return ds.parquet_dataset(
            metadata_path,
            filesystem=filesystem,
            partitioning=partitioning,
            schema=schema,
        )
    if _path_exists(filesystem, common_metadata_path):
        return ds.parquet_dataset(
            common_metadata_path,
            filesystem=filesystem,
            partitioning=partitioning,
            schema=schema,
        )
    return None


def _is_dir(filesystem: pafs.FileSystem, path: str) -> bool:
    info = filesystem.get_file_info(path)
    return info.type == pafs.FileType.Directory


def _path_exists(filesystem: pafs.FileSystem, path: str) -> bool:
    info = filesystem.get_file_info(path)
    return info.type != pafs.FileType.NotFound


__all__ = [
    "DatasetDiscoveryOptions",
    "DatasetSourceOptions",
    "PathLike",
    "normalize_dataset_source",
]
