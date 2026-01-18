"""Dataset source normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, TypeGuard, runtime_checkable

import pyarrow.dataset as ds
import pyarrow.fs as pafs
from deltalake import DeltaTable

from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike, coerce_table_like
from arrowdsl.plan.dataset_wrappers import (
    DatasetLike,
    OneShotDataset,
    is_one_shot_dataset,
    union_dataset,
)
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
            "promote_options": self.promote_options(),
        }


@dataclass(frozen=True)
class DatasetSourceOptions:
    """Options for dataset source normalization."""

    dataset_format: str = "delta"
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    partitioning: str | ds.Partitioning | None = "hive"
    filename_partitioning_schema: SchemaLike | None = None
    schema: SchemaLike | None = None
    parquet_read_options: ds.ParquetReadOptions | None = None
    storage_options: Mapping[str, str] | None = None
    delta_version: int | None = None
    delta_timestamp: str | None = None
    discovery: DatasetDiscoveryOptions | None = field(default_factory=DatasetDiscoveryOptions)

    def discovery_payload(self) -> JsonDict | None:
        """Return discovery payload for telemetry artifacts.

        Returns
        -------
        JsonDict | None
            Discovery payload including delta versioning when applicable.
        """
        payload: JsonDict | None = None
        if self.dataset_format == "delta":
            payload = {
                "dataset_format": "delta",
                "delta_version": self.delta_version,
                "delta_timestamp": self.delta_timestamp,
            }
        elif self.discovery is not None:
            payload = dict(self.discovery.payload())
            payload["dataset_format"] = self.dataset_format
        return payload


def normalize_dataset_source(
    source: PathLike | DatasetLike | _DatasetFactory | TableLike | RecordBatchReaderLike | object,
    *,
    options: DatasetSourceOptions | None = None,
) -> DatasetLike:
    """Normalize dataset sources into a pyarrow Dataset.

    Returns
    -------
    DatasetLike
        Normalized dataset instance.

    Raises
    ------
    ValueError
        Raised when a union dataset is requested with no inputs.
    """
    resolved = options or DatasetSourceOptions()
    dataset: DatasetLike | None = None
    if _is_dataset_sequence(source):
        datasets = list(source)
        if not datasets:
            msg = "UnionDataset requires at least one dataset."
            raise ValueError(msg)
        dataset = union_dataset(datasets)
    elif isinstance(source, ds.Dataset) or is_one_shot_dataset(source):
        dataset = source
    elif isinstance(source, _DatasetFactory):
        builder = source
        dataset = builder.build(schema=resolved.schema)
    elif isinstance(source, (TableLike, RecordBatchReaderLike)) or _has_arrow_capsule(source):
        coerced = coerce_table_like(source)
        dataset = ds.dataset(coerced, schema=resolved.schema)
        if isinstance(coerced, RecordBatchReaderLike):
            dataset = OneShotDataset(dataset)
    else:
        path = _coerce_pathlike(source)
        if resolved.dataset_format == "delta":
            if resolved.files:
                dataset = _delta_dataset_from_files(resolved.files, options=resolved)
            else:
                dataset = _delta_dataset_from_path(path, options=resolved)
        else:
            file_format = _resolve_file_format(resolved)
            dataset = _dataset_from_path(path, options=resolved, file_format=file_format)
    if dataset is None:
        msg = "Failed to normalize dataset source."
        raise ValueError(msg)
    return dataset


def _coerce_pathlike(source: object) -> PathLike:
    if isinstance(source, (str, Path)):
        return source
    msg = f"Dataset source must be path-like, got {type(source)}."
    raise TypeError(msg)


def _resolve_file_format(options: DatasetSourceOptions) -> str | ds.FileFormat:
    if options.dataset_format == "parquet" and options.parquet_read_options is not None:
        return ds.ParquetFileFormat(read_options=options.parquet_read_options)
    return options.dataset_format


def _resolve_partitioning(options: DatasetSourceOptions) -> str | ds.Partitioning | None:
    partitioning = options.partitioning
    if isinstance(partitioning, ds.Partitioning):
        return partitioning
    if partitioning == "filename":
        schema = options.filename_partitioning_schema or options.schema
        if schema is None:
            msg = "Filename partitioning requires a schema for partition fields."
            raise ValueError(msg)
        return ds.FilenamePartitioning(schema)
    return partitioning


def _delta_dataset_from_path(source: PathLike, *, options: DatasetSourceOptions) -> ds.Dataset:
    if options.delta_version is not None and options.delta_timestamp is not None:
        msg = "Delta dataset open requires either delta_version or delta_timestamp."
        raise ValueError(msg)
    storage = _storage_dict(options.storage_options)
    table = DeltaTable(
        str(source),
        version=options.delta_version,
        storage_options=storage,
    )
    if options.delta_timestamp is not None:
        table.load_as_version(options.delta_timestamp)
    filesystem = _delta_filesystem(source, filesystem=options.filesystem)
    return table.to_pyarrow_dataset(
        filesystem=filesystem,
        parquet_read_options=options.parquet_read_options,
        schema=options.schema,
    )


def _delta_dataset_from_files(
    files: Sequence[str],
    *,
    options: DatasetSourceOptions,
) -> ds.Dataset:
    if not files:
        msg = "Delta dataset file list cannot be empty."
        raise ValueError(msg)
    file_format = ds.ParquetFileFormat(read_options=options.parquet_read_options)
    filesystem = _normalize_filesystem(options.filesystem)
    return ds.dataset(
        list(files),
        format=file_format,
        filesystem=filesystem,
        schema=options.schema,
    )


def _delta_filesystem(source: PathLike, *, filesystem: object | None) -> pafs.FileSystem | None:
    resolved = _normalize_filesystem(filesystem)
    if resolved is not None:
        return resolved
    if isinstance(source, str) and "://" in source:
        raw_fs, normalized_path = pafs.FileSystem.from_uri(source)
        return pafs.SubTreeFileSystem(normalized_path, raw_fs)
    return None


def _dataset_from_path(
    source: PathLike,
    *,
    options: DatasetSourceOptions,
    file_format: str | ds.FileFormat,
) -> ds.Dataset:
    partitioning = _resolve_partitioning(options)
    if options.discovery is None or options.dataset_format != "parquet":
        return ds.dataset(
            source,
            format=file_format,
            filesystem=options.filesystem,
            partitioning=partitioning,
            schema=options.schema,
        )
    resolved_fs, resolved_path = from_uri(source, filesystem=options.filesystem)
    if not _is_dir(resolved_fs, resolved_path):
        return ds.dataset(
            resolved_path,
            format=file_format,
            filesystem=resolved_fs,
            partitioning=partitioning,
            schema=options.schema,
        )
    if options.discovery.use_metadata:
        metadata_dataset = _metadata_sidecar_dataset(
            resolved_fs,
            resolved_path,
            schema=options.schema,
            partitioning=partitioning,
            file_format=file_format if options.dataset_format == "parquet" else None,
        )
        if metadata_dataset is not None:
            return metadata_dataset
    selector = pafs.FileSelector(resolved_path, recursive=True)
    factory_options = ds.FileSystemFactoryOptions(
        partition_base_dir=resolved_path,
        exclude_invalid_files=options.discovery.exclude_invalid_files,
        selector_ignore_prefixes=list(options.discovery.selector_ignore_prefixes),
    )
    format_obj = ds.ParquetFileFormat(read_options=options.parquet_read_options)
    factory = ds.FileSystemDatasetFactory(resolved_fs, selector, format_obj, factory_options)
    inspected_schema = factory.inspect(promote_options=options.discovery.promote_options())
    return factory.finish(schema=options.schema or inspected_schema)


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


def from_uri(
    source: PathLike,
    *,
    filesystem: object | None = None,
) -> tuple[pafs.FileSystem, str]:
    """Resolve a filesystem and path for URI-like sources.

    Returns
    -------
    tuple[pyarrow.fs.FileSystem, str]
        Filesystem and normalized path for the source.
    """
    resolved = _normalize_filesystem(filesystem)
    if isinstance(source, str) and "://" in source:
        if resolved is not None:
            _, path = source.split("://", 1)
            return pafs.SubTreeFileSystem(path, resolved), ""
        fs, path = pafs.FileSystem.from_uri(source)
        return pafs.SubTreeFileSystem(path, fs), ""
    if resolved is not None:
        return resolved, str(source)
    return pafs.LocalFileSystem(), str(source)


def _resolve_filesystem(source: PathLike, filesystem: object | None) -> tuple[pafs.FileSystem, str]:
    return from_uri(source, filesystem=filesystem)


def _metadata_sidecar_dataset(
    filesystem: pafs.FileSystem,
    base_path: str,
    *,
    schema: SchemaLike | None,
    partitioning: str | ds.Partitioning | None,
    file_format: ds.FileFormat | None,
) -> ds.Dataset | None:
    if not _is_dir(filesystem, base_path):
        return None
    metadata_path = f"{base_path}/_metadata"
    common_metadata_path = f"{base_path}/_common_metadata"
    if _path_exists(filesystem, metadata_path):
        return ds.parquet_dataset(
            metadata_path,
            filesystem=filesystem,
            format=file_format,
            partitioning=partitioning,
            schema=schema,
        )
    if _path_exists(filesystem, common_metadata_path):
        return ds.parquet_dataset(
            common_metadata_path,
            filesystem=filesystem,
            format=file_format,
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


def _storage_dict(storage_options: Mapping[str, str] | None) -> dict[str, str] | None:
    if storage_options is None:
        return None
    return dict(storage_options)


def _has_arrow_capsule(value: object) -> bool:
    return any(
        hasattr(value, attr)
        for attr in ("__arrow_c_stream__", "__arrow_c_array__", "__dataframe__")
    )


def _is_dataset_sequence(value: object) -> TypeGuard[Sequence[ds.Dataset]]:
    if isinstance(value, (str, Path)):
        return False
    if not isinstance(value, Sequence):
        return False
    return all(isinstance(item, ds.Dataset) for item in value)


__all__ = [
    "DatasetDiscoveryOptions",
    "DatasetSourceOptions",
    "PathLike",
    "from_uri",
    "normalize_dataset_source",
]
