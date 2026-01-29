"""Dataset source normalization and wrappers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, TypeGuard, runtime_checkable

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

from arrow_utils.core.streaming import to_reader
from core_types import JsonDict, PathLike
from datafusion_engine.arrow_interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    coerce_table_like,
)


@dataclass
class OneShotDataset:
    """Wrap a dataset to enforce single-scan semantics."""

    dataset: ds.Dataset
    reader: pa.RecordBatchReader | None = None
    _scanned: bool = field(default=False, init=False, repr=False)

    @property
    def schema(self) -> SchemaLike:
        """Return the dataset schema.

        Returns
        -------
        SchemaLike
            Arrow schema for the dataset.
        """
        if self.reader is not None:
            return self.reader.schema
        return self.dataset.schema

    @classmethod
    def from_reader(cls, reader: pa.RecordBatchReader) -> OneShotDataset:
        """Create a one-shot wrapper around a record batch reader.

        Returns
        -------
        OneShotDataset
            Wrapper that enforces single-scan semantics.
        """
        dataset = ds.dataset(reader, schema=reader.schema)
        return cls(dataset=dataset, reader=reader)

    def consume(self) -> ds.Dataset:
        """Return the wrapped dataset, enforcing single-use semantics.

        Returns
        -------
        pyarrow.dataset.Dataset
            The wrapped dataset.

        Raises
        ------
        ValueError
            Raised when the dataset has already been scanned.
        """
        if self._scanned:
            msg = "One-shot dataset has already been scanned."
            raise ValueError(msg)
        self._scanned = True
        return self.dataset

    def scanner(self, *args: object, **kwargs: object) -> ds.Scanner:
        """Return a scanner while enforcing single-use semantics.

        Returns
        -------
        pyarrow.dataset.Scanner
            Scanner for the wrapped dataset.

        Raises
        ------
        ValueError
            Raised when the dataset has already been scanned.
        """
        if self._scanned:
            msg = "One-shot dataset has already been scanned."
            raise ValueError(msg)
        self._scanned = True
        if self.reader is None:
            return self.dataset.scanner(*args, **kwargs)
        return ds.Scanner.from_batches(self.reader, schema=self.schema, **kwargs)

    def __getattr__(self, name: str) -> object:
        """Delegate missing attributes to the underlying dataset.

        Returns
        -------
        object
            Attribute from the wrapped dataset.
        """
        return getattr(self.dataset, name)


type DatasetLike = ds.Dataset | OneShotDataset


def unwrap_dataset(dataset: DatasetLike) -> ds.Dataset:
    """Return a dataset, consuming one-shot wrappers when needed.

    Returns
    -------
    pyarrow.dataset.Dataset
        Dataset ready for scanning.
    """
    if isinstance(dataset, OneShotDataset):
        return dataset.consume()
    return dataset


def union_dataset(datasets: Sequence[ds.Dataset]) -> ds.Dataset:
    """Return a UnionDataset over multiple datasets.

    Returns
    -------
    pyarrow.dataset.Dataset
        Composite dataset spanning all inputs.
    """
    return ds.dataset(list(datasets))


def is_one_shot_dataset(value: object) -> bool:
    """Return whether the value is a one-shot dataset wrapper.

    Returns
    -------
    bool
        True when the value is a one-shot dataset wrapper.
    """
    return isinstance(value, OneShotDataset)


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
            Promote options value for dataset discovery.
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
    storage_options: Mapping[str, str] | None = None
    delta_version: int | None = None
    delta_timestamp: str | None = None
    discovery: DatasetDiscoveryOptions | None = field(default_factory=DatasetDiscoveryOptions)

    def discovery_payload(self) -> JsonDict | None:
        """Return discovery payload for telemetry artifacts.

        Returns
        -------
        JsonDict | None
            Telemetry payload for discovery settings, when applicable.
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


def open_dataset(
    source: PathLike | DatasetLike | _DatasetFactory | TableLike | RecordBatchReaderLike | object,
    *,
    options: DatasetSourceOptions | None = None,
) -> DatasetLike:
    """Open a dataset for scanning.

    Returns
    -------
    DatasetLike
        Dataset ready for scanning.
    """
    return normalize_dataset_source(source, options=options)


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
        Raised when a union dataset has no members or normalization fails.
    """
    resolved = options or DatasetSourceOptions()
    if resolved.dataset_format == "delta":
        msg = "Delta datasets must be registered via DataFusion TableProvider."
        raise ValueError(msg)
    dataset = _dataset_from_inputs(source, options=resolved)
    if dataset is None:
        msg = "Failed to normalize dataset source."
        raise ValueError(msg)
    return dataset


def _dataset_from_inputs(
    source: PathLike | DatasetLike | _DatasetFactory | TableLike | RecordBatchReaderLike | object,
    *,
    options: DatasetSourceOptions,
) -> DatasetLike | None:
    dataset: DatasetLike | None = None
    if _is_dataset_sequence(source):
        dataset = _dataset_from_sequence(source)
    elif isinstance(source, ds.Dataset) or is_one_shot_dataset(source):
        dataset = source
    elif isinstance(source, _DatasetFactory):
        dataset = source.build(schema=options.schema)
    elif isinstance(source, TableLike):
        dataset = ds.dataset(source, schema=options.schema)
    elif isinstance(source, RecordBatchReaderLike):
        reader = to_reader(source, schema=options.schema)
        dataset = OneShotDataset.from_reader(reader)
    elif _has_arrow_capsule(source):
        dataset = _dataset_from_capsule(source, options=options)
    else:
        path = _coerce_pathlike(source)
        file_format = _resolve_file_format(options)
        dataset = _dataset_from_path(path, options=options, file_format=file_format)
    return dataset


def _dataset_from_sequence(source: Iterable[DatasetLike]) -> DatasetLike:
    datasets = list(source)
    if not datasets:
        msg = "UnionDataset requires at least one dataset."
        raise ValueError(msg)
    return union_dataset(datasets)


def _dataset_from_capsule(
    source: object,
    *,
    options: DatasetSourceOptions,
) -> DatasetLike:
    coerced = coerce_table_like(source, requested_schema=options.schema)
    if isinstance(coerced, RecordBatchReaderLike):
        reader = to_reader(coerced, schema=options.schema)
        return OneShotDataset.from_reader(reader)
    return ds.dataset(coerced, schema=options.schema)


def _coerce_pathlike(source: object) -> PathLike:
    if isinstance(source, (str, Path)):
        return source
    msg = f"Dataset source must be path-like, got {type(source)}."
    raise TypeError(msg)


def _resolve_file_format(options: DatasetSourceOptions) -> str | ds.FileFormat:
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


def _dataset_from_path(
    source: PathLike,
    *,
    options: DatasetSourceOptions,
    file_format: str | ds.FileFormat,
) -> ds.Dataset:
    partitioning = _resolve_partitioning(options)
    return ds.dataset(
        source,
        format=file_format,
        filesystem=options.filesystem,
        partitioning=partitioning,
        schema=options.schema,
    )


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
    tuple[pafs.FileSystem, str]
        Filesystem and normalized path.
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
    "DatasetLike",
    "DatasetSourceOptions",
    "OneShotDataset",
    "PathLike",
    "from_uri",
    "is_one_shot_dataset",
    "normalize_dataset_source",
    "open_dataset",
    "union_dataset",
    "unwrap_dataset",
]
