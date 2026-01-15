"""Dataset source normalization helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

import pyarrow.dataset as ds
import pyarrow.fs as pafs

from arrowdsl.core.interop import SchemaLike

type PathLike = str | Path


@runtime_checkable
class _DatasetFactory(Protocol):
    def build(self, *, schema: SchemaLike | None = None) -> ds.Dataset: ...


def normalize_dataset_source(
    source: PathLike | ds.Dataset | _DatasetFactory,
    *,
    dataset_format: str = "parquet",
    filesystem: pafs.FileSystem | None = None,
    partitioning: str | None = "hive",
    schema: SchemaLike | None = None,
) -> ds.Dataset:
    """Normalize dataset sources into a pyarrow Dataset.

    Returns
    -------
    ds.Dataset
        Normalized dataset instance.
    """
    if isinstance(source, ds.Dataset):
        return source
    if isinstance(source, _DatasetFactory):
        builder = source
        return builder.build(schema=schema)
    return ds.dataset(
        source,
        format=dataset_format,
        filesystem=filesystem,
        partitioning=partitioning,
        schema=schema,
    )


__all__ = ["PathLike", "normalize_dataset_source"]
