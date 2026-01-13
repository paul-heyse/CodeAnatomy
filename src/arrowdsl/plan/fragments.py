"""Dataset fragment utilities for scans and telemetry."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from arrowdsl.core.interop import ComputeExpression, SchemaLike
from core_types import PathLike, ensure_path


def list_fragments(
    dataset: ds.Dataset, *, predicate: ComputeExpression | None = None
) -> list[ds.Fragment]:
    """Return dataset fragments, optionally filtered by a predicate.

    Returns
    -------
    list[ds.Fragment]
        Dataset fragments matching the predicate.
    """
    if predicate is None:
        return list(dataset.get_fragments())
    return list(dataset.get_fragments(filter=predicate))


def split_fragment_by_row_group(fragment: ds.Fragment) -> list[ds.Fragment]:
    """Split a fragment into row-group fragments when supported.

    Returns
    -------
    list[ds.Fragment]
        Row-group fragments or the original fragment when unsupported.
    """
    splitter = getattr(fragment, "split_by_row_group", None)
    if callable(splitter):
        fragments = splitter()
        return list(cast("Iterable[ds.Fragment]", fragments))
    return [fragment]


def row_group_fragments(
    dataset: ds.Dataset, *, predicate: ComputeExpression | None = None
) -> list[ds.Fragment]:
    """Return row-group fragments for a dataset.

    Returns
    -------
    list[ds.Fragment]
        Row-group fragments or dataset fragments when unsupported.
    """
    fragments = list_fragments(dataset, predicate=predicate)
    expanded: list[ds.Fragment] = []
    for fragment in fragments:
        expanded.extend(split_fragment_by_row_group(fragment))
    return expanded


def row_group_count(fragments: Sequence[ds.Fragment]) -> int:
    """Return the total row-group count for supported fragments.

    Returns
    -------
    int
        Total row-group count.
    """
    total = 0
    for fragment in fragments:
        splitter = getattr(fragment, "split_by_row_group", None)
        if callable(splitter):
            split = splitter()
            total += len(list(cast("Iterable[ds.Fragment]", split)))
    return total


def fragment_file_hints(
    fragments: Sequence[ds.Fragment],
    *,
    limit: int | None = 5,
) -> tuple[str, ...]:
    """Return a small set of fragment path hints.

    Returns
    -------
    tuple[str, ...]
        Tuple of path strings for fragment hints.
    """
    hints: list[str] = []
    for fragment in fragments:
        path = getattr(fragment, "path", None)
        if path is None:
            continue
        hints.append(str(path))
        if limit is not None and len(hints) >= limit:
            break
    return tuple(hints)


@dataclass(frozen=True)
class ParquetMetadataSpec:
    """Parquet metadata sidecar configuration."""

    schema: SchemaLike
    file_metadata: tuple[pq.FileMetaData, ...] = ()

    def write_common_metadata(
        self,
        base_dir: PathLike,
        *,
        filename: str = "_common_metadata",
    ) -> str:
        """Write a _common_metadata sidecar file.

        Returns
        -------
        str
            Path to the written metadata file.
        """
        path = ensure_path(base_dir) / filename
        pq.write_metadata(self.schema, str(path))
        return str(path)

    def write_metadata(
        self,
        base_dir: PathLike,
        *,
        filename: str = "_metadata",
    ) -> str | None:
        """Write a _metadata sidecar file with row-group stats when available.

        Returns
        -------
        str | None
            Path to the written metadata file, or ``None`` when unavailable.
        """
        if not self.file_metadata:
            return None
        path = ensure_path(base_dir) / filename
        pq.write_metadata(
            self.schema,
            str(path),
            metadata_collector=list(self.file_metadata),
        )
        return str(path)


def parquet_metadata_collector(
    fragments: Sequence[ds.Fragment],
) -> tuple[pq.FileMetaData, ...]:
    """Collect Parquet file metadata from dataset fragments.

    Returns
    -------
    tuple[pq.FileMetaData, ...]
        File metadata entries for the fragments.
    """
    collector: list[pq.FileMetaData] = []
    for fragment in fragments:
        metadata = fragment.metadata
        if metadata is None:
            continue
        collector.append(cast("pq.FileMetaData", metadata))
    return tuple(collector)


def parquet_metadata_factory(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
) -> ParquetMetadataSpec:
    """Return a ParquetMetadataSpec for a dataset.

    Returns
    -------
    ParquetMetadataSpec
        Metadata spec for sidecar generation.
    """
    fragments = list_fragments(dataset, predicate=predicate)
    file_metadata = parquet_metadata_collector(fragments)
    return ParquetMetadataSpec(schema=dataset.schema, file_metadata=file_metadata)


def scan_task_count(scanner: ds.Scanner) -> int:
    """Return the scan task count for a scanner.

    Returns
    -------
    int
        Number of scan tasks.
    """
    return sum(1 for _ in scanner.scan_tasks())


def take_row_group_fragments(
    fragments: Sequence[ds.Fragment], *, limit: int | None = None
) -> list[ds.Fragment]:
    """Return a limited subset of row-group fragments.

    Returns
    -------
    list[ds.Fragment]
        Subset of row-group fragments, limited when requested.
    """
    expanded: list[ds.Fragment] = []
    for fragment in fragments:
        expanded.extend(split_fragment_by_row_group(fragment))
        if limit is not None and len(expanded) >= limit:
            return expanded[:limit]
    return expanded


__all__ = [
    "ParquetMetadataSpec",
    "fragment_file_hints",
    "list_fragments",
    "parquet_metadata_collector",
    "parquet_metadata_factory",
    "row_group_count",
    "row_group_fragments",
    "scan_task_count",
    "split_fragment_by_row_group",
    "take_row_group_fragments",
]
