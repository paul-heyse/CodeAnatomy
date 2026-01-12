"""Parquet read/write helpers for Arrow tables."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from arrowdsl.finalize import FinalizeResult
from arrowdsl.pyarrow_protocols import TableLike
from core_types import PathLike, ensure_path


@dataclass(frozen=True)
class ParquetWriteOptions:
    """
    “Reasonable defaults” for Parquet writes in a scan-heavy pipeline.

    Notes
    -----
      - compression="zstd" is usually a good trade-off for speed/size.
      - use_dictionary=True helps with string-y categorical columns.
      - write_statistics=True helps pushdown + debugging.
    """

    compression: str = "zstd"
    use_dictionary: bool = True
    write_statistics: bool = True
    data_page_size: int | None = None
    max_rows_per_file: int = 1_000_000  # used for dataset writes
    allow_truncated_timestamps: bool = True


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


def _rm_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def write_table_parquet(
    table: TableLike,
    path: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
) -> str:
    """Write a single Parquet file to a path.

    Returns
    -------
    str
        Path to the written Parquet file.
    """
    options = opts or ParquetWriteOptions()
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    pq.write_table(
        table,
        str(target),
        compression=options.compression,
        use_dictionary=options.use_dictionary,
        write_statistics=options.write_statistics,
        data_page_size=options.data_page_size,
        allow_truncated_timestamps=options.allow_truncated_timestamps,
    )
    return str(target)


def _artifact_path(base: Path, suffix: str) -> Path:
    return base.with_name(f"{base.stem}_{suffix}{base.suffix}")


def write_finalize_result_parquet(
    result: FinalizeResult,
    path: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
) -> dict[str, str]:
    """Write a finalized table plus error artifacts to Parquet.

    Returns
    -------
    dict[str, str]
        Paths for data, errors, stats, and alignment outputs.
    """
    base = ensure_path(path)
    data_path = write_table_parquet(result.good, base, opts=opts, overwrite=overwrite)
    errors_path = write_table_parquet(
        result.errors,
        _artifact_path(base, "errors"),
        opts=opts,
        overwrite=overwrite,
    )
    stats_path = write_table_parquet(
        result.stats,
        _artifact_path(base, "error_stats"),
        opts=opts,
        overwrite=overwrite,
    )
    alignment_path = write_table_parquet(
        result.alignment,
        _artifact_path(base, "alignment"),
        opts=opts,
        overwrite=overwrite,
    )
    return {
        "data": data_path,
        "errors": errors_path,
        "stats": stats_path,
        "alignment": alignment_path,
    }


def write_dataset_parquet(
    table: TableLike,
    base_dir: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
    basename_template: str = "part-{i}.parquet",
) -> str:
    """Write a Parquet dataset directory to base_dir.

    This is the preferred storage form when you intend to scan with:
      pyarrow.dataset.dataset(base_dir, format="parquet")

    Returns
    -------
    str
        Dataset directory path.
    """
    options = opts or ParquetWriteOptions()
    base_path = ensure_path(base_dir)
    if overwrite:
        _rm_tree(base_path)
    _ensure_dir(base_path)

    # ds.write_dataset handles chunking into multiple files.
    ds.write_dataset(
        data=table,
        base_dir=str(base_path),
        format="parquet",
        basename_template=basename_template,
        max_rows_per_file=options.max_rows_per_file,
        existing_data_behavior="overwrite_or_ignore" if not overwrite else "delete_matching",
        file_options=ds.ParquetFileFormat().make_write_options(
            compression=options.compression,
            use_dictionary=options.use_dictionary,
            write_statistics=options.write_statistics,
            data_page_size=options.data_page_size,
        ),
    )
    return str(base_path)


def read_table_parquet(path: PathLike) -> TableLike:
    """Read a single Parquet file into a table.

    Returns
    -------
    TableLike
        Loaded table.
    """
    return pq.read_table(str(ensure_path(path)))


def write_named_datasets_parquet(
    datasets: Mapping[str, TableLike],
    base_dir: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
) -> dict[str, str]:
    """Write named tables to per-dataset Parquet directories.

      base_dir/<dataset_name>/part-*.parquet

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset directory path.
    """
    options = opts or ParquetWriteOptions()
    base_path = ensure_path(base_dir)
    _ensure_dir(base_path)
    out: dict[str, str] = {}
    for name, table in datasets.items():
        ds_dir = base_path / name
        write_dataset_parquet(table, ds_dir, opts=options, overwrite=overwrite)
        out[name] = str(ds_dir)
    return out
