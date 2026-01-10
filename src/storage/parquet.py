from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Dict, Mapping, Optional

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


@dataclass(frozen=True)
class ParquetWriteOptions:
    """
    “Reasonable defaults” for Parquet writes in a scan-heavy pipeline.

    Notes:
      - compression="zstd" is usually a good trade-off for speed/size.
      - use_dictionary=True helps with string-y categorical columns.
      - write_statistics=True helps pushdown + debugging.
    """
    compression: str = "zstd"
    use_dictionary: bool = True
    write_statistics: bool = True
    data_page_size: Optional[int] = None
    max_rows_per_file: int = 1_000_000  # used for dataset writes
    allow_truncated_timestamps: bool = True


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _rm_tree(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)


def write_table_parquet(
    table: pa.Table,
    path: str,
    *,
    opts: ParquetWriteOptions = ParquetWriteOptions(),
    overwrite: bool = True,
) -> str:
    """
    Write a single Parquet file to `path`.
    """
    _ensure_dir(os.path.dirname(path) or ".")
    if overwrite and os.path.exists(path):
        os.remove(path)

    pq.write_table(
        table,
        path,
        compression=opts.compression,
        use_dictionary=opts.use_dictionary,
        write_statistics=opts.write_statistics,
        data_page_size=opts.data_page_size,
        allow_truncated_timestamps=opts.allow_truncated_timestamps,
    )
    return path


def write_dataset_parquet(
    table: pa.Table,
    base_dir: str,
    *,
    opts: ParquetWriteOptions = ParquetWriteOptions(),
    overwrite: bool = True,
    basename_template: str = "part-{i}.parquet",
) -> str:
    """
    Write a Parquet *dataset directory* (multiple files) at `base_dir`.

    This is the preferred storage form when you intend to scan with:
      pyarrow.dataset.dataset(base_dir, format="parquet")
    """
    if overwrite:
        _rm_tree(base_dir)
    _ensure_dir(base_dir)

    # ds.write_dataset handles chunking into multiple files.
    ds.write_dataset(
        data=table,
        base_dir=base_dir,
        format="parquet",
        basename_template=basename_template,
        max_rows_per_file=opts.max_rows_per_file,
        existing_data_behavior="overwrite_or_ignore" if not overwrite else "delete_matching",
        file_options=ds.ParquetFileFormat().make_write_options(
            compression=opts.compression,
            use_dictionary=opts.use_dictionary,
            write_statistics=opts.write_statistics,
            data_page_size=opts.data_page_size,
        ),
    )
    return base_dir


def read_table_parquet(path: str) -> pa.Table:
    """
    Read a single Parquet file to a Table.
    """
    return pq.read_table(path)


def write_named_datasets_parquet(
    datasets: Mapping[str, pa.Table],
    base_dir: str,
    *,
    opts: ParquetWriteOptions = ParquetWriteOptions(),
    overwrite: bool = True,
) -> Dict[str, str]:
    """
    Write a set of named tables to:

      base_dir/<dataset_name>/part-*.parquet

    Returns {dataset_name: dataset_dir_path}.
    """
    _ensure_dir(base_dir)
    out: Dict[str, str] = {}
    for name, table in datasets.items():
        ds_dir = os.path.join(base_dir, name)
        write_dataset_parquet(table, ds_dir, opts=opts, overwrite=overwrite)
        out[name] = ds_dir
    return out
