"""Parquet dataset writers for observability artifacts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from arrowdsl.io.parquet import DatasetWriteConfig, ParquetWriteOptions, write_dataset_parquet


def write_obs_dataset(
    base_dir: str | Path,
    *,
    name: str,
    table: pa.Table,
    overwrite: bool = True,
    opts: ParquetWriteOptions | None = None,
) -> str:
    """Write an observability dataset directory.

    Returns
    -------
    str
        Dataset directory path.
    """
    base = Path(base_dir)
    dataset_dir = base / name
    config = DatasetWriteConfig(opts=opts, overwrite=overwrite)
    return write_dataset_parquet(table, dataset_dir, config=config)


__all__ = ["write_obs_dataset"]
