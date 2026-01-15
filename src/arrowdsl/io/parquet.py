"""Parquet read/write helpers for Arrow tables."""

from __future__ import annotations

import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.metrics import ParquetMetadataSpec, parquet_metadata_factory
from arrowdsl.schema.encoding_policy import EncodingPolicy, apply_encoding
from arrowdsl.schema.schema import SchemaTransform
from core_types import PathLike, ensure_path

type DatasetWriteInput = TableLike | RecordBatchReaderLike


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


@dataclass(frozen=True)
class ParquetMetadataConfig:
    """Configuration for dataset metadata sidecar files."""

    write_common_metadata: bool = True
    write_metadata: bool = True
    common_filename: str = "_common_metadata"
    metadata_filename: str = "_metadata"


@dataclass(frozen=True)
class DatasetWriteConfig:
    """Dataset write configuration with schema/encoding alignment."""

    opts: ParquetWriteOptions | None = None
    overwrite: bool = True
    basename_template: str = "part-{i}.parquet"
    schema: SchemaLike | None = None
    encoding_policy: EncodingPolicy | None = None
    metadata: ParquetMetadataConfig | None = None


@dataclass(frozen=True)
class NamedDatasetWriteConfig:
    """Named dataset write configuration with per-dataset schemas."""

    opts: ParquetWriteOptions | None = None
    overwrite: bool = True
    basename_template: str = "part-{i}.parquet"
    schemas: Mapping[str, SchemaLike] | None = None
    encoding_policies: Mapping[str, EncodingPolicy] | None = None
    metadata: ParquetMetadataConfig | None = None


def _ensure_dir(path: Path) -> None:
    """Ensure the directory exists for a target path.

    Parameters
    ----------
    path
        Directory path to create if missing.
    """
    path.mkdir(exist_ok=True, parents=True)


def _rm_tree(path: Path) -> None:
    """Remove a directory tree if it exists.

    Parameters
    ----------
    path
        Directory path to delete.
    """
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
    """Return a derived artifact path using a suffix.

    Parameters
    ----------
    base
        Base file path.
    suffix
        Suffix to insert before the file extension.

    Returns
    -------
    Path
        Path with the suffix inserted.
    """
    return base.with_name(f"{base.stem}_{suffix}{base.suffix}")


def _apply_schema_and_encoding(
    data: DatasetWriteInput,
    *,
    schema: SchemaLike | None,
    encoding_policy: EncodingPolicy | None,
) -> DatasetWriteInput:
    """Apply schema transforms and encoding policies to dataset inputs.

    Parameters
    ----------
    data
        Input table or record batch reader.
    schema
        Optional schema to enforce.
    encoding_policy
        Optional encoding policy to apply.

    Returns
    -------
    DatasetWriteInput
        Transformed data, preserving input type when possible.
    """
    if schema is None and encoding_policy is None:
        return data
    table = data.read_all() if isinstance(data, RecordBatchReaderLike) else data
    if schema is not None:
        transform = SchemaTransform(
            schema=schema,
            safe_cast=True,
            keep_extra_columns=False,
            on_error="unsafe",
        )
        table = transform.apply(table)
    if encoding_policy is not None:
        table = apply_encoding(table, policy=encoding_policy)
    return table


def write_parquet_metadata_sidecars(
    base_dir: PathLike,
    *,
    schema: SchemaLike,
    metadata: Sequence[pq.FileMetaData] | None = None,
    config: ParquetMetadataConfig | None = None,
) -> dict[str, str]:
    """Write Parquet metadata sidecar files for a dataset directory.

    Returns
    -------
    dict[str, str]
        Mapping of metadata file keys to paths.
    """
    config = config or ParquetMetadataConfig()
    spec = ParquetMetadataSpec(schema=schema, file_metadata=tuple(metadata or ()))
    base_path = ensure_path(base_dir)
    out: dict[str, str] = {}
    if config.write_common_metadata:
        out["common_metadata"] = spec.write_common_metadata(
            base_path,
            filename=config.common_filename,
        )
    if config.write_metadata:
        metadata_path = spec.write_metadata(
            base_path,
            filename=config.metadata_filename,
        )
        if metadata_path is not None:
            out["metadata"] = metadata_path
    return out


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
    table: DatasetWriteInput,
    base_dir: PathLike,
    *,
    config: DatasetWriteConfig | None = None,
) -> str:
    """Write a Parquet dataset directory to base_dir.

    Accepts tables or RecordBatchReader inputs; readers are streamed when no schema or
    encoding policy is provided.

    This is the preferred storage form when you intend to scan with:
      pyarrow.dataset.dataset(base_dir, format="parquet")

    Returns
    -------
    str
        Dataset directory path.
    """
    config = config or DatasetWriteConfig()
    options = config.opts or ParquetWriteOptions()
    metadata_config = config.metadata or ParquetMetadataConfig()
    base_path = ensure_path(base_dir)
    if config.overwrite:
        _rm_tree(base_path)
    _ensure_dir(base_path)

    data = _apply_schema_and_encoding(
        table,
        schema=config.schema,
        encoding_policy=config.encoding_policy,
    )

    ds.write_dataset(
        data=data,
        base_dir=str(base_path),
        format="parquet",
        basename_template=config.basename_template,
        max_rows_per_file=options.max_rows_per_file,
        existing_data_behavior=(
            "overwrite_or_ignore" if not config.overwrite else "delete_matching"
        ),
        file_options=ds.ParquetFileFormat().make_write_options(
            compression=options.compression,
            use_dictionary=options.use_dictionary,
            write_statistics=options.write_statistics,
            data_page_size=options.data_page_size,
        ),
    )
    if metadata_config.write_common_metadata or metadata_config.write_metadata:
        dataset = ds.dataset(str(base_path), format="parquet")
        metadata_spec = parquet_metadata_factory(dataset)
        write_parquet_metadata_sidecars(
            base_path,
            schema=metadata_spec.schema,
            metadata=metadata_spec.file_metadata if metadata_config.write_metadata else None,
            config=metadata_config,
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
    datasets: Mapping[str, DatasetWriteInput],
    base_dir: PathLike,
    *,
    config: NamedDatasetWriteConfig | None = None,
) -> dict[str, str]:
    """Write named tables or readers to per-dataset Parquet directories.

      base_dir/<dataset_name>/part-*.parquet

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset directory path.
    """
    config = config or NamedDatasetWriteConfig()
    options = config.opts or ParquetWriteOptions()
    base_path = ensure_path(base_dir)
    _ensure_dir(base_path)
    out: dict[str, str] = {}
    for name, table in datasets.items():
        ds_dir = base_path / name
        schema = config.schemas.get(name) if config.schemas is not None else None
        encoding_policy = (
            config.encoding_policies.get(name) if config.encoding_policies is not None else None
        )
        write_dataset_parquet(
            table,
            ds_dir,
            config=DatasetWriteConfig(
                opts=options,
                overwrite=config.overwrite,
                basename_template=config.basename_template,
                schema=schema,
                encoding_policy=encoding_policy,
                metadata=config.metadata,
            ),
        )
        out[name] = str(ds_dir)
    return out


__all__ = [
    "DatasetWriteConfig",
    "DatasetWriteInput",
    "NamedDatasetWriteConfig",
    "ParquetMetadataConfig",
    "ParquetWriteOptions",
    "read_table_parquet",
    "write_dataset_parquet",
    "write_finalize_result_parquet",
    "write_named_datasets_parquet",
    "write_parquet_metadata_sidecars",
    "write_table_parquet",
]
