"""Parquet read/write helpers for Arrow tables."""

from __future__ import annotations

import inspect
import re
import shutil
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from arrowdsl.core.context import OrderingLevel
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.core.metrics import (
    ParquetMetadataSpec,
    list_fragments,
    parquet_metadata_factory,
    row_group_stats,
)
from arrowdsl.core.ordering_policy import ordering_keys_for_schema
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.schema.encoding_policy import EncodingPolicy, apply_encoding
from arrowdsl.schema.metadata import ordering_from_schema
from arrowdsl.schema.schema import SchemaTransform
from arrowdsl.schema.serialization import schema_fingerprint
from core_types import PathLike, ensure_path
from engine.plan_product import PlanProduct

type DatasetWriteInput = TableLike | RecordBatchReaderLike | PlanProduct


def _coerce_plan_product(value: DatasetWriteInput) -> TableLike | RecordBatchReaderLike:
    if isinstance(value, PlanProduct):
        if value.writer_strategy != "arrow":
            msg = "Parquet dataset writes require an Arrow writer strategy."
            raise ValueError(msg)
        return value.value()
    return value


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
    row_group_size: int | None = None
    max_rows_per_file: int = 1_000_000  # used for dataset writes
    allow_truncated_timestamps: bool = True


@dataclass(frozen=True)
class ParquetMetadataConfig:
    """Configuration for dataset metadata sidecar files."""

    write_common_metadata: bool = True
    write_metadata: bool = True
    common_filename: str = "_common_metadata"
    metadata_filename: str = "_metadata"
    max_metadata_files: int | None = 10_000
    max_row_group_stats_files: int | None = 200
    max_row_group_stats: int | None = 1_000


@dataclass(frozen=True)
class DatasetWriteConfig:
    """Dataset write configuration with schema/encoding alignment."""

    opts: ParquetWriteOptions | None = None
    overwrite: bool = True
    basename_template: str = "part-{i}.parquet"
    schema: SchemaLike | None = None
    encoding_policy: EncodingPolicy | None = None
    metadata: ParquetMetadataConfig | None = None
    preserve_order: bool | None = None
    file_visitor: Callable[[object], None] | None = None
    max_open_files: int | None = None
    reporter: Callable[[DatasetWriteReport], None] | None = None


@dataclass(frozen=True)
class NamedDatasetWriteConfig:
    """Named dataset write configuration with per-dataset schemas."""

    opts: ParquetWriteOptions | None = None
    overwrite: bool = True
    basename_template: str = "part-{i}.parquet"
    schemas: Mapping[str, SchemaLike] | None = None
    encoding_policies: Mapping[str, EncodingPolicy] | None = None
    metadata: ParquetMetadataConfig | None = None
    preserve_order: bool | None = None
    file_visitors: Mapping[str, Callable[[object], None]] | None = None
    max_open_files: int | None = None
    reporter: Callable[[DatasetWriteReport], None] | None = None


@dataclass(frozen=True)
class DatasetWriteReport:
    """Report describing a dataset write action."""

    dataset_name: str | None
    base_dir: str
    files: tuple[str, ...]
    metadata_paths: Mapping[str, str] | None
    schema_fingerprint: str | None
    ordering_keys: tuple[tuple[str, str], ...] | None
    file_sort_order: tuple[str, ...] | None
    preserve_order: bool
    writer_strategy: str
    row_group_size: int | None
    max_rows_per_file: int
    sorting_columns: tuple[Mapping[str, object], ...] | None = None
    row_group_stats: tuple[Mapping[str, object], ...] | None = None
    datafusion_write_policy: Mapping[str, object] | None = None


def _ensure_dir(path: Path) -> None:
    """Ensure the directory exists for a target path.

    Parameters
    ----------
    path
        Directory path to create if missing.
    """
    path.mkdir(exist_ok=True, parents=True)


def _capture_file_visitor(
    visitor: Callable[[object], None] | None,
    *,
    paths: list[str],
) -> Callable[[object], None]:
    def _wrapped(record: object) -> None:
        path = getattr(record, "path", None)
        if path is not None:
            paths.append(str(path))
        if visitor is not None:
            visitor(record)

    return _wrapped


def _rm_tree(path: Path) -> None:
    """Remove a directory tree if it exists.

    Parameters
    ----------
    path
        Directory path to delete.
    """
    if path.exists():
        shutil.rmtree(path)


def _delete_partition_dir(base_dir: Path, partition: Mapping[str, str]) -> None:
    parts = [f"{key}={value}" for key, value in partition.items()]
    if not parts:
        return
    target = base_dir.joinpath(*parts)
    _rm_tree(target)


def _schema_fingerprint_for_data(data: object) -> str | None:
    schema = getattr(data, "schema", None)
    if schema is None:
        return None
    return schema_fingerprint(schema)


def _ordering_keys_payload(schema: SchemaLike | None) -> tuple[tuple[str, str], ...] | None:
    if schema is None:
        return None
    return tuple((key[0], key[1]) for key in ordering_keys_for_schema(schema))


def _file_sort_order_payload(schema: SchemaLike | None) -> tuple[str, ...] | None:
    if schema is None:
        return None
    keys = ordering_keys_for_schema(schema)
    if not keys:
        return None
    return tuple(key[0] for key in keys)


_BASENAME_TOKEN_PATTERN = re.compile(r"{([^{}]+)}")


def _basename_template_tokens(template: str) -> tuple[str, ...]:
    return tuple(match.group(1) for match in _BASENAME_TOKEN_PATTERN.finditer(template))


def _validate_basename_template(schema: SchemaLike | None, template: str) -> None:
    tokens = _basename_template_tokens(template)
    if not tokens:
        return
    allowed = {"i"}
    if schema is None:
        missing = [token for token in tokens if token not in allowed]
        if missing:
            msg = f"basename_template tokens require schema fields: {missing}"
            raise ValueError(msg)
        return
    schema_names = set(schema.names)
    missing = [token for token in tokens if token not in allowed and token not in schema_names]
    if missing:
        msg = f"basename_template tokens missing in schema: {missing}"
        raise ValueError(msg)


def parquet_supports_sorting_columns() -> bool:
    """Return whether Parquet sorting_columns are supported by PyArrow.

    Returns
    -------
    bool
        True when sorting_columns are supported by the installed PyArrow.
    """
    try:
        params = inspect.signature(ds.ParquetFileFormat().make_write_options).parameters
    except (TypeError, ValueError):
        return False
    return "sorting_columns" in params


def _sorting_columns_for_schema(schema: SchemaLike | None) -> list[pq.SortingColumn] | None:
    if schema is None:
        return None
    ordering = ordering_from_schema(schema)
    if ordering.level != OrderingLevel.EXPLICIT or not ordering.keys:
        return None
    indices = {name: idx for idx, name in enumerate(schema.names)}
    columns: list[pq.SortingColumn] = []
    for name, order in ordering.keys:
        idx = indices.get(name)
        if idx is None:
            msg = f"Ordering key {name!r} missing from schema."
            raise ValueError(msg)
        descending = order.lower().startswith("desc")
        columns.append(pq.SortingColumn(column_index=idx, descending=descending, nulls_first=False))
    return columns or None


def _sorting_columns_payload(
    schema: SchemaLike | None, sorting_columns: Sequence[pq.SortingColumn] | None
) -> tuple[Mapping[str, object], ...] | None:
    if schema is None or not sorting_columns:
        return None
    names = list(schema.names)
    payload: list[Mapping[str, object]] = []
    for col in sorting_columns:
        name = names[col.column_index] if col.column_index < len(names) else str(col.column_index)
        payload.append(
            {
                "column": name,
                "order": "descending" if col.descending else "ascending",
                "nulls_first": col.nulls_first,
            }
        )
    return tuple(payload) if payload else None


def _parquet_write_options(
    *,
    options: ParquetWriteOptions,
    sorting_columns: Sequence[pq.SortingColumn] | None,
) -> ds.FileWriteOptions:
    base_kwargs = {
        "compression": options.compression,
        "use_dictionary": options.use_dictionary,
        "write_statistics": options.write_statistics,
        "data_page_size": options.data_page_size,
        "row_group_size": options.row_group_size,
        "allow_truncated_timestamps": options.allow_truncated_timestamps,
    }
    if sorting_columns:
        try:
            return ds.ParquetFileFormat().make_write_options(
                sorting_columns=sorting_columns,
                **base_kwargs,
            )
        except TypeError:
            pass
    return ds.ParquetFileFormat().make_write_options(**base_kwargs)


def _parquet_writer(
    path: Path,
    *,
    schema: SchemaLike,
    options: ParquetWriteOptions,
    sorting_columns: Sequence[pq.SortingColumn] | None,
) -> pq.ParquetWriter:
    base_kwargs = {
        "compression": options.compression,
        "use_dictionary": options.use_dictionary,
        "write_statistics": options.write_statistics,
        "data_page_size": options.data_page_size,
        "allow_truncated_timestamps": options.allow_truncated_timestamps,
    }
    if sorting_columns:
        try:
            return pq.ParquetWriter(
                str(path),
                schema,
                sorting_columns=sorting_columns,
                **base_kwargs,
            )
        except TypeError:
            pass
    return pq.ParquetWriter(str(path), schema, **base_kwargs)


def _write_table_with_sorting(
    table: TableLike,
    path: Path,
    *,
    options: ParquetWriteOptions,
    sorting_columns: Sequence[pq.SortingColumn] | None,
) -> None:
    base_kwargs = {
        "compression": options.compression,
        "use_dictionary": options.use_dictionary,
        "write_statistics": options.write_statistics,
        "data_page_size": options.data_page_size,
    }
    if sorting_columns:
        try:
            pq.write_table(table, str(path), sorting_columns=sorting_columns, **base_kwargs)
        except TypeError:
            pass
        else:
            return
    pq.write_table(table, str(path), **base_kwargs)


def _row_group_columns(schema: SchemaLike | None) -> tuple[str, ...]:
    if schema is None:
        return ()
    return tuple(name for name, _ in ordering_keys_for_schema(schema))


def _row_group_stats_payload(
    dataset: ds.Dataset,
    *,
    schema: SchemaLike | None,
    metadata_config: ParquetMetadataConfig,
    file_count: int | None,
) -> tuple[Mapping[str, object], ...] | None:
    columns = _row_group_columns(schema)
    if not columns:
        return None
    if (
        file_count is not None
        and metadata_config.max_row_group_stats_files is not None
        and file_count > metadata_config.max_row_group_stats_files
    ):
        return None
    fragments = list_fragments(dataset)
    if metadata_config.max_row_group_stats_files is not None:
        fragments = fragments[: metadata_config.max_row_group_stats_files]
    payload = row_group_stats(
        fragments,
        schema=schema if schema is not None else dataset.schema,
        columns=columns,
        max_row_groups=metadata_config.max_row_group_stats,
    )
    return tuple(payload) if payload else None


def _effective_metadata_config(
    config: ParquetMetadataConfig,
    *,
    file_count: int | None,
) -> ParquetMetadataConfig:
    if file_count is None or config.max_metadata_files is None:
        return config
    if file_count <= config.max_metadata_files:
        return config
    return replace(config, write_metadata=False)


def _write_metadata_sidecars(
    base_path: Path,
    *,
    dataset: ds.Dataset,
    metadata_config: ParquetMetadataConfig,
    file_count: int | None,
) -> Mapping[str, str] | None:
    if not (metadata_config.write_common_metadata or metadata_config.write_metadata):
        return None
    effective = _effective_metadata_config(metadata_config, file_count=file_count)
    if not (effective.write_common_metadata or effective.write_metadata):
        return None
    if effective.write_metadata:
        metadata_spec = parquet_metadata_factory(dataset)
        schema = metadata_spec.schema
        metadata = metadata_spec.file_metadata
    else:
        schema = dataset.schema
        metadata = None
    return write_parquet_metadata_sidecars(
        base_path,
        schema=schema,
        metadata=metadata,
        config=effective,
    )


def _open_parquet_dataset(
    base_path: Path,
    *,
    partitioning: ds.Partitioning | None = None,
) -> ds.Dataset:
    return ds.dataset(str(base_path), format="parquet", partitioning=partitioning)


def _maybe_open_dataset(
    base_path: Path,
    *,
    metadata_config: ParquetMetadataConfig,
    needs_report: bool,
    partitioning: ds.Partitioning | None = None,
) -> ds.Dataset | None:
    if metadata_config.write_common_metadata or metadata_config.write_metadata or needs_report:
        return _open_parquet_dataset(base_path, partitioning=partitioning)
    return None


def _maybe_write_metadata_sidecars(
    base_path: Path,
    *,
    dataset: ds.Dataset | None,
    metadata_config: ParquetMetadataConfig,
    file_count: int,
) -> Mapping[str, str] | None:
    if not (metadata_config.write_common_metadata or metadata_config.write_metadata):
        return None
    resolved_dataset = dataset or _open_parquet_dataset(base_path)
    return _write_metadata_sidecars(
        base_path,
        dataset=resolved_dataset,
        metadata_config=metadata_config,
        file_count=file_count,
    )


@dataclass(frozen=True)
class _DatasetWriteReportContext:
    config: DatasetWriteConfig
    base_path: Path
    written_files: Sequence[str]
    data_schema: SchemaLike | None
    options: ParquetWriteOptions
    metadata_config: ParquetMetadataConfig
    dataset: ds.Dataset | None
    metadata_paths: Mapping[str, str] | None
    schema_fingerprint: str | None
    sorting_columns: Sequence[pq.SortingColumn] | None


def _emit_dataset_write_report(
    context: _DatasetWriteReportContext,
) -> None:
    if context.config.reporter is None:
        return
    ordering_keys = _ordering_keys_payload(context.data_schema)
    file_sort_order = _file_sort_order_payload(context.data_schema)
    sorting_payload = _sorting_columns_payload(context.data_schema, context.sorting_columns)
    row_group_payload = (
        _row_group_stats_payload(
            context.dataset,
            schema=context.data_schema,
            metadata_config=context.metadata_config,
            file_count=len(context.written_files),
        )
        if context.dataset is not None
        else None
    )
    report = DatasetWriteReport(
        dataset_name=None,
        base_dir=str(context.base_path),
        files=tuple(context.written_files),
        metadata_paths=context.metadata_paths,
        schema_fingerprint=context.schema_fingerprint,
        ordering_keys=ordering_keys,
        file_sort_order=file_sort_order,
        preserve_order=bool(context.config.preserve_order),
        writer_strategy="arrow",
        row_group_size=context.options.row_group_size,
        max_rows_per_file=context.options.max_rows_per_file,
        sorting_columns=sorting_payload,
        row_group_stats=row_group_payload,
    )
    context.config.reporter(report)


def write_table_parquet(
    table: DatasetWriteInput,
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

    data = _coerce_plan_product(table)
    sorting_columns = _sorting_columns_for_schema(getattr(data, "schema", None))
    if isinstance(data, RecordBatchReaderLike):
        writer = _parquet_writer(
            target,
            schema=data.schema,
            options=options,
            sorting_columns=sorting_columns,
        )
        with writer:
            for batch in data:
                writer.write_batch(batch)
        return str(target)

    _write_table_with_sorting(
        data,
        target,
        options=options,
        sorting_columns=sorting_columns,
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


def _validate_projection_schema(
    schema: SchemaLike,
    *,
    source_schema: SchemaLike,
) -> None:
    """Ensure the requested schema is a projection of the source schema.

    Parameters
    ----------
    schema
        Requested output schema.
    source_schema
        Source schema for the data being written.

    Raises
    ------
    ValueError
        Raised when the requested schema is not a pure projection.
    """
    requested = set(schema.names)
    available = set(source_schema.names)
    missing = sorted(requested - available)
    mismatched = sorted(
        name
        for name in requested
        if name in available and schema.field(name).type != source_schema.field(name).type
    )
    if not missing and not mismatched:
        return
    msg = "Requested schema must be a projection-only subset of the input schema."
    details: list[str] = []
    if missing:
        details.append(f"missing={missing}")
    if mismatched:
        details.append(f"mismatched_types={mismatched}")
    msg_full = "{} ({})".format(msg, ", ".join(details))
    raise ValueError(msg_full)


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
    data = _coerce_plan_product(data)
    if isinstance(data, RecordBatchReaderLike):
        if schema is not None and encoding_policy is None:
            _validate_projection_schema(schema, source_schema=data.schema)
            return _project_reader(data, schema=schema)
        table = data.read_all()
    else:
        table = data
    if schema is not None:
        _validate_projection_schema(schema, source_schema=table.schema)
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
    encoding policy is provided. When ``config.schema`` is set it must be a projection-only
    subset of the input schema (no new fields or type changes).

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
        _coerce_plan_product(table),
        schema=config.schema,
        encoding_policy=config.encoding_policy,
    )
    written_files: list[str] = []
    file_visitor = _capture_file_visitor(config.file_visitor, paths=written_files)
    data_schema = getattr(data, "schema", None)
    schema_fp = _schema_fingerprint_for_data(data)
    _validate_basename_template(data_schema or config.schema, config.basename_template)
    sorting_columns = _sorting_columns_for_schema(data_schema)

    ds.write_dataset(
        data=data,
        base_dir=str(base_path),
        format="parquet",
        basename_template=config.basename_template,
        max_rows_per_file=options.max_rows_per_file,
        max_rows_per_group=options.row_group_size or options.max_rows_per_file,
        preserve_order=bool(config.preserve_order),
        existing_data_behavior=(
            "overwrite_or_ignore" if not config.overwrite else "delete_matching"
        ),
        file_visitor=file_visitor,
        max_open_files=config.max_open_files,
        file_options=_parquet_write_options(
            options=options,
            sorting_columns=sorting_columns,
        ),
    )
    dataset = _maybe_open_dataset(
        base_path,
        metadata_config=metadata_config,
        needs_report=config.reporter is not None,
    )
    metadata_paths = _maybe_write_metadata_sidecars(
        base_path,
        dataset=dataset,
        metadata_config=metadata_config,
        file_count=len(written_files),
    )
    _emit_dataset_write_report(
        _DatasetWriteReportContext(
            config=config,
            base_path=base_path,
            written_files=written_files,
            data_schema=data_schema,
            options=options,
            metadata_config=metadata_config,
            dataset=dataset,
            metadata_paths=metadata_paths,
            schema_fingerprint=schema_fp,
            sorting_columns=sorting_columns,
        )
    )
    return str(base_path)


@dataclass
class _RowCountTracker:
    count: int = 0


def _counted_reader(
    reader: RecordBatchReaderLike,
) -> tuple[RecordBatchReaderLike, Callable[[], int]]:
    tracker = _RowCountTracker()

    def _batches() -> Iterable[pa.RecordBatch]:
        for batch in reader:
            tracker.count += batch.num_rows
            yield batch

    counted = pa.RecordBatchReader.from_batches(reader.schema, _batches())
    return counted, lambda: tracker.count


def _project_reader(
    reader: RecordBatchReaderLike,
    *,
    schema: SchemaLike,
) -> RecordBatchReaderLike:
    if list(reader.schema.names) == list(schema.names):
        return reader
    _validate_projection_schema(schema, source_schema=reader.schema)
    columns = list(schema.names)

    def _batches() -> Iterable[pa.RecordBatch]:
        for batch in reader:
            yield batch.select(columns)

    return pa.RecordBatchReader.from_batches(schema, _batches())


@dataclass(frozen=True)
class _PartitionedWriteContext:
    config: DatasetWriteConfig
    options: ParquetWriteOptions
    metadata_config: ParquetMetadataConfig
    base_path: Path
    expected_schema: SchemaLike
    written_files: list[str]
    file_visitor: Callable[[object], None]


def _prepare_partitioned_write(
    readers: Iterable[RecordBatchReaderLike],
    base_dir: PathLike,
    *,
    config: DatasetWriteConfig | None,
) -> tuple[_PartitionedWriteContext, list[RecordBatchReaderLike]]:
    config = config or DatasetWriteConfig()
    options = config.opts or ParquetWriteOptions()
    metadata_config = config.metadata or ParquetMetadataConfig()
    base_path = ensure_path(base_dir)
    if config.overwrite:
        _rm_tree(base_path)
    _ensure_dir(base_path)
    if config.encoding_policy is not None:
        msg = "Partitioned streaming does not support encoding policies."
        raise ValueError(msg)
    reader_list = list(readers)
    if not reader_list:
        msg = "Partitioned dataset write requires at least one reader."
        raise ValueError(msg)
    expected_schema = config.schema or reader_list[0].schema
    _validate_basename_template(expected_schema, config.basename_template)
    written_files: list[str] = []
    file_visitor = _capture_file_visitor(config.file_visitor, paths=written_files)
    context = _PartitionedWriteContext(
        config=config,
        options=options,
        metadata_config=metadata_config,
        base_path=base_path,
        expected_schema=expected_schema,
        written_files=written_files,
        file_visitor=file_visitor,
    )
    return context, reader_list


def _write_partitioned_readers(
    readers: Iterable[RecordBatchReaderLike],
    *,
    context: _PartitionedWriteContext,
) -> int:
    total_rows = 0
    sorting_columns = _sorting_columns_for_schema(context.expected_schema)
    for reader in readers:
        if context.config.schema is None and reader.schema != context.expected_schema:
            msg = "Partitioned write requires consistent reader schemas."
            raise ValueError(msg)
        resolved_reader = (
            _project_reader(reader, schema=context.expected_schema)
            if context.config.schema is not None
            else reader
        )
        counted, rows_fn = _counted_reader(resolved_reader)
        ds.write_dataset(
            data=counted,
            base_dir=str(context.base_path),
            format="parquet",
            basename_template=context.config.basename_template,
            max_rows_per_file=context.options.max_rows_per_file,
            max_rows_per_group=context.options.row_group_size or context.options.max_rows_per_file,
            preserve_order=bool(context.config.preserve_order),
            existing_data_behavior="overwrite_or_ignore",
            file_visitor=context.file_visitor,
            max_open_files=context.config.max_open_files,
            file_options=_parquet_write_options(
                options=context.options,
                sorting_columns=sorting_columns,
            ),
        )
        total_rows += rows_fn()
    return total_rows


def write_partitioned_dataset_parquet(
    readers: Iterable[RecordBatchReaderLike],
    base_dir: PathLike,
    *,
    config: DatasetWriteConfig | None = None,
) -> str:
    """Write a Parquet dataset from partitioned readers.

    Returns
    -------
    str
        Dataset directory path.

    Raises
    ------
    ValueError
        Raised when partitioned writes are misconfigured.
    """
    context, reader_list = _prepare_partitioned_write(readers, base_dir, config=config)
    total_rows = _write_partitioned_readers(reader_list, context=context)
    dataset = ds.dataset(str(context.base_path), format="parquet")
    if context.expected_schema is not None and dataset.schema != context.expected_schema:
        msg = "Partitioned write schema mismatch detected."
        raise ValueError(msg)
    row_count = dataset.count_rows()
    if int(row_count) != total_rows:
        msg = f"Partitioned write row-count mismatch (expected={total_rows}, got={row_count})."
        raise ValueError(msg)
    metadata_paths: Mapping[str, str] | None = None
    if context.metadata_config.write_common_metadata or context.metadata_config.write_metadata:
        metadata_paths = _write_metadata_sidecars(
            context.base_path,
            dataset=dataset,
            metadata_config=context.metadata_config,
            file_count=len(context.written_files),
        )
    if context.config.reporter is not None:
        ordering_keys = _ordering_keys_payload(context.expected_schema)
        file_sort_order = _file_sort_order_payload(context.expected_schema)
        sorting_columns = _sorting_columns_for_schema(context.expected_schema)
        sorting_payload = _sorting_columns_payload(context.expected_schema, sorting_columns)
        row_group_payload = _row_group_stats_payload(
            dataset,
            schema=context.expected_schema,
            metadata_config=context.metadata_config,
            file_count=len(context.written_files),
        )
        report = DatasetWriteReport(
            dataset_name=None,
            base_dir=str(context.base_path),
            files=tuple(context.written_files),
            metadata_paths=metadata_paths,
            schema_fingerprint=_schema_fingerprint_for_data(dataset),
            ordering_keys=ordering_keys,
            file_sort_order=file_sort_order,
            preserve_order=bool(context.config.preserve_order),
            writer_strategy="arrow",
            row_group_size=context.options.row_group_size,
            max_rows_per_file=context.options.max_rows_per_file,
            sorting_columns=sorting_payload,
            row_group_stats=row_group_payload,
        )
        context.config.reporter(report)
    return str(context.base_path)


def upsert_dataset_partitions_parquet(
    table: TableLike,
    *,
    base_dir: PathLike,
    partition_cols: Sequence[str],
    delete_partitions: Sequence[Mapping[str, str]] = (),
    config: DatasetWriteConfig | None = None,
) -> str:
    """Upsert a partitioned Parquet dataset by deleting matching partitions.

    Parameters
    ----------
    table:
        Table to write.
    base_dir:
        Root dataset directory.
    partition_cols:
        Columns used for hive-style partitioning.
    delete_partitions:
        Explicit partitions to delete (e.g., removed file_ids).
    config:
        Optional dataset write configuration overrides.

    Returns
    -------
    str
        Dataset directory path.
    """
    config = config or DatasetWriteConfig()
    options = config.opts or ParquetWriteOptions()
    metadata_config = config.metadata or ParquetMetadataConfig()
    base_path = ensure_path(base_dir)
    _ensure_dir(base_path)
    for partition in delete_partitions:
        _delete_partition_dir(base_path, partition)

    data = _apply_schema_and_encoding(
        table,
        schema=config.schema,
        encoding_policy=config.encoding_policy,
    )
    _validate_basename_template(data.schema, config.basename_template)
    written_files: list[str] = []
    file_visitor = _capture_file_visitor(config.file_visitor, paths=written_files)
    partitioning = ds.partitioning(
        pa.schema([pa.field(name, data.schema.field(name).type) for name in partition_cols]),
        flavor="hive",
    )
    sorting_columns = _sorting_columns_for_schema(data.schema)
    ds.write_dataset(
        data=data,
        base_dir=str(base_path),
        format="parquet",
        partitioning=partitioning,
        basename_template=config.basename_template,
        max_rows_per_file=options.max_rows_per_file,
        max_rows_per_group=options.max_rows_per_file,
        existing_data_behavior="delete_matching",
        file_visitor=file_visitor,
        file_options=_parquet_write_options(
            options=options,
            sorting_columns=sorting_columns,
        ),
    )
    dataset = _maybe_open_dataset(
        base_path,
        metadata_config=metadata_config,
        needs_report=config.reporter is not None,
        partitioning=partitioning,
    )
    metadata_paths = _maybe_write_metadata_sidecars(
        base_path,
        dataset=dataset,
        metadata_config=metadata_config,
        file_count=len(written_files),
    )
    _emit_dataset_write_report(
        _DatasetWriteReportContext(
            config=config,
            base_path=base_path,
            written_files=written_files,
            data_schema=data.schema,
            options=options,
            metadata_config=metadata_config,
            dataset=dataset,
            metadata_paths=metadata_paths,
            schema_fingerprint=_schema_fingerprint_for_data(data),
            sorting_columns=sorting_columns,
        )
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

    When per-dataset schemas are provided, each schema must be a projection-only subset
    of the corresponding input schema.

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
        report_hook: Callable[[DatasetWriteReport], None] | None = None
        reporter = config.reporter
        if reporter is not None:

            def _reporter(
                report: DatasetWriteReport,
                *,
                dataset_name: str = name,
                reporter_fn: Callable[[DatasetWriteReport], None] = reporter,
            ) -> None:
                reporter_fn(replace(report, dataset_name=dataset_name))

            report_hook = _reporter
        write_dataset_parquet(
            _coerce_plan_product(table),
            ds_dir,
            config=DatasetWriteConfig(
                opts=options,
                overwrite=config.overwrite,
                basename_template=config.basename_template,
                schema=schema,
                encoding_policy=encoding_policy,
                metadata=config.metadata,
                preserve_order=config.preserve_order,
                file_visitor=(
                    config.file_visitors.get(name) if config.file_visitors is not None else None
                ),
                max_open_files=config.max_open_files,
                reporter=report_hook,
            ),
        )
        out[name] = str(ds_dir)
    return out


__all__ = [
    "DatasetWriteConfig",
    "DatasetWriteInput",
    "DatasetWriteReport",
    "NamedDatasetWriteConfig",
    "ParquetMetadataConfig",
    "ParquetWriteOptions",
    "parquet_supports_sorting_columns",
    "read_table_parquet",
    "upsert_dataset_partitions_parquet",
    "write_dataset_parquet",
    "write_finalize_result_parquet",
    "write_named_datasets_parquet",
    "write_parquet_metadata_sidecars",
    "write_partitioned_dataset_parquet",
    "write_table_parquet",
]
