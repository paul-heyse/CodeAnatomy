"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TypedDict, cast

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import DataFrameWriteOptions, ParquetWriterOptions, col
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr, SortExpr
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.context import DeterminismTier
from arrowdsl.core.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.core.streaming import to_reader
from arrowdsl.io.delta import (
    DeltaWriteOptions,
    DeltaWriteResult,
    StorageOptions,
    apply_delta_write_policies,
    write_dataset_delta,
    write_named_datasets_delta,
    write_table_delta,
)
from arrowdsl.io.delta_config import DeltaSchemaPolicy, DeltaWritePolicy
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    DatasetWriteInput,
    DatasetWriteReport,
    NamedDatasetWriteConfig,
    ParquetMetadataConfig,
    ParquetWriteOptions,
    write_dataset_parquet,
    write_named_datasets_parquet,
    write_parquet_metadata_sidecars,
    write_partitioned_dataset_parquet,
    write_table_parquet,
)
from arrowdsl.plan.metrics import parquet_metadata_factory
from arrowdsl.plan.ordering_policy import ordering_keys_for_schema
from arrowdsl.plan.schema_utils import plan_schema
from arrowdsl.schema.schema import schema_fingerprint
from core_types import PathLike
from datafusion_engine.bridge import (
    DataFusionDmlOptions,
    datafusion_partitioned_readers,
    datafusion_view_sql,
    execute_dml,
    ibis_plan_to_datafusion,
    ibis_to_datafusion,
)
from datafusion_engine.runtime import DataFusionRuntimeProfile
from engine.plan_policy import WriterStrategy
from engine.plan_product import PlanProduct
from ibis_engine.execution import (
    IbisExecutionContext,
    materialize_ibis_plan,
    stream_ibis_plan,
)
from ibis_engine.plan import IbisPlan
from schema_spec.specs import DataFusionWritePolicy
from sqlglot_tools.bridge import IbisCompilerBackend

type IbisWriteInput = DatasetWriteInput | IbisPlan | IbisTable | PlanProduct

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataFusionWriterConfig:
    """Options for DataFusion native writers."""

    write_options: DataFrameWriteOptions | None = None
    parquet_options: ParquetWriterOptions | None = None
    sort_by: Sequence[str] | None = None
    partitioned_streaming: bool = False
    allow_non_deterministic_partitioned_streaming: bool = False
    dml_statement: str | None = None
    dml_options: DataFusionDmlOptions | None = None


@dataclass(frozen=True)
class _DataFusionWriteContext:
    value: IbisPlan | IbisTable
    base_dir: PathLike
    execution: IbisExecutionContext
    dataset_config: DatasetWriteConfig
    write_config: DataFusionWriterConfig | None
    write_policy: DataFusionWritePolicy | None


@dataclass(frozen=True)
class IbisDatasetWriteOptions:
    """Options for writing a single dataset from Ibis inputs."""

    config: DatasetWriteConfig | None = None
    batch_size: int | None = None
    prefer_reader: bool = True
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None
    datafusion_write: DataFusionWriterConfig | None = None
    datafusion_write_policy: DataFusionWritePolicy | None = None
    parquet_reporter: Callable[[DatasetWriteReport], None] | None = None
    delta_reporter: Callable[[DeltaWriteResult], None] | None = None
    delta_options: DeltaWriteOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class IbisNamedDatasetWriteOptions:
    """Options for writing named datasets from Ibis inputs."""

    config: NamedDatasetWriteConfig | None = None
    batch_size: int | None = None
    prefer_reader: bool = True
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None
    datafusion_write_policy: DataFusionWritePolicy | None = None
    parquet_reporter: Callable[[DatasetWriteReport], None] | None = None
    delta_reporter: Callable[[DeltaWriteResult], None] | None = None
    delta_options: DeltaWriteOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None


def ibis_plan_to_reader(
    plan: IbisPlan,
    *,
    batch_size: int | None = None,
    execution: IbisExecutionContext | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        Reader yielding batches from the plan.
    """
    if execution is not None:
        if batch_size is not None and execution.batch_size != batch_size:
            execution = replace(execution, batch_size=batch_size)
        return stream_ibis_plan(plan, execution=execution)
    if batch_size is None:
        return to_reader(plan)
    return plan.to_reader(batch_size=batch_size)


def ibis_table_to_reader(
    table: IbisTable,
    *,
    batch_size: int | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis table expression.

    Returns
    -------
    RecordBatchReaderLike
        Reader yielding batches from the table.
    """
    if batch_size is None:
        return to_reader(table)
    return table.to_pyarrow_batches(chunk_size=batch_size)


def ibis_to_table(
    value: IbisPlan | IbisTable,
    *,
    execution: IbisExecutionContext | None = None,
) -> TableLike:
    """Materialize an Ibis plan or table to an Arrow table.

    Returns
    -------
    TableLike
        Materialized Arrow table.
    """
    if isinstance(value, IbisPlan):
        if execution is not None:
            return materialize_ibis_plan(value, execution=execution)
        return value.to_table()
    return value.to_pyarrow()


def _coerce_write_input(
    value: IbisWriteInput,
    *,
    batch_size: int | None,
    prefer_reader: bool,
    execution: IbisExecutionContext | None,
) -> RecordBatchReaderLike | TableLike:
    if isinstance(value, PlanProduct):
        return value.value()
    if isinstance(value, IbisPlan):
        return (
            ibis_plan_to_reader(value, batch_size=batch_size, execution=execution)
            if prefer_reader
            else ibis_to_table(value, execution=execution)
        )
    if isinstance(value, IbisTable):
        return (
            ibis_table_to_reader(value, batch_size=batch_size)
            if prefer_reader
            else value.to_pyarrow()
        )
    if _is_arrow_table_like(value) or _has_arrow_capsule(value):
        return coerce_table_like(value)
    return value


def _is_arrow_table_like(value: object) -> bool:
    return isinstance(value, (pa.Table, pa.RecordBatchReader))


def _has_arrow_capsule(value: object) -> bool:
    return any(
        hasattr(value, attr)
        for attr in ("__arrow_c_stream__", "__arrow_c_array__", "__dataframe__")
    )


def _resolve_writer_strategy(
    *,
    options: IbisDatasetWriteOptions,
    data: IbisWriteInput,
) -> WriterStrategy:
    if options.writer_strategy is not None:
        return options.writer_strategy
    if isinstance(data, PlanProduct):
        return data.writer_strategy
    return "arrow"


def _resolve_metadata_policy(
    config: DatasetWriteConfig,
    *,
    determinism: DeterminismTier | None,
    reporter: Callable[[DatasetWriteReport], None] | None,
) -> DatasetWriteConfig:
    updated = config
    if reporter is not None and updated.reporter is None:
        updated = replace(updated, reporter=reporter)
    if determinism is None or updated.metadata is not None:
        return updated
    if determinism == DeterminismTier.BEST_EFFORT:
        return replace(
            updated,
            metadata=ParquetMetadataConfig(write_common_metadata=False, write_metadata=False),
        )
    return replace(updated, metadata=ParquetMetadataConfig())


def _resolve_named_metadata_policy(
    config: NamedDatasetWriteConfig,
    *,
    determinism: DeterminismTier | None,
    reporter: Callable[[DatasetWriteReport], None] | None,
) -> NamedDatasetWriteConfig:
    updated = config
    if reporter is not None and updated.reporter is None:
        updated = replace(updated, reporter=reporter)
    if determinism is None or updated.metadata is not None:
        return updated
    if determinism == DeterminismTier.BEST_EFFORT:
        return replace(
            updated,
            metadata=ParquetMetadataConfig(write_common_metadata=False, write_metadata=False),
        )
    return replace(updated, metadata=ParquetMetadataConfig())


def _write_datafusion_dataset(context: _DataFusionWriteContext) -> str:
    runtime_profile = context.execution.ctx.runtime.datafusion
    if runtime_profile is None or context.execution.ibis_backend is None:
        msg = "DataFusion writer requires a runtime profile and Ibis backend."
        raise ValueError(msg)
    effective_write_policy = context.write_policy or runtime_profile.write_policy
    df_ctx = runtime_profile.session_context()
    backend = cast("IbisCompilerBackend", context.execution.ibis_backend)
    df = (
        ibis_plan_to_datafusion(context.value, backend=backend, ctx=df_ctx)
        if isinstance(context.value, IbisPlan)
        else ibis_to_datafusion(context.value, backend=backend, ctx=df_ctx)
    )
    if context.write_config is not None and context.write_config.dml_statement is not None:
        dml_statement = _format_dml_statement(
            context.write_config.dml_statement,
            base_dir=context.base_dir,
        )
        _register_temp_view(df, name="write_source", runtime_profile=runtime_profile)
        dml_options = context.write_config.dml_options or DataFusionDmlOptions()
        record_hook = _dml_record_hook(runtime_profile)
        if record_hook is not None and dml_options.record_hook is None:
            dml_options = replace(dml_options, record_hook=record_hook)
        execute_dml(df_ctx, dml_statement, options=dml_options)
        return str(context.base_dir)
    if context.write_config is not None and context.write_config.partitioned_streaming:
        if (
            context.execution.ctx.determinism != DeterminismTier.BEST_EFFORT
            and not context.write_config.allow_non_deterministic_partitioned_streaming
        ):
            msg = "Partitioned streaming is restricted to Tier 0 determinism by default."
            raise ValueError(msg)
        readers = datafusion_partitioned_readers(df)
        if not readers:
            msg = "Partitioned streaming requested but DataFusion does not support it."
            raise ValueError(msg)
        return write_partitioned_dataset_parquet(
            readers,
            context.base_dir,
            config=context.dataset_config,
        )
    write_with_options = getattr(df, "write_parquet_with_options", None)
    if callable(write_with_options):
        write_options = _build_datafusion_write_options(
            context.value,
            execution=context.execution,
            write_config=context.write_config,
            write_policy=effective_write_policy,
        )
        parquet_options = _build_parquet_write_options(
            dataset_config=context.dataset_config,
            write_config=context.write_config,
            write_policy=effective_write_policy,
        )
        if write_options is not None or parquet_options is not None:
            write_with_options(str(context.base_dir), parquet_options, write_options)
            return _finalize_datafusion_write(
                context.value,
                context.base_dir,
                execution=context.execution,
                dataset_config=context.dataset_config,
                write_policy=effective_write_policy,
            )
    write_parquet = getattr(df, "write_parquet", None)
    if callable(write_parquet):
        write_parquet(str(context.base_dir))
        return _finalize_datafusion_write(
            context.value,
            context.base_dir,
            execution=context.execution,
            dataset_config=context.dataset_config,
            write_policy=effective_write_policy,
        )
    msg = "DataFusion writer is unavailable for the configured backend."
    raise ValueError(msg)


def _finalize_datafusion_write(
    value: IbisPlan | IbisTable,
    base_dir: PathLike,
    *,
    execution: IbisExecutionContext,
    dataset_config: DatasetWriteConfig,
    write_policy: DataFusionWritePolicy | None,
) -> str:
    dataset, files = _datafusion_dataset_snapshot(base_dir)
    metadata_paths = _datafusion_metadata_sidecars(
        base_dir,
        dataset=dataset,
        dataset_config=dataset_config,
        file_count=len(files),
    )
    if dataset_config.reporter is not None:
        schema = _schema_for_write(value, execution=execution)
        report = DatasetWriteReport(
            dataset_name=None,
            base_dir=str(base_dir),
            files=files,
            metadata_paths=metadata_paths,
            schema_fingerprint=schema_fingerprint(dataset.schema),
            ordering_keys=_ordering_keys_payload(schema),
            file_sort_order=_file_sort_order_payload(schema),
            preserve_order=bool(dataset_config.preserve_order),
            writer_strategy="datafusion",
            row_group_size=(dataset_config.opts or ParquetWriteOptions()).row_group_size,
            max_rows_per_file=(dataset_config.opts or ParquetWriteOptions()).max_rows_per_file,
            datafusion_write_policy=_datafusion_write_policy_payload(write_policy),
        )
        dataset_config.reporter(report)
    return str(base_dir)


def _datafusion_dataset_snapshot(base_dir: PathLike) -> tuple[ds.Dataset, tuple[str, ...]]:
    dataset = ds.dataset(str(base_dir), format="parquet")
    files: list[str] = []
    for fragment in dataset.get_fragments():
        path = getattr(fragment, "path", None)
        if path is None:
            continue
        files.append(str(path))
    return dataset, tuple(sorted(set(files)))


def _datafusion_metadata_sidecars(
    base_dir: PathLike,
    *,
    dataset: ds.Dataset,
    dataset_config: DatasetWriteConfig,
    file_count: int | None,
) -> Mapping[str, str] | None:
    metadata_config = dataset_config.metadata or ParquetMetadataConfig()
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
        base_dir,
        schema=schema,
        metadata=metadata,
        config=effective,
    )


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


def _register_temp_view(
    df: object,
    *,
    name: str,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> None:
    create_view = getattr(df, "create_or_replace_temp_view", None)
    if callable(create_view):
        create_view(name)
        if runtime_profile is not None:
            runtime_profile.record_view_definition(
                name=name,
                sql=datafusion_view_sql(cast("DataFrame", df)),
            )
        return
    create_view = getattr(df, "create_temp_view", None)
    if callable(create_view):
        create_view(name)
        if runtime_profile is not None:
            runtime_profile.record_view_definition(
                name=name,
                sql=datafusion_view_sql(cast("DataFrame", df)),
            )
        return
    msg = "DataFusion DataFrame does not support temp view registration."
    raise ValueError(msg)


def _format_dml_statement(statement: str, *, base_dir: PathLike) -> str:
    if "{table}" in statement or "{path}" in statement:
        return statement.format(table="write_source", path=str(base_dir))
    return statement


def _dml_record_hook(
    runtime_profile: DataFusionRuntimeProfile,
) -> Callable[[Mapping[str, object]], None] | None:
    diagnostics = runtime_profile.diagnostics_sink
    if diagnostics is None:
        return None
    record_artifact = diagnostics.record_artifact

    def _record(payload: Mapping[str, object]) -> None:
        record_artifact("datafusion_dml_statements_v1", payload)

    return _record


def _build_datafusion_write_options(
    value: IbisPlan | IbisTable,
    *,
    execution: IbisExecutionContext,
    write_config: DataFusionWriterConfig | None,
    write_policy: DataFusionWritePolicy | None,
) -> DataFrameWriteOptions | None:
    if write_config is not None and write_config.write_options is not None:
        return write_config.write_options
    sort_by = None
    partition_by: Sequence[str] | None = None
    single_file_output: bool | None = None
    if write_config is not None and write_config.sort_by is not None:
        sort_by = list(write_config.sort_by)
    elif write_policy is not None and write_policy.sort_by:
        sort_by = list(write_policy.sort_by)
    elif execution.ctx.determinism == DeterminismTier.CANONICAL:
        schema = _schema_for_write(value, execution=execution)
        sort_by = [col for col, _ in ordering_keys_for_schema(schema)]
    if write_policy is not None:
        partition_by = list(write_policy.partition_by) or None
        single_file_output = write_policy.single_file_output
    return _create_write_options_from_policy(
        sort_by=sort_by,
        partition_by=partition_by,
        single_file_output=single_file_output,
    )


def _create_write_options(*, sort_by: Sequence[str]) -> DataFrameWriteOptions | None:
    return _create_write_options_from_policy(
        sort_by=list(sort_by),
        partition_by=None,
        single_file_output=None,
    )


def _create_write_options_from_policy(
    *,
    sort_by: Sequence[str] | None,
    partition_by: Sequence[str] | None,
    single_file_output: bool | None,
) -> DataFrameWriteOptions | None:
    try:
        kwargs: _DataFrameWriteOptionsKwargs = {}
        if sort_by:
            kwargs["sort_by"] = [col(name) for name in sort_by]
        if partition_by:
            kwargs["partition_by"] = list(partition_by)
        if single_file_output is not None:
            kwargs["single_file_output"] = single_file_output
        if not kwargs:
            return None
        return DataFrameWriteOptions(**kwargs)
    except TypeError:
        logger.warning("DataFrameWriteOptions constructor mismatch; using DataFusion defaults.")
        return None


def _build_parquet_write_options(
    *,
    dataset_config: DatasetWriteConfig,
    write_config: DataFusionWriterConfig | None,
    write_policy: DataFusionWritePolicy | None,
) -> ParquetWriterOptions | None:
    if write_config is not None and write_config.parquet_options is not None:
        return write_config.parquet_options
    if write_policy is not None:
        return _create_parquet_options_from_policy(write_policy)
    opts = dataset_config.opts
    if opts is None:
        return None
    return _create_parquet_options(opts)


class _ParquetWriterOptionsKwargs(TypedDict, total=False):
    compression: str | None
    max_row_group_size: int
    data_pagesize_limit: int
    dictionary_enabled: bool | None
    statistics_enabled: str | None


class _DataFrameWriteOptionsKwargs(TypedDict, total=False):
    partition_by: Sequence[str] | str
    single_file_output: bool
    sort_by: Expr | Sequence[Expr] | Sequence[SortExpr] | SortExpr


def _create_parquet_options(options: ParquetWriteOptions) -> ParquetWriterOptions | None:
    try:
        kwargs: _ParquetWriterOptionsKwargs = {"compression": options.compression}
        if options.row_group_size is not None:
            kwargs["max_row_group_size"] = options.row_group_size
        if options.data_page_size is not None:
            kwargs["data_pagesize_limit"] = options.data_page_size
        kwargs["dictionary_enabled"] = options.use_dictionary
        kwargs["statistics_enabled"] = "page" if options.write_statistics else "none"
        return ParquetWriterOptions(**kwargs)
    except TypeError:
        logger.warning("ParquetWriterOptions constructor mismatch; using DataFusion defaults.")
        return None


def _create_parquet_options_from_policy(
    policy: DataFusionWritePolicy,
) -> ParquetWriterOptions | None:
    try:
        kwargs: _ParquetWriterOptionsKwargs = {}
        if policy.compression is not None:
            kwargs["compression"] = policy.compression
        if policy.max_row_group_size is not None:
            kwargs["max_row_group_size"] = policy.max_row_group_size
        if policy.dictionary_enabled is not None:
            kwargs["dictionary_enabled"] = policy.dictionary_enabled
        if policy.statistics_enabled is not None:
            kwargs["statistics_enabled"] = policy.statistics_enabled
        if not kwargs:
            return None
        return ParquetWriterOptions(**kwargs)
    except TypeError:
        logger.warning("ParquetWriterOptions constructor mismatch; using DataFusion defaults.")
        return None


def _schema_for_write(
    value: IbisPlan | IbisTable,
    *,
    execution: IbisExecutionContext,
) -> SchemaLike:
    if isinstance(value, IbisPlan):
        return plan_schema(value, ctx=execution.ctx)
    return value.schema().to_pyarrow()


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


def _datafusion_write_policy_payload(
    policy: DataFusionWritePolicy | None,
) -> Mapping[str, object] | None:
    if policy is None:
        return None
    return {
        "partition_by": list(policy.partition_by),
        "single_file_output": policy.single_file_output,
        "sort_by": list(policy.sort_by),
        "compression": policy.compression,
        "statistics_enabled": policy.statistics_enabled,
        "max_row_group_size": policy.max_row_group_size,
        "dictionary_enabled": policy.dictionary_enabled,
    }


def write_ibis_table_parquet(
    table: IbisPlan | IbisTable,
    path: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
    execution: IbisExecutionContext | None = None,
) -> str:
    """Write an Ibis plan/table as a single Parquet file.

    Returns
    -------
    str
        Written file path.
    """
    return write_table_parquet(
        ibis_to_table(table, execution=execution),
        path,
        opts=opts,
        overwrite=overwrite,
    )


def write_ibis_dataset_parquet(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisDatasetWriteOptions | None = None,
) -> str:
    """Write an Ibis plan/table or Arrow input as a Parquet dataset.

    Returns
    -------
    str
        Output dataset directory.

    Raises
    ------
    ValueError
        Raised when a DataFusion writer is requested without required inputs.
    """
    options = options or IbisDatasetWriteOptions()
    config = options.config or DatasetWriteConfig()
    determinism = None
    if isinstance(data, PlanProduct):
        determinism = data.determinism_tier
    elif options.execution is not None:
        determinism = options.execution.ctx.determinism
    if determinism is not None and config.preserve_order is None:
        preserve = determinism != DeterminismTier.BEST_EFFORT
        config = replace(config, preserve_order=preserve)
    config = _resolve_metadata_policy(
        config,
        determinism=determinism,
        reporter=options.parquet_reporter,
    )
    writer_strategy = _resolve_writer_strategy(options=options, data=data)
    if writer_strategy == "datafusion":
        if options.execution is None or not isinstance(data, (IbisPlan, IbisTable)):
            msg = "DataFusion writer requires an Ibis plan/table and execution context."
            raise ValueError(msg)
        return _write_datafusion_dataset(
            _DataFusionWriteContext(
                value=data,
                base_dir=base_dir,
                execution=options.execution,
                dataset_config=config,
                write_config=options.datafusion_write,
                write_policy=options.datafusion_write_policy,
            )
        )
    value = _coerce_write_input(
        data,
        batch_size=options.batch_size,
        prefer_reader=options.prefer_reader,
        execution=options.execution,
    )
    return write_dataset_parquet(value, base_dir, config=config)


def write_ibis_table_delta(
    table: IbisPlan | IbisTable,
    path: PathLike,
    *,
    options: DeltaWriteOptions | None = None,
    storage_options: StorageOptions | None = None,
    execution: IbisExecutionContext | None = None,
) -> DeltaWriteResult:
    """Write an Ibis plan/table as a Delta table.

    Returns
    -------
    DeltaWriteResult
        Metadata about the Delta write operation.
    """
    resolved = options or DeltaWriteOptions()
    return write_table_delta(
        ibis_to_table(table, execution=execution),
        str(path),
        options=resolved,
        storage_options=storage_options,
    )


def write_ibis_dataset_delta(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisDatasetWriteOptions | None = None,
) -> DeltaWriteResult:
    """Write an Ibis plan/table or Arrow input as a Delta table.

    Returns
    -------
    DeltaWriteResult
        Metadata about the Delta write operation.

    Raises
    ------
    ValueError
        Raised when a DataFusion writer is requested without a compatible execution context.
    ValueError
        Raised when the DataFusion backend cannot produce an Arrow table.
    """
    options = options or IbisDatasetWriteOptions()
    delta_options = options.delta_options or DeltaWriteOptions()
    delta_options = apply_delta_write_policies(
        delta_options,
        write_policy=options.delta_write_policy,
        schema_policy=options.delta_schema_policy,
    )
    writer_strategy = _resolve_writer_strategy(options=options, data=data)
    if writer_strategy == "datafusion":
        if options.execution is None or not isinstance(data, (IbisPlan, IbisTable)):
            msg = "DataFusion writer requires an Ibis plan/table and execution context."
            raise ValueError(msg)
        runtime_profile = options.execution.ctx.runtime.datafusion
        if runtime_profile is None or options.execution.ibis_backend is None:
            msg = "DataFusion writer requires a runtime profile and Ibis backend."
            raise ValueError(msg)
        df_ctx = runtime_profile.session_context()
        backend = cast("IbisCompilerBackend", options.execution.ibis_backend)
        df = (
            ibis_plan_to_datafusion(data, backend=backend, ctx=df_ctx)
            if isinstance(data, IbisPlan)
            else ibis_to_datafusion(data, backend=backend, ctx=df_ctx)
        )
        to_arrow = getattr(df, "to_arrow_table", None)
        if not callable(to_arrow):
            msg = "DataFusion DataFrame missing to_arrow_table."
            raise ValueError(msg)
        table = cast("TableLike", to_arrow())
        result = write_table_delta(
            table,
            str(base_dir),
            options=delta_options,
            storage_options=options.storage_options,
        )
        if options.delta_reporter is not None:
            options.delta_reporter(result)
        return result
    value = _coerce_write_input(
        data,
        batch_size=options.batch_size,
        prefer_reader=options.prefer_reader,
        execution=options.execution,
    )
    result = write_dataset_delta(
        value,
        str(base_dir),
        options=delta_options,
        storage_options=options.storage_options,
    )
    if options.delta_reporter is not None:
        options.delta_reporter(result)
    return result


def write_ibis_named_datasets_parquet(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisNamedDatasetWriteOptions | None = None,
) -> dict[str, str]:
    """Write a mapping of Ibis/Arrow datasets to Parquet directories.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to output directories.

    Raises
    ------
    ValueError
        Raised when a non-Arrow writer strategy is requested.
    """
    options = options or IbisNamedDatasetWriteOptions()
    config = options.config or NamedDatasetWriteConfig()
    if options.execution is not None and config.preserve_order is None:
        preserve = options.execution.ctx.determinism != DeterminismTier.BEST_EFFORT
        config = replace(config, preserve_order=preserve)
    config = _resolve_named_metadata_policy(
        config,
        determinism=options.execution.ctx.determinism if options.execution is not None else None,
        reporter=options.parquet_reporter,
    )
    writer_strategy = options.writer_strategy or "arrow"
    if writer_strategy != "arrow":
        msg = "Named dataset writes only support the Arrow writer strategy."
        raise ValueError(msg)
    for name, value in datasets.items():
        if isinstance(value, PlanProduct) and value.writer_strategy != "arrow":
            msg = (
                "Named dataset writes do not support non-Arrow PlanProduct writer strategy "
                f"for dataset {name!r}."
            )
            raise ValueError(msg)
    converted = {
        name: _coerce_write_input(
            value,
            batch_size=options.batch_size,
            prefer_reader=options.prefer_reader,
            execution=options.execution,
        )
        for name, value in datasets.items()
    }
    return write_named_datasets_parquet(converted, base_dir, config=config)


def write_ibis_named_datasets_delta(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisNamedDatasetWriteOptions | None = None,
) -> dict[str, DeltaWriteResult]:
    """Write a mapping of Ibis/Arrow datasets to Delta tables.

    Returns
    -------
    dict[str, DeltaWriteResult]
        Mapping of dataset names to Delta write metadata.

    Raises
    ------
    ValueError
        Raised when a non-Arrow writer strategy is requested.
    """
    options = options or IbisNamedDatasetWriteOptions()
    writer_strategy = options.writer_strategy or "arrow"
    if writer_strategy != "arrow":
        msg = "Named dataset writes only support the Arrow writer strategy."
        raise ValueError(msg)
    for name, value in datasets.items():
        if isinstance(value, PlanProduct) and value.writer_strategy != "arrow":
            msg = (
                "Named dataset writes do not support non-Arrow PlanProduct writer strategy "
                f"for dataset {name!r}."
            )
            raise ValueError(msg)
    converted = {
        name: _coerce_write_input(
            value,
            batch_size=options.batch_size,
            prefer_reader=options.prefer_reader,
            execution=options.execution,
        )
        for name, value in datasets.items()
    }
    delta_options = options.delta_options or DeltaWriteOptions()
    delta_options = apply_delta_write_policies(
        delta_options,
        write_policy=options.delta_write_policy,
        schema_policy=options.delta_schema_policy,
    )
    results = write_named_datasets_delta(
        converted,
        str(base_dir),
        options=delta_options,
        storage_options=options.storage_options,
    )
    if options.delta_reporter is not None:
        for result in results.values():
            options.delta_reporter(result)
    return results


__all__ = [
    "DataFusionWriterConfig",
    "DeltaWriteOptions",
    "DeltaWriteResult",
    "IbisDatasetWriteOptions",
    "IbisNamedDatasetWriteOptions",
    "IbisWriteInput",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "write_ibis_dataset_delta",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_delta",
    "write_ibis_named_datasets_parquet",
    "write_ibis_table_delta",
    "write_ibis_table_parquet",
]
