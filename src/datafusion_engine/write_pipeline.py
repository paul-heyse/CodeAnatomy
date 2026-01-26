"""Unified write pipeline for all DataFusion output paths.

This module provides a single writing surface with explicit format policy,
partitioning, and schema constraints while using AST-first compilation and
DataFusion-native writers (streaming + DataFrame writes).
"""

from __future__ import annotations

import shutil
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from datafusion import (
    DataFrameWriteOptions,
    InsertOp,
    ParquetColumnOptions,
    ParquetWriterOptions,
    SQLOptions,
)

from schema_spec.policies import DataFusionWritePolicy, ParquetColumnPolicy
from sqlglot_tools.compat import exp

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.streaming_executor import StreamingExecutionResult
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.table_provider_metadata import table_provider_metadata


class WriteFormat(Enum):
    """Supported output formats."""

    PARQUET = auto()
    CSV = auto()
    JSON = auto()
    ARROW = auto()


class WriteMode(Enum):
    """Write behavior for existing data."""

    ERROR = auto()
    OVERWRITE = auto()
    APPEND = auto()


class WriteMethod(Enum):
    """Write execution method."""

    COPY = auto()
    STREAMING = auto()
    INSERT = auto()


@dataclass(frozen=True)
class ParquetWritePolicy:
    """Parquet-specific write options.

    Encapsulates all Parquet-specific configuration including compression,
    row group sizing, statistics, bloom filters, and per-column overrides.

    Parameters
    ----------
    compression
        Compression codec (zstd, snappy, gzip, lz4, brotli, none).
    compression_level
        Codec-specific compression level, if applicable.
    row_group_size
        Target number of rows per row group.
    data_page_size
        Target size in bytes for data pages.
    dictionary_enabled
        Enable dictionary encoding.
    statistics_enabled
        Statistics level: "none", "chunk", or "page".
    bloom_filter_enabled
        Enable bloom filter generation.
    bloom_filter_fpp
        Bloom filter false positive probability.
    column_overrides
        Per-column overrides mapping column names to option dictionaries.

    Examples
    --------
    >>> policy = ParquetWritePolicy(
    ...     compression="zstd",
    ...     compression_level=9,
    ...     column_overrides={"id": {"compression": "none"}},
    ... )
    >>> options = policy.to_copy_options()
    >>> options["compression"]
    'zstd(9)'
    """

    compression: str = "zstd"
    compression_level: int | None = None
    row_group_size: int = 1_000_000
    data_page_size: int = 1_048_576
    dictionary_enabled: bool = True
    statistics_enabled: str = "page"  # none, chunk, page
    bloom_filter_enabled: bool = False
    bloom_filter_fpp: float = 0.05

    # Per-column overrides: column_name -> {option: value}
    column_overrides: dict[str, dict[str, str]] = field(default_factory=dict)

    def to_copy_options(self) -> dict[str, str]:
        """Convert to COPY statement options.

        Translates this policy into a dictionary of options suitable for
        passing to DataFusion COPY TO statements.

        Returns
        -------
        dict[str, str]
            Option dictionary mapping option names to string values.

        Notes
        -----
        Per-column overrides are encoded with the pattern "option::column_name".
        """
        options = {
            "compression": self.compression,
            "max_row_group_size": str(self.row_group_size),
            "data_page_size": str(self.data_page_size),
            "dictionary_enabled": str(self.dictionary_enabled).lower(),
            "statistics_enabled": self.statistics_enabled,
        }

        if self.compression_level is not None:
            options["compression"] = f"{self.compression}({self.compression_level})"

        if self.bloom_filter_enabled:
            options["bloom_filter_enabled"] = "true"
            options["bloom_filter_fpp"] = str(self.bloom_filter_fpp)

        # Add per-column overrides
        for col, overrides in self.column_overrides.items():
            for opt, val in overrides.items():
                options[f"{opt}::{col}"] = val

        return options

    def to_dataset_options(self) -> dict[str, object]:
        """Convert to PyArrow dataset write options.

        Returns
        -------
        dict[str, object]
            PyArrow dataset write options compatible with ParquetFileFormat.
        """
        options: dict[str, object] = {
            "compression": self.compression,
            "compression_level": self.compression_level,
            "data_page_size": self.data_page_size,
            "use_dictionary": self.dictionary_enabled,
        }
        stats_value = _statistics_flag(self.statistics_enabled)
        if stats_value is not None:
            options["write_statistics"] = stats_value
        return {key: value for key, value in options.items() if value is not None}


@dataclass(frozen=True)
class WriteRequest:
    """Unified write request specification.

    Encapsulates all information needed to write a dataset,
    regardless of the underlying mechanism (COPY, INSERT, Arrow writer).

    Parameters
    ----------
    source
        SQL query string or SQLGlot expression defining the source data.
    destination
        Path or table name for output.
    format
        Output format (PARQUET, CSV, JSON, ARROW).
    mode
        Write mode for handling existing data.
    partition_by
        Column names for Hive-style partitioning.
    parquet_policy
        Parquet-specific write options, if format is PARQUET.
    format_options
        Format-specific COPY/streaming options for the underlying writer.
    single_file_output
        Hint to prefer single-file output when supported.

    Examples
    --------
    >>> request = WriteRequest(
    ...     source="SELECT * FROM events",
    ...     destination="/data/events.parquet",
    ...     format=WriteFormat.PARQUET,
    ...     mode=WriteMode.OVERWRITE,
    ...     partition_by=("year", "month"),
    ...     parquet_policy=ParquetWritePolicy(compression="zstd"),
    ... )
    """

    source: str | exp.Expression  # SQL query or AST
    destination: str  # Path or table name
    format: WriteFormat = WriteFormat.PARQUET
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
    parquet_policy: ParquetWritePolicy | None = None
    format_options: dict[str, object] | None = None
    single_file_output: bool | None = None
    table_name: str | None = None
    constraints: tuple[str, ...] = ()


@dataclass(frozen=True)
class WriteViewRequest:
    """Write request specification for registered views.

    Parameters
    ----------
    view_name
        Registered view name to write.
    destination
        Output destination path or table name.
    format
        Output format for the write.
    mode
        Write mode for existing data.
    partition_by
        Partition columns for Hive-style partitioning.
    parquet_policy
        Parquet-specific write policy overrides.
    format_options
        Format-specific write options.
    single_file_output
        Hint to prefer single-file output when supported.
    table_name
        Optional target table name for INSERT-based writes.
    constraints
        Optional SQL constraints for INSERT-based writes.
    """

    view_name: str
    destination: str
    format: WriteFormat = WriteFormat.PARQUET
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
    parquet_policy: ParquetWritePolicy | None = None
    format_options: dict[str, object] | None = None
    single_file_output: bool | None = None
    table_name: str | None = None
    constraints: tuple[str, ...] = ()


@dataclass(frozen=True)
class WriteResult:
    """Result of a write operation."""

    request: WriteRequest
    method: WriteMethod
    sql: str | None
    duration_ms: float | None = None


class WritePipeline:
    """Unified write pipeline for all output paths.

    Provides consistent write semantics across DataFusion-native writers
    (streaming dataset writes and DataFrame writer APIs).

    Parameters
    ----------
    ctx
        DataFusion session context.
    profile
        SQL policy profile for parsing external SQL sources.

    Examples
    --------
    >>> from datafusion import SessionContext
    >>> from datafusion_engine.sql_policy_engine import SQLPolicyProfile
    >>> ctx = SessionContext()
    >>> profile = SQLPolicyProfile()
    >>> pipeline = WritePipeline(ctx, profile)
    >>> request = WriteRequest(
    ...     source="SELECT * FROM events",
    ...     destination="/data/events.parquet",
    ...     format=WriteFormat.PARQUET,
    ... )
    >>> pipeline.write(request)
    """

    def __init__(
        self,
        ctx: SessionContext,
        profile: SQLPolicyProfile,
        *,
        sql_options: SQLOptions | None = None,
        recorder: DiagnosticsRecorder | None = None,
    ) -> None:
        """Initialize write pipeline.

        Parameters
        ----------
        ctx
            DataFusion session context.
        profile
            SQL policy profile for SQL generation.
        sql_options
            Optional SQL execution options for COPY statements.
        recorder
            Optional diagnostics recorder for write operations.
        """
        self.ctx = ctx
        self.profile = profile
        self.sql_options = sql_options
        self.recorder = recorder

    def _resolved_sql_options(self) -> SQLOptions:
        if self.sql_options is not None:
            return self.sql_options
        return SQLOptions()

    @staticmethod
    def _df_has_rows(df: DataFrame) -> bool:
        batches = df.collect()
        return any(batch.num_rows > 0 for batch in batches)

    def _source_expr(self, request: WriteRequest) -> exp.Expression:
        if isinstance(request.source, exp.Expression):
            return request.source
        from sqlglot_tools.optimizer import StrictParseOptions, parse_sql_strict

        return parse_sql_strict(
            request.source,
            dialect=self.profile.read_dialect,
            options=StrictParseOptions(
                error_level=self.profile.error_level,
                unsupported_level=self.profile.unsupported_level,
            ),
        )

    def _execute_expr(self, expr: exp.Expression) -> DataFrame:
        from datafusion_engine.compile_options import DataFusionCompileOptions
        from datafusion_engine.execution_facade import DataFusionExecutionFacade

        options = DataFusionCompileOptions(
            sql_options=self._resolved_sql_options(),
            sql_policy_profile=self.profile,
            cache=False,
        )
        facade = DataFusionExecutionFacade(ctx=self.ctx, runtime_profile=None)
        plan = facade.compile(expr, options=options)
        result = facade.execute(plan)
        if result.dataframe is None:
            msg = "AST execution did not return a DataFusion DataFrame."
            raise ValueError(msg)
        return result.dataframe

    def _validate_constraints(
        self,
        *,
        source_expr: exp.Expression,
        constraints: tuple[str, ...],
    ) -> None:
        if not constraints:
            return
        from sqlglot_tools.optimizer import (
            StrictParseOptions,
            build_select,
            parse_sql_strict,
        )

        for constraint in constraints:
            if not constraint.strip():
                continue
            constraint_expr = parse_sql_strict(
                constraint,
                dialect=self.profile.read_dialect,
                options=StrictParseOptions(
                    error_level=self.profile.error_level,
                    unsupported_level=self.profile.unsupported_level,
                ),
            )
            subquery = exp.Subquery(
                this=source_expr.copy(),
                alias=exp.TableAlias(this=exp.to_identifier("input")),
            )
            query_expr = build_select(
                [exp.Literal.number(1)],
                from_=subquery,
                where=exp.not_(constraint_expr),
                limit=1,
            )
            df = self._execute_expr(query_expr)
            if self._df_has_rows(df):
                msg = f"Delta constraint violated: {constraint}"
                raise ValueError(msg)

    def write_via_streaming(
        self,
        request: WriteRequest,
    ) -> WriteResult:
        """Write using DataFusion-native writers.

        Uses Arrow streaming execution for partitioned parquet datasets and
        DataFusion DataFrame writers for non-partitioned file outputs.

        Parameters
        ----------
        request
            Write request specification.

        Raises
        ------
        NotImplementedError
            If format is not PARQUET or streaming is not supported.

        Notes
        -----
        This method is preferred for large datasets or when partitioning
        is required, as it allows streaming writes without full
        materialization.

        Returns
        -------
        WriteResult
            Write result metadata for the streaming operation.
        """
        from datafusion_engine.streaming_executor import StreamingExecutionResult

        start = time.perf_counter()
        source_expr = self._source_expr(request)
        if request.constraints:
            self._validate_constraints(source_expr=source_expr, constraints=request.constraints)
        df = self._execute_expr(source_expr)
        result = StreamingExecutionResult(df=df)

        table_target = self._table_target(request)
        if table_target is not None:
            self._write_table(df, request=request, table_name=table_target)
            duration_ms = (time.perf_counter() - start) * 1000.0
            write_result = WriteResult(
                request=request,
                method=WriteMethod.INSERT,
                sql=None,
                duration_ms=duration_ms,
            )
            self._record_write_artifact(write_result)
            return write_result

        # Write based on format
        if request.format == WriteFormat.PARQUET:
            self._write_parquet(result, request=request)
        elif request.format == WriteFormat.CSV:
            self._write_csv(df, request=request)
        elif request.format == WriteFormat.JSON:
            self._write_json(df, request=request)
        elif request.format == WriteFormat.ARROW:
            self._write_arrow(result, request=request)
        else:
            msg = f"Streaming write for {request.format}"
            raise NotImplementedError(msg)

        duration_ms = (time.perf_counter() - start) * 1000.0
        write_result = WriteResult(
            request=request,
            method=WriteMethod.STREAMING,
            sql=None,
            duration_ms=duration_ms,
        )
        self._record_write_artifact(write_result)
        return write_result

    def write(
        self,
        request: WriteRequest,
        *,
        prefer_streaming: bool = True,
    ) -> WriteResult:
        """Write using best available method.

        Chooses between COPY-based and streaming write paths based on
        format, partitioning requirements, and preference hint.

        Parameters
        ----------
        request
            Write request specification.
        prefer_streaming
            If True, prefer streaming write for PARQUET format.

        Returns
        -------
        WriteResult
            Write result metadata for the executed write.

        Notes
        -----
        The unified writer always compiles from AST and executes a
        DataFusion DataFrame. Partitioned parquet uses streaming
        dataset writes; other formats use DataFusion-native writers.
        """
        _ = prefer_streaming
        return self.write_via_streaming(request)

    def write_view(
        self,
        request: WriteViewRequest,
        *,
        prefer_streaming: bool = True,
    ) -> WriteResult:
        """Write a registered view using the unified pipeline.

        Parameters
        ----------
        request
            Write request specifying the registered view.
        prefer_streaming
            Prefer streaming writes when possible.

        Returns
        -------
        WriteResult
            Write result metadata.
        """
        write_request = WriteRequest(
            source=exp.select("*").from_(request.view_name),
            destination=request.destination,
            format=request.format,
            mode=request.mode,
            partition_by=request.partition_by,
            parquet_policy=request.parquet_policy,
            format_options=request.format_options,
            single_file_output=request.single_file_output,
            table_name=request.table_name,
            constraints=request.constraints,
        )
        return self.write(write_request, prefer_streaming=prefer_streaming)

    @staticmethod
    def _mode_to_behavior(mode: WriteMode) -> str:
        """Convert WriteMode to PyArrow existing_data_behavior.

        Parameters
        ----------
        mode
            Write mode enum value.

        Returns
        -------
        str
            PyArrow existing_data_behavior value.
        """
        return {
            WriteMode.ERROR: "error",
            WriteMode.OVERWRITE: "delete_matching",
            WriteMode.APPEND: "overwrite_or_ignore",
        }[mode]

    def _record_write_artifact(
        self,
        result: WriteResult,
    ) -> None:
        """Record write operation in diagnostics.

        Parameters
        ----------
        result
            Write result metadata to record.

        Notes
        -----
        Records `write_operation` diagnostics when a recorder is configured.
        """
        if self.recorder is None:
            return
        from datafusion_engine.diagnostics import WriteRecord

        self.recorder.record_write(
            WriteRecord(
                destination=result.request.destination,
                format_=result.request.format.name.lower(),
                method=result.method.name.lower(),
                duration_ms=result.duration_ms or 0.0,
            )
        )

    @staticmethod
    def _prepare_destination(request: WriteRequest) -> Path:
        path = Path(request.destination)
        if request.mode == WriteMode.ERROR and path.exists():
            msg = f"Destination already exists: {path}"
            raise ValueError(msg)
        if (
            request.mode == WriteMode.APPEND
            and path.exists()
            and request.format != WriteFormat.PARQUET
        ):
            msg = f"Append mode is only supported for parquet datasets: {path}"
            raise ValueError(msg)
        if request.mode == WriteMode.OVERWRITE and path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _write_table(df: DataFrame, *, request: WriteRequest, table_name: str) -> None:
        if request.partition_by:
            msg = "Table writes do not support partition_by."
            raise ValueError(msg)
        if request.mode == WriteMode.ERROR:
            msg = "Table writes require APPEND or OVERWRITE mode."
            raise ValueError(msg)
        insert_op = InsertOp.APPEND if request.mode == WriteMode.APPEND else InsertOp.OVERWRITE
        df.write_table(
            table_name,
            write_options=DataFrameWriteOptions(insert_operation=insert_op),
        )

    def _write_parquet(
        self,
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
    ) -> None:
        if request.partition_by:
            policy = request.parquet_policy or ParquetWritePolicy()
            dataset_options: dict[str, Any] = dict(policy.to_dataset_options())
            if request.format_options:
                dataset_options.update(request.format_options)
            from datafusion_engine.streaming_executor import PipeToDatasetOptions

            result.pipe_to_dataset(
                request.destination,
                options=PipeToDatasetOptions(
                    file_format="parquet",
                    partitioning=list(request.partition_by),
                    existing_data_behavior=self._mode_to_behavior(request.mode),
                    max_rows_per_group=policy.row_group_size,
                    format_options=dataset_options,
                ),
            )
            return
        path = self._prepare_destination(request)
        policy = request.parquet_policy or ParquetWritePolicy()
        parquet_options = _parquet_writer_options(policy)
        write_options = DataFrameWriteOptions(
            single_file_output=bool(request.single_file_output)
            if request.single_file_output is not None
            else False,
        )
        result.df.write_parquet_with_options(
            path,
            options=parquet_options,
            write_options=write_options,
        )

    def _write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        if request.partition_by:
            msg = "CSV writes do not support partition_by."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        with_header = False
        if request.format_options and "with_header" in request.format_options:
            with_header = bool(request.format_options["with_header"])
        write_options = DataFrameWriteOptions(
            single_file_output=bool(request.single_file_output)
            if request.single_file_output is not None
            else False,
        )
        df.write_csv(path, with_header=with_header, write_options=write_options)

    def _write_json(self, df: DataFrame, *, request: WriteRequest) -> None:
        if request.partition_by:
            msg = "JSON writes do not support partition_by."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        write_options = DataFrameWriteOptions(
            single_file_output=bool(request.single_file_output)
            if request.single_file_output is not None
            else False,
        )
        df.write_json(path, write_options=write_options)

    def _write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None:
        if request.partition_by:
            msg = "Arrow writes do not support partition_by."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        table = result.to_table()
        with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

    def _table_target(self, request: WriteRequest) -> str | None:
        target = request.table_name or request.destination
        metadata = table_provider_metadata(id(self.ctx), table_name=target)
        if metadata is None:
            return None
        if metadata.supports_insert is False:
            return None
        return target


def _parquet_writer_options(policy: ParquetWritePolicy) -> ParquetWriterOptions:
    column_options = _parquet_column_options(policy.column_overrides)
    return ParquetWriterOptions(
        compression=policy.compression,
        compression_level=policy.compression_level,
        dictionary_enabled=policy.dictionary_enabled,
        statistics_enabled=policy.statistics_enabled,
        max_row_group_size=policy.row_group_size,
        data_pagesize_limit=policy.data_page_size,
        bloom_filter_on_write=policy.bloom_filter_enabled,
        bloom_filter_fpp=policy.bloom_filter_fpp,
        column_specific_options=column_options or None,
    )


def _parquet_column_options(
    overrides: Mapping[str, Mapping[str, str]] | None,
) -> dict[str, ParquetColumnOptions]:
    if not overrides:
        return {}
    resolved: dict[str, ParquetColumnOptions] = {}
    for name, options in overrides.items():
        resolved[name] = ParquetColumnOptions(
            encoding=_option_str(options.get("encoding")),
            dictionary_enabled=_option_bool(
                options.get("dictionary_enabled"), label="dictionary_enabled"
            ),
            compression=_option_str(options.get("compression")),
            statistics_enabled=_option_str(options.get("statistics_enabled")),
            bloom_filter_enabled=_option_bool(
                options.get("bloom_filter_enabled"),
                label="bloom_filter_enabled",
            ),
            bloom_filter_fpp=_option_float(
                options.get("bloom_filter_fpp"), label="bloom_filter_fpp"
            ),
            bloom_filter_ndv=_option_int(options.get("bloom_filter_ndv"), label="bloom_filter_ndv"),
        )
    return resolved


def _option_str(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _option_bool(value: str | None, *, label: str) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "false"}:
        return normalized == "true"
    msg = f"Invalid boolean override for {label}: {value}"
    raise ValueError(msg)


def _option_int(value: str | None, *, label: str) -> int | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return int(normalized)
    except ValueError as exc:
        msg = f"Invalid integer override for {label}: {value}"
        raise ValueError(msg) from exc


def _option_float(value: str | None, *, label: str) -> float | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError as exc:
        msg = f"Invalid float override for {label}: {value}"
        raise ValueError(msg) from exc


def parquet_policy_from_datafusion(
    policy: DataFusionWritePolicy | None,
) -> ParquetWritePolicy | None:
    """Translate DataFusionWritePolicy into ParquetWritePolicy.

    Parameters
    ----------
    policy
        DataFusion write policy to translate.

    Returns
    -------
    ParquetWritePolicy | None
        Converted Parquet write policy, or None when policy is missing.
    """
    if policy is None:
        return None
    column_overrides = _column_overrides_from_policy(policy.parquet_column_options)
    return ParquetWritePolicy(
        compression=policy.parquet_compression or "zstd",
        row_group_size=policy.parquet_row_group_size or 1_000_000,
        dictionary_enabled=policy.parquet_dictionary_enabled
        if policy.parquet_dictionary_enabled is not None
        else True,
        statistics_enabled=policy.parquet_statistics_enabled or "page",
        bloom_filter_enabled=policy.parquet_bloom_filter_on_write or False,
        column_overrides=column_overrides,
    )


def _column_overrides_from_policy(
    options: Mapping[str, ParquetColumnPolicy] | None,
) -> dict[str, dict[str, str]]:
    if not options:
        return {}
    overrides: dict[str, dict[str, str]] = {}
    for name, option in options.items():
        payload = option.payload()
        converted = {
            key: value
            for key, value in ((key, _option_value_to_str(value)) for key, value in payload.items())
            if value is not None
        }
        if converted:
            overrides[name] = converted
    return overrides


def _option_value_to_str(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def _statistics_flag(value: str) -> bool | None:
    normalized = value.strip().lower()
    return normalized != "none"
