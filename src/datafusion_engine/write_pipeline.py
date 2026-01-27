"""Unified write pipeline for all DataFusion output paths.

This module provides a single writing surface with explicit format policy,
partitioning, and schema constraints while using AST-first compilation and
DataFusion-native writers (streaming + DataFrame writes).

Canonical write surfaces (Scope 15)
------------------------------------
All write operations route through DataFusion-native APIs:

1. **CSV/JSON/Arrow**: `DataFrame.write_csv()`, `DataFrame.write_json()`, Arrow IPC
2. **Parquet**: `DataFrame.write_parquet()` with `DataFrameWriteOptions`
3. **Table inserts**: `DataFrame.write_table()` with `InsertOp.APPEND/OVERWRITE`
4. **Delta (transitional)**: Streaming writes via Ibis bridge until DataFusion
   native Delta writer supports partitioning and schema evolution

Pattern
-------
>>> from datafusion import DataFrameWriteOptions
>>> from datafusion_engine.write_pipeline import WritePipeline, WriteRequest, WriteFormat
>>> pipeline = WritePipeline(ctx, profile)
>>> request = WriteRequest(
...     source="SELECT * FROM events",
...     destination="/data/events",
...     format=WriteFormat.DELTA,
...     partition_by=("year", "month"),
... )
>>> pipeline.write(request)
"""

from __future__ import annotations

import shutil
import time
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from datafusion import DataFrameWriteOptions, InsertOp, SQLOptions

from sqlglot_tools.compat import exp

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.streaming_executor import StreamingExecutionResult
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.table_provider_metadata import table_provider_metadata


class WriteFormat(Enum):
    """Supported output formats."""

    DELTA = auto()
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
        Output format (DELTA, CSV, JSON, ARROW).
    mode
        Write mode for handling existing data.
    partition_by
        Column names for Hive-style partitioning.
    format_options
        Format-specific COPY/streaming options for the underlying writer.
    single_file_output
        Hint to prefer single-file output when supported.

    Examples
    --------
    >>> request = WriteRequest(
    ...     source="SELECT * FROM events",
    ...     destination="/data/events",
    ...     format=WriteFormat.DELTA,
    ...     mode=WriteMode.OVERWRITE,
    ...     partition_by=("year", "month"),
    ... )
    """

    source: str | exp.Expression  # SQL query or AST
    destination: str  # Path or table name
    format: WriteFormat = WriteFormat.DELTA
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
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
    format: WriteFormat = WriteFormat.DELTA
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
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
    ...     destination="/data/events",
    ...     format=WriteFormat.DELTA,
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
        runtime_profile: DataFusionRuntimeProfile | None = None,
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
        runtime_profile
            Optional DataFusion runtime profile for Delta writes.
        """
        self.ctx = ctx
        self.profile = profile
        self.sql_options = sql_options
        self.recorder = recorder
        self.runtime_profile = runtime_profile

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

        # DEPRECATED: SQLGlot-based SQL validation for DataFusion queries.
        # For DataFusion query validation, prefer DataFusion's native SQL parser
        # with SQLOptions for ingress gating.
        return parse_sql_strict(
            request.source,
            dialect=self.profile.read_dialect or "datafusion",
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
            parse_sql_strict,  # DEPRECATED: Use DataFusion's native SQL parser for validation
        )

        for constraint in constraints:
            if not constraint.strip():
                continue
            # DEPRECATED: Use DataFusion's native SQL parser for validation instead
            constraint_expr = parse_sql_strict(
                constraint,
                dialect=self.profile.read_dialect or "datafusion",
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

        Uses Arrow streaming execution for Delta datasets and
        DataFusion DataFrame writers for non-Delta file outputs.

        Parameters
        ----------
        request
            Write request specification.

        Raises
        ------
        NotImplementedError
            If the write format is not supported by the streaming writer.

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
        if request.format == WriteFormat.DELTA:
            self._write_delta(result, request=request)
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
            If True, prefer streaming write for DELTA format.

        Returns
        -------
        WriteResult
            Write result metadata for the executed write.

        Notes
        -----
        The unified writer always compiles from AST and executes a
        DataFusion DataFrame. Delta uses streaming dataset writes;
        other formats use DataFusion-native writers.
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
            format_options=request.format_options,
            single_file_output=request.single_file_output,
            table_name=request.table_name,
            constraints=request.constraints,
        )
        return self.write(write_request, prefer_streaming=prefer_streaming)

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
            and request.format != WriteFormat.DELTA
        ):
            msg = f"Append mode is only supported for delta datasets: {path}"
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
        """Write to registered table via DataFusion-native DataFrame.write_table.

        Parameters
        ----------
        df
            DataFusion DataFrame to write.
        request
            Write request specification.
        table_name
            Target table name for INSERT operation.

        Raises
        ------
        ValueError
            Raised when partition_by is specified or mode is ERROR.
        """
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

    def _write_delta(
        self,
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
    ) -> None:
        """Write Delta table via DataFusion-native streaming writer.

        This method currently routes through the Ibis bridge for Delta writes
        to handle edge cases (partitioning, schema evolution, constraints).
        Future versions will use DataFusion's native Delta writer when available.

        Parameters
        ----------
        result
            Streaming execution result with Arrow stream.
        request
            Write request specification with Delta options.

        Raises
        ------
        ValueError
            Raised when runtime profile is missing or destination already exists in ERROR mode.
        """
        if self.runtime_profile is None:
            msg = "Delta writes require a DataFusion runtime profile."
            raise ValueError(msg)
        if request.mode == WriteMode.ERROR and Path(request.destination).exists():
            msg = f"Delta destination already exists: {request.destination}"
            raise ValueError(msg)
        from ibis_engine.execution_factory import ibis_execution_from_profile
        from ibis_engine.io_bridge import IbisDatasetWriteOptions, write_ibis_dataset_delta
        from ibis_engine.sources import IbisDeltaWriteOptions

        delta_options = IbisDeltaWriteOptions(
            mode=_delta_mode(request.mode),
            partition_by=list(request.partition_by) if request.partition_by else None,
            configuration=_delta_configuration(request.format_options),
        )
        execution = ibis_execution_from_profile(self.runtime_profile)
        write_ibis_dataset_delta(
            result.to_arrow_stream(),
            request.destination,
            options=IbisDatasetWriteOptions(
                execution=execution,
                writer_strategy="datafusion",
                delta_options=delta_options,
            ),
            table_name=request.table_name,
        )

    def _write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write CSV via DataFusion-native DataFrame writer.

        Parameters
        ----------
        df
            DataFusion DataFrame to write.
        request
            Write request specification.

        Raises
        ------
        ValueError
            Raised when partition_by is specified (not supported for CSV).
        """
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
        """Write JSON via DataFusion-native DataFrame writer.

        Parameters
        ----------
        df
            DataFusion DataFrame to write.
        request
            Write request specification.

        Raises
        ------
        ValueError
            Raised when partition_by is specified (not supported for JSON).
        """
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


def _delta_mode(mode: WriteMode) -> Literal["append", "overwrite"]:
    if mode == WriteMode.OVERWRITE:
        return "overwrite"
    return "append"


def _delta_configuration(
    options: Mapping[str, object] | None,
) -> Mapping[str, str | None] | None:
    if not options:
        return None
    resolved: dict[str, str | None] = {}
    for key, value in options.items():
        name = str(key)
        if value is None:
            resolved[name] = None
        elif isinstance(value, str):
            resolved[name] = value
        else:
            resolved[name] = str(value)
    return resolved or None


def _statistics_flag(value: str) -> bool | None:
    normalized = value.strip().lower()
    return normalized != "none"
