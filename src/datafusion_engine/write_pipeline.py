"""Unified write pipeline for all DataFusion output paths.

This module provides a single writing surface with explicit format policy,
partitioning, and schema constraints while using DataFusion-native writers
(streaming + DataFrame writes).

Canonical write surfaces (Scope 15)
------------------------------------
All write operations route through DataFusion-native APIs:

1. **CSV/JSON/Arrow**: `DataFrame.write_csv()`, `DataFrame.write_json()`, Arrow IPC
2. **Parquet**: `DataFrame.write_parquet()` with `DataFrameWriteOptions`
3. **Table inserts**: `DataFrame.write_table()` with `InsertOp.APPEND/OVERWRITE`
4. **Delta**: Streaming writes via Delta Lake writer with partitioning support

Pattern
-------
>>> from datafusion import DataFrameWriteOptions
>>> from datafusion_engine.write_pipeline import WritePipeline, WriteRequest, WriteFormat
>>> pipeline = WritePipeline(ctx)
>>> request = WriteRequest(
...     source="SELECT * FROM events",
...     destination="/data/events",
...     format=WriteFormat.DELTA,
...     partition_by=("year", "month"),
... )
>>> pipeline.write(request)
"""

from __future__ import annotations

import contextlib
import importlib
import shutil
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from datafusion import DataFrameWriteOptions, InsertOp, SQLOptions
from datafusion.dataframe import DataFrame

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.streaming_executor import StreamingExecutionResult
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
class WriteRequest:
    """Unified write request specification.

    Encapsulates all information needed to write a dataset,
    regardless of the underlying mechanism (COPY, INSERT, Arrow writer).

    Parameters
    ----------
    source
        DataFusion DataFrame or SQL query string defining the source data.
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

    source: DataFrame | str
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
    sql_options
        Optional SQL execution options for SQL ingress.

    Examples
    --------
    >>> from datafusion import SessionContext
    >>> ctx = SessionContext()
    >>> pipeline = WritePipeline(ctx)
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
        sql_options
            Optional SQL execution options for COPY statements.
        recorder
            Optional diagnostics recorder for write operations.
        runtime_profile
            Optional DataFusion runtime profile for Delta writes.
        """
        self.ctx = ctx
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

    def _execute_sql(self, sql: str) -> DataFrame:
        return self.ctx.sql_with_options(sql, self._resolved_sql_options())

    def _source_df(self, request: WriteRequest) -> DataFrame:
        if isinstance(request.source, DataFrame):
            return request.source
        return self._execute_sql(request.source)

    def _validate_constraints(
        self,
        *,
        df: DataFrame,
        constraints: tuple[str, ...],
    ) -> None:
        if not constraints:
            return
        view_name = f"__write_constraints_{uuid.uuid4().hex}"
        self.ctx.register_table(view_name, df)
        try:
            for constraint in constraints:
                if not constraint.strip():
                    continue
                query = f"SELECT 1 FROM {view_name} WHERE NOT ({constraint}) LIMIT 1"
                constraint_df = self._execute_sql(query)
                if self._df_has_rows(constraint_df):
                    msg = f"Delta constraint violated: {constraint}"
                    raise ValueError(msg)
        finally:
            with contextlib.suppress(Exception):
                self.ctx.deregister_table(view_name)

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
        df = self._source_df(request)
        if request.constraints:
            self._validate_constraints(df=df, constraints=request.constraints)
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

        if request.format == WriteFormat.DELTA and self._try_write_delta_provider(
            df, request=request
        ):
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
        The unified writer executes a DataFusion DataFrame. Delta uses
        streaming dataset writes; other formats use DataFusion-native writers.
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
            source=self.ctx.table(request.view_name),
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

    @staticmethod
    def _write_delta(
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
    ) -> None:
        """Write Delta table via DataFusion-native streaming writer.

        This method writes Delta tables directly using the Delta Lake writer.

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
        if request.mode == WriteMode.ERROR and Path(request.destination).exists():
            msg = f"Delta destination already exists: {request.destination}"
            raise ValueError(msg)
        from deltalake import write_deltalake

        write_deltalake(
            request.destination,
            result.to_arrow_stream(),
            mode=_delta_mode(request.mode),
            partition_by=list(request.partition_by) if request.partition_by else None,
            configuration=_delta_configuration(request.format_options),
        )

    def _try_write_delta_provider(self, df: DataFrame, *, request: WriteRequest) -> bool:
        if request.partition_by:
            return False
        if request.mode == WriteMode.ERROR:
            return False
        try:
            module = importlib.import_module("datafusion_ext")
        except ImportError:
            return False
        provider_factory = getattr(module, "delta_table_provider_from_session", None)
        if not callable(provider_factory):
            return False
        temp_name = request.table_name or f"__delta_write_{uuid.uuid4().hex}"
        storage_payload = None
        provider = provider_factory(
            self.ctx,
            request.destination,
            storage_payload,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
        adapter.register_delta_table_provider(temp_name, provider, overwrite=True)
        try:
            self._write_table(df, request=request, table_name=temp_name)
        finally:
            deregister = getattr(self.ctx, "deregister_table", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister(temp_name)
        return True

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
