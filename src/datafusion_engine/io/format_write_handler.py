"""Format-specific write helpers for WritePipeline."""

from __future__ import annotations

import shutil
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa
from datafusion import DataFrameWriteOptions, InsertOp

from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.io.write_core import WriteFormat, WriteMode
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.tables.metadata import table_provider_metadata
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.io.write_core import WriteRequest
    from datafusion_engine.io.write_pipeline import WritePipeline
    from datafusion_engine.session.streaming import StreamingExecutionResult


class _FormatWritePipeline(Protocol):
    def write_csv(self, df: DataFrame, *, request: WriteRequest) -> None: ...

    def write_json(self, df: DataFrame, *, request: WriteRequest) -> None: ...

    def write_parquet(self, df: DataFrame, *, request: WriteRequest) -> None: ...

    def write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None: ...


class FormatWriteHandler:
    """Adapter wrapper for format-specific pipeline write entrypoints."""

    def __init__(self, pipeline: _FormatWritePipeline) -> None:
        """Initialize handler with a pipeline implementing format writers."""
        self._pipeline = pipeline

    def write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Delegate CSV writes to the injected pipeline."""
        self._pipeline.write_csv(df, request=request)

    def write_json(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Delegate JSON writes to the injected pipeline."""
        self._pipeline.write_json(df, request=request)

    def write_parquet(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Delegate Parquet writes to the injected pipeline."""
        self._pipeline.write_parquet(df, request=request)

    def write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None:
        """Delegate Arrow writes to the injected pipeline."""
        self._pipeline.write_arrow(result, request=request)


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _copy_options_clause(options: dict[str, str]) -> str | None:
    if not options:
        return None
    joined = ", ".join(f"{key} {value}" for key, value in options.items())
    return f"OPTIONS ({joined})"


def _copy_format_token(write_format: WriteFormat) -> str:
    if write_format == WriteFormat.CSV:
        return "CSV"
    if write_format == WriteFormat.JSON:
        return "JSON"
    if write_format == WriteFormat.ARROW:
        return "PARQUET"
    return str(write_format.value).upper()


def prepare_destination(request: WriteRequest) -> Path:
    """Resolve and prepare output destination path for file-based writes.

    Raises:
        ValueError: If destination/mode combination is invalid.

    Returns:
        Path: Prepared destination path.
    """
    path = Path(request.destination)
    if request.mode == WriteMode.ERROR and path.exists():
        msg = f"Destination already exists: {path}"
        raise ValueError(msg)
    if request.mode == WriteMode.APPEND and path.exists() and request.format != WriteFormat.DELTA:
        msg = f"Append mode is only supported for delta datasets: {path}"
        raise ValueError(msg)
    if request.mode == WriteMode.OVERWRITE and path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def temp_view_name(prefix: str, *, request: WriteRequest) -> str:
    """Build deterministic temporary view name for staging writes.

    Returns:
        str: Temporary view name.
    """
    digest = hash_sha256_hex(f"{prefix}:{request.destination}:{id(request)}".encode())[:8]
    return f"__{prefix}_{digest}"


def write_copy(pipeline: WritePipeline, df: DataFrame, *, request: WriteRequest) -> str:
    """Write dataset using `COPY ... TO` SQL and return executed SQL text.

    Raises:
        ValueError: If request options are incompatible with COPY.

    Returns:
        str: Executed COPY SQL statement.
    """
    if request.single_file_output:
        msg = "COPY does not support single_file_output."
        raise ValueError(msg)
    path = prepare_destination(request)
    temp_view = temp_view_name("copy", request=request)
    adapter = DataFusionIOAdapter(ctx=pipeline.ctx, profile=pipeline.runtime_profile)
    adapter.register_view(temp_view, df, overwrite=True, temporary=True)
    try:
        format_token = _copy_format_token(request.format)
        sql = (
            f"COPY (SELECT * FROM {_sql_identifier(temp_view)}) "
            f"TO {_sql_string_literal(str(path))} STORED AS {format_token}"
        )
        if request.partition_by:
            partition_cols = ", ".join(_sql_identifier(col) for col in request.partition_by)
            sql = f"{sql} PARTITIONED BY ({partition_cols})"
        copy_options: dict[str, str] = {}
        if (
            request.format == WriteFormat.CSV
            and request.format_options
            and "with_header" in request.format_options
        ):
            copy_options["format.has_header"] = str(
                bool(request.format_options["with_header"])
            ).lower()
        options_clause = _copy_options_clause(copy_options)
        if options_clause:
            sql = f"{sql} {options_clause}"
        allow_statements = True
        sql_options = sql_options_for_profile(pipeline.runtime_profile).with_allow_statements(
            allow_statements
        )
        df_stmt = pipeline.ctx.sql_with_options(sql, sql_options)
        if df_stmt is None:
            msg = "COPY statement did not return a DataFusion DataFrame."
            raise ValueError(msg)
        df_stmt.collect()
    finally:
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(temp_view)
    return sql


def write_insert(
    pipeline: WritePipeline,
    df: DataFrame,
    *,
    request: WriteRequest,
    table_name: str,
) -> str:
    """Write dataset using SQL `INSERT` and return executed SQL text.

    Raises:
        ValueError: If write mode is incompatible with table insert semantics.

    Returns:
        str: Executed INSERT SQL statement.
    """
    if request.mode == WriteMode.ERROR:
        msg = "Table writes require APPEND or OVERWRITE mode."
        raise ValueError(msg)
    temp_view = temp_view_name("insert", request=request)
    adapter = DataFusionIOAdapter(ctx=pipeline.ctx, profile=pipeline.runtime_profile)
    adapter.register_view(temp_view, df, overwrite=True, temporary=True)
    try:
        verb = "OVERWRITE" if request.mode == WriteMode.OVERWRITE else "INTO"
        sql = (
            f"INSERT {verb} {_sql_identifier(table_name)} "
            f"SELECT * FROM {_sql_identifier(temp_view)}"
        )
        allow_statements = True
        allow_dml = True
        sql_options = (
            sql_options_for_profile(pipeline.runtime_profile)
            .with_allow_statements(allow_statements)
            .with_allow_dml(allow_dml)
        )
        df_stmt = pipeline.ctx.sql_with_options(sql, sql_options)
        if df_stmt is None:
            msg = "INSERT statement did not return a DataFusion DataFrame."
            raise ValueError(msg)
        df_stmt.collect()
    finally:
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(temp_view)
    return sql


def write_table(df: DataFrame, *, request: WriteRequest, table_name: str) -> None:
    """Write DataFrame directly to a registered table target.

    Raises:
        ValueError: If write mode is incompatible with table writes.
    """
    if request.mode == WriteMode.ERROR:
        msg = "Table writes require APPEND or OVERWRITE mode."
        raise ValueError(msg)
    insert_op = InsertOp.APPEND if request.mode == WriteMode.APPEND else InsertOp.OVERWRITE
    df.write_table(
        table_name,
        write_options=DataFrameWriteOptions(
            insert_operation=insert_op,
            partition_by=request.partition_by or None,
        ),
    )


def write_csv(_pipeline: object, df: DataFrame, *, request: WriteRequest) -> None:
    """Write DataFrame to CSV output path.

    Raises:
        ValueError: If request options are incompatible with CSV writes.
    """
    if request.partition_by:
        msg = "CSV writes do not support partition_by."
        raise ValueError(msg)
    path = prepare_destination(request)
    with_header = False
    if request.format_options and "with_header" in request.format_options:
        with_header = bool(request.format_options["with_header"])
    write_options = DataFrameWriteOptions(
        single_file_output=bool(request.single_file_output)
        if request.single_file_output is not None
        else False,
    )
    df.write_csv(path, with_header=with_header, write_options=write_options)


def write_json(_pipeline: object, df: DataFrame, *, request: WriteRequest) -> None:
    """Write DataFrame to JSON output path.

    Raises:
        ValueError: If request options are incompatible with JSON writes.
    """
    if request.partition_by:
        msg = "JSON writes do not support partition_by."
        raise ValueError(msg)
    path = prepare_destination(request)
    write_options = DataFrameWriteOptions(
        single_file_output=bool(request.single_file_output)
        if request.single_file_output is not None
        else False,
    )
    df.write_json(path, write_options=write_options)


def write_arrow(
    _pipeline: object,
    result: StreamingExecutionResult,
    *,
    request: WriteRequest,
) -> None:
    """Write streaming execution output to Arrow IPC file.

    Raises:
        ValueError: If request options are incompatible with Arrow writes.
    """
    if request.partition_by:
        msg = "Arrow writes do not support partition_by."
        raise ValueError(msg)
    path = prepare_destination(request)
    table = result.to_table()
    with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)


def table_target(pipeline: WritePipeline, request: WriteRequest) -> str | None:
    """Resolve table target name when destination is an insert-capable table.

    Returns:
        str | None: Target table name when insert-capable, otherwise ``None``.
    """
    target = request.table_name or request.destination
    metadata = table_provider_metadata(pipeline.ctx, table_name=target)
    if metadata is None:
        return None
    if metadata.supports_insert is False:
        return None
    return target


__all__ = [
    "FormatWriteHandler",
    "prepare_destination",
    "table_target",
    "temp_view_name",
    "write_arrow",
    "write_copy",
    "write_csv",
    "write_insert",
    "write_json",
    "write_table",
]
