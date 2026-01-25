"""
Streaming execution via Arrow C Stream protocol.

Provides zero-copy streaming access to DataFusion query results
without full materialization, enabling efficient processing of large
datasets through Arrow RecordBatch iteration.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.dataset as ds

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext, SQLOptions

from datafusion_engine.compile_options import DataFusionSqlPolicy


@dataclass(frozen=True)
class StreamingExecutionResult:
    """
    Lazy execution result that can be consumed as Arrow stream OR collected.

    Implements deferred materialization - no data is executed until
    a terminal operation (to_table, to_pandas, pipe_to_*) is called.

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to stream.

    Examples
    --------
    >>> result = StreamingExecutionResult(df=ctx.sql("SELECT * FROM tbl"))
    >>> # Stream to partitioned dataset
    >>> result.pipe_to_dataset("/out", partitioning=["year", "month"])
    >>> # Or materialize to table
    >>> table = result.to_table()
    """

    df: DataFrame

    @property
    def schema(self) -> pa.Schema:
        """
        Get schema without executing.

        Returns
        -------
        pa.Schema
            Arrow schema of the result.
        """
        return self.df.schema()

    def to_arrow_stream(self) -> pa.RecordBatchReader:
        """
        Zero-copy streaming via Arrow C Stream protocol.

        DataFusion DataFrames implement __arrow_c_stream__,
        allowing direct consumption as Arrow RecordBatchReader
        without intermediate serialization.

        Returns
        -------
        pa.RecordBatchReader
            Streaming reader over query results.

        Notes
        -----
        This method enables zero-copy data transfer from DataFusion
        to PyArrow using the Arrow C Stream interface.
        """
        return pa.RecordBatchReader.from_stream(self.df)

    def to_batches(self) -> Iterator[pa.RecordBatch]:
        """
        Iterate over record batches.

        Yields
        ------
        pa.RecordBatch
            Record batches from the query result.

        Notes
        -----
        Actual batch sizes depend on upstream operators and
        DataFusion's execution plan configuration.
        """
        reader = self.to_arrow_stream()
        yield from reader

    def to_table(self) -> pa.Table:
        """
        Materialize full result as Arrow Table.

        Returns
        -------
        pa.Table
            Materialized table with all query results.

        Warnings
        --------
        This method materializes the entire result in memory.
        For large datasets, prefer streaming methods.
        """
        return pa.Table.from_batches(
            list(self.to_batches()),
            schema=self.schema,
        )

    def to_pandas(self) -> Any:
        """
        Materialize as pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            Pandas DataFrame with all query results.

        Warnings
        --------
        This method materializes the entire result in memory.
        For large datasets, prefer streaming methods.

        Notes
        -----
        Pandas is not a strict dependency. This method will
        fail if pandas is not installed.
        """
        return self.to_table().to_pandas()

    def pipe_to_dataset(  # noqa: PLR0913
        self,
        base_dir: str,
        *,
        file_format: str = "parquet",
        partitioning: ds.Partitioning | list[str] | None = None,
        existing_data_behavior: str = "error",
        file_visitor: Callable[[str], None] | None = None,
        max_partitions: int = 1024,
        max_open_files: int = 1024,
        max_rows_per_file: int = 10_000_000,
        min_rows_per_group: int = 0,
        max_rows_per_group: int = 1_000_000,
        **format_options: Any,
    ) -> None:
        """
        Stream partitioned dataset to disk.

        Writes results as a partitioned dataset using PyArrow's
        dataset API. Supports Hive-style partitioning and custom
        partition schemes.

        Parameters
        ----------
        base_dir : str
            Base directory for dataset output.
        file_format : str, default="parquet"
            Output format (parquet, ipc, csv, etc.).
        partitioning : ds.Partitioning or list of str, optional
            Partition scheme. If list of column names, creates
            Hive-style string partitioning.
        existing_data_behavior : str, default="error"
            How to handle existing data. One of:
            - "error": Raise if data exists
            - "overwrite_or_ignore": Overwrite matching partitions
            - "delete_matching": Delete and replace
        file_visitor : callable, optional
            Callback invoked with each written file path.
            Useful for generating metadata sidecars.
        max_partitions : int, default=1024
            Maximum number of partitions to write.
        max_open_files : int, default=1024
            Maximum number of files to keep open.
        max_rows_per_file : int, default=10_000_000
            Maximum rows per output file.
        min_rows_per_group : int, default=0
            Minimum rows per row group (Parquet).
        max_rows_per_group : int, default=1_000_000
            Maximum rows per row group (Parquet).
        **format_options
            Format-specific options (compression, etc.).

        Examples
        --------
        >>> result.pipe_to_dataset(
        ...     "/data/events",
        ...     partitioning=["year", "month", "day"],
        ...     compression="zstd",
        ...     compression_level=9,
        ... )
        """
        # Build partitioning if list of column names
        if isinstance(partitioning, list):
            partitioning = ds.partitioning(
                pa.schema([(col, pa.string()) for col in partitioning]),
                flavor="hive",
            )

        # Build format options
        if file_format == "parquet":
            file_options = ds.ParquetFileFormat().make_write_options(**format_options)
        else:
            file_options = None

        ds.write_dataset(
            self.to_arrow_stream(),
            base_dir=base_dir,
            format=file_format,
            partitioning=partitioning,
            existing_data_behavior=existing_data_behavior,
            file_visitor=file_visitor,
            max_partitions=max_partitions,
            max_open_files=max_open_files,
            max_rows_per_file=max_rows_per_file,
            min_rows_per_group=min_rows_per_group,
            max_rows_per_group=max_rows_per_group,
            file_options=file_options,
        )

    def pipe_to_parquet(
        self,
        path: str,
        *,
        compression: str = "zstd",
        compression_level: int | None = None,
        row_group_size: int = 1_000_000,
    ) -> None:
        """
        Stream to single Parquet file.

        Writes results to a single Parquet file using streaming
        writer. For partitioned output, use pipe_to_dataset.

        Parameters
        ----------
        path : str
            Output file path.
        compression : str, default="zstd"
            Compression codec (zstd, snappy, gzip, lz4, brotli, none).
        compression_level : int, optional
            Codec-specific compression level.
        row_group_size : int, default=1_000_000
            Target rows per row group.

        Examples
        --------
        >>> result.pipe_to_parquet(
        ...     "/data/output.parquet",
        ...     compression="zstd",
        ...     compression_level=9,
        ... )
        """
        import pyarrow.parquet as pq

        with pq.ParquetWriter(
            path,
            self.schema,
            compression=compression,
            compression_level=compression_level,
        ) as writer:
            for batch in self.to_batches():
                writer.write_batch(batch, row_group_size=row_group_size)


class StreamingExecutor:
    """
    Executor that produces streaming results.

    All execution paths return StreamingExecutionResult,
    deferring materialization to the caller. This enables
    efficient processing of large datasets without full
    materialization.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context.

    Examples
    --------
    >>> from datafusion import SessionContext
    >>> ctx = SessionContext()
    >>> executor = StreamingExecutor(ctx)
    >>> result = executor.execute_sql("SELECT * FROM large_table")
    >>> result.pipe_to_dataset("/output", partitioning=["year"])
    """

    def __init__(
        self,
        ctx: SessionContext,
        *,
        sql_options: SQLOptions | None = None,
    ) -> None:
        """
        Initialize streaming executor.

        Parameters
        ----------
        ctx : SessionContext
            DataFusion session context.
        sql_options : SQLOptions | None, optional
            Optional SQL options to apply for query execution.
        """
        self.ctx = ctx
        self.sql_options = sql_options or DataFusionSqlPolicy().to_sql_options()

    def execute_sql(
        self,
        sql: str,
        *,
        sql_options: SQLOptions | None = None,
        **params: Any,
    ) -> StreamingExecutionResult:
        """
        Execute SQL and return streaming result.

        Parameters
        ----------
        sql : str
            SQL query to execute.
        sql_options : SQLOptions | None, optional
            Optional SQL options to override the executor defaults.
        **params
            Query parameters for parameterized queries.

        Returns
        -------
        StreamingExecutionResult
            Streaming result wrapper.

        Examples
        --------
        >>> result = executor.execute_sql("SELECT * FROM tbl WHERE id = $1", $1=42)
        >>> table = result.to_table()
        """
        resolved_options = sql_options or self.sql_options
        df = self.ctx.sql_with_options(sql, resolved_options, **params)
        return StreamingExecutionResult(df=df)

    def from_table(
        self,
        table_name: str,
    ) -> StreamingExecutionResult:
        """
        Get streaming result from registered table.

        Parameters
        ----------
        table_name : str
            Name of registered table.

        Returns
        -------
        StreamingExecutionResult
            Streaming result wrapper.

        Examples
        --------
        >>> ctx.register_parquet("events", "/data/events.parquet")
        >>> result = executor.from_table("events")
        >>> for batch in result.to_batches():
        ...     process(batch)
        """
        df = self.ctx.table(table_name)
        return StreamingExecutionResult(df=df)
