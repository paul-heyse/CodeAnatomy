"""Streaming execution via Arrow C Stream protocol.

Provides zero-copy streaming access to DataFusion query results
without full materialization, enabling efficient processing of large
datasets through Arrow RecordBatch iteration.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pyarrow.dataset as ds

if TYPE_CHECKING:
    import polars as pl
    from datafusion import DataFrame, SessionContext, SQLOptions

from datafusion import SQLOptions

from datafusion_engine.sql.options import sql_options_for_profile


@dataclass(frozen=True)
class PipeToDatasetOptions:
    """Options for streaming partitioned datasets to disk."""

    partitioning: ds.Partitioning | list[str] | None = None
    existing_data_behavior: str = "error"
    file_visitor: Callable[[str], None] | None = None
    max_partitions: int = 1024
    max_open_files: int = 1024
    max_rows_per_file: int = 10_000_000
    min_rows_per_group: int = 0
    max_rows_per_group: int = 1_000_000


@dataclass(frozen=True)
class StreamingExecutionResult:
    """Lazy execution result that can be consumed as Arrow stream OR collected.

    Implements deferred materialization - no data is executed until
    a terminal operation (to_table, to_polars, pipe_to_*) is called.

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to stream.

    Examples:
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
        """Get schema without executing.

        Returns:
        -------
        pa.Schema
            Arrow schema of the result.
        """
        return self.df.schema()

    def _execution_partition_count(self) -> int | None:
        plan_fn = getattr(self.df, "execution_plan", None)
        if not callable(plan_fn):
            return None
        try:
            plan = plan_fn()
        except (RuntimeError, TypeError, ValueError):
            return None
        count_fn = getattr(plan, "partition_count", None)
        if not callable(count_fn):
            return None
        try:
            count = count_fn()
        except (RuntimeError, TypeError, ValueError):
            return None
        if isinstance(count, int):
            return count
        return None

    def to_arrow_stream(self) -> pa.RecordBatchReader:
        """Zero-copy streaming via Arrow C Stream protocol.

        DataFusion DataFrames implement __arrow_c_stream__,
        allowing direct consumption as Arrow RecordBatchReader
        without intermediate serialization.

        Returns:
        -------
        pa.RecordBatchReader
            Streaming reader over query results.

        Notes:
        -----
        This method enables zero-copy data transfer from DataFusion
        to PyArrow using the Arrow C Stream interface.
        """
        partition_count = self._execution_partition_count()
        if partition_count == 0:
            return pa.RecordBatchReader.from_batches(self.schema, [])
        return pa.RecordBatchReader.from_stream(self.df)

    def to_batches(self) -> Iterator[pa.RecordBatch]:
        """Iterate over record batches.

        Yields:
        ------
        pa.RecordBatch
            Record batches from the query result.

        Notes:
        -----
        Actual batch sizes depend on upstream operators and
        DataFusion's execution plan configuration.
        """
        reader = self.to_arrow_stream()
        yield from reader

    def to_table(self) -> pa.Table:
        """Materialize full result as Arrow Table.

        Returns:
        -------
        pa.Table
            Materialized table with all query results.

        Warnings:
        --------
        This method materializes the entire result in memory.
        For large datasets, prefer streaming methods.
        """
        return pa.Table.from_batches(
            list(self.to_batches()),
            schema=self.schema,
        )

    def to_polars(self) -> pl.DataFrame:
        """Materialize as polars DataFrame.

        Returns:
        -------
        pl.DataFrame
            Polars DataFrame with all query results.

        Warnings:
        --------
        This method materializes the entire result in memory.
        For large datasets, prefer streaming methods.
        """
        import polars as pl

        return cast("pl.DataFrame", pl.from_arrow(self.to_table()))

    def pipe_to_dataset(
        self,
        base_dir: str,
        *,
        options: PipeToDatasetOptions | None = None,
    ) -> None:
        """Stream partitioned dataset to disk.

        Writes results as a partitioned dataset using PyArrow's
        dataset API. Supports Hive-style partitioning and custom
        partition schemes.

        Parameters
        ----------
        base_dir : str
            Base directory for dataset output.
        options : PipeToDatasetOptions | None
            Dataset streaming configuration.

        Examples:
        --------
        >>> result.pipe_to_dataset(
        ...     "/data/events",
        ...     options=PipeToDatasetOptions(partitioning=["year", "month", "day"]),
        ... )
        """
        # Build partitioning if list of column names
        resolved = options or PipeToDatasetOptions()

        if isinstance(resolved.partitioning, list):
            partitioning = ds.partitioning(
                pa.schema([(col, pa.string()) for col in resolved.partitioning]),
                flavor="hive",
            )
        else:
            partitioning = resolved.partitioning

        ds.write_dataset(
            self.to_arrow_stream(),
            base_dir=base_dir,
            format="delta",
            partitioning=partitioning,
            existing_data_behavior=resolved.existing_data_behavior,
            file_visitor=resolved.file_visitor,
            max_partitions=resolved.max_partitions,
            max_open_files=resolved.max_open_files,
            max_rows_per_file=resolved.max_rows_per_file,
            min_rows_per_group=resolved.min_rows_per_group,
            max_rows_per_group=resolved.max_rows_per_group,
        )


class StreamingExecutor:
    """Executor that produces streaming results.

    All execution paths return StreamingExecutionResult,
    deferring materialization to the caller. This enables
    efficient processing of large datasets without full
    materialization.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context.

    Examples:
    --------
    >>> from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    >>> profile = DataFusionRuntimeProfile()
    >>> ctx = profile.session_context()
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
        """Initialize streaming executor.

        Parameters
        ----------
        ctx : SessionContext
            DataFusion session context.
        sql_options : SQLOptions | None, optional
            Optional SQL options to apply for query execution.
        """
        self.ctx = ctx
        self.sql_options = sql_options or sql_options_for_profile(None)

    def execute_sql(
        self,
        sql: str,
        *,
        sql_options: SQLOptions | None = None,
        **params: Any,
    ) -> StreamingExecutionResult:
        """Execute SQL and return streaming result.

        Args:
            sql: Description.
                    sql_options: Description.
                    **params: Description.

        Returns:
            StreamingExecutionResult: Result.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        resolved_options = sql_options or self.sql_options
        from datafusion_engine.tables.param import register_table_params, resolve_param_bindings

        bindings = resolve_param_bindings(params or None)
        if bindings.param_values:
            msg = "Scalar SQL parameters are not supported for streaming execution."
            raise ValueError(msg)
        with register_table_params(self.ctx, bindings):
            df = self.ctx.sql_with_options(sql, resolved_options)
        return StreamingExecutionResult(df=df)

    def from_table(
        self,
        table_name: str,
    ) -> StreamingExecutionResult:
        """Get streaming result from registered table.

        Parameters
        ----------
        table_name : str
            Name of registered table.

        Returns:
        -------
        StreamingExecutionResult
            Streaming result wrapper.

        Examples:
        --------
        >>> from datafusion_engine.session.facade import DataFusionExecutionFacade
        >>> from datafusion_engine.dataset.registry import DatasetLocation
        >>> from datafusion_engine.session.runtime import DataFusionRuntimeProfile
        >>> profile = DataFusionRuntimeProfile()
        >>> ctx = profile.session_context()
        >>> facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
        >>> facade.register_dataset(
        ...     name="events",
        ...     location=DatasetLocation(path="/data/events", format="delta"),
        ... )
        >>> result = executor.from_table("events")
        >>> for batch in result.to_batches():
        ...     process(batch)
        """
        df = self.ctx.table(table_name)
        return StreamingExecutionResult(df=df)
