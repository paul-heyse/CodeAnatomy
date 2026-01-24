"""Unified write pipeline for all DataFusion output paths.

This module provides a single writing surface with explicit format policy,
partitioning, and schema constraints, while avoiding inconsistent Ibis vs
DataFusion write behavior. Supports both COPY-based and streaming writes
with consistent semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

from sqlglot_tools.compat import exp

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.sql_policy_engine import SQLPolicyProfile


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
            "row_group_size": str(self.row_group_size),
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

    def to_copy_ast(
        self,
        profile: SQLPolicyProfile,
    ) -> exp.Copy:
        """Build COPY statement as SQLGlot AST.

        Converts this write request into a COPY TO expression using
        SQLGlot AST construction. The resulting expression can be
        rendered to dialect-specific SQL.

        Parameters
        ----------
        profile
            SQL policy profile for dialect selection.

        Returns
        -------
        exp.Copy
            SQLGlot COPY expression ready for execution.

        Notes
        -----
        This method uses `sqlglot_tools.ddl_builders.build_copy_to_ast`
        to construct the AST with proper option encoding.
        """
        from sqlglot_tools.compat import parse_one
        from sqlglot_tools.ddl_builders import build_copy_to_ast

        # Parse source if string
        if isinstance(self.source, str):
            query = parse_one(self.source, dialect=profile.read_dialect)
        else:
            query = self.source

        # Build options
        options: dict[str, object] = {"format": self.format.name}

        if self.parquet_policy:
            options.update(self.parquet_policy.to_copy_options())

        return build_copy_to_ast(
            query=query,
            path=self.destination,
            file_format=self.format.name,
            options=options,
            partition_by=self.partition_by,
        )


class WritePipeline:
    """Unified write pipeline for all output paths.

    Provides consistent write semantics across COPY, INSERT,
    and streaming Arrow writers. Chooses the most efficient
    write path based on format and request characteristics.

    Parameters
    ----------
    ctx
        DataFusion session context.
    profile
        SQL policy profile for SQL generation.

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
    ) -> None:
        """Initialize write pipeline.

        Parameters
        ----------
        ctx
            DataFusion session context.
        profile
            SQL policy profile for SQL generation.
        """
        self.ctx = ctx
        self.profile = profile

    def write_via_copy(
        self,
        request: WriteRequest,
    ) -> None:
        """Write using SQL COPY statement.

        Executes a COPY TO statement to write query results to disk.
        This is the simplest and most robust write path for DataFusion,
        but offers less control over partitioning and streaming behavior.

        Parameters
        ----------
        request
            Write request specification.

        Notes
        -----
        COPY statements are executed synchronously and fully materialize
        the result before writing. For large datasets with custom
        partitioning needs, consider `write_via_streaming`.
        """
        copy_ast = request.to_copy_ast(self.profile)
        sql = copy_ast.sql(dialect=self.profile.write_dialect)

        # Execute COPY
        self.ctx.sql(sql).collect()

        # Record artifact
        self._record_write_artifact(request, sql)

    def write_via_streaming(
        self,
        request: WriteRequest,
    ) -> None:
        """Write using streaming Arrow writer.

        Uses the streaming executor to write results without full
        materialization. Supports advanced partitioning and fine-grained
        control over output file structure.

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
        """
        from datafusion_engine.streaming_executor import StreamingExecutor

        executor = StreamingExecutor(self.ctx)

        # Get streaming result from source
        if isinstance(request.source, str):
            result = executor.execute_sql(request.source)
        else:
            sql = request.source.sql(dialect=self.profile.write_dialect)
            result = executor.execute_sql(sql)

        # Write based on format
        if request.format == WriteFormat.PARQUET:
            if request.partition_by:
                # pipe_to_dataset accepts **format_options, but these need to be
                # passed separately from the copy options which are string-based
                result.pipe_to_dataset(
                    request.destination,
                    partitioning=list(request.partition_by),
                    existing_data_behavior=self._mode_to_behavior(request.mode),
                )
            else:
                policy = request.parquet_policy or ParquetWritePolicy()
                result.pipe_to_parquet(
                    request.destination,
                    compression=policy.compression,
                    compression_level=policy.compression_level,
                    row_group_size=policy.row_group_size,
                )
        else:
            msg = f"Streaming write for {request.format}"
            raise NotImplementedError(msg)

        self._record_write_artifact(request, "streaming")

    def write(
        self,
        request: WriteRequest,
        *,
        prefer_streaming: bool = True,
    ) -> None:
        """Write using best available method.

        Chooses between COPY-based and streaming write paths based on
        format, partitioning requirements, and preference hint.

        Parameters
        ----------
        request
            Write request specification.
        prefer_streaming
            If True, prefer streaming write for PARQUET format.

        Notes
        -----
        The decision logic is:
        - Partitioned PARQUET: always use streaming
        - PARQUET with prefer_streaming: use streaming
        - All other cases: use COPY
        """
        if prefer_streaming and request.format == WriteFormat.PARQUET:
            self.write_via_streaming(request)
        else:
            self.write_via_copy(request)

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
        request: WriteRequest,
        method: str,
    ) -> None:
        """Record write operation in diagnostics.

        Parameters
        ----------
        request
            Write request that was executed.
        method
            Write method used (SQL string or "streaming").

        Notes
        -----
        Implementation depends on diagnostics infrastructure.
        Currently a no-op placeholder for future diagnostic collection.
        """
