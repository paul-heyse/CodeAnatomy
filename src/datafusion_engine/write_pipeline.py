"""Unified write pipeline for all DataFusion output paths.

This module provides a single writing surface with explicit format policy,
partitioning, and schema constraints, while avoiding inconsistent Ibis vs
DataFusion write behavior. Supports both COPY-based and streaming writes
with consistent semantics.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from datafusion import SQLOptions

from datafusion_engine.compile_options import DataFusionSqlPolicy
from sqlglot_tools.compat import exp

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile
    from schema_spec.policies import DataFusionWritePolicy, ParquetColumnPolicy


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
        if self.format_options:
            options.update(self.format_options)

        if self.parquet_policy:
            options.update(self.parquet_policy.to_copy_options())

        return build_copy_to_ast(
            query=query,
            path=self.destination,
            file_format=self.format.name,
            options=options,
            partition_by=self.partition_by,
        )


@dataclass(frozen=True)
class WriteResult:
    """Result of a write operation."""

    request: WriteRequest
    method: WriteMethod
    sql: str | None
    duration_ms: float | None = None


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
        return DataFusionSqlPolicy(allow_dml=True, allow_statements=True).to_sql_options()

    def write_via_copy(
        self,
        request: WriteRequest,
    ) -> WriteResult:
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

        Returns
        -------
        WriteResult
            Write result metadata for the COPY operation.
        """
        start = time.perf_counter()
        sql, df = self.copy_dataframe(request)
        df.collect()
        duration_ms = (time.perf_counter() - start) * 1000.0
        result = WriteResult(
            request=request,
            method=WriteMethod.COPY,
            sql=sql,
            duration_ms=duration_ms,
        )
        self._record_write_artifact(result)
        return result

    def copy_dataframe(
        self,
        request: WriteRequest,
    ) -> tuple[str, DataFrame]:
        """Return the COPY statement SQL and DataFrame without collecting.

        Parameters
        ----------
        request
            Write request specification.

        Returns
        -------
        tuple[str, DataFrame]
            COPY statement SQL text and DataFusion DataFrame.
        """
        copy_ast = request.to_copy_ast(self.profile)
        sql = copy_ast.sql(dialect=self.profile.write_dialect)
        df = self.ctx.sql_with_options(sql, self._resolved_sql_options())
        return sql, df

    def write_via_streaming(
        self,
        request: WriteRequest,
    ) -> WriteResult:
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

        Returns
        -------
        WriteResult
            Write result metadata for the streaming operation.
        """
        from datafusion_engine.streaming_executor import StreamingExecutor

        start = time.perf_counter()
        executor = StreamingExecutor(self.ctx, sql_options=self._resolved_sql_options())
        sql = self._source_sql(request)
        result = executor.execute_sql(sql)

        # Write based on format
        if request.format == WriteFormat.PARQUET:
            if request.partition_by:
                policy = request.parquet_policy or ParquetWritePolicy()
                # pipe_to_dataset accepts **format_options, but these need to be
                # passed separately from the copy options which are string-based
                dataset_options: dict[str, Any] = dict(policy.to_dataset_options())
                if request.format_options:
                    dataset_options.update(request.format_options)
                result.pipe_to_dataset(
                    request.destination,
                    partitioning=list(request.partition_by),
                    existing_data_behavior=self._mode_to_behavior(request.mode),
                    max_rows_per_group=policy.row_group_size,
                    **dataset_options,
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

        duration_ms = (time.perf_counter() - start) * 1000.0
        write_result = WriteResult(
            request=request,
            method=WriteMethod.STREAMING,
            sql=sql,
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
        The decision logic is:
        - Partitioned PARQUET: always use streaming
        - PARQUET with prefer_streaming: use streaming
        - All other cases: use COPY
        """
        if request.format == WriteFormat.PARQUET:
            if request.partition_by:
                return self.write_via_streaming(request)
            if prefer_streaming and request.single_file_output is not False:
                return self.write_via_streaming(request)
        return self.write_via_copy(request)

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
        self.recorder.record_write(
            destination=result.request.destination,
            format_=result.request.format.name.lower(),
            method=result.method.name.lower(),
            duration_ms=result.duration_ms or 0.0,
        )

    def _source_sql(self, request: WriteRequest) -> str:
        if isinstance(request.source, str):
            return request.source
        return request.source.sql(dialect=self.profile.write_dialect)


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
            for key, value in (
                (key, _option_value_to_str(value)) for key, value in payload.items()
            )
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
