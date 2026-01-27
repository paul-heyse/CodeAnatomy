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
import shutil
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from datafusion import DataFrameWriteOptions, InsertOp, SQLOptions
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_delta_constraints,
    resolve_delta_schema_policy,
    resolve_delta_write_policy,
)
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    delta_table_version,
    enable_delta_features,
    idempotent_commit_properties,
    write_delta_table,
)
from storage.deltalake.config import delta_schema_configuration, delta_write_configuration
from storage.deltalake.delta import DEFAULT_DELTA_FEATURE_PROPERTIES, IdempotentWriteOptions

if TYPE_CHECKING:
    from datafusion import SessionContext
    from deltalake import CommitProperties

    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.streaming_executor import StreamingExecutionResult
    from obs.datafusion_runs import DataFusionRun
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


def _merge_constraints(
    primary: tuple[str, ...],
    secondary: tuple[str, ...],
) -> tuple[str, ...]:
    """Merge constraint tuples deterministically while removing blanks.

    Returns
    -------
    tuple[str, ...]
        Deduplicated constraint strings in deterministic order.
    """
    if not primary and not secondary:
        return ()
    ordered: dict[str, None] = {}
    for constraint_text in primary:
        normalized = constraint_text.strip()
        if normalized:
            ordered.setdefault(normalized, None)
    for constraint_text in secondary:
        normalized = constraint_text.strip()
        if normalized:
            ordered.setdefault(normalized, None)
    return tuple(ordered)


@dataclass(frozen=True)
class _DeltaPolicyContext:
    table_properties: dict[str, str]
    target_file_size: int | None
    storage_options: dict[str, str] | None
    log_storage_options: dict[str, str] | None


@dataclass(frozen=True)
class _DeltaCommitContext:
    method_label: str
    mode: str
    dataset_name: str | None = None
    dataset_location: DatasetLocation | None = None


def _delta_policy_context(
    *,
    options: Mapping[str, object],
    dataset_location: DatasetLocation | None,
) -> _DeltaPolicyContext:
    write_policy = resolve_delta_write_policy(dataset_location) if dataset_location else None
    schema_policy = resolve_delta_schema_policy(dataset_location) if dataset_location else None
    table_properties = _delta_table_properties(options)
    policy_props = delta_write_configuration(write_policy)
    if policy_props:
        table_properties.update(policy_props)
    schema_props = delta_schema_configuration(schema_policy)
    if schema_props:
        table_properties.update(schema_props)
    policy_target_file_size = write_policy.target_file_size if write_policy is not None else None
    target_file_size = _delta_target_file_size(
        options,
        fallback=policy_target_file_size,
    )
    storage_options, log_storage_options = _delta_storage_options(
        options,
        dataset_location=dataset_location,
    )
    return _DeltaPolicyContext(
        table_properties=table_properties,
        target_file_size=target_file_size,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )


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
    delta_result: DeltaWriteResult | None = None
    delta_features: Mapping[str, str] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None


@dataclass(frozen=True)
class DeltaWriteSpec:
    """Declarative specification for deterministic Delta writes."""

    table_uri: str
    mode: Literal["append", "overwrite"]
    method_label: str
    commit_properties: CommitProperties
    commit_metadata: Mapping[str, str]
    commit_key: str
    partition_by: tuple[str, ...] = ()
    table_properties: Mapping[str, str] = field(default_factory=dict)
    target_file_size: int | None = None
    schema_mode: Literal["merge", "overwrite"] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run: DataFusionRun | None = None
    storage_options: Mapping[str, str] | None = None
    log_storage_options: Mapping[str, str] | None = None


@dataclass(frozen=True)
class DeltaWriteOutcome:
    """Write outcome metadata for Delta writes."""

    delta_result: DeltaWriteResult
    enabled_features: Mapping[str, str]
    commit_app_id: str | None = None
    commit_version: int | None = None


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

    def _dataset_location_for_destination(
        self,
        destination: str,
    ) -> tuple[str, DatasetLocation] | None:
        """Resolve a dataset binding for a destination when possible.

        Returns
        -------
        tuple[str, DatasetLocation] | None
            Dataset name and location when resolved.
        """
        if self.runtime_profile is None:
            return None
        location = self.runtime_profile.dataset_location(destination)
        if location is not None:
            return destination, location
        normalized_destination = str(destination)
        for name, loc in sorted(self.runtime_profile.extract_dataset_locations.items()):
            if str(loc.path) == normalized_destination:
                return name, loc
        for name, loc in sorted(self.runtime_profile.scip_dataset_locations.items()):
            if str(loc.path) == normalized_destination:
                return name, loc
        for catalog in self.runtime_profile.registry_catalogs.values():
            for name in catalog.names():
                loc = catalog.get(name)
                if str(loc.path) == normalized_destination:
                    return name, loc
        return None

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
        dataset_binding = self._dataset_location_for_destination(request.destination)
        dataset_name = dataset_binding[0] if dataset_binding is not None else None
        dataset_location = dataset_binding[1] if dataset_binding is not None else None
        dataset_constraints = (
            resolve_delta_constraints(dataset_location)
            if dataset_location is not None and request.format == WriteFormat.DELTA
            else ()
        )
        combined_constraints = _merge_constraints(request.constraints, dataset_constraints)
        if combined_constraints:
            self._validate_constraints(df=df, constraints=combined_constraints)
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

        delta_outcome: DeltaWriteOutcome | None = None
        if (
            request.format == WriteFormat.DELTA
            and not request.partition_by
            and request.mode != WriteMode.ERROR
        ):
            provider_spec = self._delta_write_spec(
                request,
                method_label="provider_insert",
                dataset_name=dataset_name,
                dataset_location=dataset_location,
            )
            delta_outcome = self._try_write_delta_provider(
                df,
                request=request,
                spec=provider_spec,
            )
            if delta_outcome is not None:
                duration_ms = (time.perf_counter() - start) * 1000.0
                write_result = WriteResult(
                    request=request,
                    method=WriteMethod.INSERT,
                    sql=None,
                    duration_ms=duration_ms,
                    delta_result=delta_outcome.delta_result,
                    delta_features=delta_outcome.enabled_features,
                    commit_app_id=delta_outcome.commit_app_id,
                    commit_version=delta_outcome.commit_version,
                )
                self._record_write_artifact(write_result)
                return write_result

        # Write based on format
        if request.format == WriteFormat.DELTA:
            streaming_spec = self._delta_write_spec(
                request,
                method_label="delta_writer",
                dataset_name=dataset_name,
                dataset_location=dataset_location,
            )
            delta_outcome = self._write_delta(
                result,
                request=request,
                spec=streaming_spec,
            )
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
            delta_result=delta_outcome.delta_result if delta_outcome is not None else None,
            delta_features=(delta_outcome.enabled_features if delta_outcome is not None else None),
            commit_app_id=delta_outcome.commit_app_id if delta_outcome is not None else None,
            commit_version=delta_outcome.commit_version if delta_outcome is not None else None,
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

    def _delta_write_spec(
        self,
        request: WriteRequest,
        *,
        method_label: str,
        dataset_name: str | None = None,
        dataset_location: DatasetLocation | None = None,
    ) -> DeltaWriteSpec:
        """Build a deterministic Delta write specification for a request.

        Parameters
        ----------
        request
            Write request for a Delta destination.
        method_label
            Write method label used for commit metadata.
        dataset_name
            Optional dataset name for commit metadata and keying.
        dataset_location
            Optional dataset location for policy-derived defaults.

        Returns
        -------
        DeltaWriteSpec
            Deterministic write specification including commit properties.
        """
        options = request.format_options or {}
        mode = _delta_mode(request.mode)
        policy_ctx = _delta_policy_context(
            options=options,
            dataset_location=dataset_location,
        )
        commit_metadata = _delta_commit_metadata(
            request,
            options,
            context=_DeltaCommitContext(
                method_label=method_label,
                mode=mode,
                dataset_name=dataset_name,
                dataset_location=dataset_location,
            ),
        )
        commit_key = dataset_name or request.destination
        commit_run: DataFusionRun | None = None
        idempotent = _delta_idempotent_options(options)
        if idempotent is None:
            reserved = self._reserve_runtime_commit(
                commit_key=commit_key,
                commit_metadata=commit_metadata,
                method_label=method_label,
                mode=mode,
            )
            if reserved is not None:
                idempotent, commit_run = reserved
        if idempotent is not None:
            commit_metadata["commit_app_id"] = idempotent.app_id
            commit_metadata["commit_version"] = str(idempotent.version)
        if commit_run is not None:
            commit_metadata["commit_run_id"] = commit_run.run_id
        commit_properties = idempotent_commit_properties(
            operation="write_pipeline",
            mode=mode,
            idempotent=idempotent,
            extra_metadata=commit_metadata,
        )
        commit_metadata = _commit_metadata_from_properties(commit_properties)
        commit_app_id = idempotent.app_id if idempotent is not None else None
        commit_version = idempotent.version if idempotent is not None else None
        return DeltaWriteSpec(
            table_uri=request.destination,
            mode=mode,
            method_label=method_label,
            commit_properties=commit_properties,
            commit_metadata=commit_metadata,
            commit_key=commit_key,
            partition_by=request.partition_by,
            table_properties=policy_ctx.table_properties,
            target_file_size=policy_ctx.target_file_size,
            schema_mode=_delta_schema_mode(options),
            commit_app_id=commit_app_id,
            commit_version=commit_version,
            commit_run=commit_run,
            storage_options=policy_ctx.storage_options,
            log_storage_options=policy_ctx.log_storage_options,
        )

    def _reserve_runtime_commit(
        self,
        *,
        commit_key: str,
        commit_metadata: Mapping[str, str],
        method_label: str,
        mode: Literal["append", "overwrite"],
    ) -> tuple[IdempotentWriteOptions, DataFusionRun] | None:
        """Reserve an idempotent Delta commit from the runtime profile.

        Returns
        -------
        tuple[IdempotentWriteOptions, DataFusionRun] | None
            Idempotent write options and run metadata when reserved.
        """
        if self.runtime_profile is None:
            return None
        commit_options, commit_run = self.runtime_profile.reserve_delta_commit(
            key=commit_key,
            metadata={
                "destination": commit_key,
                "method": method_label,
                "mode": mode,
                "format": "delta",
            },
            commit_metadata=commit_metadata,
        )
        return commit_options, commit_run

    @dataclass(frozen=True)
    class _DeltaCommitFinalizeContext:
        spec: DeltaWriteSpec
        delta_version: int
        duration_ms: float | None = None
        row_count: int | None = None
        status: str = "ok"
        error: str | None = None

    def _finalize_delta_commit(self, context: _DeltaCommitFinalizeContext) -> None:
        """Finalize a reserved idempotent Delta commit when present.

        Also persists write metadata to the plan artifact store when enabled.
        """
        if self.runtime_profile is None:
            return
        spec = context.spec
        if spec.commit_run is not None:
            metadata: dict[str, object] = {
                "destination": spec.table_uri,
                "method": spec.method_label,
                "mode": spec.mode,
                "delta_version": context.delta_version,
            }
            if spec.commit_app_id is not None:
                metadata["commit_app_id"] = spec.commit_app_id
            if spec.commit_version is not None:
                metadata["commit_version"] = spec.commit_version
            self.runtime_profile.finalize_delta_commit(
                key=spec.commit_key,
                run=spec.commit_run,
                metadata=metadata,
            )
        self._persist_write_artifact(context)

    def _persist_write_artifact(self, context: _DeltaCommitFinalizeContext) -> None:
        """Persist write metadata to the plan artifact store."""
        if self.runtime_profile is None:
            return
        from datafusion_engine.plan_artifact_store import (
            WriteArtifactRequest,
            persist_write_artifact,
        )

        spec = context.spec
        commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
        persist_write_artifact(
            self.runtime_profile,
            request=WriteArtifactRequest(
                destination=spec.commit_key,
                write_format="delta",
                mode=spec.mode,
                method=spec.method_label,
                table_uri=spec.table_uri,
                delta_version=context.delta_version,
                commit_app_id=spec.commit_app_id,
                commit_version=spec.commit_version,
                commit_run_id=commit_run_id,
                partition_by=spec.partition_by,
                table_properties=dict(spec.table_properties),
                commit_metadata=dict(spec.commit_metadata),
                duration_ms=context.duration_ms,
                row_count=context.row_count,
                status=context.status,
                error=context.error,
            ),
        )

    def _write_delta(
        self,
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteOutcome:
        """Write a Delta table using a deterministic write specification.

        Parameters
        ----------
        result
            Streaming execution result with Arrow stream access.
        request
            Write request specification.
        spec
            Delta write specification with commit and table properties.

        Returns
        -------
        DeltaWriteOutcome
            Delta write outcome including version and enabled feature metadata.

        Raises
        ------
        ValueError
            Raised when ERROR mode encounters an existing destination.
        RuntimeError
            Raised when the Delta version cannot be resolved after writing.
        """
        local_path = Path(spec.table_uri)
        existing_version = delta_table_version(
            spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if request.mode == WriteMode.ERROR and (local_path.exists() or existing_version is not None):
            msg = f"Delta destination already exists: {spec.table_uri}"
            raise ValueError(msg)
        delta_options = DeltaWriteOptions(
            mode=spec.mode,
            schema_mode=spec.schema_mode,
            partition_by=spec.partition_by,
            configuration=spec.table_properties,
            commit_properties=spec.commit_properties,
            commit_metadata=spec.commit_metadata,
            target_file_size=spec.target_file_size,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        delta_result = write_delta_table(
            result.to_arrow_stream(),
            spec.table_uri,
            options=delta_options,
        )
        enabled_features = enable_delta_features(
            spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            features=spec.table_properties,
            commit_metadata=spec.commit_metadata,
        )
        if not enabled_features:
            enabled_features = dict(spec.table_properties)
        final_version = delta_table_version(
            spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if final_version is None:
            final_version = delta_result.version
        if final_version is None:
            msg = f"Failed to resolve Delta version after write: {spec.table_uri}"
            raise RuntimeError(msg)
        self._finalize_delta_commit(
            self._DeltaCommitFinalizeContext(
                spec=spec,
                delta_version=final_version,
            )
        )
        return DeltaWriteOutcome(
            delta_result=DeltaWriteResult(path=spec.table_uri, version=final_version),
            enabled_features=enabled_features,
            commit_app_id=spec.commit_app_id,
            commit_version=spec.commit_version,
        )

    def _try_write_delta_provider(
        self,
        df: DataFrame,
        *,
        request: WriteRequest,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteOutcome | None:
        if request.partition_by:
            return None
        if request.mode == WriteMode.ERROR:
            return None
        existing_version = delta_table_version(
            spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if existing_version is None:
            return None
        try:
            from datafusion_engine.delta_control_plane import (
                DeltaProviderRequest,
                delta_provider_from_session,
            )
        except ImportError:
            return None
        temp_name = request.table_name or f"__delta_write_{uuid.uuid4().hex}"
        storage_options = _delta_storage_payload(
            spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        try:
            bundle = delta_provider_from_session(
                self.ctx,
                request=DeltaProviderRequest(
                    table_uri=spec.table_uri,
                    storage_options=storage_options,
                    version=existing_version,
                    timestamp=None,
                    delta_scan=None,
                ),
            )
        except (RuntimeError, TypeError, ValueError):
            return None
        provider = bundle.provider
        from datafusion_engine.io_adapter import DataFusionIOAdapter
        from datafusion_engine.table_provider_capsule import TableProviderCapsule

        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
        adapter.register_delta_table_provider(
            temp_name,
            TableProviderCapsule(provider),
            overwrite=True,
        )
        try:
            self._write_table(df, request=request, table_name=temp_name)
        finally:
            deregister = getattr(self.ctx, "deregister_table", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister(temp_name)
        enabled_features = enable_delta_features(
            spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            features=spec.table_properties,
            commit_metadata=spec.commit_metadata,
        )
        if not enabled_features:
            enabled_features = dict(spec.table_properties)
        final_version = delta_table_version(
            spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if final_version is None:
            msg = f"Failed to resolve Delta version after provider write: {spec.table_uri}"
            raise RuntimeError(msg)
        self._finalize_delta_commit(
            self._DeltaCommitFinalizeContext(
                spec=spec,
                delta_version=final_version,
            )
        )
        return DeltaWriteOutcome(
            delta_result=DeltaWriteResult(path=spec.table_uri, version=final_version),
            enabled_features=enabled_features,
            commit_app_id=spec.commit_app_id,
            commit_version=spec.commit_version,
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


def _delta_table_properties(options: Mapping[str, object]) -> dict[str, str]:
    properties: dict[str, str] = {}
    table_props = _string_mapping(options.get("table_properties"))
    if table_props is None:
        table_props = _string_mapping(options.get("delta_table_properties"))
    if table_props:
        properties.update(table_props)
    for key, value in options.items():
        key_str = str(key)
        if not key_str.startswith("delta.") or value is None:
            continue
        properties[key_str] = str(value)
    properties.update(DEFAULT_DELTA_FEATURE_PROPERTIES)
    return properties


def _delta_target_file_size(
    options: Mapping[str, object],
    *,
    fallback: int | None = None,
) -> int | None:
    value = options.get("target_file_size")
    if isinstance(value, int) and value > 0:
        return value
    alt_value = options.get("delta_target_file_size")
    if isinstance(alt_value, int) and alt_value > 0:
        return alt_value
    if isinstance(fallback, int) and fallback > 0:
        return fallback
    return None


def _delta_schema_mode(options: Mapping[str, object]) -> Literal["merge", "overwrite"] | None:
    value = options.get("schema_mode")
    if value is None:
        value = options.get("delta_schema_mode")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "merge":
            return "merge"
        if normalized == "overwrite":
            return "overwrite"
    return None


def _delta_storage_options(
    options: Mapping[str, object],
    *,
    dataset_location: DatasetLocation | None,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    storage_options: dict[str, str] = {}
    log_storage_options: dict[str, str] = {}
    if dataset_location is not None:
        loc_storage = _string_mapping(dataset_location.storage_options)
        if loc_storage:
            storage_options.update(loc_storage)
        loc_log_storage = _string_mapping(dataset_location.delta_log_storage_options)
        if loc_log_storage:
            log_storage_options.update(loc_log_storage)
    option_storage = _string_mapping(options.get("storage_options"))
    if option_storage:
        storage_options.update(option_storage)
    option_log_storage = _string_mapping(options.get("log_storage_options"))
    if option_log_storage:
        log_storage_options.update(option_log_storage)
    if not log_storage_options and storage_options:
        log_storage_options = dict(storage_options)
    return storage_options or None, log_storage_options or None


def _delta_commit_metadata(
    request: WriteRequest,
    options: Mapping[str, object],
    *,
    context: _DeltaCommitContext,
) -> dict[str, str]:
    metadata: dict[str, str] = {
        "engine": "datafusion",
        "operation": "write_pipeline",
        "method": context.method_label,
        "mode": context.mode,
        "format": request.format.name.lower(),
        "destination": request.destination,
    }
    if context.dataset_name:
        metadata["dataset_name"] = context.dataset_name
    if context.dataset_location is not None:
        metadata["dataset_path"] = str(context.dataset_location.path)
        if context.dataset_location.delta_version is not None:
            metadata["delta_version_pin"] = str(context.dataset_location.delta_version)
        if context.dataset_location.delta_timestamp is not None:
            metadata["delta_timestamp_pin"] = context.dataset_location.delta_timestamp
    if request.partition_by:
        metadata["partition_by"] = ",".join(request.partition_by)
    user_meta = _string_mapping(options.get("commit_metadata"))
    if user_meta is None:
        user_meta = _string_mapping(options.get("delta_commit_metadata"))
    if user_meta:
        metadata.update(user_meta)
    return metadata


def _delta_idempotent_options(options: Mapping[str, object]) -> IdempotentWriteOptions | None:
    app_id = options.get("app_id")
    version = options.get("version")
    idempotent = options.get("idempotent")
    if isinstance(idempotent, Mapping):
        if app_id is None:
            app_id = idempotent.get("app_id")
        if version is None:
            version = idempotent.get("version")
    if not isinstance(app_id, str):
        return None
    normalized_app_id = app_id.strip()
    if not normalized_app_id:
        return None
    if not isinstance(version, int) or version < 0:
        return None
    return IdempotentWriteOptions(app_id=normalized_app_id, version=version)


def _commit_metadata_from_properties(commit_properties: CommitProperties) -> dict[str, str]:
    custom_metadata = getattr(commit_properties, "custom_metadata", None)
    if not isinstance(custom_metadata, Mapping):
        return {}
    return {str(key): str(value) for key, value in custom_metadata.items()}


def _string_mapping(value: object | None) -> dict[str, str] | None:
    if not isinstance(value, Mapping):
        return None
    resolved = {str(key): str(item) for key, item in value.items() if item is not None}
    return resolved or None


def _delta_storage_payload(
    storage_options: Mapping[str, str] | None,
    *,
    log_storage_options: Mapping[str, str] | None,
) -> dict[str, str] | None:
    merged: dict[str, str] = {}
    if storage_options:
        merged.update(storage_options)
    if log_storage_options:
        merged.update(log_storage_options)
    if not merged:
        return None
    return merged


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
