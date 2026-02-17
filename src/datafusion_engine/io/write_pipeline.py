"""Write pipeline implementation split from write_core."""

from __future__ import annotations

import shutil
import time
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import DataFrameWriteOptions, InsertOp, SessionContext, SQLOptions
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.delta.service import DeltaFeatureMutationRequest
from datafusion_engine.delta.store_policy import apply_delta_store_policy
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.io.write_core import (
    DeltaWriteOutcome,
    DeltaWriteSpec,
    StreamingWriteContext,
    StreamingWriteOutcome,
    WriteFormat,
    WriteMethod,
    WriteMode,
    WriteRequest,
    WriteResult,
    WriteViewRequest,
    _AdaptiveFileSizeDecision,
    _apply_zorder_sort,
    _copy_format_token,
    _delta_policy_context,
    _DeltaCommitContext,
    _DeltaWriteSpecInputs,
    _require_runtime_profile,
    _stats_decision_from_policy,
)
from datafusion_engine.io.write_delta import (
    _apply_delta_check_constraints,
    _apply_explicit_delta_features,
    _apply_policy_commit_metadata,
    _delta_commit_metadata,
    _delta_feature_gate_override,
    _delta_idempotent_options,
    _delta_lineage_columns,
    _delta_maintenance_policy_override,
    _delta_mode,
    _delta_schema_mode,
    _replace_where_predicate,
    _resolve_delta_schema_policy,
    _schema_columns,
    _validate_delta_protocol_support,
    commit_metadata_from_properties,
)
from datafusion_engine.schema.contracts import delta_constraints_for_location
from datafusion_engine.sql.helpers import sql_identifier as _sql_identifier
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.tables.metadata import table_provider_metadata
from schema_spec.dataset_spec import (
    ArrowValidationOptions,
    DatasetSpec,
    validate_arrow_table,
    validation_policy_to_arrow_options,
)
from serde_artifacts import DeltaStatsDecisionEnvelope
from serde_msgspec import convert
from storage.deltalake import (
    DeltaWriteResult,
    canonical_table_uri,
    idempotent_commit_properties,
    snapshot_key_for_table,
)
from storage.deltalake.delta_write import IdempotentWriteOptions
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion_engine.lineage.diagnostics import DiagnosticsRecorder
    from datafusion_engine.obs.datafusion_runs import DataFusionRun
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from semantics.program_manifest import ManifestDatasetResolver

_RETRYABLE_DELTA_STREAM_ERROR_MARKERS: tuple[str, ...] = (
    "c data interface error",
    "expected 3 buffers for imported type string",
)


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _copy_options_clause(options: Mapping[str, str]) -> str | None:
    if not options:
        return None
    items = ", ".join(
        f"{_sql_string_literal(key)} {_sql_string_literal(value)}"
        for key, value in sorted(options.items(), key=lambda item: item[0])
    )
    return f"OPTIONS ({items})"


def _is_retryable_delta_stream_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_DELTA_STREAM_ERROR_MARKERS)


def _is_delta_observability_operation(operation: str | None) -> bool:
    if operation is None:
        return False
    return operation.startswith(
        (
            "delta_mutation_",
            "delta_snapshot_",
            "delta_scan_plan",
            "delta_maintenance_",
            "delta_observability_",
        )
    )


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

    Examples:
    --------
    >>> from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    >>> profile = DataFusionRuntimeProfile()
    >>> ctx = profile.session_context()
    >>> pipeline = WritePipeline(ctx, runtime_profile=profile)
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
        dataset_resolver: ManifestDatasetResolver | None = None,
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
        dataset_resolver
            Optional manifest-based dataset resolver.
        """
        self.ctx = ctx
        self.sql_options = sql_options
        self.recorder = recorder
        self.runtime_profile = runtime_profile
        self.dataset_resolver = dataset_resolver

    def _resolved_sql_options(self) -> SQLOptions:
        if self.sql_options is not None:
            return self.sql_options
        if self.runtime_profile is not None:
            return self.runtime_profile.sql_options()
        return sql_options_for_profile(None)

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

    @staticmethod
    def _resolve_validation_options(
        *,
        dataset_spec: DatasetSpec | None,
        overrides: DatasetLocationOverrides | None,
    ) -> ArrowValidationOptions | None:
        if overrides is not None:
            if overrides.validation is not None:
                return overrides.validation
            if overrides.dataframe_validation is not None:
                return validation_policy_to_arrow_options(overrides.dataframe_validation)
        if dataset_spec is None:
            return None
        if dataset_spec.policies.validation is not None:
            return dataset_spec.policies.validation
        return validation_policy_to_arrow_options(dataset_spec.policies.dataframe_validation)

    def _validate_dataframe(
        self,
        df: DataFrame,
        *,
        dataset_spec: DatasetSpec | None,
        overrides: DatasetLocationOverrides | None,
    ) -> None:
        if dataset_spec is None:
            return

        validation = self._resolve_validation_options(
            dataset_spec=dataset_spec,
            overrides=overrides,
        )
        if validation is None:
            return
        table = df.to_arrow_table()
        validate_arrow_table(
            table,
            spec=dataset_spec.table_spec,
            options=validation,
            runtime_profile=self.runtime_profile,
        )

    def _dataset_location_for_destination(
        self,
        destination: str,
    ) -> tuple[str, DatasetLocation] | None:
        """Resolve a dataset binding for a destination when possible.

        Returns:
        -------
        tuple[str, DatasetLocation] | None
            Dataset name and location when resolved, or ``None`` when the
            resolver is not available.
        """
        if self.runtime_profile is None:
            return None
        normalized_destination = str(destination)
        resolver = self.dataset_resolver
        if resolver is not None:
            loc = resolver.location(destination)
            if loc is not None:
                return destination, loc
            return self._match_dataset_location(
                (
                    (name, resolved)
                    for name in resolver.names()
                    if (resolved := resolver.location(name)) is not None
                ),
                normalized_destination=normalized_destination,
            )
        profile = self.runtime_profile
        candidates = dict(profile.data_sources.dataset_templates)
        candidates.update(profile.data_sources.extract_output.dataset_locations)
        loc = candidates.get(destination)
        if loc is not None:
            return destination, loc
        return self._match_dataset_location(
            candidates.items(),
            normalized_destination=normalized_destination,
        )

    def _match_dataset_location(
        self,
        candidates: Iterable[tuple[str, DatasetLocation]],
        *,
        normalized_destination: str,
    ) -> tuple[str, DatasetLocation] | None:
        """Return the first dataset location matching the destination path.

        Returns:
        -------
        tuple[str, DatasetLocation] | None
            Dataset name and location when the destination matches.
        """
        if self.runtime_profile is None:
            return None
        for name, loc in candidates:
            resolved = apply_delta_store_policy(
                loc,
                policy=self.runtime_profile.policies.delta_store_policy,
            )
            if str(resolved.path) == normalized_destination:
                return name, resolved
        return None

    def _dataset_binding(
        self,
        destination: str,
    ) -> tuple[str | None, DatasetLocation | None]:
        """Resolve dataset name and location for a destination.

        Parameters
        ----------
        destination
            Target destination path or table name.

        Returns:
        -------
        tuple[str | None, DatasetLocation | None]
            Dataset name and location, or (None, None) when unavailable.
        """
        binding = self._dataset_location_for_destination(destination)
        if binding is None:
            return None, None
        return binding

    def _prepare_streaming_context(self, request: WriteRequest) -> StreamingWriteContext:
        start = time.perf_counter()
        df = self._source_df(request)
        dataset_name, dataset_location = self._dataset_binding(request.destination)
        dataset_spec = dataset_location.dataset_spec if dataset_location is not None else None
        overrides = dataset_location.overrides if dataset_location is not None else None
        self._validate_dataframe(
            df,
            dataset_spec=dataset_spec,
            overrides=overrides,
        )
        schema_columns = _schema_columns(df)
        lineage_columns = _delta_lineage_columns(df)
        df = _apply_zorder_sort(
            df,
            request=request,
            dataset_location=dataset_location,
            schema_columns=schema_columns,
            lineage_columns=lineage_columns,
        )
        table_target = self._table_target(request)
        return StreamingWriteContext(
            request=request,
            start=start,
            df=df,
            dataset_name=dataset_name,
            dataset_location=dataset_location,
            schema_columns=schema_columns,
            lineage_columns=lineage_columns,
            table_target=table_target,
        )

    def _write_streaming_table_target(
        self,
        context: StreamingWriteContext,
    ) -> WriteResult | None:
        if context.table_target is None:
            return None
        rows_written = self._maybe_count_rows(context.df)
        sql_text = self._write_insert(
            context.df,
            request=context.request,
            table_name=context.table_target,
        )
        duration_ms = (time.perf_counter() - context.start) * 1000.0
        write_result = WriteResult(
            request=context.request,
            method=WriteMethod.INSERT,
            sql=sql_text,
            duration_ms=duration_ms,
            rows_written=rows_written,
        )
        self._record_write_artifact(write_result)
        return write_result

    def _streaming_outcome(self, context: StreamingWriteContext) -> StreamingWriteOutcome:
        from datafusion_engine.session.streaming import StreamingExecutionResult

        result = StreamingExecutionResult(df=context.df)
        if context.request.format == WriteFormat.DELTA:
            streaming_spec = self._delta_write_spec(
                context.request,
                method_label="delta_writer",
                inputs=_DeltaWriteSpecInputs(
                    dataset_name=context.dataset_name,
                    dataset_location=context.dataset_location,
                    schema_columns=context.schema_columns,
                    lineage_columns=context.lineage_columns,
                ),
            )
            delta_outcome = self._write_delta(
                result,
                request=context.request,
                spec=streaming_spec,
            )
            return StreamingWriteOutcome(
                method=WriteMethod.STREAMING,
                sql_text=None,
                rows_written=None,
                delta_outcome=delta_outcome,
            )
        rows_written = self._maybe_count_rows(context.df)
        sql_text = self._write_copy(context.df, request=context.request)
        return StreamingWriteOutcome(
            method=WriteMethod.COPY, sql_text=sql_text, rows_written=rows_written
        )

    def _finalize_streaming_result(
        self,
        context: StreamingWriteContext,
        outcome: StreamingWriteOutcome,
    ) -> WriteResult:
        duration_ms = (time.perf_counter() - context.start) * 1000.0
        delta_outcome = outcome.delta_outcome
        write_result = WriteResult(
            request=context.request,
            method=outcome.method,
            sql=outcome.sql_text,
            duration_ms=duration_ms,
            rows_written=outcome.rows_written,
            delta_result=delta_outcome.delta_result if delta_outcome is not None else None,
            delta_features=(delta_outcome.enabled_features if delta_outcome is not None else None),
            commit_app_id=delta_outcome.commit_app_id if delta_outcome is not None else None,
            commit_version=delta_outcome.commit_version if delta_outcome is not None else None,
        )
        self._record_write_artifact(write_result)
        return write_result

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

        Notes:
        -----
        This method is preferred for large datasets or when partitioning
        is required, as it allows streaming writes without full
        materialization.

        Returns:
        -------
        WriteResult
            Write result metadata for the streaming operation.
        """
        context = self._prepare_streaming_context(request)
        table_result = self._write_streaming_table_target(context)
        if table_result is not None:
            return table_result
        outcome = self._streaming_outcome(context)
        return self._finalize_streaming_result(context, outcome)

    def write(
        self,
        request: WriteRequest,
    ) -> WriteResult:
        """Write using best available method.

        Chooses between COPY-based and streaming write paths based on
        format and partitioning requirements.

        Parameters
        ----------
        request
            Write request specification.

        Returns:
        -------
        WriteResult
            Write result metadata for the executed write.

        Notes:
        -----
        The unified writer executes a DataFusion DataFrame. Delta uses
        streaming dataset writes; other formats use DataFusion-native writers.
        """
        return self.write_via_streaming(request)

    def write_view(
        self,
        request: WriteViewRequest,
    ) -> WriteResult:
        """Write a registered view using the unified pipeline.

        Parameters
        ----------
        request
            Write request specifying the registered view.

        Returns:
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
        return self.write(write_request)

    def _record_write_artifact(
        self,
        result: WriteResult,
    ) -> None:
        """Record write operation in diagnostics.

        Parameters
        ----------
        result
            Write result metadata to record.

        Notes:
        -----
        Records `write_operation` diagnostics when a recorder is configured.
        """
        if self.recorder is None:
            return
        from datafusion_engine.lineage.diagnostics import WriteRecord

        self.recorder.record_write(
            WriteRecord(
                destination=result.request.destination,
                format_=result.request.format.name.lower(),
                method=result.method.name.lower(),
                rows_written=result.rows_written,
                duration_ms=result.duration_ms or 0.0,
                sql=result.sql,
                delta_features=(
                    dict(result.delta_features) if result.delta_features is not None else None
                ),
            )
        )

    def _record_adaptive_write_policy(
        self,
        decision: _AdaptiveFileSizeDecision,
    ) -> None:
        if self.runtime_profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import ADAPTIVE_WRITE_POLICY_SPEC

        record_artifact(
            self.runtime_profile,
            ADAPTIVE_WRITE_POLICY_SPEC,
            {
                "base_target_file_size": decision.base_target_file_size,
                "adaptive_target_file_size": decision.adaptive_target_file_size,
                "estimated_rows": decision.estimated_rows,
                "reason": decision.reason,
            },
        )

    def _maybe_count_rows(self, df: DataFrame) -> int | None:
        if self.recorder is None:
            return None
        try:
            return df.count()
        except (RuntimeError, TypeError, ValueError):
            return None

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
    def _temp_view_name(prefix: str, *, request: WriteRequest) -> str:
        digest = hash_sha256_hex(f"{prefix}:{request.destination}:{id(request)}".encode())[:8]
        return f"__{prefix}_{digest}"

    def _write_copy(self, df: DataFrame, *, request: WriteRequest) -> str:
        if request.single_file_output:
            msg = "COPY does not support single_file_output."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        temp_view = self._temp_view_name("copy", request=request)
        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
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
            sql_options = sql_options_for_profile(self.runtime_profile).with_allow_statements(
                allow_statements
            )
            df_stmt = self.ctx.sql_with_options(sql, sql_options)
            if df_stmt is None:
                msg = "COPY statement did not return a DataFusion DataFrame."
                raise ValueError(msg)
            df_stmt.collect()
        finally:
            with suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_view)
        return sql

    def _write_insert(self, df: DataFrame, *, request: WriteRequest, table_name: str) -> str:
        if request.mode == WriteMode.ERROR:
            msg = "Table writes require APPEND or OVERWRITE mode."
            raise ValueError(msg)
        temp_view = self._temp_view_name("insert", request=request)
        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
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
                sql_options_for_profile(self.runtime_profile)
                .with_allow_statements(allow_statements)
                .with_allow_dml(allow_dml)
            )
            df_stmt = self.ctx.sql_with_options(sql, sql_options)
            if df_stmt is None:
                msg = "INSERT statement did not return a DataFusion DataFrame."
                raise ValueError(msg)
            df_stmt.collect()
        finally:
            with suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_view)
        return sql

    @staticmethod
    def _write_table(df: DataFrame, *, request: WriteRequest, table_name: str) -> None:
        """Write to registered table via DataFusion-native DataFrame.write_table.

        Args:
            df: Description.
            request: Description.
            table_name: Description.

        Raises:
            ValueError: If the operation cannot be completed.
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

    def _prepare_commit_metadata(
        self,
        *,
        commit_key: str,
        commit_metadata: dict[str, str],
        method_label: str,
        mode: Literal["append", "overwrite"],
        options: Mapping[str, object],
    ) -> tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]:
        """Finalize commit metadata with idempotent and runtime commit context.

        Parameters
        ----------
        commit_key
            Commit key used for idempotent reservation.
        commit_metadata
            Base commit metadata entries to extend.
        method_label
            Method label used for commit metadata.
        mode
            Delta write mode string.
        options
            Format options used to resolve idempotent settings.

        Returns:
        -------
        tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]
            Updated metadata, idempotent options, and reserved commit run.
        """
        metadata = dict(commit_metadata)
        commit_run: DataFusionRun | None = None
        idempotent = _delta_idempotent_options(options)
        if idempotent is None:
            reserved = self._reserve_runtime_commit(
                commit_key=commit_key,
                commit_metadata=metadata,
                method_label=method_label,
                mode=mode,
            )
            if reserved is not None:
                idempotent, commit_run = reserved
        if idempotent is not None:
            metadata["commit_app_id"] = idempotent.app_id
            metadata["commit_version"] = str(idempotent.version)
        if commit_run is not None:
            metadata["commit_run_id"] = commit_run.run_id
        return metadata, idempotent, commit_run

    def _delta_write_spec(
        self,
        request: WriteRequest,
        *,
        method_label: str,
        inputs: _DeltaWriteSpecInputs,
    ) -> DeltaWriteSpec:
        """Build a deterministic Delta write specification for a request.

        Parameters
        ----------
        request
            Write request for a Delta destination.
        method_label
            Write method label used for commit metadata.
        inputs
            Input context for policy resolution and lineage-derived settings.

        Returns:
        -------
        DeltaWriteSpec
            Deterministic write specification including commit properties.
        """
        options = request.format_options or {}
        mode = _delta_mode(request.mode)
        schema_policy = _resolve_delta_schema_policy(
            options,
            dataset_location=inputs.dataset_location,
        )
        maintenance_policy = _delta_maintenance_policy_override(options)
        if maintenance_policy is None:
            maintenance_policy = (
                inputs.dataset_location.delta_maintenance_policy
                if inputs.dataset_location is not None
                else None
            )
        policy_ctx = _delta_policy_context(
            options=options,
            dataset_location=inputs.dataset_location,
            request_partition_by=request.partition_by,
            schema_columns=inputs.schema_columns,
            lineage_columns=inputs.lineage_columns,
            plan_bundle=inputs.plan_bundle,
        )
        if policy_ctx.adaptive_file_size_decision is not None:
            self._record_adaptive_write_policy(policy_ctx.adaptive_file_size_decision)
        feature_gate = _delta_feature_gate_override(options)
        if feature_gate is None and inputs.dataset_location is not None:
            feature_gate = inputs.dataset_location.delta_feature_gate
        stats_decision = _stats_decision_from_policy(
            dataset_name=inputs.dataset_name or request.destination,
            policy_ctx=policy_ctx,
            lineage_columns=inputs.lineage_columns,
        )
        extra_constraints = delta_constraints_for_location(
            inputs.dataset_location,
            extra_checks=request.constraints,
        )
        commit_metadata = _delta_commit_metadata(
            request,
            options,
            context=_DeltaCommitContext(
                method_label=method_label,
                mode=mode,
                dataset_name=inputs.dataset_name,
                dataset_location=inputs.dataset_location,
            ),
        )
        commit_metadata = _apply_policy_commit_metadata(
            commit_metadata,
            policy_ctx=policy_ctx,
            extra_constraints=extra_constraints,
        )
        commit_key = inputs.dataset_name or request.destination
        commit_metadata, idempotent, commit_run = self._prepare_commit_metadata(
            commit_key=commit_key,
            commit_metadata=commit_metadata,
            method_label=method_label,
            mode=mode,
            options=options,
        )
        commit_properties = idempotent_commit_properties(
            operation="write_pipeline",
            mode=mode,
            idempotent=idempotent,
            extra_metadata=commit_metadata,
        )
        commit_metadata = commit_metadata_from_properties(commit_properties)
        commit_app_id = idempotent.app_id if idempotent is not None else None
        commit_version = idempotent.version if idempotent is not None else None
        return DeltaWriteSpec(
            table_uri=request.destination,
            mode=mode,
            method_label=method_label,
            commit_properties=commit_properties,
            commit_metadata=commit_metadata,
            commit_key=commit_key,
            dataset_location=inputs.dataset_location,
            write_policy=policy_ctx.write_policy,
            schema_policy=policy_ctx.schema_policy,
            maintenance_policy=maintenance_policy,
            partition_by=policy_ctx.partition_by,
            zorder_by=policy_ctx.zorder_by,
            enable_features=policy_ctx.enable_features,
            feature_gate=feature_gate,
            table_properties=policy_ctx.table_properties,
            target_file_size=policy_ctx.target_file_size,
            schema_mode=_delta_schema_mode(
                options,
                schema_policy=schema_policy,
            ),
            writer_properties=policy_ctx.writer_properties,
            stats_decision=stats_decision,
            commit_app_id=commit_app_id,
            commit_version=commit_version,
            commit_run=commit_run,
            storage_options=policy_ctx.storage_options,
            log_storage_options=policy_ctx.log_storage_options,
            replace_predicate=_replace_where_predicate(options),
            extra_constraints=extra_constraints,
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

        Returns:
        -------
        tuple[IdempotentWriteOptions, DataFusionRun] | None
            Idempotent write options and run metadata when reserved.
        """
        if self.runtime_profile is None:
            return None
        commit_options, commit_run = self.runtime_profile.delta_ops.reserve_delta_commit(
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
            self.runtime_profile.delta_ops.finalize_delta_commit(
                key=spec.commit_key,
                run=spec.commit_run,
                metadata=metadata,
            )
        self._persist_write_artifact(context)

    def _persist_write_artifact(self, context: _DeltaCommitFinalizeContext) -> None:
        """Persist write metadata to the plan artifact store."""
        if self.runtime_profile is None:
            return
        from datafusion_engine.plan.artifact_store_core import (
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
                delta_write_policy=spec.write_policy,
                delta_schema_policy=spec.schema_policy,
                partition_by=spec.partition_by,
                table_properties=dict(spec.table_properties),
                commit_metadata=dict(spec.commit_metadata),
                stats_decision=spec.stats_decision,
                duration_ms=context.duration_ms,
                row_count=context.row_count,
                status=context.status,
                error=context.error,
            ),
        )
        if spec.stats_decision is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_STATS_DECISION_SPEC
        from serde_msgspec import to_builtins

        envelope = DeltaStatsDecisionEnvelope(payload=spec.stats_decision)
        validated = convert(
            to_builtins(envelope, str_keys=True),
            target_type=DeltaStatsDecisionEnvelope,
            strict=True,
        )
        record_artifact(
            self.runtime_profile,
            DELTA_STATS_DECISION_SPEC,
            cast("dict[str, object]", to_builtins(validated, str_keys=True)),
        )

    def _record_delta_mutation(
        self,
        *,
        spec: DeltaWriteSpec,
        delta_result: DeltaWriteResult,
        operation: str,
        constraint_status: str,
    ) -> None:
        if self.runtime_profile is None:
            return
        operation_name = spec.commit_metadata.get("operation")
        if _is_delta_observability_operation(operation_name):
            return
        from datafusion_engine.delta.observability import (
            DeltaMutationArtifact,
            DeltaOperationReport,
            record_delta_mutation,
        )

        operation_report = DeltaOperationReport.from_payload(
            delta_result.report,
            operation=operation,
            commit_metadata=spec.commit_metadata,
        )

        commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
        record_delta_mutation(
            self.runtime_profile,
            artifact=DeltaMutationArtifact(
                table_uri=spec.table_uri,
                operation=operation,
                report=operation_report.to_payload(),
                dataset_name=spec.commit_key,
                mode=spec.mode,
                commit_metadata=spec.commit_metadata,
                commit_app_id=spec.commit_app_id,
                commit_version=spec.commit_version,
                commit_run_id=commit_run_id,
                constraint_status=constraint_status,
                constraint_violations=(),
            ),
            ctx=self.ctx,
        )

    def _run_post_write_maintenance(
        self,
        *,
        spec: DeltaWriteSpec,
        delta_version: int,
        initial_version: int | None,
        write_report: Mapping[str, object] | None,
    ) -> None:
        if self.runtime_profile is None:
            return
        from datafusion_engine.delta.maintenance import (
            DeltaMaintenancePlanInput,
            WriteOutcomeMetrics,
            build_write_outcome_metrics,
            maintenance_decision_artifact_payload,
            resolve_maintenance_from_execution,
            run_delta_maintenance,
        )
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_MAINTENANCE_DECISION_SPEC

        metrics: WriteOutcomeMetrics | None = None
        if write_report is not None:
            metrics = build_write_outcome_metrics(
                write_report,
                initial_version=initial_version,
            )
            if metrics.final_version is None:
                metrics = WriteOutcomeMetrics(
                    files_created=metrics.files_created,
                    total_file_count=metrics.total_file_count,
                    version_delta=metrics.version_delta,
                    final_version=delta_version,
                )
        elif delta_version >= 0:
            metrics = WriteOutcomeMetrics(final_version=delta_version)

        plan_input = DeltaMaintenancePlanInput(
            dataset_location=spec.dataset_location,
            table_uri=spec.table_uri,
            dataset_name=spec.commit_key,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            delta_version=delta_version,
            delta_timestamp=None,
            feature_gate=spec.feature_gate,
            policy=spec.maintenance_policy,
        )
        decision = resolve_maintenance_from_execution(
            plan_input,
            metrics=metrics,
        )
        record_artifact(
            self.runtime_profile,
            DELTA_MAINTENANCE_DECISION_SPEC,
            maintenance_decision_artifact_payload(
                decision,
                dataset_name=spec.commit_key,
            ),
        )
        plan = decision.plan
        if plan is None:
            return
        run_delta_maintenance(self.ctx, plan=plan, runtime_profile=self.runtime_profile)

    def _write_delta(
        self,
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteOutcome:
        """Write a Delta table using a deterministic write specification.

        Args:
            result: Streaming execution result payload.
            request: Write request metadata.
            spec: Resolved Delta write specification.

        Returns:
            DeltaWriteOutcome: Result.

        Raises:
            DataFusionEngineError: If Delta write or maintenance operations fail.
            ValueError: If deterministic write preconditions are violated.
        """
        runtime_profile = _require_runtime_profile(
            self.runtime_profile,
            operation="delta writes",
        )
        local_path = Path(spec.table_uri)
        delta_service = runtime_profile.delta_ops.delta_service()
        existing_version = delta_service.table_version(
            path=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if request.mode == WriteMode.ERROR and (
            local_path.exists() or existing_version is not None
        ):
            msg = f"Delta destination already exists: {spec.table_uri}"
            raise ValueError(msg)
        _validate_delta_protocol_support(
            runtime_profile=self.runtime_profile,
            delta_service=delta_service,
            table_uri=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            gate=spec.feature_gate,
        )
        _ = existing_version
        delta_result = self._write_delta_bootstrap(result, spec=spec)
        feature_request = DeltaFeatureMutationRequest(
            path=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            commit_metadata=spec.commit_metadata,
            dataset_name=spec.commit_key,
            gate=spec.feature_gate,
        )
        feature_options = delta_service.features.feature_mutation_options(feature_request)
        enabled_features = delta_service.features.enable_features(
            feature_options,
            features=spec.table_properties,
        )
        _apply_explicit_delta_features(
            spec=spec,
            delta_service=delta_service,
        )
        constraint_status = _apply_delta_check_constraints(
            spec=spec,
            delta_service=delta_service,
        )
        self._record_delta_mutation(
            spec=spec,
            delta_result=delta_result,
            operation="write",
            constraint_status=constraint_status,
        )
        if not enabled_features:
            enabled_features = dict(spec.table_properties)
        final_version = delta_service.table_version(
            path=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if final_version is None:
            if self.runtime_profile is not None:
                from datafusion_engine.lineage.diagnostics import record_artifact
                from serde_artifact_specs import DELTA_WRITE_VERSION_MISSING_SPEC

                record_artifact(
                    self.runtime_profile,
                    DELTA_WRITE_VERSION_MISSING_SPEC,
                    {
                        "event_time_unix_ms": int(time.time() * 1000),
                        "table_uri": spec.table_uri,
                        "mode": spec.mode,
                    },
                )
            msg = (
                "Committed Delta write did not resolve a table version; "
                f"table_uri={spec.table_uri} mode={spec.mode}"
            )
            raise DataFusionEngineError(msg, kind=ErrorKind.DELTA)
        if self.runtime_profile is not None:
            from datafusion_engine.delta.observability import (
                DeltaFeatureStateArtifact,
                record_delta_feature_state,
            )

            commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
            record_delta_feature_state(
                self.runtime_profile,
                artifact=DeltaFeatureStateArtifact(
                    table_uri=spec.table_uri,
                    enabled_features=enabled_features,
                    dataset_name=spec.commit_key,
                    delta_version=final_version,
                    commit_metadata=spec.commit_metadata,
                    commit_app_id=spec.commit_app_id,
                    commit_version=spec.commit_version,
                    commit_run_id=commit_run_id,
                ),
            )
        self._finalize_delta_commit(
            self._DeltaCommitFinalizeContext(
                spec=spec,
                delta_version=final_version,
            )
        )
        self._run_post_write_maintenance(
            spec=spec,
            delta_version=final_version,
            initial_version=existing_version,
            write_report=delta_result.report,
        )
        canonical_uri = canonical_table_uri(spec.table_uri)
        return DeltaWriteOutcome(
            delta_result=DeltaWriteResult(
                path=canonical_uri,
                version=final_version,
                report=delta_result.report,
                snapshot_key=snapshot_key_for_table(spec.table_uri, final_version),
            ),
            enabled_features=enabled_features,
            commit_app_id=spec.commit_app_id,
            commit_version=spec.commit_version,
        )

    def _write_delta_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult:
        from datafusion_engine.delta.control_plane_core import DeltaCommitOptions
        from datafusion_engine.delta.transactions import write_transaction
        from datafusion_engine.delta.write_ipc_payload import (
            DeltaWriteRequestOptions,
            build_delta_write_request,
        )
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_WRITE_BOOTSTRAP_SPEC
        from storage.deltalake.delta_runtime_ops import commit_metadata_from_properties
        from utils.storage_options import merged_storage_options

        table = result.df.to_arrow_table()
        storage = merged_storage_options(spec.storage_options, spec.log_storage_options)
        partition_by = list(spec.partition_by) if spec.partition_by else None
        storage_options = dict(storage) if storage else None
        commit_options = DeltaCommitOptions(
            metadata=commit_metadata_from_properties(spec.commit_properties),
            app_transaction=None,
        )
        request = build_delta_write_request(
            table_uri=spec.table_uri,
            table=table,
            options=DeltaWriteRequestOptions(
                mode=spec.mode,
                schema_mode=spec.schema_mode,
                storage_options=storage_options,
                partition_columns=partition_by,
                target_file_size=spec.target_file_size,
                extra_constraints=spec.extra_constraints,
                commit_options=commit_options,
            ),
        )
        report = write_transaction(self.ctx, request=request)
        if self.runtime_profile is not None:
            row_count = int(table.num_rows)
            record_artifact(
                self.runtime_profile,
                DELTA_WRITE_BOOTSTRAP_SPEC,
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "table_uri": spec.table_uri,
                    "mode": spec.mode,
                    "row_count": row_count,
                },
            )
        return DeltaWriteResult(
            path=canonical_table_uri(spec.table_uri),
            version=None,
            report=report,
        )

    @staticmethod
    def _delta_insert_table_name(spec: DeltaWriteSpec) -> str:
        base = spec.commit_key or "delta_write"
        normalized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in base)
        digest = hash_sha256_hex(spec.table_uri.encode("utf-8"))[:8]
        return f"{normalized}_{digest}"

    def _register_delta_insert_target(self, spec: DeltaWriteSpec, *, table_name: str) -> None:
        from datafusion_engine.dataset.resolution import (
            DatasetResolutionRequest,
            resolve_dataset_provider,
        )
        from datafusion_engine.io.adapter import DataFusionIOAdapter
        from datafusion_engine.tables.metadata import TableProviderCapsule

        location = spec.dataset_location
        if location is None:
            overrides = None
            if spec.feature_gate is not None:
                from schema_spec.dataset_spec import DeltaPolicyBundle

                overrides = DatasetLocationOverrides(
                    delta=DeltaPolicyBundle(feature_gate=spec.feature_gate)
                )
            location = DatasetLocation(
                path=spec.table_uri,
                format="delta",
                storage_options=dict(spec.storage_options or {}),
                delta_log_storage_options=dict(spec.log_storage_options or {}),
                overrides=overrides,
            )
        if self.runtime_profile is not None:
            location = apply_delta_store_policy(
                location, policy=self.runtime_profile.policies.delta_store_policy
            )
        resolution = resolve_dataset_provider(
            DatasetResolutionRequest(
                ctx=self.ctx,
                location=location,
                runtime_profile=self.runtime_profile,
                name=table_name,
            )
        )
        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
        adapter.register_table(
            table_name,
            TableProviderCapsule(resolution.provider),
            overwrite=True,
        )

    def _write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write CSV via DataFusion-native DataFrame writer.

        Args:
            df: Description.
            request: Description.

        Raises:
            ValueError: If the operation cannot be completed.
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

        Args:
            df: Description.
            request: Description.

        Raises:
            ValueError: If the operation cannot be completed.
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
        metadata = table_provider_metadata(self.ctx, table_name=target)
        if metadata is None:
            return None
        if metadata.supports_insert is False:
            return None
        return target
