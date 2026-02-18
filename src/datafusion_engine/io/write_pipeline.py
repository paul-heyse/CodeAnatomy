"""Write pipeline implementation split from write_core."""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from datafusion import SessionContext, SQLOptions
from datafusion.dataframe import DataFrame

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.delta.store_policy import apply_delta_store_policy
from datafusion_engine.io import delta_write_handler, format_write_handler
from datafusion_engine.io.write_core import (
    DeltaWriteOutcome,
    DeltaWriteSpec,
    StreamingWriteContext,
    StreamingWriteOutcome,
    WriteFormat,
    WriteMethod,
    WriteRequest,
    WriteResult,
    WriteViewRequest,
    _AdaptiveFileSizeDecision,
    _apply_zorder_sort,
    _DeltaWriteSpecInputs,
)
from datafusion_engine.io.write_delta import (
    _delta_lineage_columns,
    _schema_columns,
)
from datafusion_engine.sql.options import sql_options_for_profile
from schema_spec.dataset_spec import (
    ArrowValidationOptions,
    DatasetSpec,
    validate_arrow_table,
    validation_policy_to_arrow_options,
)
from storage.deltalake import (
    DeltaWriteResult,
)
from storage.deltalake.delta_write import IdempotentWriteOptions

if TYPE_CHECKING:
    from datafusion_engine.delta.observability import DeltaOperationReport
    from datafusion_engine.lineage.diagnostics import DiagnosticsRecorder
    from datafusion_engine.obs.datafusion_runs import DataFusionRun
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from semantics.program_manifest import ManifestDatasetResolver

_RETRYABLE_DELTA_STREAM_ERROR_MARKERS: tuple[str, ...] = (
    "c data interface error",
    "expected 3 buffers for imported type string",
)


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
    """Unified write pipeline for DataFusion output paths."""

    def __init__(
        self,
        ctx: SessionContext,
        *,
        sql_options: SQLOptions | None = None,
        recorder: DiagnosticsRecorder | None = None,
        runtime_profile: DataFusionRuntimeProfile | None = None,
        dataset_resolver: ManifestDatasetResolver | None = None,
    ) -> None:
        """Initialize write pipeline."""
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
        from datafusion_engine.session.streaming import as_record_batch_reader

        reader = as_record_batch_reader(df)
        return any(batch.num_rows > 0 for batch in reader)

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
        from datafusion_engine.session.streaming import as_record_batch_reader

        reader = as_record_batch_reader(df)
        validate_arrow_table(
            cast("TableLike", reader),
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
        candidates = dict(profile.dataset_candidates(destination))
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
        """Return first dataset location matching destination path."""
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
        """Resolve dataset name/location pair for destination.

        Returns:
            tuple[str | None, DatasetLocation | None]: Dataset name/location, or `(None, None)`.
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
        """Write using DataFusion-native streaming and writer APIs.

        Returns:
            WriteResult: Write artifact describing the executed operation.
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
        """Write using the best available method for the request.

        Returns:
            WriteResult: Write artifact describing the executed operation.
        """
        return self.write_via_streaming(request)

    def write_view(
        self,
        request: WriteViewRequest,
    ) -> WriteResult:
        """Write a registered view using the unified pipeline.

        Returns:
            WriteResult: Write artifact describing the executed operation.
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
        """Record write operation in diagnostics."""
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
        return format_write_handler.prepare_destination(request)

    @staticmethod
    def _temp_view_name(prefix: str, *, request: WriteRequest) -> str:
        return format_write_handler.temp_view_name(prefix, request=request)

    def _write_copy(self, df: DataFrame, *, request: WriteRequest) -> str:
        return format_write_handler.write_copy(self, df, request=request)

    def _write_insert(self, df: DataFrame, *, request: WriteRequest, table_name: str) -> str:
        return format_write_handler.write_insert(
            self,
            df,
            request=request,
            table_name=table_name,
        )

    @staticmethod
    def _write_table(df: DataFrame, *, request: WriteRequest, table_name: str) -> None:
        format_write_handler.write_table(df, request=request, table_name=table_name)

    def _prepare_commit_metadata(
        self,
        *,
        commit_key: str,
        commit_metadata: dict[str, str],
        method_label: str,
        mode: Literal["append", "overwrite"],
        options: Mapping[str, object],
    ) -> tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]:
        return delta_write_handler.prepare_commit_metadata(
            self,
            commit_key=commit_key,
            commit_metadata=commit_metadata,
            method_label=method_label,
            mode=mode,
            options=options,
        )

    def _delta_write_spec(
        self,
        request: WriteRequest,
        *,
        method_label: str,
        inputs: _DeltaWriteSpecInputs,
    ) -> DeltaWriteSpec:
        return delta_write_handler.delta_write_spec(
            self,
            request,
            method_label=method_label,
            inputs=inputs,
        )

    def _reserve_runtime_commit(
        self,
        *,
        commit_key: str,
        commit_metadata: Mapping[str, str],
        method_label: str,
        mode: Literal["append", "overwrite"],
    ) -> tuple[IdempotentWriteOptions, DataFusionRun] | None:
        return delta_write_handler.reserve_runtime_commit(
            self,
            commit_key=commit_key,
            commit_metadata=commit_metadata,
            method_label=method_label,
            mode=mode,
        )

    @dataclass(frozen=True)
    class _DeltaCommitFinalizeContext:
        spec: DeltaWriteSpec
        delta_version: int
        duration_ms: float | None = None
        row_count: int | None = None
        status: str = "ok"
        error: str | None = None

    def _finalize_delta_commit(self, context: _DeltaCommitFinalizeContext) -> None:
        from datafusion_engine.io import delta_write_handler

        delta_write_handler.finalize_delta_commit(self, context)

    def _persist_write_artifact(self, context: _DeltaCommitFinalizeContext) -> None:
        from datafusion_engine.io import delta_write_handler

        delta_write_handler.persist_write_artifact(self, context)

    def _record_delta_mutation(
        self,
        *,
        spec: DeltaWriteSpec,
        delta_result: DeltaWriteResult,
        operation: str,
        constraint_status: str,
    ) -> None:
        from datafusion_engine.io import delta_write_handler

        delta_write_handler.record_delta_mutation(
            self,
            spec=spec,
            delta_result=delta_result,
            operation=operation,
            constraint_status=constraint_status,
        )

    def _run_post_write_maintenance(
        self,
        *,
        spec: DeltaWriteSpec,
        delta_version: int,
        initial_version: int | None,
        write_report: DeltaOperationReport | None,
    ) -> None:
        from datafusion_engine.io import delta_write_handler

        delta_write_handler.run_post_write_maintenance(
            self,
            spec=spec,
            delta_version=delta_version,
            initial_version=initial_version,
            write_report=write_report,
        )

    def _write_delta(
        self,
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteOutcome:
        from datafusion_engine.io import delta_write_handler

        return delta_write_handler.write_delta(self, result, request=request, spec=spec)

    def _write_delta_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult:
        from datafusion_engine.io import delta_write_handler

        return delta_write_handler.write_delta_bootstrap(self, result, spec=spec)

    @staticmethod
    def _delta_insert_table_name(spec: DeltaWriteSpec) -> str:
        from datafusion_engine.io import delta_write_handler

        return delta_write_handler.delta_insert_table_name(spec)

    def _register_delta_insert_target(self, spec: DeltaWriteSpec, *, table_name: str) -> None:
        from datafusion_engine.io import delta_write_handler

        delta_write_handler.register_delta_insert_target(self, spec, table_name=table_name)

    def _write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        format_write_handler.write_csv(self, df, request=request)

    def _write_json(self, df: DataFrame, *, request: WriteRequest) -> None:
        format_write_handler.write_json(self, df, request=request)

    def _write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None:
        format_write_handler.write_arrow(self, result, request=request)

    def _table_target(self, request: WriteRequest) -> str | None:
        return format_write_handler.table_target(self, request)
