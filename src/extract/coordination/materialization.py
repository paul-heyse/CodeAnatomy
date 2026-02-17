"""Extract plan materialization utilities."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion.dataframe import DataFrame

from core_types import DeterminismTier
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import dataset_schema
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.plan.result_types import (
    PlanExecutionOptions,
    PlanScanOverrides,
)
from datafusion_engine.plan.result_types import (
    execute_plan_artifact as execute_plan_artifact_helper,
)
from datafusion_engine.schema.contracts import SchemaContract
from datafusion_engine.schema.finalize import FinalizeContext, FinalizeOptions, normalize_only
from datafusion_engine.schema.policy import SchemaPolicy
from datafusion_engine.session.facade import DataFusionExecutionFacade, ExecutionResult
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.views.graph import _validate_schema_contract
from extract.coordination.extract_plan_builder import (
    ExtractPlanOptions,
    apply_query_and_project,
    datafusion_plan_from_reader,
    extract_plan_from_reader,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    raw_plan_from_rows,
    record_batch_reader_from_row_batches,
    record_batch_reader_from_rows,
)
from extract.coordination.extract_recorder import (
    record_extract_compile as _record_extract_compile,
)
from extract.coordination.extract_recorder import (
    record_extract_execution as _record_extract_execution,
)
from extract.coordination.extract_recorder import (
    record_extract_udf_parity as _record_extract_udf_parity,
)
from extract.coordination.extract_recorder import (
    record_extract_view_artifact as _record_extract_view_artifact,
)
from extract.coordination.schema_ops import (
    ExtractNormalizeOptions,
    apply_pipeline_kernels,
    finalize_context_for_dataset,
    normalized_schema_policy_for_dataset,
)
from extract.session import ExtractSession
from extraction.diagnostics import EngineEventRecorder, ExtractQualityEvent
from extraction.materialize_pipeline import write_extract_outputs

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.scheduling import ScanUnit
    from semantics.compile_context import SemanticExecutionContext
    from semantics.program_manifest import ManifestDatasetResolver


@dataclass(frozen=True)
class _NormalizationContext:
    name: str
    runtime_profile: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    finalize_ctx: FinalizeContext
    apply_post_kernels: bool


@dataclass(frozen=True)
class _ProfileDatasetResolver:
    """Compatibility resolver backed by runtime-profile dataset templates."""

    runtime_profile: DataFusionRuntimeProfile

    def location(self, name: str) -> DatasetLocation | None:
        return self.runtime_profile.data_sources.dataset_templates.get(name)

    def has_location(self, name: str) -> bool:
        return name in self.runtime_profile.data_sources.dataset_templates

    def names(self) -> Sequence[str]:
        return tuple(self.runtime_profile.data_sources.dataset_templates.keys())


@dataclass(frozen=True)
class ExtractMaterializeOptions:
    """Options for materializing extract plans."""

    normalize: ExtractNormalizeOptions | None = None
    prefer_reader: bool = False
    apply_post_kernels: bool = False


@dataclass(frozen=True)
class _StreamingMaterializeRequest:
    """Inputs required to materialize streaming extract output."""

    name: str
    df: DataFrame
    plan: DataFusionPlanArtifact
    runtime_profile: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    normalization_ctx: _NormalizationContext
    options: ExtractMaterializeOptions
    streaming_supported: bool
    dataset_resolver: ManifestDatasetResolver | None = None


def _require_schema_policy(name: str, policy: SchemaPolicy | None) -> SchemaPolicy:
    if policy is None:
        msg = f"Missing schema policy for {name!r} normalization."
        raise ValueError(msg)
    return policy


def _normalize_reader(
    context: _NormalizationContext,
    reader: RecordBatchReaderLike,
) -> RecordBatchReaderLike:
    resolved_policy = _require_schema_policy(
        context.name,
        context.finalize_ctx.schema_policy,
    )
    if resolved_policy.keep_extra_columns:
        msg = f"Streaming normalization does not support keep_extra_columns for {context.name!r}."
        raise ValueError(msg)
    schema = resolved_policy.resolved_schema()

    def _iter_batches() -> Iterator[pa.RecordBatch]:
        for batch in reader:
            table = pa.Table.from_batches([batch], schema=batch.schema)
            processed = (
                apply_pipeline_kernels(context.name, table) if context.apply_post_kernels else table
            )
            aligned = resolved_policy.apply(processed)
            if aligned.column_names != schema.names:
                aligned = aligned.select(schema.names)
            yield from cast("pa.Table", aligned).to_batches()

    return pa.RecordBatchReader.from_batches(schema, _iter_batches())


def _normalize_table(
    context: _NormalizationContext,
    table: TableLike,
) -> TableLike:
    resolved_policy = _require_schema_policy(
        context.name,
        context.finalize_ctx.schema_policy,
    )
    processed = apply_pipeline_kernels(context.name, table) if context.apply_post_kernels else table
    return normalize_only(
        processed,
        contract=context.finalize_ctx.contract,
        options=FinalizeOptions(
            schema_policy=resolved_policy,
            runtime_profile=context.runtime_profile,
            determinism_tier=context.determinism_tier,
        ),
    )


def extract_dataset_location_or_raise(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> DatasetLocation:
    """Return extract dataset location, raising when missing.

    Raises:
        ValueError: If no extract dataset location is configured.

    Returns:
        DatasetLocation: Resolved extract dataset location.
    """
    location = _dataset_location(runtime_profile, name)
    if location is None:
        msg = f"No extract dataset location configured for {name!r}."
        raise ValueError(msg)
    return location


def _dataset_location(
    runtime_profile: DataFusionRuntimeProfile,
    name: str,
    *,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> DatasetLocation | None:
    if dataset_resolver is None:
        return runtime_profile.data_sources.dataset_templates.get(name)
    return dataset_resolver.location(name)


def _resolve_dataset_resolver(
    dataset_resolver: ManifestDatasetResolver | None = None,
    execution_context: SemanticExecutionContext | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> ManifestDatasetResolver:
    if dataset_resolver is not None:
        return dataset_resolver
    if execution_context is not None:
        return execution_context.dataset_resolver
    if runtime_profile is not None:
        return _ProfileDatasetResolver(runtime_profile)
    msg = (
        "ManifestDatasetResolver is required for extract coordination materialization. "
        "Provide dataset_resolver, execution_context, or runtime_profile."
    )
    raise ValueError(msg)


def _streaming_supported_for_extract(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    normalize: ExtractNormalizeOptions | None,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> bool:
    if (
        _dataset_location(
            runtime_profile,
            name,
            dataset_resolver=dataset_resolver,
        )
        is None
    ):
        return False
    policy = normalized_schema_policy_for_dataset(
        name,
        runtime_profile=runtime_profile,
        normalize=normalize,
    )
    return not policy.keep_extra_columns


def _build_normalization_context(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    determinism_tier: DeterminismTier,
    options: ExtractMaterializeOptions,
) -> _NormalizationContext:
    finalize_ctx = finalize_context_for_dataset(
        name,
        runtime_profile=runtime_profile,
        normalize=options.normalize,
    )
    return _NormalizationContext(
        name=name,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        finalize_ctx=finalize_ctx,
        apply_post_kernels=options.apply_post_kernels,
    )


def _plan_scan_units_for_extract(
    plan: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[tuple[ScanUnit, ...], tuple[str, ...]]:
    from datafusion_engine.lineage.reporting import extract_lineage
    from datafusion_engine.lineage.scheduling import plan_scan_unit

    if dataset_resolver is None:
        return (), ()
    session_runtime = runtime_profile.session_runtime()
    scan_units: dict[str, ScanUnit] = {}
    for scan in extract_lineage(
        plan.optimized_logical_plan,
        udf_snapshot=plan.artifacts.udf_snapshot,
    ).scans:
        location = dataset_resolver.location(scan.dataset_name)
        if location is None:
            continue
        unit = plan_scan_unit(
            session_runtime.ctx,
            dataset_name=scan.dataset_name,
            location=location,
            lineage=scan,
        )
        scan_units[unit.key] = unit
    units = tuple(sorted(scan_units.values(), key=lambda unit: unit.key))
    return units, tuple(unit.key for unit in units)


def _execute_extract_plan_bundle(
    name: str,
    plan: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[ExecutionResult, tuple[ScanUnit, ...], tuple[str, ...]]:
    session_runtime = runtime_profile.session_runtime()
    scan_units, scan_keys = _plan_scan_units_for_extract(
        plan,
        runtime_profile=runtime_profile,
        dataset_resolver=dataset_resolver,
    )
    execution = execute_plan_artifact_helper(
        session_runtime.ctx,
        plan,
        options=PlanExecutionOptions(
            runtime_profile=runtime_profile,
            view_name=name,
            scan=PlanScanOverrides(
                scan_units=scan_units,
                scan_keys=scan_keys,
                apply_scan_overrides=True,
            ),
        ),
    )
    return execution.execution_result, execution.scan_units, execution.scan_keys


def _write_and_record_extract_output(
    name: str,
    plan: DataFusionPlanArtifact,
    output: TableLike | pa.RecordBatchReader,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> None:
    resolved_dataset_resolver = _resolve_dataset_resolver(
        dataset_resolver=dataset_resolver,
        runtime_profile=runtime_profile,
    )
    write_extract_outputs(
        name,
        output,
        runtime_profile=runtime_profile,
        dataset_resolver=resolved_dataset_resolver,
    )
    recorder = EngineEventRecorder(runtime_profile)
    try:
        _register_extract_view(name, runtime_profile=runtime_profile)
    except (RuntimeError, ValueError, TypeError, OSError, KeyError) as exc:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="postprocess",
                    status="register_view_failed",
                    rows=None,
                    location_path=None,
                    location_format=None,
                    issue=str(exc),
                )
            ]
        )
    try:
        _record_extract_view_artifact(
            name,
            plan,
            schema=_arrow_schema_from_output(output),
            runtime_profile=runtime_profile,
        )
    except (RuntimeError, ValueError, TypeError, OSError, KeyError) as exc:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="postprocess",
                    status="view_artifact_failed",
                    rows=None,
                    location_path=None,
                    location_format=None,
                    issue=str(exc),
                )
            ]
        )
    try:
        _validate_extract_schema_contract(
            name,
            schema=_arrow_schema_from_output(output),
            runtime_profile=runtime_profile,
        )
    except (RuntimeError, ValueError, TypeError, OSError, KeyError) as exc:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="postprocess",
                    status="schema_contract_failed",
                    rows=None,
                    location_path=None,
                    location_format=None,
                    issue=str(exc),
                )
            ]
        )


def _materialize_streaming_output(
    request: _StreamingMaterializeRequest,
) -> pa.RecordBatchReader | None:
    if not request.streaming_supported:
        return None
    reader = cast("RecordBatchReaderLike", request.df.execute_stream())
    reader_for_write = _normalize_reader(request.normalization_ctx, reader)
    _write_and_record_extract_output(
        request.name,
        request.plan,
        reader_for_write,
        runtime_profile=request.runtime_profile,
        dataset_resolver=request.dataset_resolver,
    )
    if not request.options.prefer_reader:
        return None
    reader = cast("RecordBatchReaderLike", request.df.execute_stream())
    reader_result = ExecutionResult.from_reader(reader)
    normalized_reader = _normalize_reader(request.normalization_ctx, reader)
    _record_extract_execution(
        request.name,
        reader_result,
        runtime_profile=request.runtime_profile,
    )
    return normalized_reader


def materialize_extract_plan(
    name: str,
    plan: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    determinism_tier: DeterminismTier,
    options: ExtractMaterializeOptions | None = None,
    execution_context: SemanticExecutionContext | None = None,
) -> TableLike | pa.RecordBatchReader:
    """Materialize an extract plan bundle and normalize output.

    Returns:
        TableLike | pa.RecordBatchReader: Materialized extract output in the
            requested output mode.
    """
    resolved = options or ExtractMaterializeOptions()
    dataset_resolver = _resolve_dataset_resolver(
        execution_context=execution_context,
        runtime_profile=runtime_profile,
    )
    _record_extract_compile(name, plan, runtime_profile=runtime_profile)
    _record_extract_udf_parity(name, runtime_profile=runtime_profile)
    streaming_supported = _streaming_supported_for_extract(
        name,
        runtime_profile=runtime_profile,
        normalize=resolved.normalize,
        dataset_resolver=dataset_resolver,
    )
    normalization_ctx = _build_normalization_context(
        name,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=resolved,
    )
    result, _scan_units, _scan_keys = _execute_extract_plan_bundle(
        name,
        plan,
        runtime_profile=runtime_profile,
        dataset_resolver=dataset_resolver,
    )
    df = result.require_dataframe()
    streaming_reader = _materialize_streaming_output(
        _StreamingMaterializeRequest(
            name=name,
            df=df,
            plan=plan,
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            normalization_ctx=normalization_ctx,
            options=resolved,
            streaming_supported=streaming_supported,
            dataset_resolver=dataset_resolver,
        )
    )
    if streaming_reader is not None:
        return streaming_reader
    table = df.to_arrow_table()
    table_result = ExecutionResult.from_table(table)
    normalized = _normalize_table(normalization_ctx, table)
    if not streaming_supported:
        _write_and_record_extract_output(
            name,
            plan,
            normalized,
            runtime_profile=runtime_profile,
            dataset_resolver=dataset_resolver,
        )
    if resolved.prefer_reader:
        if isinstance(normalized, pa.Table):
            resolved_table = cast("pa.Table", normalized)
            _record_extract_execution(name, table_result, runtime_profile=runtime_profile)
            return pa.RecordBatchReader.from_batches(
                resolved_table.schema,
                resolved_table.to_batches(),
            )
        _record_extract_execution(name, table_result, runtime_profile=runtime_profile)
        return normalized
    _record_extract_execution(name, table_result, runtime_profile=runtime_profile)
    return normalized


def materialize_extract_plan_table(
    name: str,
    plan: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    determinism_tier: DeterminismTier,
    options: ExtractMaterializeOptions | None = None,
    execution_context: SemanticExecutionContext | None = None,
) -> TableLike:
    """Materialize an extract plan and always return a table-like result.

    Returns:
        TableLike: Materialized table output.
    """
    result = materialize_extract_plan(
        name,
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=options,
        execution_context=execution_context,
    )
    if isinstance(result, pa.RecordBatchReader):
        reader = cast("pa.RecordBatchReader", result)
        return cast("TableLike", reader.read_all())
    return result


def materialize_extract_plan_reader(
    name: str,
    plan: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    determinism_tier: DeterminismTier,
    options: ExtractMaterializeOptions | None = None,
    execution_context: SemanticExecutionContext | None = None,
) -> pa.RecordBatchReader:
    """Materialize an extract plan and always return a RecordBatchReader.

    Returns:
        pa.RecordBatchReader: Materialized reader output.

    Raises:
        TypeError: If the materialized output cannot be converted to a reader.
    """
    resolved_options = options or ExtractMaterializeOptions()
    reader_options = (
        resolved_options
        if resolved_options.prefer_reader
        else ExtractMaterializeOptions(
            normalize=resolved_options.normalize,
            prefer_reader=True,
            apply_post_kernels=resolved_options.apply_post_kernels,
        )
    )
    result = materialize_extract_plan(
        name,
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=reader_options,
        execution_context=execution_context,
    )
    if isinstance(result, pa.RecordBatchReader):
        return result
    if isinstance(result, pa.Table):
        return pa.RecordBatchReader.from_batches(result.schema, result.to_batches())
    msg = f"Unsupported extract output type for reader materialization: {type(result)!r}"
    raise TypeError(msg)


def materialize_extract_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    session: ExtractSession,
    plan_options: ExtractPlanOptions | None = None,
    materialize_options: ExtractMaterializeOptions | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Materialize an extract plan derived from an input reader.

    Returns:
        TableLike | RecordBatchReaderLike: Materialized output for the input
            reader.
    """
    resolved_plan = plan_options or ExtractPlanOptions()
    plan = extract_plan_from_reader(
        name,
        reader,
        session=session,
        options=resolved_plan,
    )
    resolved_materialize = materialize_options
    if resolved_materialize is None:
        resolved_materialize = ExtractMaterializeOptions(normalize=resolved_plan.normalize)
    elif resolved_materialize.normalize is None and resolved_plan.normalize is not None:
        resolved_materialize = ExtractMaterializeOptions(
            normalize=resolved_plan.normalize,
            prefer_reader=resolved_materialize.prefer_reader,
            apply_post_kernels=resolved_materialize.apply_post_kernels,
        )
    return materialize_extract_plan(
        name,
        plan,
        runtime_profile=session.runtime_profile,
        determinism_tier=session.determinism_tier,
        options=resolved_materialize,
    )


def _register_extract_view(name: str, *, runtime_profile: DataFusionRuntimeProfile) -> None:
    """Register a view for a materialized extract dataset."""
    location = _dataset_location(runtime_profile, name)
    if location is None:
        return
    session_runtime = runtime_profile.session_runtime()
    facade = DataFusionExecutionFacade(
        ctx=session_runtime.ctx,
        runtime_profile=runtime_profile,
    )
    facade.register_dataset(name=name, location=location)


def _validate_extract_schema_contract(
    name: str,
    *,
    schema: pa.Schema,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Validate extract outputs against expected ABI schema.

    Raises:
        TypeError: If the expected dataset schema cannot be resolved.
    """
    if _dataset_location(runtime_profile, name) is None:
        return
    expected = dataset_schema(name)
    if not isinstance(expected, pa.Schema):
        msg = f"Expected schema unavailable for extract dataset {name!r}."
        raise TypeError(msg)
    contract = SchemaContract.from_arrow_schema(name, expected)
    session_runtime = runtime_profile.session_runtime()
    _validate_schema_contract(
        session_runtime.ctx,
        contract,
        schema=schema,
    )


def _arrow_schema_from_output(output: TableLike | RecordBatchReaderLike) -> pa.Schema:
    schema = getattr(output, "schema", None)
    if callable(schema):
        schema = schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve schema for extract output."
    raise TypeError(msg)


__all__ = [
    "ExtractMaterializeOptions",
    "ExtractPlanOptions",
    "apply_query_and_project",
    "datafusion_plan_from_reader",
    "extract_dataset_location_or_raise",
    "extract_plan_from_reader",
    "extract_plan_from_row_batches",
    "extract_plan_from_rows",
    "materialize_extract_plan",
    "materialize_extract_plan_reader",
    "materialize_extract_plan_table",
    "materialize_extract_reader",
    "raw_plan_from_rows",
    "record_batch_reader_from_row_batches",
    "record_batch_reader_from_rows",
]
