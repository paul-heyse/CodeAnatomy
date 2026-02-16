"""Emission helpers for semantic diagnostics snapshots and observability events."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.write import WritePipeline
from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
from obs.diagnostics import (
    SemanticQualityArtifact,
    record_semantic_quality_artifact,
    record_semantic_quality_events,
)
from obs.metrics import record_quality_issue_counts
from obs.otel.run_context import get_run_id, reset_run_id, set_run_id
from semantics.diagnostics import (
    DEFAULT_MAX_ISSUE_ROWS,
    SEMANTIC_DIAGNOSTIC_VIEW_NAMES,
    dataframe_row_count,
    semantic_diagnostic_view_builders,
    semantic_quality_issue_batches,
)
from semantics.incremental.metadata import (
    SemanticDiagnosticsSnapshot,
    write_semantic_diagnostics_snapshots,
)
from semantics.incremental.runtime import IncrementalRuntime, IncrementalRuntimeBuildRequest
from semantics.incremental.state_store import StateStore
from semantics.naming import canonical_output_name
from semantics.output_materialization import (
    SemanticOutputWriteContext,
    semantic_output_locations,
    write_semantic_output,
)
from utils.env_utils import env_value
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.plan.bundle_artifact import DataFrameBuilder
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


FinalizeBuilder = Callable[[str, "DataFrameBuilder"], "DataFrameBuilder"]


@dataclass
class SemanticDiagnosticsContext:
    """Context bundle for semantic diagnostics emission."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    dataset_resolver: ManifestDatasetResolver
    schema_policy: SchemaEvolutionPolicy
    diagnostics_sink: DiagnosticsSink | None
    output_locations: dict[str, DatasetLocation]
    write_context: SemanticOutputWriteContext | None
    state_store: StateStore | None
    storage_options: Mapping[str, str] | None
    incremental_runtime: IncrementalRuntime | None = None

    def ensure_incremental_runtime(self) -> IncrementalRuntime | None:
        """Return cached incremental runtime, creating it lazily when needed."""
        if self.incremental_runtime is not None:
            return self.incremental_runtime
        self.incremental_runtime = IncrementalRuntime.build(
            IncrementalRuntimeBuildRequest(
                profile=self.runtime_profile,
                dataset_resolver=self.dataset_resolver,
            )
        )
        return self.incremental_runtime

    def write_snapshot(self, view_name: str, df: DataFrame) -> str | None:
        """Persist one diagnostics snapshot and return the artifact URI when written.

        Returns:
            str | None: Persisted artifact URI/path when written, else ``None``.
        """
        if view_name in self.output_locations and self.write_context is not None:
            write_semantic_output(
                view_name=view_name,
                output_location=self.output_locations[view_name],
                write_context=self.write_context,
            )
            return str(self.output_locations[view_name].path)
        if self.state_store is None:
            return None
        runtime = self.ensure_incremental_runtime()
        if runtime is None:
            return None
        snapshot = SemanticDiagnosticsSnapshot(
            name=view_name,
            table=df.to_arrow_table(),
            destination=self.state_store.semantic_diagnostics_path(view_name),
        )
        updated = write_semantic_diagnostics_snapshots(
            runtime=runtime,
            snapshots={view_name: snapshot},
            storage_options=self.storage_options,
        )
        return updated.get(view_name)


@dataclass(frozen=True)
class SemanticQualityDiagnosticsRequest:
    """Request payload for emitting semantic quality diagnostics."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    dataset_resolver: ManifestDatasetResolver
    schema_policy: SchemaEvolutionPolicy
    requested_outputs: Collection[str] | None
    manifest: SemanticProgramManifest
    finalize_builder: FinalizeBuilder | None = None


@contextmanager
def run_context_guard() -> Iterator[None]:
    """Ensure a run id exists while emitting diagnostics."""
    token = None
    if get_run_id() is None:
        token = set_run_id(uuid7_str())
    try:
        yield
    finally:
        if token is not None:
            reset_run_id(token)


def resolve_semantic_diagnostics_state_store() -> StateStore | None:
    """Resolve diagnostics state store from environment configuration.

    Returns:
        StateStore | None: Configured diagnostics state store, or ``None`` when unset.
    """
    state_dir_value = env_value("CODEANATOMY_STATE_DIR")
    if not state_dir_value:
        return None
    store = StateStore(root=Path(state_dir_value).expanduser())
    store.ensure_dirs()
    return store


def build_semantic_diagnostics_context(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver,
    schema_policy: SchemaEvolutionPolicy,
) -> SemanticDiagnosticsContext | None:
    """Build diagnostics emission context or return ``None`` when disabled.

    Returns:
        SemanticDiagnosticsContext | None: Emission context when any sink is enabled.
    """
    diagnostics_sink = runtime_profile.diagnostics.diagnostics_sink
    output_locations = semantic_output_locations(
        view_names=SEMANTIC_DIAGNOSTIC_VIEW_NAMES,
        runtime_profile=runtime_profile,
    )
    state_store = resolve_semantic_diagnostics_state_store()
    if diagnostics_sink is None and not output_locations and state_store is None:
        return None
    write_context = None
    if output_locations:
        pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
        write_context = SemanticOutputWriteContext(
            ctx=ctx,
            pipeline=pipeline,
            runtime_profile=runtime_profile,
            schema_policy=schema_policy,
        )
    storage_options: Mapping[str, str] | None = None
    if output_locations:
        first_loc = next(iter(output_locations.values()))
        loc_opts = first_loc.storage_options
        if loc_opts:
            storage_options = dict(loc_opts)
    return SemanticDiagnosticsContext(
        ctx=ctx,
        runtime_profile=runtime_profile,
        dataset_resolver=dataset_resolver,
        schema_policy=schema_policy,
        diagnostics_sink=diagnostics_sink,
        output_locations=output_locations,
        write_context=write_context,
        state_store=state_store,
        storage_options=storage_options,
    )


def emit_semantic_quality_view(
    context: SemanticDiagnosticsContext,
    *,
    view_name: str,
    builder: Callable[[SessionContext], DataFrame],
) -> None:
    """Emit one semantic quality view to sinks and diagnostics storage."""
    try:
        df = builder(context.ctx)
    except ValueError:
        return
    schema_hash = schema_identity_hash(arrow_schema_from_df(df))
    row_count = dataframe_row_count(df)
    artifact_uri = context.write_snapshot(view_name, df)
    if context.diagnostics_sink is None:
        return
    record_semantic_quality_artifact(
        context.diagnostics_sink,
        artifact=SemanticQualityArtifact(
            name=view_name,
            row_count=row_count,
            schema_hash=schema_hash,
            artifact_uri=artifact_uri,
            run_id=get_run_id(),
        ),
    )
    for batch in semantic_quality_issue_batches(
        view_name=view_name,
        df=df,
        max_rows=DEFAULT_MAX_ISSUE_ROWS,
    ):
        record_quality_issue_counts(
            issue_kind=batch.issue_kind,
            count=len(batch.rows),
        )
        record_semantic_quality_events(
            context.diagnostics_sink,
            name="semantic_quality_issues_v1",
            rows=batch.rows,
        )


def emit_semantic_quality_views(
    context: SemanticDiagnosticsContext,
    *,
    requested_outputs: Collection[str] | None,
    manifest: SemanticProgramManifest,
    finalize_builder: FinalizeBuilder | None = None,
) -> None:
    """Emit all selected semantic diagnostic views."""
    builders = semantic_diagnostic_view_builders()
    view_names = SEMANTIC_DIAGNOSTIC_VIEW_NAMES
    if requested_outputs is not None:
        resolved = {canonical_output_name(name, manifest=manifest) for name in requested_outputs}
        view_names = tuple(name for name in view_names if name in resolved)
    for view_name in view_names:
        builder = builders.get(view_name)
        if builder is None:
            continue
        finalized = (
            finalize_builder(view_name, builder) if finalize_builder is not None else builder
        )
        emit_semantic_quality_view(context, view_name=view_name, builder=finalized)


def emit_semantic_quality_diagnostics(request: SemanticQualityDiagnosticsRequest) -> None:
    """Emit semantic quality diagnostics when runtime profile enables it."""
    if not request.runtime_profile.diagnostics.emit_semantic_quality_diagnostics:
        return
    context = build_semantic_diagnostics_context(
        request.ctx,
        runtime_profile=request.runtime_profile,
        dataset_resolver=request.dataset_resolver,
        schema_policy=request.schema_policy,
    )
    if context is None:
        return
    with run_context_guard():
        emit_semantic_quality_views(
            context,
            requested_outputs=request.requested_outputs,
            manifest=request.manifest,
            finalize_builder=request.finalize_builder,
        )


__all__ = [
    "SemanticDiagnosticsContext",
    "SemanticQualityDiagnosticsRequest",
    "build_semantic_diagnostics_context",
    "emit_semantic_quality_diagnostics",
    "emit_semantic_quality_view",
    "emit_semantic_quality_views",
    "resolve_semantic_diagnostics_state_store",
    "run_context_guard",
]
