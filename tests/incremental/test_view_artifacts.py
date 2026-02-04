"""Tests for incremental view artifacts and invalidation snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

from datafusion_engine.lineage.datafusion import referenced_tables_from_plan
from datafusion_engine.lineage.diagnostics import DiagnosticsSink
from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.plan.udf_analysis import extract_udfs_from_plan_bundle
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DiagnosticsConfig,
    record_view_definition,
)
from datafusion_engine.views.artifacts import (
    ViewArtifactLineage,
    ViewArtifactRequest,
    build_view_artifact_from_bundle,
)
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.invalidations import (
    build_invalidation_snapshot,
    read_invalidation_snapshot,
    write_invalidation_snapshot,
)
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.state_store import StateStore
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import require_delta_extension, require_deltalake

require_deltalake()
require_delta_extension()


@dataclass
class _DiagnosticsSink(DiagnosticsSink):
    artifacts: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    events: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    metrics: list[tuple[str, float, dict[str, str]]] = field(default_factory=list)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        self.events.setdefault(name, []).extend(dict(row) for row in rows)

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        self.artifacts.setdefault(name, []).append(dict(payload))

    def record_event(self, name: str, properties: Mapping[str, object]) -> None:
        self.events.setdefault(name, []).append(dict(properties))

    def record_metric(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        self.metrics.append((name, value, dict(tags)))

    def events_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        return {name: list(rows) for name, rows in self.events.items()}

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        return {name: list(rows) for name, rows in self.artifacts.items()}


def _runtime_with_sink() -> tuple[IncrementalRuntime, _DiagnosticsSink]:
    sink = _DiagnosticsSink()
    df_profile = DataFusionRuntimeProfile(
        diagnostics=DiagnosticsConfig(diagnostics_sink=sink),
    )
    runtime = IncrementalRuntime.build(profile=df_profile)
    return runtime, sink


def _record_view_artifact(
    runtime: IncrementalRuntime,
    *,
    name: str,
    table: pa.Table,
) -> None:
    ctx = runtime.session_context()
    session_runtime = runtime.profile.session_runtime()

    register_arrow_table(ctx, name=name, value=table)
    df = ctx.table(name)
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    required_udfs = tuple(sorted(extract_udfs_from_plan_bundle(bundle)))
    referenced_tables = referenced_tables_from_plan(bundle.optimized_logical_plan)
    artifact = build_view_artifact_from_bundle(
        bundle,
        request=ViewArtifactRequest(
            name=name,
            schema=df.schema(),
            lineage=ViewArtifactLineage(
                required_udfs=required_udfs,
                referenced_tables=referenced_tables,
            ),
            runtime_hash=None,
        ),
    )
    record_view_definition(runtime.profile, artifact=artifact)


def test_record_view_artifact_payload_has_plan_fingerprint() -> None:
    """Ensure view artifacts yield stable plan fingerprints."""
    runtime, _ = _runtime_with_sink()
    table = pa.table({"a": [1, 2]})

    _record_view_artifact(runtime, name="test_plan", table=table)
    _record_view_artifact(runtime, name="test_plan", table=table)

    snapshot = runtime.profile.view_registry_snapshot()
    assert snapshot is not None
    assert len(snapshot) == 1
    plan_fingerprint = snapshot[0].get("plan_fingerprint")
    assert isinstance(plan_fingerprint, str)
    assert plan_fingerprint


def test_invalidation_snapshot_round_trip(tmp_path: Path) -> None:
    """Persist and reload invalidation snapshots from Delta."""
    runtime, _sink = _runtime_with_sink()
    table = pa.table({"a": [1, 2]})
    _record_view_artifact(runtime, name="test_plan", table=table)

    context = DeltaAccessContext(runtime=runtime)
    store = StateStore(tmp_path)

    snapshot = build_invalidation_snapshot(context, state_store=store)
    path = write_invalidation_snapshot(store, snapshot, context=context)
    assert path

    loaded = read_invalidation_snapshot(store, context=context)
    assert loaded is not None
    assert loaded.incremental_plan_fingerprints == snapshot.incremental_plan_fingerprints
    assert loaded.incremental_metadata_hash == snapshot.incremental_metadata_hash
    assert loaded.runtime_profile_hash == snapshot.runtime_profile_hash
