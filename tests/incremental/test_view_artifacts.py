"""Tests for incremental view artifacts and invalidation snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa
import pytest

from datafusion_engine.lineage.diagnostics import DiagnosticsSink
from datafusion_engine.lineage.reporting import referenced_tables_from_plan
from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact
from datafusion_engine.plan.udf_analysis import extract_udfs_from_plan_bundle
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_profile_config import (
    DiagnosticsConfig,
    FeatureGatesConfig,
)
from datafusion_engine.session.runtime_session import record_view_definition
from datafusion_engine.views.artifacts import (
    ViewArtifactLineage,
    ViewArtifactRequest,
    build_view_artifact_from_bundle,
)
from semantics.compile_context import build_semantic_execution_context
from semantics.incremental.runtime import IncrementalRuntime, IncrementalRuntimeBuildRequest
from serde_schema_registry import ArtifactSpec
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

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, object]) -> None:
        self.artifacts.setdefault(name.canonical_name, []).append(dict(payload))

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
        features=FeatureGatesConfig(enforce_delta_ffi_provider=False),
        diagnostics=DiagnosticsConfig(diagnostics_sink=sink),
    )
    runtime = IncrementalRuntime.build(
        IncrementalRuntimeBuildRequest(
            profile=df_profile,
            dataset_resolver=build_semantic_execution_context(
                runtime_profile=df_profile
            ).dataset_resolver,
        )
    )
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
    bundle = build_plan_artifact(
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
    """Ensure view artifacts yield stable plan fingerprints.

    Raises:
        ValueError: Re-raised when plan recording fails for reasons unrelated to wheel gating.
    """
    runtime, _ = _runtime_with_sink()
    table = pa.table({"a": [1, 2]})

    try:
        _record_view_artifact(runtime, name="test_plan", table=table)
        _record_view_artifact(runtime, name="test_plan", table=table)
    except ValueError as exc:
        if "substrait_bytes" in str(exc):
            pytest.skip("Rust plan-bundle Substrait bytes are unavailable for this wheel build.")
        raise

    snapshot = runtime.profile.view_registry_snapshot()
    assert snapshot is not None
    assert len(snapshot) == 1
    plan_fingerprint = snapshot[0].get("plan_fingerprint")
    assert isinstance(plan_fingerprint, str)
    assert plan_fingerprint
