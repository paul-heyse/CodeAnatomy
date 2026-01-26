"""Tests for incremental view artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import ibis
import pyarrow as pa

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from datafusion_engine.diagnostics import DiagnosticsSink
from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.runtime import IncrementalRuntime
from incremental.sqlglot_artifacts import record_view_artifact
from sqlglot_tools.optimizer import ast_policy_fingerprint


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
    df_profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    runtime_profile = runtime_profile_factory("default").with_datafusion(df_profile)
    ctx = ExecutionContext(runtime=runtime_profile)
    runtime = IncrementalRuntime.build(ctx=ctx)
    return runtime, sink


def test_record_view_artifact_payload_has_plan_hash() -> None:
    """Ensure view artifacts yield stable plan hashes."""
    runtime, _ = _runtime_with_sink()
    expr = ibis.memtable({"a": [1, 2]})
    schema = pa.schema([pa.field("a", pa.int64(), nullable=True)])

    artifact_one = record_view_artifact(runtime, name="test_plan", expr=expr, schema=schema)
    artifact_two = record_view_artifact(runtime, name="test_plan", expr=expr, schema=schema)

    plan_hash_one = ast_policy_fingerprint(
        ast_fingerprint=artifact_one.ast_fingerprint,
        policy_hash=artifact_one.policy_hash,
    )
    plan_hash_two = ast_policy_fingerprint(
        ast_fingerprint=artifact_two.ast_fingerprint,
        policy_hash=artifact_two.policy_hash,
    )
    assert plan_hash_one
    assert plan_hash_one == plan_hash_two
    snapshot = runtime.profile.view_registry_snapshot()
    assert snapshot is not None
    assert len(snapshot) == 1
