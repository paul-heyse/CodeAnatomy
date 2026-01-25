"""Tests for incremental invalidation snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import ibis

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from datafusion_engine.diagnostics import DiagnosticsSink
from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.delta_context import DeltaAccessContext
from incremental.invalidations import (
    build_invalidation_snapshot,
    read_invalidation_snapshot,
    write_invalidation_snapshot,
)
from incremental.runtime import IncrementalRuntime
from incremental.sqlglot_artifacts import record_sqlglot_plan_artifact
from incremental.state_store import StateStore


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


def test_invalidation_snapshot_round_trip(tmp_path: Path) -> None:
    """Persist and reload invalidation snapshots from Delta."""
    runtime, _sink = _runtime_with_sink()
    expr = ibis.memtable({"a": [1, 2]})
    record_sqlglot_plan_artifact(runtime, name="test_plan", expr=expr)

    context = DeltaAccessContext(runtime=runtime)
    store = StateStore(tmp_path)

    snapshot = build_invalidation_snapshot(context, state_store=store)
    path = write_invalidation_snapshot(store, snapshot, context=context)
    assert path

    loaded = read_invalidation_snapshot(store, context=context)
    assert loaded is not None
    assert loaded.incremental_plan_hashes == snapshot.incremental_plan_hashes
    assert loaded.incremental_metadata_hash == snapshot.incremental_metadata_hash
    assert loaded.runtime_profile_hash == snapshot.runtime_profile_hash
