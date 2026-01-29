"""Tests for determinism audit bundles in plan details."""

from __future__ import annotations

import pyarrow as pa
import pytest
import rustworkx as rx

from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile
from hamilton_pipeline.plan_artifacts import build_plan_artifact_bundle
from relspec.execution_plan import ExecutionPlan
from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata

datafusion = pytest.importorskip("datafusion")

_SHA256_HEX_LENGTH = 64


def test_plan_bundle_determinism_audit_bundle() -> None:
    """Ensure determinism audit bundle captures core hash inputs."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    from datafusion_engine.ingest import datafusion_from_arrow

    datafusion_from_arrow(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    audit = bundle.plan_details.get("determinism_audit")
    assert isinstance(audit, dict)
    assert audit.get("plan_fingerprint") == bundle.plan_fingerprint
    assert audit.get("planning_env_hash") == bundle.artifacts.planning_env_hash
    assert audit.get("rulepack_hash") == bundle.artifacts.rulepack_hash
    assert audit.get("information_schema_hash") == bundle.artifacts.information_schema_hash
    assert audit.get("udf_snapshot_hash") == bundle.artifacts.udf_snapshot_hash
    assert audit.get("function_registry_hash") == bundle.artifacts.function_registry_hash
    df_settings_hash = audit.get("df_settings_hash")
    assert isinstance(df_settings_hash, str)
    assert len(df_settings_hash) == _SHA256_HEX_LENGTH


def _stub_execution_plan() -> ExecutionPlan:
    from relspec.evidence import EvidenceCatalog

    ordered_tasks = ("task_a", "task_b")
    schedule = TaskSchedule(ordered_tasks=ordered_tasks, generations=(ordered_tasks,))
    schedule_metadata = task_schedule_metadata(schedule)
    empty_graph = rx.PyDiGraph(multigraph=False, check_cycle=False)
    task_graph = TaskGraph(
        graph=empty_graph,
        evidence_idx={},
        task_idx={},
        output_policy="all_producers",
    )
    return ExecutionPlan(
        view_nodes=(),
        task_graph=task_graph,
        task_dependency_graph=empty_graph,
        reduced_task_dependency_graph=empty_graph,
        evidence=EvidenceCatalog(),
        task_schedule=schedule,
        schedule_metadata=schedule_metadata,
        plan_fingerprints={},
        plan_task_signatures={},
        plan_snapshots={},
        output_contracts={},
        plan_signature="plan:stub",
        task_dependency_signature="deps:stub",
        reduced_task_dependency_signature="deps:stub:reduced",
        reduction_node_map={},
        reduction_edge_count=0,
        reduction_removed_edge_count=0,
        diagnostics=GraphDiagnostics(status="ok"),
        critical_path_task_names=(),
        critical_path_length_weighted=0.0,
        bottom_level_costs=dict.fromkeys(ordered_tasks, 1.0),
        slack_by_task=dict.fromkeys(ordered_tasks, 0.0),
        task_plan_metrics={},
        task_costs={},
        dependency_map={},
        dataset_specs={},
        active_tasks=frozenset(ordered_tasks),
    )


def test_plan_schedule_artifact_hash_is_deterministic() -> None:
    """Ensure schedule/validation artifact hashes are deterministic."""
    plan = _stub_execution_plan()
    bundle_a = build_plan_artifact_bundle(plan=plan, run_id="run-1")
    bundle_b = build_plan_artifact_bundle(plan=plan, run_id="run-1")
    assert bundle_a.schedule_artifact_id == bundle_b.schedule_artifact_id
    assert bundle_a.validation_artifact_id == bundle_b.validation_artifact_id
    assert len(bundle_a.schedule_artifact_id) == _SHA256_HEX_LENGTH
    assert len(bundle_a.validation_artifact_id) == _SHA256_HEX_LENGTH
