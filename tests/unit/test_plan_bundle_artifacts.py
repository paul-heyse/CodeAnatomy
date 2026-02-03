"""Tests for DataFusion plan bundle artifacts."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
import rustworkx as rx

from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.udf.catalog import rewrite_tag_index
from datafusion_engine.udf.runtime import rust_udf_snapshot_hash
from hamilton_pipeline.plan_artifacts import build_plan_artifact_bundle
from relspec.execution_plan import ExecutionPlan
from relspec.rustworkx_graph import GraphDiagnostics, TaskGraph
from relspec.rustworkx_schedule import TaskSchedule, task_schedule_metadata
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import require_datafusion
from tests.test_helpers.semantic_registry_runtime import semantic_registry_runtime

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import SessionRuntime

require_datafusion()

_SHA256_HEX_LENGTH = 64


def _session_context() -> tuple[SessionContext, SessionRuntime]:
    return semantic_registry_runtime()


def test_datafusion_unparser_payload_is_deterministic() -> None:
    """Capture deterministic plan display payloads for bundles."""
    ctx, session_runtime = _session_context()
    register_arrow_table(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT id, label FROM events")
    first = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    second = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    assert first.display_optimized_plan() == second.display_optimized_plan()


def test_plan_bundle_includes_fingerprint() -> None:
    """Record plan fingerprints for DataFusion plan bundles."""
    ctx, session_runtime = _session_context()
    register_arrow_table(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT events.id FROM events WHERE events.id = 1")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    assert bundle.plan_fingerprint


def test_plan_bundle_projection_is_deterministic() -> None:
    """Record projection requirements for DataFusion plan bundles."""
    ctx, session_runtime = _session_context()
    register_arrow_table(
        ctx,
        name="events",
        value=pa.table(
            {
                "id": [1, 2],
                "label": ["a", "b"],
                "extra": [10, 20],
            }
        ),
    )
    df = ctx.sql("SELECT events.id FROM events WHERE events.id = 1")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    plan_text = bundle.display_optimized_plan() or bundle.display_logical_plan() or ""
    assert "label" not in plan_text
    assert "extra" not in plan_text


def test_plan_bundle_graphviz_is_optional() -> None:
    """Capture optional GraphViz payloads for plan bundles."""
    ctx, session_runtime = _session_context()
    register_arrow_table(
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
    _ = bundle.graphviz()


def test_plan_bundle_determinism_audit_bundle() -> None:
    """Ensure determinism audit bundle captures core hash inputs."""
    ctx, session_runtime = _session_context()
    register_arrow_table(
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


def _udf_snapshot_payload() -> dict[str, object]:
    return {
        "scalar": ["my_udf"],
        "aggregate": [],
        "window": [],
        "table": [],
        "aliases": {},
        "parameter_names": {"my_udf": ["x", "y"]},
        "volatility": {"my_udf": "immutable"},
        "rewrite_tags": {"my_udf": ["tag_a"]},
        "signature_inputs": {"my_udf": [["Int64", "Utf8"]]},
        "return_types": {"my_udf": ["Utf8"]},
        "simplify": {"my_udf": True},
        "coerce_types": {"my_udf": False},
        "short_circuits": {"my_udf": True},
        "custom_udfs": [],
    }


def test_plan_bundle_captures_udf_planner_snapshot() -> None:
    """Ensure planner metadata is recorded from UDF snapshots."""
    ctx, session_runtime = _session_context()
    snapshot = _udf_snapshot_payload()
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    rewrite_tags = tuple(sorted(rewrite_tag_index(snapshot)))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    session_runtime = replace(
        session_runtime,
        udf_snapshot=snapshot,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
    )
    register_arrow_table(
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
    planner_snapshot = bundle.artifacts.udf_planner_snapshot
    assert isinstance(planner_snapshot, dict)
    assert planner_snapshot.get("status") == "ok"
    functions = planner_snapshot.get("functions")
    assert isinstance(functions, list)
    function = next(item for item in functions if item.get("name") == "my_udf")
    assert function.get("volatility") == "immutable"
    assert function.get("parameter_names") == ("x", "y")
    signature = function.get("signature")
    assert isinstance(signature, dict)
    assert signature.get("inputs") == (("Int64", "Utf8"),)
    assert signature.get("returns") == ("Utf8",)
    assert function.get("return_type") == "Utf8"
    assert function.get("rewrite_tags") == ("tag_a",)
    assert function.get("has_simplify") is True
    assert function.get("has_coerce_types") is False
    assert function.get("short_circuits") is True


def test_plan_bundle_captures_async_udf_settings() -> None:
    """Ensure async UDF runtime settings are captured in plan artifacts."""
    try:
        import datafusion.substrait  # noqa: F401
    except ImportError:
        pytest.skip("datafusion.substrait is required for plan bundle construction.")
    try:
        import datafusion_ext  # noqa: F401
    except ImportError:
        pytest.skip("datafusion_ext is required for plan bundle construction.")
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

    async_timeout_ms = 2500
    async_batch_size = 128
    profile = DataFusionRuntimeProfile(
        enable_async_udfs=True,
        async_udf_timeout_ms=async_timeout_ms,
        async_udf_batch_size=async_batch_size,
    )
    runtime = profile.session_runtime()
    ctx = runtime.ctx
    register_arrow_table(ctx, name="events", value=pa.table({"id": [1, 2]}))
    df = ctx.sql("SELECT id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(
            compute_execution_plan=False,
            session_runtime=runtime,
        ),
    )
    async_payload = bundle.artifacts.planning_env_snapshot.get("async_udf")
    assert isinstance(async_payload, dict)
    assert async_payload["enable_async_udfs"] is True
    assert async_payload["async_udf_timeout_ms"] == async_timeout_ms
    assert async_payload["async_udf_batch_size"] == async_batch_size
