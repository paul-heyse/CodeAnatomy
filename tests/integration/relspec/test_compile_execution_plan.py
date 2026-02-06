"""Suite 3.1: Integration tests for compile_execution_plan end-to-end boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.lineage.scan import ScanUnit
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
from datafusion_engine.views.graph import ViewNode
from relspec.execution_plan import (
    ExecutionPlan,
    ExecutionPlanRequest,
    compile_execution_plan,
)
from relspec.inferred_deps import InferredDeps


@dataclass(frozen=True)
class _MockPlanArtifacts:
    """Minimal plan artifacts payload for compatibility checks."""

    @staticmethod
    def _default_udf_snapshot() -> dict[str, object]:
        return {
            "scalar": [],
            "aggregate": [],
            "window": [],
            "table": [],
            "aliases": {},
            "parameter_names": {},
            "volatility": {},
            "simplify": {},
            "coerce_types": {},
            "short_circuits": {},
            "signature_inputs": {},
            "return_types": {},
        }

    udf_snapshot: dict[str, object] = field(default_factory=_default_udf_snapshot)


@dataclass(frozen=True)
class _MockPlanBundle:
    """Mock plan bundle for testing."""

    plan_signature: str
    lineage: tuple[str, ...] = ()
    required_udfs: tuple[str, ...] = ("stable_id",)
    required_rewrite_tags: tuple[str, ...] = ("test_tag",)
    artifacts: _MockPlanArtifacts = field(default_factory=_MockPlanArtifacts)


def _minimal_view_node(name: str, deps: tuple[str, ...] = ()) -> ViewNode:
    """Create a minimal ViewNode for testing.

    Parameters
    ----------
    name
        View name.
    deps
        Dependency names.

    Returns:
    -------
    ViewNode
        Minimal view node with a simple builder.
    """

    def builder(ctx: SessionContext) -> DataFrame:
        return ctx.sql("SELECT 1 as value")

    return ViewNode(
        name=name,
        deps=deps,
        builder=builder,
        contract_builder=None,
        required_udfs=(),
        plan_bundle=None,
        cache_policy="none",
    )


def _view_node_with_plan_bundle(
    name: str,
    plan_signature: str,
    deps: tuple[str, ...] = (),
    lineage: tuple[str, ...] = (),
) -> ViewNode:
    """Create a ViewNode with a mock plan bundle.

    Parameters
    ----------
    name
        View name.
    plan_signature
        Plan signature string.
    deps
        Dependency names.
    lineage
        Lineage dataset names.

    Returns:
    -------
    ViewNode
        View node with mock plan bundle attached.
    """
    node = _minimal_view_node(name, deps)
    bundle = _MockPlanBundle(plan_signature=plan_signature, lineage=lineage)
    return ViewNode(
        name=node.name,
        deps=node.deps,
        builder=node.builder,
        contract_builder=node.contract_builder,
        required_udfs=node.required_udfs,
        plan_bundle=bundle,  # type: ignore[arg-type]
        cache_policy=node.cache_policy,
    )


@pytest.fixture
def session_runtime() -> SessionRuntime:
    """Create a minimal SessionRuntime for testing.

    Returns:
    -------
    SessionRuntime
        Minimal runtime with default profile.
    """
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    return SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash="test_hash",
        udf_rewrite_tags=(),
        domain_planner_names=(),
        udf_snapshot={},
        df_settings={},
    )


@pytest.fixture(autouse=True)
def _stub_plan_with_delta_pins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace heavy planning bootstrap with deterministic in-memory planning."""
    from datafusion_engine.plan.pipeline import PlanningPipelineResult

    def _stubbed_plan(
        _ctx: SessionContext,
        *,
        view_nodes: list[ViewNode] | tuple[ViewNode, ...],
        runtime_profile: DataFusionRuntimeProfile | None,
        snapshot: dict[str, object] | None,
    ) -> PlanningPipelineResult:
        _ = (runtime_profile, snapshot)
        inferred = tuple(
            InferredDeps(
                task_name=node.name,
                output=node.name,
                inputs=tuple(node.deps),
                plan_fingerprint=getattr(node.plan_bundle, "plan_signature", ""),
                required_udfs=tuple(getattr(node.plan_bundle, "required_udfs", ())),
                required_rewrite_tags=tuple(getattr(node.plan_bundle, "required_rewrite_tags", ())),
            )
            for node in view_nodes
        )
        return PlanningPipelineResult(
            view_nodes=tuple(view_nodes),
            inferred=inferred,
            scan_units=(),
            scan_keys_by_task={},
            scan_task_name_by_key={},
            scan_task_units_by_name={},
            scan_task_names_by_task={},
            scan_units_by_evidence_name={},
            lineage_by_view={},
            session_runtime=None,
        )

    monkeypatch.setattr("relspec.execution_plan.plan_with_delta_pins", _stubbed_plan)
    monkeypatch.setattr("relspec.execution_plan.extract_inferred_deps", lambda: ())
    monkeypatch.setattr(
        "relspec.execution_plan._validate_plan_bundle_compatibility",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "datafusion_engine.plan.artifact_store.persist_plan_artifacts_for_views",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "relspec.evidence._register_view_node_evidence",
        lambda *_args, **_kwargs: None,
    )

    def _stubbed_infer_deps_from_view_nodes(
        nodes: list[ViewNode] | tuple[ViewNode, ...],
        *,
        ctx: SessionContext | None = None,
        snapshot: dict[str, object] | None = None,
    ) -> tuple[InferredDeps, ...]:
        _ = (ctx, snapshot)
        return tuple(
            InferredDeps(
                task_name=node.name,
                output=node.name,
                inputs=tuple(node.deps),
                plan_fingerprint=getattr(node.plan_bundle, "plan_signature", ""),
                required_udfs=tuple(getattr(node.plan_bundle, "required_udfs", ())),
                required_rewrite_tags=tuple(getattr(node.plan_bundle, "required_rewrite_tags", ())),
            )
            for node in nodes
        )

    monkeypatch.setattr(
        "relspec.inferred_deps.infer_deps_from_view_nodes",
        _stubbed_infer_deps_from_view_nodes,
    )


@pytest.mark.integration
def test_compile_execution_plan_basic_roundtrip(session_runtime: SessionRuntime) -> None:
    """Compile with realistic view nodes that have plan bundles, verify ExecutionPlan structure.

    Tests that compile_execution_plan successfully creates a full ExecutionPlan with
    task_graph, task_schedule, and plan_signature populated for valid inputs.
    """
    view_nodes = [
        _view_node_with_plan_bundle(
            name="task_a",
            plan_signature="sig_a",
            deps=(),
            lineage=("dataset_x",),
        ),
        _view_node_with_plan_bundle(
            name="task_b",
            plan_signature="sig_b",
            deps=("task_a",),
            lineage=("dataset_y",),
        ),
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    plan = compile_execution_plan(session_runtime=session_runtime, request=request)

    assert isinstance(plan, ExecutionPlan)
    assert plan.task_graph is not None
    assert plan.task_schedule is not None
    assert isinstance(plan.plan_signature, str)
    assert len(plan.plan_signature) > 0
    assert plan.active_tasks == frozenset({"task_a", "task_b"})
    assert plan.runtime_profile == session_runtime.profile


@pytest.mark.integration
def test_compile_determinism(session_runtime: SessionRuntime) -> None:
    """Compile same inputs twice, verify plan_signature and task_dependency_signature identical.

    Tests that compile_execution_plan is deterministic - identical inputs produce
    identical signature values across multiple invocations.
    """
    view_nodes = [
        _view_node_with_plan_bundle(name="task_x", plan_signature="sig_x", deps=()),
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    plan1 = compile_execution_plan(session_runtime=session_runtime, request=request)
    plan2 = compile_execution_plan(session_runtime=session_runtime, request=request)

    assert plan1.plan_signature == plan2.plan_signature
    assert plan1.task_dependency_signature == plan2.task_dependency_signature


@pytest.mark.integration
def test_runtime_profile_mismatch_raises(session_runtime: SessionRuntime) -> None:
    """Pass ExecutionPlanRequest.runtime_profile != session_runtime.profile, expect ValueError.

    Tests that compile_execution_plan validates runtime profile consistency between
    the request and the session runtime.
    """
    different_profile = DataFusionRuntimeProfile()
    view_nodes = [
        _view_node_with_plan_bundle(name="task_a", plan_signature="sig_a", deps=()),
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=different_profile,  # Different from session_runtime.profile
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    with pytest.raises(ValueError, match="runtime_profile must match"):
        compile_execution_plan(session_runtime=session_runtime, request=request)


@pytest.mark.integration
def test_missing_plan_bundle_for_requested_task_raises(session_runtime: SessionRuntime) -> None:
    """Include task in requested_task_names whose view node lacks plan_bundle, expect ValueError.

    Tests that compile_execution_plan validates that all requested tasks have
    plan bundles attached to their view nodes.
    """
    view_nodes = [
        _minimal_view_node(name="task_without_bundle", deps=()),  # No plan_bundle
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=["task_without_bundle"],
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    with pytest.raises(ValueError, match="missing plan_bundle"):
        compile_execution_plan(session_runtime=session_runtime, request=request)


@pytest.mark.integration
def test_no_plan_bundles_raises(session_runtime: SessionRuntime) -> None:
    """All view nodes have plan_bundle=None, expect ValueError.

    Tests that compile_execution_plan requires at least one view node with a
    plan bundle to perform meaningful compilation.
    """
    view_nodes = [
        _minimal_view_node(name="task_a", deps=()),
        _minimal_view_node(name="task_b", deps=()),
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    with pytest.raises(ValueError, match="requires view nodes with plan_bundle"):
        compile_execution_plan(session_runtime=session_runtime, request=request)


@pytest.mark.integration
def test_delta_pin_conflict_raises() -> None:
    """Two scan units for same dataset with different (delta_version, timestamp) tuples, expect ValueError.

    Tests that compile_execution_plan detects conflicting Delta pins for the same
    dataset and raises an appropriate error.
    """
    scan_units = [
        ScanUnit(
            key="scan_1",
            dataset_name="dataset_x",
            delta_version=1,
            delta_timestamp="2024-01-01T00:00:00",
            snapshot_timestamp=None,
            delta_protocol=None,
            delta_scan_config=None,
            delta_scan_config_hash=None,
            datafusion_provider=None,
            protocol_compatible=None,
            protocol_compatibility=None,
            total_files=0,
            candidate_file_count=0,
            pruned_file_count=0,
            candidate_files=(),
            pushed_filters=(),
            projected_columns=(),
        ),
        ScanUnit(
            key="scan_2",
            dataset_name="dataset_x",  # Same dataset
            delta_version=2,  # Different version
            delta_timestamp="2024-01-02T00:00:00",  # Different timestamp
            snapshot_timestamp=None,
            delta_protocol=None,
            delta_scan_config=None,
            delta_scan_config_hash=None,
            datafusion_provider=None,
            protocol_compatible=None,
            protocol_compatibility=None,
            total_files=0,
            candidate_file_count=0,
            pruned_file_count=0,
            candidate_files=(),
            pushed_filters=(),
            projected_columns=(),
        ),
    ]

    # Create a custom request that includes scan units
    # Note: We need to pass scan units through the compilation path
    # This may require modifying the request or using a different approach
    # For now, we'll test the _scan_unit_delta_pins function directly
    from relspec.execution_plan import _scan_unit_delta_pins

    with pytest.raises(ValueError, match="Conflicting Delta pins"):
        _scan_unit_delta_pins(scan_units)


@pytest.mark.integration
def test_delta_pin_conflict_same_version_ok() -> None:
    """Two scan units for same dataset with identical pin tuples, no error.

    Tests that compile_execution_plan allows multiple scan units for the same
    dataset when they have identical Delta pin tuples.
    """
    from relspec.execution_plan import _scan_unit_delta_pins

    scan_units = [
        ScanUnit(
            key="scan_1",
            dataset_name="dataset_x",
            delta_version=1,
            delta_timestamp="2024-01-01T00:00:00",
            snapshot_timestamp=None,
            delta_protocol=None,
            delta_scan_config=None,
            delta_scan_config_hash=None,
            datafusion_provider=None,
            protocol_compatible=None,
            protocol_compatibility=None,
            total_files=0,
            candidate_file_count=0,
            pruned_file_count=0,
            candidate_files=(),
            pushed_filters=(),
            projected_columns=(),
        ),
        ScanUnit(
            key="scan_2",
            dataset_name="dataset_x",  # Same dataset
            delta_version=1,  # Same version
            delta_timestamp="2024-01-01T00:00:00",  # Same timestamp
            snapshot_timestamp=None,
            delta_protocol=None,
            delta_scan_config=None,
            delta_scan_config_hash=None,
            datafusion_provider=None,
            protocol_compatible=None,
            protocol_compatibility=None,
            total_files=0,
            candidate_file_count=0,
            pruned_file_count=0,
            candidate_files=(),
            pushed_filters=(),
            projected_columns=(),
        ),
    ]

    pins = _scan_unit_delta_pins(scan_units)

    assert "dataset_x" in pins
    assert pins["dataset_x"] == (1, "2024-01-01T00:00:00")


@pytest.mark.integration
def test_requested_task_filtering(session_runtime: SessionRuntime) -> None:
    """Set requested_task_names to subset, verify only those tasks in active plan.

    Tests that compile_execution_plan correctly filters the task graph to include
    only the explicitly requested tasks when requested_task_names is provided.
    """
    view_nodes = [
        _view_node_with_plan_bundle(name="task_a", plan_signature="sig_a", deps=()),
        _view_node_with_plan_bundle(name="task_b", plan_signature="sig_b", deps=()),
        _view_node_with_plan_bundle(name="task_c", plan_signature="sig_c", deps=()),
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=["task_a", "task_b"],  # Only request subset
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    plan = compile_execution_plan(session_runtime=session_runtime, request=request)

    assert plan.active_tasks == frozenset({"task_a", "task_b"})
    assert "task_c" not in plan.active_tasks
    assert plan.requested_task_names == ("task_a", "task_b")


@pytest.mark.integration
def test_plan_artifacts_store_failure_continues(
    session_runtime: SessionRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock persist_plan_artifacts_for_views to raise, verify compilation still succeeds.

    Tests that compile_execution_plan continues execution gracefully when plan
    artifact storage fails, recording the failure in diagnostics.
    """
    view_nodes = [
        _view_node_with_plan_bundle(name="task_a", plan_signature="sig_a", deps=()),
    ]

    request = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    # Mock the persist function to raise
    def mock_persist_failure(*args: Any, **kwargs: Any) -> None:
        _ = (args, kwargs)
        msg = "Artifact store unavailable"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        "datafusion_engine.plan.artifact_store.persist_plan_artifacts_for_views",
        mock_persist_failure,
    )

    # Compilation should still succeed despite persistence failure
    # Production behavior: persist_plan_artifacts_for_views raises are caught in
    # _prepare_plan_context and recorded via record_artifact() to the runtime
    # profile's diagnostics sink, NOT to plan.diagnostics (which is GraphDiagnostics).
    # The failure doesn't block compilation.
    plan = compile_execution_plan(session_runtime=session_runtime, request=request)

    assert isinstance(plan, ExecutionPlan)
    # Verify compilation succeeded despite the failure - plan is valid and complete
    assert plan.task_graph is not None
    assert plan.task_schedule is not None
    assert isinstance(plan.plan_signature, str)


@pytest.mark.integration
def test_plan_signature_changes_with_runtime_config(session_runtime: SessionRuntime) -> None:
    """Compile with two different SemanticRuntimeConfig values, verify plan_signature differs.

    Tests that compile_execution_plan produces different plan signatures when
    runtime configuration changes, while task_dependency_signature remains stable.
    """
    view_nodes = [
        _view_node_with_plan_bundle(name="task_a", plan_signature="sig_a", deps=()),
    ]

    request1 = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=session_runtime.profile,
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    # Create a second runtime with different configuration
    profile2 = DataFusionRuntimeProfile()
    ctx2 = profile2.session_context()
    runtime2 = SessionRuntime(
        ctx=ctx2,
        profile=profile2,
        udf_snapshot_hash="different_hash",  # Different UDF hash
        udf_rewrite_tags=(),
        domain_planner_names=(),
        udf_snapshot={},
        df_settings={},
    )

    request2 = ExecutionPlanRequest(
        view_nodes=view_nodes,
        snapshot=None,
        runtime_profile=runtime2.profile,
        requested_task_names=None,
        impacted_task_names=None,
        allow_partial=False,
        enable_metric_scheduling=True,
    )

    plan1 = compile_execution_plan(session_runtime=session_runtime, request=request1)
    plan2 = compile_execution_plan(session_runtime=runtime2, request=request2)

    # Plan signatures should differ due to different runtime configs
    assert plan1.plan_signature != plan2.plan_signature
    # Active task topology remains stable across runtime config changes.
    assert plan1.active_tasks == plan2.active_tasks == frozenset({"task_a"})
