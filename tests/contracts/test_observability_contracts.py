"""Observability contract tests for engine diagnostics events."""

from __future__ import annotations

import pytest

from engine.spec_builder import (
    FilterTransform,
    InputRelation,
    JoinGraph,
    OutputTarget,
    RuleIntent,
    RuntimeConfig,
    SchemaContract,
    SemanticExecutionSpec,
    ViewDefinition,
)
from obs.diagnostics_report import build_diagnostics_report


def _spec_fixture() -> SemanticExecutionSpec:
    return SemanticExecutionSpec(
        version=1,
        input_relations=(InputRelation(logical_name="repo_files_v1", delta_location="/tmp/repo"),),
        view_definitions=(
            ViewDefinition(
                name="cpg_nodes_view",
                view_kind="filter",
                view_dependencies=(),
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
            ViewDefinition(
                name="cpg_edges_view",
                view_kind="filter",
                view_dependencies=(),
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
        ),
        join_graph=JoinGraph(edges=(), constraints=()),
        output_targets=(
            OutputTarget(
                table_name="cpg_nodes",
                source_view="cpg_nodes_view",
                columns=(),
                delta_location="/tmp/cpg_nodes",
            ),
            OutputTarget(
                table_name="cpg_edges",
                source_view="cpg_edges_view",
                columns=(),
                delta_location="/tmp/cpg_edges",
            ),
        ),
        rule_intents=(RuleIntent(name="semantic_integrity", rule_class="SemanticIntegrity"),),
        rulepack_profile="Default",
        typed_parameters=(),
        runtime=RuntimeConfig(),
        spec_hash=b"",
    )


def _run_result_fixture() -> dict[str, object]:
    return {
        "outputs": [
            {
                "table_name": "cpg_nodes",
                "delta_location": "/tmp/cpg_nodes",
                "rows_written": 12,
                "partition_count": 1,
            },
            {
                "table_name": "cpg_edges",
                "delta_location": "/tmp/cpg_edges",
                "rows_written": 20,
                "partition_count": 2,
            },
        ],
        "trace_metrics_summary": {
            "elapsed_compute_nanos": 2_000_000_000,
            "warning_count_total": 0,
            "warning_counts_by_code": {},
        },
        "warnings": [],
    }


def test_record_observability_emits_required_engine_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine import build_orchestrator as orchestrator_mod

    events: list[tuple[str, dict[str, object], str]] = []

    def _capture_event(
        event_name: str,
        *,
        payload: dict[str, object],
        event_kind: str,
    ) -> None:
        events.append((event_name, payload, event_kind))

    monkeypatch.setattr(orchestrator_mod, "emit_diagnostics_event", _capture_event)
    monkeypatch.setattr("obs.engine_metrics_bridge.record_engine_metrics", lambda _run: None)

    orchestrator_mod._record_observability(  # noqa: SLF001
        _spec_fixture(),
        _run_result_fixture(),
    )

    event_names = [event_name for event_name, _, _ in events]
    assert "engine_spec_summary_v1" in event_names
    assert "engine_execution_summary_v1" in event_names
    assert event_names.count("engine_output_v1") == 2

    spec_payload = next(payload for name, payload, _ in events if name == "engine_spec_summary_v1")
    assert {"view_count", "view_names", "output_target_count"} <= set(spec_payload)
    assert spec_payload["view_names"] == ["cpg_nodes_view", "cpg_edges_view"]

    output_payloads = [payload for name, payload, _ in events if name == "engine_output_v1"]
    for payload in output_payloads:
        assert {"table_name", "delta_location", "rows_written", "partition_count"} <= set(payload)


def test_diagnostics_report_plan_execution_diff_uses_engine_events() -> None:
    spans: list[dict[str, object]] = []
    gauges: dict[str, object] = {}
    snapshot = {
        "spans": spans,
        "gauges": gauges,
        "logs": [
            {
                "attributes": {
                    "event.name": "engine_spec_summary_v1",
                    "view_count": 2,
                    "view_names": ["cpg_nodes", "cpg_edges"],
                }
            },
            {
                "attributes": {
                    "event.name": "engine_output_v1",
                    "table_name": "cpg_nodes",
                    "delta_location": "/tmp/cpg_nodes",
                }
            },
            {
                "attributes": {
                    "event.name": "engine_output_v1",
                    "table_name": "cpg_edges",
                    "delta_location": "/tmp/cpg_edges",
                }
            },
        ],
    }

    report = build_diagnostics_report(snapshot)

    assert report.plan_execution_diff["expected_task_count"] == 2
    assert report.plan_execution_diff["executed_task_count"] == 2
    assert report.plan_execution_diff["missing_task_count"] == 0
    assert report.plan_execution_diff["unexpected_task_count"] == 0
