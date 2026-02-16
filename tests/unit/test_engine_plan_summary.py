"""Unit tests for engine plan summary artifacts."""

from __future__ import annotations

from obs.engine_artifacts import record_engine_execution_summary, record_engine_plan_summary
from planning_engine.spec_contracts import (
    InputRelation,
    JoinEdge,
    JoinGraph,
    OutputTarget,
    RuleIntent,
    RuntimeConfig,
    SemanticExecutionSpec,
    ViewDefinition,
)

PLAN_VIEW_COUNT = 3
RULE_INTENT_COUNT = 2
TABLES_MATERIALIZED = 2
TOTAL_ROWS_WRITTEN = 12
OUTPUT_BATCHES = 2
ELAPSED_COMPUTE_NANOS = 123_000_000
WARNING_COUNT = 2


def _spec_fixture() -> SemanticExecutionSpec:
    return SemanticExecutionSpec(
        version=1,
        input_relations=(InputRelation(logical_name="repo_files_v1", delta_location="/tmp/repo"),),
        view_definitions=(
            ViewDefinition(
                name="left_view",
                view_kind="filter",
                view_dependencies=(),
                transform={"kind": "Filter", "source": "repo_files_v1", "predicate": "TRUE"},
                output_schema={"columns": {}},
            ),
            ViewDefinition(
                name="right_view",
                view_kind="filter",
                view_dependencies=(),
                transform={"kind": "Filter", "source": "repo_files_v1", "predicate": "TRUE"},
                output_schema={"columns": {}},
            ),
            ViewDefinition(
                name="joined_view",
                view_kind="relate",
                view_dependencies=("left_view", "right_view"),
                transform={
                    "kind": "Relate",
                    "left": "left_view",
                    "right": "right_view",
                    "join_type": "Inner",
                    "join_keys": [{"left_key": "id", "right_key": "id"}],
                },
                output_schema={"columns": {}},
            ),
        ),
        join_graph=JoinGraph(
            edges=(
                JoinEdge(
                    left_relation="left_view",
                    right_relation="right_view",
                    join_type="Inner",
                    left_keys=("id",),
                    right_keys=("id",),
                ),
            ),
            constraints=(),
        ),
        output_targets=(
            OutputTarget(
                table_name="cpg_nodes",
                source_view="joined_view",
                columns=(),
                delta_location="/tmp/cpg_nodes",
            ),
        ),
        rule_intents=(
            RuleIntent(name="semantic_integrity", rule_class="SemanticIntegrity"),
            RuleIntent(name="cost_shape", rule_class="CostShape"),
        ),
        rulepack_profile="Default",
        typed_parameters=(),
        runtime=RuntimeConfig(),
        spec_hash=b"",
    )


def _run_result_fixture() -> dict[str, object]:
    return {
        "spec_hash": bytes.fromhex("10" * 32),
        "envelope_hash": bytes.fromhex("20" * 32),
        "outputs": [
            {"table_name": "cpg_nodes", "rows_written": 5},
            {"table_name": "cpg_edges", "rows_written": 7},
        ],
        "trace_metrics_summary": {
            "output_rows": 12,
            "output_batches": 2,
            "elapsed_compute_nanos": 123_000_000,
            "spill_file_count": 1,
            "spilled_bytes": 512,
            "operator_count": 4,
        },
        "warnings": [
            {"code": "W001", "message": "first warning"},
            {"code": "W002", "message": "second warning"},
        ],
    }


def test_plan_summary_from_spec() -> None:
    """Test plan summary from spec."""
    summary = record_engine_plan_summary(_spec_fixture())

    assert summary.view_count == PLAN_VIEW_COUNT
    assert summary.join_edge_count == 1
    assert summary.rule_intent_count == RULE_INTENT_COUNT
    assert summary.input_relation_count == 1
    assert summary.output_target_count == 1
    assert summary.rulepack_profile == "Default"


def test_execution_summary_from_run_result() -> None:
    """Test execution summary from run result."""
    summary = record_engine_execution_summary(_run_result_fixture())

    assert summary.tables_materialized == TABLES_MATERIALIZED
    assert summary.total_rows_written == TOTAL_ROWS_WRITTEN
    assert summary.output_rows == TOTAL_ROWS_WRITTEN
    assert summary.output_batches == OUTPUT_BATCHES
    assert summary.elapsed_compute_nanos == ELAPSED_COMPUTE_NANOS
    assert summary.warning_count == WARNING_COUNT
    assert summary.warning_codes == ("W001", "W002")


def test_summary_field_types() -> None:
    """Test summary field types."""
    plan_summary = record_engine_plan_summary(_spec_fixture())
    execution_summary = record_engine_execution_summary(_run_result_fixture())

    assert isinstance(plan_summary.spec_hash, str)
    assert isinstance(plan_summary.view_count, int)
    assert isinstance(plan_summary.join_edge_count, int)
    assert isinstance(execution_summary.spec_hash, str)
    assert isinstance(execution_summary.envelope_hash, str)
    assert isinstance(execution_summary.warning_codes, tuple)
