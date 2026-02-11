"""Unit tests for engine plan summary artifacts."""

from __future__ import annotations

from obs.engine_artifacts import record_engine_execution_summary, record_engine_plan_summary
from planning_engine.spec_builder import (
    FilterTransform,
    InputRelation,
    JoinGraph,
    JoinKeyPair,
    OutputTarget,
    RelateTransform,
    RuleIntent,
    RuntimeConfig,
    SchemaContract,
    SemanticExecutionSpec,
    ViewDefinition,
)


def _spec_fixture() -> SemanticExecutionSpec:
    return SemanticExecutionSpec(
        version=1,
        input_relations=(InputRelation(logical_name="repo_files_v1", delta_location="/tmp/repo"),),
        view_definitions=(
            ViewDefinition(
                name="left_view",
                view_kind="filter",
                view_dependencies=(),
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
            ViewDefinition(
                name="right_view",
                view_kind="filter",
                view_dependencies=(),
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
            ViewDefinition(
                name="joined_view",
                view_kind="relate",
                view_dependencies=("left_view", "right_view"),
                transform=RelateTransform(
                    left="left_view",
                    right="right_view",
                    join_type="Inner",
                    join_keys=(JoinKeyPair(left_key="id", right_key="id"),),
                ),
                output_schema=SchemaContract(),
            ),
        ),
        join_graph=JoinGraph(edges=(), constraints=()),
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
    summary = record_engine_plan_summary(_spec_fixture())

    assert summary.view_count == 3
    assert summary.join_edge_count == 1
    assert summary.rule_intent_count == 2
    assert summary.input_relation_count == 1
    assert summary.output_target_count == 1
    assert summary.rulepack_profile == "Default"


def test_execution_summary_from_run_result() -> None:
    summary = record_engine_execution_summary(_run_result_fixture())

    assert summary.tables_materialized == 2
    assert summary.total_rows_written == 12
    assert summary.output_rows == 12
    assert summary.output_batches == 2
    assert summary.elapsed_compute_nanos == 123_000_000
    assert summary.warning_count == 2
    assert summary.warning_codes == ("W001", "W002")


def test_summary_field_types() -> None:
    plan_summary = record_engine_plan_summary(_spec_fixture())
    execution_summary = record_engine_execution_summary(_run_result_fixture())

    assert isinstance(plan_summary.spec_hash, str)
    assert isinstance(plan_summary.view_count, int)
    assert isinstance(plan_summary.join_edge_count, int)
    assert isinstance(execution_summary.spec_hash, str)
    assert isinstance(execution_summary.envelope_hash, str)
    assert isinstance(execution_summary.warning_codes, tuple)
