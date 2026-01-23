"""Tests for task specification module."""

from __future__ import annotations

from relspec.rules.definitions import RuleDefinition
from relspec.rustworkx_graph import build_rule_graph_from_definitions
from relspec.task_spec import (
    TaskClassification,
    TaskClassificationOverrides,
    TaskClassificationRequest,
    TaskSpec,
    classify_task,
    classify_tasks_from_graph,
    summarize_task_classifications,
    tasks_requiring_materialization,
    tasks_with_cache_policy,
)

EXPECTED_TOTAL_TASKS: int = 4


def _rule(
    name: str,
    *,
    inputs: tuple[str, ...],
    output: str,
    priority: int = 100,
) -> RuleDefinition:
    return RuleDefinition(
        name=name,
        domain="cpg",
        kind="test",
        inputs=inputs,
        output=output,
        priority=priority,
    )


def test_task_spec_creation() -> None:
    """Create TaskSpec with all fields."""
    spec = TaskSpec(
        name="test_task",
        kind="compute",
        output="test_output",
        cache_policy="session",
        plan_fingerprint="abc123",
    )
    assert spec.name == "test_task"
    assert spec.kind == "compute"
    assert spec.output == "test_output"
    assert spec.cache_policy == "session"
    assert spec.plan_fingerprint == "abc123"


def test_task_spec_defaults() -> None:
    """TaskSpec default values."""
    spec = TaskSpec(name="test", kind="view", output="out")
    assert spec.cache_policy == "none"
    assert spec.plan_fingerprint is None
    assert spec.estimated_cost is None
    assert spec.row_estimate is None
    assert spec.metadata == {}


def test_classify_task_no_inputs() -> None:
    """Tasks with no inputs are views."""
    spec = classify_task(
        TaskClassificationRequest(
            rule_name="source_rule",
            output="source_table",
            inputs=(),
        ),
    )
    assert spec.kind == "view"
    assert spec.cache_policy == "none"


def test_classify_task_few_inputs() -> None:
    """Tasks with 1-3 inputs are compute."""
    spec = classify_task(
        TaskClassificationRequest(
            rule_name="transform_rule",
            output="transformed",
            inputs=("table_a", "table_b"),
        ),
    )
    assert spec.kind == "compute"
    assert spec.cache_policy == "session"


def test_classify_task_many_inputs() -> None:
    """Tasks with >3 inputs are materializations."""
    spec = classify_task(
        TaskClassificationRequest(
            rule_name="join_rule",
            output="joined",
            inputs=("t1", "t2", "t3", "t4"),
        ),
    )
    assert spec.kind == "materialization"
    assert spec.cache_policy == "session"


def test_classify_task_force_materialize() -> None:
    """Force materialization via override."""
    spec = classify_task(
        TaskClassificationRequest(
            rule_name="important_rule",
            output="important",
            inputs=("single_input",),  # Would normally be compute
        ),
        overrides=TaskClassificationOverrides(force_materialize={"important_rule"}),
    )
    assert spec.kind == "materialization"
    assert spec.cache_policy == "persistent"


def test_classify_task_force_view() -> None:
    """Force view via override."""
    spec = classify_task(
        TaskClassificationRequest(
            rule_name="lightweight_rule",
            output="lightweight",
            inputs=("t1", "t2", "t3", "t4"),  # Would normally be materialization
        ),
        overrides=TaskClassificationOverrides(force_view={"lightweight_rule"}),
    )
    assert spec.kind == "view"
    assert spec.cache_policy == "none"


def test_classify_task_with_fingerprint() -> None:
    """Include plan fingerprint in classification."""
    spec = classify_task(
        TaskClassificationRequest(
            rule_name="rule",
            output="out",
            inputs=("input",),
            plan_fingerprint="sha256abc",
        ),
    )
    assert spec.plan_fingerprint == "sha256abc"


def test_classify_tasks_from_graph() -> None:
    """Classify all tasks in a rule graph."""
    rules = (
        _rule("source", inputs=(), output="src_out"),
        _rule("transform", inputs=("src_out",), output="trans_out"),
        _rule("join", inputs=("trans_out", "t2", "t3", "t4"), output="final"),
    )
    graph = build_rule_graph_from_definitions(rules)
    tasks = classify_tasks_from_graph(graph)

    assert tasks["source"].kind == "view"
    assert tasks["transform"].kind == "compute"
    assert tasks["join"].kind == "materialization"


def test_classify_tasks_from_graph_with_overrides() -> None:
    """Apply overrides when classifying from graph."""
    rules = (
        _rule("source", inputs=(), output="src_out"),
        _rule("important", inputs=("src_out",), output="imp_out"),
    )
    graph = build_rule_graph_from_definitions(rules)
    tasks = classify_tasks_from_graph(
        graph,
        force_materialize={"important"},
    )

    assert tasks["source"].kind == "view"
    assert tasks["important"].kind == "materialization"


def test_summarize_task_classifications() -> None:
    """Summarize classifications."""
    tasks = {
        "view1": TaskSpec(name="view1", kind="view", output="v1"),
        "view2": TaskSpec(name="view2", kind="view", output="v2"),
        "compute1": TaskSpec(name="compute1", kind="compute", output="c1"),
        "mat1": TaskSpec(name="mat1", kind="materialization", output="m1"),
    }
    summary = summarize_task_classifications(tasks)

    assert summary.total_tasks == EXPECTED_TOTAL_TASKS
    assert summary.views == ("view1", "view2")
    assert summary.computes == ("compute1",)
    assert summary.materializations == ("mat1",)


def test_tasks_requiring_materialization() -> None:
    """Filter tasks requiring materialization."""
    tasks = [
        TaskSpec(name="view1", kind="view", output="v1"),
        TaskSpec(name="mat1", kind="materialization", output="m1"),
        TaskSpec(name="mat2", kind="materialization", output="m2"),
        TaskSpec(name="compute1", kind="compute", output="c1"),
    ]
    mat_tasks = tasks_requiring_materialization(tasks)
    assert mat_tasks == ("mat1", "mat2")


def test_tasks_with_cache_policy() -> None:
    """Filter tasks by cache policy."""
    tasks = [
        TaskSpec(name="t1", kind="view", output="o1", cache_policy="none"),
        TaskSpec(name="t2", kind="compute", output="o2", cache_policy="session"),
        TaskSpec(name="t3", kind="compute", output="o3", cache_policy="session"),
        TaskSpec(name="t4", kind="materialization", output="o4", cache_policy="persistent"),
    ]

    assert tasks_with_cache_policy(tasks, "none") == ("t1",)
    assert tasks_with_cache_policy(tasks, "session") == ("t2", "t3")
    assert tasks_with_cache_policy(tasks, "persistent") == ("t4",)


def test_task_classification_dataclass() -> None:
    """TaskClassification fields."""
    classification = TaskClassification(
        views=("v1", "v2"),
        computes=("c1",),
        materializations=("m1",),
        total_tasks=EXPECTED_TOTAL_TASKS,
    )
    assert classification.views == ("v1", "v2")
    assert classification.total_tasks == EXPECTED_TOTAL_TASKS
