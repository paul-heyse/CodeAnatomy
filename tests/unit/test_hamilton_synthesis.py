"""Tests for Hamilton DAG synthesis module."""

from __future__ import annotations

from relspec.hamilton_synthesis import (
    DAGComparisonResult,
    HamiltonDAGSpec,
    HamiltonNodeSpec,
    compare_hamilton_dags,
    hamilton_dag_from_rules,
    synthesize_hamilton_dag,
    validate_dag_against_declared,
)
from relspec.rules.definitions import RuleDefinition
from relspec.rustworkx_graph import build_rule_graph_from_definitions
from relspec.task_spec import TaskSpec

EXPECTED_NODE_COUNT_TWO: int = 2


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


def test_hamilton_node_spec_creation() -> None:
    """Create HamiltonNodeSpec with all fields."""
    spec = HamiltonNodeSpec(
        name="test_node",
        function_name="compute_test_node",
        inputs=("input_a", "input_b"),
        output_type="TableLike",
        original_rule="test_rule",
        tags={"layer": "generated"},
        cache=True,
    )
    assert spec.name == "test_node"
    assert spec.function_name == "compute_test_node"
    assert spec.inputs == ("input_a", "input_b")
    assert spec.output_type == "TableLike"
    assert spec.cache is True


def test_hamilton_node_spec_defaults() -> None:
    """HamiltonNodeSpec default values."""
    spec = HamiltonNodeSpec(
        name="node",
        function_name="compute_node",
        inputs=(),
    )
    assert spec.output_type == "TableLike"
    assert spec.original_rule is None
    assert spec.tags == {}
    assert spec.cache is False


def test_hamilton_dag_spec_creation() -> None:
    """Create HamiltonDAGSpec."""
    nodes = (
        HamiltonNodeSpec(name="node1", function_name="compute_node1", inputs=()),
        HamiltonNodeSpec(name="node2", function_name="compute_node2", inputs=("node1",)),
    )
    dag = HamiltonDAGSpec(
        nodes=nodes,
        module_name="test_module",
        imports=("from hamilton.function_modifiers import cache",),
    )
    assert len(dag.nodes) == EXPECTED_NODE_COUNT_TWO
    assert dag.module_name == "test_module"


def test_hamilton_dag_spec_to_module_source() -> None:
    """Generate Python source from DAG spec."""
    nodes = (
        HamiltonNodeSpec(
            name="node1",
            function_name="compute_node1",
            inputs=(),
            original_rule="rule1",
        ),
        HamiltonNodeSpec(
            name="node2",
            function_name="compute_node2",
            inputs=("node1",),
            original_rule="rule2",
            cache=True,
        ),
    )
    dag = HamiltonDAGSpec(
        nodes=nodes,
        module_name="test_dag",
        imports=("from hamilton.function_modifiers import cache, tag",),
    )

    source = dag.to_module_source()

    # Check essential elements
    assert "from __future__ import annotations" in source
    assert "def compute_node1" in source
    assert "def compute_node2" in source
    assert "TableLike" in source
    assert "__all__" in source


def test_synthesize_hamilton_dag() -> None:
    """Synthesize DAG from rule graph."""
    rules = (
        _rule("alpha", inputs=("src1",), output="out1"),
        _rule("beta", inputs=("out1", "src2"), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)
    dag = synthesize_hamilton_dag(graph)

    assert len(dag.nodes) == EXPECTED_NODE_COUNT_TWO
    node_names = {n.name for n in dag.nodes}
    assert "alpha" in node_names
    assert "beta" in node_names

    # Check beta has alpha's output as dependency
    beta_node = next(n for n in dag.nodes if n.name == "beta")
    assert "out1" in beta_node.inputs or "src2" in beta_node.inputs


def test_synthesize_hamilton_dag_with_tasks() -> None:
    """Synthesize DAG with task specifications."""
    rules = (
        _rule("alpha", inputs=("src1",), output="out1"),
        _rule("beta", inputs=("out1",), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)
    tasks = {
        "alpha": TaskSpec(name="alpha", kind="view", output="out1", cache_policy="none"),
        "beta": TaskSpec(name="beta", kind="materialization", output="out2", cache_policy="persistent"),
    }
    dag = synthesize_hamilton_dag(graph, tasks)

    alpha_node = next(n for n in dag.nodes if n.name == "alpha")
    beta_node = next(n for n in dag.nodes if n.name == "beta")

    assert alpha_node.cache is False  # view with no caching
    assert beta_node.cache is True  # materialization with caching


def test_hamilton_dag_from_rules() -> None:
    """Convenience function with automatic classification."""
    rules = (
        _rule("source", inputs=(), output="src_out"),
        _rule("transform", inputs=("src_out",), output="trans_out"),
    )
    graph = build_rule_graph_from_definitions(rules)
    dag = hamilton_dag_from_rules(graph, module_name="generated")

    assert dag.module_name == "generated"
    assert len(dag.nodes) == EXPECTED_NODE_COUNT_TWO


def test_hamilton_dag_from_rules_with_overrides() -> None:
    """Force materialization in DAG generation."""
    rules = (
        _rule("important", inputs=("src",), output="out"),
    )
    graph = build_rule_graph_from_definitions(rules)
    dag = hamilton_dag_from_rules(
        graph,
        force_materialize={"important"},
    )

    node = dag.nodes[0]
    assert node.cache is True  # Forced materialization enables caching


def test_compare_hamilton_dags_identical() -> None:
    """Compare identical DAGs."""
    nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=("a",)),
    )
    dag1 = HamiltonDAGSpec(nodes=nodes, module_name="test")
    dag2 = HamiltonDAGSpec(nodes=nodes, module_name="test")

    result = compare_hamilton_dags(dag1, dag2)

    assert result.is_identical is True
    assert result.nodes_added == ()
    assert result.nodes_removed == ()
    assert result.dependency_changes == {}


def test_compare_hamilton_dags_added_nodes() -> None:
    """Detect added nodes between DAGs."""
    old_nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=()),
    )
    new_nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=()),
        HamiltonNodeSpec(name="node2", function_name="f2", inputs=("node1",)),
    )
    old_dag = HamiltonDAGSpec(nodes=old_nodes, module_name="test")
    new_dag = HamiltonDAGSpec(nodes=new_nodes, module_name="test")

    result = compare_hamilton_dags(old_dag, new_dag)

    assert result.is_identical is False
    assert "node2" in result.nodes_added
    assert result.nodes_removed == ()


def test_compare_hamilton_dags_removed_nodes() -> None:
    """Detect removed nodes between DAGs."""
    old_nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=()),
        HamiltonNodeSpec(name="node2", function_name="f2", inputs=()),
    )
    new_nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=()),
    )
    old_dag = HamiltonDAGSpec(nodes=old_nodes, module_name="test")
    new_dag = HamiltonDAGSpec(nodes=new_nodes, module_name="test")

    result = compare_hamilton_dags(old_dag, new_dag)

    assert result.is_identical is False
    assert "node2" in result.nodes_removed


def test_compare_hamilton_dags_dependency_changes() -> None:
    """Detect dependency changes between DAGs."""
    old_nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=("a", "b")),
    )
    new_nodes = (
        HamiltonNodeSpec(name="node1", function_name="f1", inputs=("a", "c")),
    )
    old_dag = HamiltonDAGSpec(nodes=old_nodes, module_name="test")
    new_dag = HamiltonDAGSpec(nodes=new_nodes, module_name="test")

    result = compare_hamilton_dags(old_dag, new_dag)

    assert result.is_identical is False
    assert "node1" in result.dependency_changes
    old_deps, new_deps = result.dependency_changes["node1"]
    assert old_deps == ("a", "b")
    assert new_deps == ("a", "c")


def test_validate_dag_against_declared() -> None:
    """Validate generated DAG against declared dependencies."""
    nodes = (
        HamiltonNodeSpec(name="rule1", function_name="f1", inputs=("a", "b")),
        HamiltonNodeSpec(name="rule2", function_name="f2", inputs=("rule1",)),
    )
    dag = HamiltonDAGSpec(nodes=nodes, module_name="test")

    declared = {
        "rule1": ("a", "b"),
        "rule2": ("rule1",),
    }

    result = validate_dag_against_declared(dag, declared)
    assert result.is_identical is True


def test_validate_dag_against_declared_mismatch() -> None:
    """Detect mismatches between generated and declared."""
    nodes = (
        HamiltonNodeSpec(name="rule1", function_name="f1", inputs=("a", "c")),  # 'c' instead of 'b'
    )
    dag = HamiltonDAGSpec(nodes=nodes, module_name="test")

    declared = {
        "rule1": ("a", "b"),
    }

    result = validate_dag_against_declared(dag, declared)
    assert result.is_identical is False
    assert "rule1" in result.dependency_changes


def test_dag_comparison_result_dataclass() -> None:
    """DAGComparisonResult fields."""
    result = DAGComparisonResult(
        nodes_added=("new_node",),
        nodes_removed=("old_node",),
        dependency_changes={"changed": (("a",), ("b",))},
        is_identical=False,
    )
    assert result.nodes_added == ("new_node",)
    assert result.nodes_removed == ("old_node",)
    assert result.is_identical is False
