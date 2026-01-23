"""Hamilton DAG synthesis from rustworkx rule graphs.

This module generates Hamilton DAG specifications from rustworkx rule graphs,
enabling calculation-driven scheduling where the dependency graph is derived
from actual expression analysis.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from relspec.rustworkx_graph import RuleGraph
    from relspec.task_spec import TaskSpec


def _dependency_map_local(graph: RuleGraph) -> dict[str, tuple[str, ...]]:
    """Return rule -> evidence dependency map (local impl to avoid circular imports).

    This is a local copy of hamilton_pipeline.graph_synthesis.dependency_map
    to avoid circular import issues when testing.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of rule names to evidence dependencies.
    """
    from relspec.rustworkx_graph import EvidenceNode, GraphNode

    mapping: dict[str, tuple[str, ...]] = {}
    for rule_name, node_idx in graph.rule_idx.items():
        deps: list[str] = []
        for pred_idx in graph.graph.predecessor_indices(node_idx):
            pred = graph.graph[pred_idx]
            if not isinstance(pred, GraphNode) or pred.kind != "evidence":
                continue
            payload = pred.payload
            if not isinstance(payload, EvidenceNode):
                continue
            deps.append(payload.name)
        mapping[rule_name] = tuple(sorted(set(deps)))
    return mapping


@dataclass(frozen=True)
class HamiltonNodeSpec:
    """Specification for a Hamilton DAG node.

    Attributes
    ----------
    name : str
        Hamilton node name (function name).
    function_name : str
        Python function name to generate.
    inputs : tuple[str, ...]
        Input parameter names (dependencies).
    output_type : str
        Return type annotation.
    original_rule : str | None
        Original rule name this node was derived from.
    tags : Mapping[str, str]
        Hamilton tags for observability.
    cache : bool
        Whether to enable caching.
    """

    name: str
    function_name: str
    inputs: tuple[str, ...]
    output_type: str = "TableLike"
    original_rule: str | None = None
    tags: Mapping[str, str] = field(default_factory=dict)
    cache: bool = False


@dataclass(frozen=True)
class HamiltonDAGSpec:
    """Specification for a complete Hamilton DAG module.

    Attributes
    ----------
    nodes : tuple[HamiltonNodeSpec, ...]
        Node specifications in topological order.
    module_name : str
        Generated module name.
    imports : tuple[str, ...]
        Import statements for the module.
    docstring : str
        Module docstring.
    """

    nodes: tuple[HamiltonNodeSpec, ...]
    module_name: str
    imports: tuple[str, ...] = ()
    docstring: str = "Auto-generated Hamilton DAG module."

    def to_module_source(self) -> str:
        """Generate Python module source code.

        Returns
        -------
        str
            Python source code for a Hamilton module.
        """
        lines: list[str] = []

        # Module docstring
        lines.append(f'"""{self.docstring}"""')
        lines.append("")
        lines.append("from __future__ import annotations")
        lines.append("")

        # Imports
        lines.append("from typing import TYPE_CHECKING")
        lines.append("")
        if self.imports:
            lines.extend(self.imports)
            lines.append("")

        # Type checking imports
        lines.append("if TYPE_CHECKING:")
        lines.append("    from relspec.runtime_artifacts import TableLike")
        lines.append("")
        lines.append("")

        # Generate functions
        for node in self.nodes:
            func_source = _generate_node_function(node)
            lines.append(func_source)
            lines.append("")
            lines.append("")

        # __all__ export
        all_names = [node.function_name for node in self.nodes]
        all_str = ", ".join(f'"{name}"' for name in sorted(all_names))
        lines.append(f"__all__ = [{all_str}]")
        lines.append("")

        return "\n".join(lines)


def _generate_node_function(node: HamiltonNodeSpec) -> str:
    """Generate source code for a Hamilton node function.

    Parameters
    ----------
    node : HamiltonNodeSpec
        Node specification.

    Returns
    -------
    str
        Python function source code.
    """
    lines: list[str] = []

    # Decorators
    if node.cache:
        lines.append("@cache()")

    if node.tags:
        tag_items = ", ".join(f'{k}="{v}"' for k, v in sorted(node.tags.items()))
        lines.append(f"@tag({tag_items})")

    # Function signature
    if node.inputs:
        params = ", ".join(f"{name}: TableLike" for name in node.inputs)
        lines.append(f"def {node.function_name}({params}) -> {node.output_type}:")
    else:
        lines.append(f"def {node.function_name}() -> {node.output_type}:")

    # Docstring
    if node.original_rule:
        lines.append(f'    """Compute {node.original_rule}."""')
    else:
        lines.append(f'    """Compute {node.name}."""')

    # Placeholder body
    lines.append("    raise NotImplementedError")

    return "\n".join(lines)


def synthesize_hamilton_dag(
    graph: RuleGraph,
    tasks: Mapping[str, TaskSpec] | None = None,
    *,
    module_name: str = "generated_dag",
) -> HamiltonDAGSpec:
    """Generate Hamilton DAG specification from rule graph.

    Parameters
    ----------
    graph : RuleGraph
        Rustworkx rule graph.
    tasks : Mapping[str, TaskSpec] | None
        Task specifications for cache/tag metadata.
    module_name : str
        Name for the generated module.

    Returns
    -------
    HamiltonDAGSpec
        Hamilton DAG specification.
    """
    from relspec.rustworkx_graph import GraphNode, RuleNode

    deps = _dependency_map_local(graph)
    task_map = tasks or {}

    nodes: list[HamiltonNodeSpec] = []

    # Get topological order
    import rustworkx as rx

    try:
        ordered_indices = rx.topological_sort(graph.graph)
    except rx.DAGHasCycle:
        # Fall back to unsorted if cycle detected
        ordered_indices = list(graph.graph.node_indices())

    # Generate nodes in topological order
    for idx in ordered_indices:
        node = graph.graph[idx]
        if not isinstance(node, GraphNode) or node.kind != "rule":
            continue
        if not isinstance(node.payload, RuleNode):
            continue

        rule = node.payload
        task = task_map.get(rule.name)

        # Build tags
        tags: dict[str, str] = {"layer": "generated"}
        if task is not None:
            tags["kind"] = task.kind
            if task.cache_policy != "none":
                tags["cache"] = task.cache_policy

        nodes.append(
            HamiltonNodeSpec(
                name=rule.name,
                function_name=f"compute_{rule.name}",
                inputs=deps.get(rule.name, ()),
                output_type="TableLike",
                original_rule=rule.name,
                tags=tags,
                cache=task.cache_policy != "none" if task else False,
            )
        )

    imports = (
        "from hamilton.function_modifiers import cache, tag",
    )

    return HamiltonDAGSpec(
        nodes=tuple(nodes),
        module_name=module_name,
        imports=imports,
        docstring=f"Auto-generated Hamilton DAG for {len(nodes)} rules.",
    )


def hamilton_dag_from_rules(
    graph: RuleGraph,
    *,
    module_name: str = "generated_dag",
    force_materialize: set[str] | None = None,
    force_view: set[str] | None = None,
) -> HamiltonDAGSpec:
    """Generate Hamilton DAG with automatic task classification.

    Convenience function that classifies tasks and generates the DAG.

    Parameters
    ----------
    graph : RuleGraph
        Rustworkx rule graph.
    module_name : str
        Name for the generated module.
    force_materialize : set[str] | None
        Rules to force as materializations.
    force_view : set[str] | None
        Rules to force as views.

    Returns
    -------
    HamiltonDAGSpec
        Hamilton DAG specification.
    """
    from relspec.task_spec import classify_tasks_from_graph

    tasks = classify_tasks_from_graph(
        graph,
        force_materialize=force_materialize,
        force_view=force_view,
    )

    return synthesize_hamilton_dag(graph, tasks, module_name=module_name)


@dataclass(frozen=True)
class DAGComparisonResult:
    """Result of comparing two Hamilton DAG specifications.

    Attributes
    ----------
    nodes_added : tuple[str, ...]
        Nodes in new but not in old.
    nodes_removed : tuple[str, ...]
        Nodes in old but not in new.
    dependency_changes : Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]]
        Nodes with changed dependencies: name -> (old_deps, new_deps).
    is_identical : bool
        Whether the DAGs are structurally identical.
    """

    nodes_added: tuple[str, ...] = ()
    nodes_removed: tuple[str, ...] = ()
    dependency_changes: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]] = field(
        default_factory=dict
    )
    is_identical: bool = True


def compare_hamilton_dags(
    old: HamiltonDAGSpec,
    new: HamiltonDAGSpec,
) -> DAGComparisonResult:
    """Compare two Hamilton DAG specifications.

    Parameters
    ----------
    old : HamiltonDAGSpec
        Previous DAG specification.
    new : HamiltonDAGSpec
        New DAG specification.

    Returns
    -------
    DAGComparisonResult
        Comparison result with differences.
    """
    old_nodes = {node.name: node for node in old.nodes}
    new_nodes = {node.name: node for node in new.nodes}

    old_names = set(old_nodes.keys())
    new_names = set(new_nodes.keys())

    added = tuple(sorted(new_names - old_names))
    removed = tuple(sorted(old_names - new_names))

    dep_changes: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    for name in old_names & new_names:
        old_deps = old_nodes[name].inputs
        new_deps = new_nodes[name].inputs
        if old_deps != new_deps:
            dep_changes[name] = (old_deps, new_deps)

    is_identical = not added and not removed and not dep_changes

    return DAGComparisonResult(
        nodes_added=added,
        nodes_removed=removed,
        dependency_changes=dep_changes,
        is_identical=is_identical,
    )


def validate_dag_against_declared(
    dag: HamiltonDAGSpec,
    declared_deps: Mapping[str, tuple[str, ...]],
) -> DAGComparisonResult:
    """Validate generated DAG against declared dependencies.

    Parameters
    ----------
    dag : HamiltonDAGSpec
        Generated DAG specification.
    declared_deps : Mapping[str, tuple[str, ...]]
        Declared dependencies from rule definitions.

    Returns
    -------
    DAGComparisonResult
        Validation result showing discrepancies.
    """
    generated_deps = {node.name: node.inputs for node in dag.nodes}

    declared_names = set(declared_deps.keys())
    generated_names = set(generated_deps.keys())

    # Treat declared as "old" and generated as "new" for comparison semantics
    added = tuple(sorted(generated_names - declared_names))
    removed = tuple(sorted(declared_names - generated_names))

    dep_changes: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    for name in declared_names & generated_names:
        declared = declared_deps[name]
        generated = generated_deps[name]
        if set(declared) != set(generated):
            dep_changes[name] = (declared, generated)

    is_identical = not added and not removed and not dep_changes

    return DAGComparisonResult(
        nodes_added=added,
        nodes_removed=removed,
        dependency_changes=dep_changes,
        is_identical=is_identical,
    )


__all__ = [
    "DAGComparisonResult",
    "HamiltonDAGSpec",
    "HamiltonNodeSpec",
    "compare_hamilton_dags",
    "hamilton_dag_from_rules",
    "synthesize_hamilton_dag",
    "validate_dag_against_declared",
]
