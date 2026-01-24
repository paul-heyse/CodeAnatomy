# Rustworkx Inference Graph Enhancements Plan

Date: January 24, 2026

Goals
- Improve scheduling determinism and scale for inference-driven task graphs.
- Add richer diagnostics without incurring worst-case cycle detection costs by default.
- Use rustworkx-native features for generations, subgraph scheduling, and export.

----------------------------------------------------------------------
Scope 1: Preallocated + batch graph construction
----------------------------------------------------------------------
Status: Implemented
Why
- Task graphs are fully known at build time (tasks + evidence + edges).
- rustworkx supports capacity hints and batch insertion to reduce Python <-> Rust overhead.

Representative pattern
```python
node_count_hint = len(evidence_names) + len(tasks)
edge_count_hint = sum(len(task.sources) + 1 for task in tasks)

graph = rx.PyDiGraph(
    multigraph=False,
    check_cycle=False,
    attrs={"label": "relspec"},
    node_count_hint=node_count_hint,
    edge_count_hint=edge_count_hint,
)

# Batch node inserts
node_payloads: list[GraphNode] = [
    GraphNode("evidence", EvidenceNode(name)) for name in sorted(evidence_names)
] + [
    GraphNode("task", task) for task in sorted(tasks, key=lambda item: item.name)
]
node_indices = graph.add_nodes_from(node_payloads)

# Batch edges (source, target, payload)
edge_payloads: list[tuple[int, int, GraphEdge]] = [
    (source_idx, task_idx, GraphEdge(...)),
    (task_idx, output_idx, GraphEdge(...)),
]
graph.add_edges_from(edge_payloads)
```

Target files
- src/relspec/rustworkx_graph.py
- src/relspec/graph_inference.py (if build flow changes)
- tests covering graph shape / diagnostics

Implementation checklist
- [x] Compute node_count_hint/edge_count_hint from inferred deps.
- [x] Replace per-node add loop with add_nodes_from.
- [x] Replace per-edge add loop with add_edges_from.
- [x] Preserve deterministic node ordering and index maps.
- [x] Verify graph snapshot hashing remains stable.

Decommission candidates
- src/relspec/rustworkx_graph.py: per-node/per-edge insertion loops once batch APIs are used.

----------------------------------------------------------------------
Scope 2: Cycle diagnostics without full enumeration by default
----------------------------------------------------------------------
Status: Implemented
Why
- simple_cycles can be expensive on large graphs.
- We only need to know there is a cycle by default; full enumeration should be opt-in.

Representative pattern
```python
def task_graph_diagnostics(
    graph: TaskGraph,
    *,
    include_cycles: bool = False,
) -> GraphDiagnostics:
    g = graph.graph
    if not rx.is_directed_acyclic_graph(g):
        cycle_sample = rx.digraph_find_cycle(g)
        cycles = tuple(tuple(cycle) for cycle in rx.simple_cycles(g)) if include_cycles else ()
        scc = tuple(tuple(component) for component in rx.strongly_connected_components(g))
        return GraphDiagnostics(
            status="cycle",
            cycle_sample=tuple(cycle_sample),
            cycles=cycles,
            scc=scc,
        )
    ...
```

Target files
- src/relspec/rustworkx_graph.py

Implementation checklist
- [x] Add an include_cycles toggle to diagnostics API.
- [x] Add a cycle_sample field to GraphDiagnostics.
- [x] Replace unconditional simple_cycles with digraph_find_cycle + opt-in full cycles.
- [x] Update callers/tests to pass include_cycles when needed.

Decommission candidates
- None (behavioral replacement within existing diagnostics).

----------------------------------------------------------------------
Scope 3: Connectivity health checks
----------------------------------------------------------------------
Status: Implemented
Why
- Isolates and weak components identify orphaned tasks/evidence early.
- Connectivity signals help validate inferred dependency coverage.

Representative pattern
```python
components = tuple(tuple(c) for c in rx.weakly_connected_components(g))
isolates = tuple(rx.isolates(g))

return GraphDiagnostics(
    status="ok",
    weak_components=components,
    isolates=isolates,
    ...,
)
```

Target files
- src/relspec/rustworkx_graph.py

Implementation checklist
- [x] Add weak_components and isolates fields to GraphDiagnostics.
- [x] Compute these only for DAG status == ok.
- [x] Provide helper for mapping isolate node ids to task/evidence names.

Decommission candidates
- None.

----------------------------------------------------------------------
Scope 4: Task-only DAG generations
----------------------------------------------------------------------
Status: Implemented
Why
- Topological generations provide canonical parallel waves.
- Evidence nodes are an execution artifact; generations should be task-only.

Representative pattern
```python
def _task_dependency_graph(graph: TaskGraph) -> rx.PyDiGraph:
    # Build a task-only DAG using evidence nodes to derive edges.
    dep_graph = rx.PyDiGraph(multigraph=False, check_cycle=False)
    node_indices = dep_graph.add_nodes_from(tasks)
    dep_graph.add_edges_from(edge_payloads)
    return dep_graph


def task_generations(graph: TaskGraph) -> tuple[tuple[str, ...], ...]:
    dep_graph = _task_dependency_graph(graph)
    generations = rx.topological_generations(dep_graph)
    return tuple(tuple(dep_graph[idx].name for idx in gen) for gen in generations)
```

Target files
- src/relspec/rustworkx_schedule.py
- src/relspec/rustworkx_graph.py (helper for task-only graph)

Implementation checklist
- [x] Introduce a task-only view of the graph with stable node mapping.
- [x] Use topological_generations or layers to build schedule.generations.
- [x] Ensure ordering within a generation respects priority/name ordering.
- [x] Keep evidence gating (validate_edge_requirements) for readiness checks.

Decommission candidates
- src/relspec/rustworkx_schedule.py: manual generation assembly once generations are derived from rustworkx.

----------------------------------------------------------------------
Scope 5: Impact subgraph scheduling for incremental runs
----------------------------------------------------------------------
Status: Implemented
Why
- Filtering a full schedule by allowed task names hides dependency structure.
- Scheduling a derived subgraph preserves ordering/diagnostics and reduces work.

Representative pattern
```python
def task_graph_impact_subgraph(graph: TaskGraph, task_names: Iterable[str]) -> TaskGraph:
    impacted: set[int] = set()
    for name in task_names:
        idx = graph.task_idx.get(name)
        if idx is None:
            continue
        impacted.add(idx)
        impacted.update(rx.ancestors(graph.graph, idx))
        impacted.update(rx.descendants(graph.graph, idx))
    return task_graph_subgraph(graph, node_ids=impacted)
```

Target files
- src/hamilton_pipeline/modules/task_execution.py
- src/relspec/rustworkx_graph.py (rehydration helper)
- src/relspec/rustworkx_schedule.py (schedule on subgraph)

Implementation checklist
- [x] Add helper to build a TaskGraph view from a rustworkx subgraph + mapping.
- [x] Replace allowed_task_names with impact subgraph scheduling.
- [x] Ensure evidence seeding considers only the subgraph outputs.
- [x] Add tests for incremental schedule correctness.

Decommission candidates
- src/hamilton_pipeline/modules/task_execution.py: allowed_task_names and per-task filtering logic.

----------------------------------------------------------------------
Scope 6: Node-link JSON exports for diagnostics and tooling
----------------------------------------------------------------------
Status: Implemented
Why
- Node-link JSON is a compact, standard interchange format for tooling and UIs.
- Helps debug planning by exporting exact rustworkx payloads.

Representative pattern
```python
def task_graph_node_link_json(graph: TaskGraph) -> str:
    return rx.node_link_json(
        graph.graph,
        graph_attrs=_node_link_graph_attrs,
        node_attrs=_node_link_node_attrs,
        edge_attrs=_node_link_edge_attrs,
    )
```

Target files
- src/relspec/rustworkx_graph.py
- docs/plans/rustworkx_inference_enhancement_plan.md (this plan)

Implementation checklist
- [x] Add a node_link export helper (task_graph_node_link or similar).
- [x] Optionally gate output behind a diagnostics flag.
- [x] Ensure payloads are JSON-serializable (or provide a custom encoder).

Decommission candidates
- None.

----------------------------------------------------------------------
Decommission and delete list (post-implementation)
----------------------------------------------------------------------
- src/hamilton_pipeline/modules/task_execution.py: allowed_task_names (replaced by impact subgraph scheduling).
- src/relspec/rustworkx_schedule.py: manual generation assembly (replaced by topological_generations).
- src/relspec/rustworkx_graph.py: per-node/per-edge insertion loops (replaced by add_nodes_from/add_edges_from).
