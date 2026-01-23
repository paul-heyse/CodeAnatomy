# Rustworkx DAG Orchestration Plan

## Goals
- Make a rustworkx DAG the canonical, inspectable representation of relspec rule sequencing.
- Replace bespoke readiness loops with TopologicalSorter for deterministic, policy-aware scheduling.
- Provide transparent diagnostics, DOT exports, and stable graph signatures for reproducibility.
- Add first-class impact analysis (ancestors and descendants) for incremental runs and provenance.
- Keep Hamilton as the outer orchestrator while using rustworkx for fine-grained scheduling.

## Non-goals
- Remove or replace Hamilton as the top-level pipeline orchestrator.
- Rewrite DataFusion, Ibis, or SQLGlot compilers as part of this work.
- Change rule semantics without explicit policy gates and migration notes.

## Guiding Principles
- Graph is the source of truth for dependency structure and scheduling.
- Determinism over convenience: stable ordering, stable hashes, stable snapshots.
- Separate scheduling (graph) from execution (compiled outputs).
- Store metadata on nodes and edges, not in ad hoc side tables.
- Prefer rustworkx algorithms over custom graph logic where they apply.

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- Quality gates before merge:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`

## Scope 1: Canonical rustworkx graph model (rules + evidence)
Objective: define a bipartite DAG (rule nodes + evidence nodes) with stable identifiers,
explicit edge payloads, and all_producers semantics for multi-producer outputs.

Status: Completed.

Representative code
```python
# src/relspec/rustworkx_graph.py
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

import rustworkx as rx

from relspec.rules.definitions import EvidenceSpec, RuleDefinition

NodeKind = Literal["evidence", "rule"]
OutputPolicy = Literal["all_producers"]


@dataclass(frozen=True)
class EvidenceNode:
    name: str
    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleNode:
    name: str
    output: str
    inputs: tuple[str, ...]
    priority: int
    evidence: EvidenceSpec | None


GraphNode = tuple[NodeKind, object]


@dataclass(frozen=True)
class RuleGraph:
    graph: rx.PyDiGraph
    evidence_idx: Mapping[str, int]
    rule_idx: Mapping[str, int]


def build_rule_graph(
    rules: Sequence[RuleDefinition],
    *,
    output_policy: OutputPolicy = "all_producers",
) -> RuleGraph:
    graph = rx.PyDiGraph(multigraph=False, check_cycle=False, attrs={"label": "relspec_rules"})
    evidence_idx: dict[str, int] = {}
    rule_idx: dict[str, int] = {}

    evidence_names = _collect_evidence_names(rules)

    for name in sorted(evidence_names):
        evidence_idx[name] = graph.add_node(("evidence", EvidenceNode(name=name)))

    for rule in rules:
        node = RuleNode(
            name=rule.name,
            output=rule.output,
            inputs=rule.inputs,
            priority=rule.priority,
            evidence=rule.evidence,
        )
        rule_idx[rule.name] = graph.add_node(("rule", node))

    edges: list[tuple[int, int, dict[str, object]]] = []
    for rule in rules:
        rule_node = rule_idx[rule.name]
        for input_name in rule.inputs:
            edges.append(
                (evidence_idx[input_name], rule_node, {"kind": "requires", "name": input_name})
            )
        edges.append(
            (rule_node, evidence_idx[rule.output], {"kind": "produces", "name": rule.output})
        )

    graph.add_edges_from(edges)
    return RuleGraph(graph=graph, evidence_idx=evidence_idx, rule_idx=rule_idx)
```

Target files
- `src/relspec/rustworkx_graph.py` (new)
- `src/relspec/graph.py`
- `src/relspec/rules/definitions.py`
- `src/relspec/rules/evidence.py`
- `tests/unit/test_relspec_graph.py`

Implementation checklist
- [x] Define RuleGraph, EvidenceNode, RuleNode, and OutputPolicy types.
- [x] Enforce all_producers semantics for multi-producer outputs.
- [x] Encode "requires" and "produces" edge payloads with evidence metadata.
- [x] Provide adapters for both RuleDefinition and RelationshipRule inputs.
- [x] Add tests for node/edge counts, payloads, and deterministic node IDs.

## Scope 2: Deterministic scheduling and readiness loop
Objective: replace bespoke readiness scans with TopologicalSorter and stable tie-breaking,
support partial runs via seed evidence nodes, and expose generation waves for parallelism.

Status: Completed.

Representative code
```python
# src/relspec/rustworkx_schedule.py
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import rustworkx as rx

from relspec.rustworkx_graph import EvidenceNode, RuleGraph, RuleNode


@dataclass(frozen=True)
class RuleSchedule:
    ordered_rules: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]


def schedule_rules(
    graph: RuleGraph,
    *,
    seed_evidence: Iterable[str] = (),
) -> RuleSchedule:
    seed_nodes = [graph.evidence_idx[name] for name in seed_evidence if name in graph.evidence_idx]
    sorter = rx.TopologicalSorter(graph.graph, check_cycle=True, initial=seed_nodes)

    ordered: list[str] = []
    while sorter.is_active():
        ready = sorter.get_ready()
        if not ready:
            break
        ready_rules = [
            idx for idx in ready if graph.graph[idx][0] == "rule"
        ]
        ready_rules.sort(
            key=lambda idx: (
                graph.graph[idx][1].priority,
                graph.graph[idx][1].name,
            )
        )
        ready_evidence = [idx for idx in ready if graph.graph[idx][0] == "evidence"]
        for idx in ready_rules:
            ordered.append(graph.graph[idx][1].name)
        sorter.done(ready_evidence + ready_rules)

    generations = tuple(
        tuple(graph.graph[idx][1].name for idx in generation if graph.graph[idx][0] == "rule")
        for generation in rx.topological_generations(graph.graph)
    )
    return RuleSchedule(ordered_rules=tuple(ordered), generations=generations)
```

Target files
- `src/relspec/rustworkx_schedule.py` (new)
- `src/relspec/graph.py`
- `tests/unit/test_relspec_graph.py`

Implementation checklist
- [x] Implement TopologicalSorter scheduling with deterministic tie-breaks.
- [x] Support `initial` seed evidence nodes for partial runs.
- [x] Expose both total order and generation waves for parallelism.
- [x] Add `lexicographical_topological_sort` for deterministic snapshots.
- [x] Include critical-path diagnostics using `dag_longest_path_length`.

## Scope 3: Diagnostics, cycles, and visualization
Objective: add cycle reporting, SCC grouping for fixpoints, transitive reduction for clarity,
and DOT export for CI artifacts and local inspection.

Status: Completed.

Representative code
```python
# src/relspec/rustworkx_graph.py
import rustworkx as rx


def dag_diagnostics(graph: RuleGraph) -> dict[str, object]:
    g = graph.graph
    if not rx.is_directed_acyclic_graph(g):
        return {
            "status": "cycle",
            "cycles": rx.simple_cycles(g),
            "scc": rx.strongly_connected_components(g),
        }
    reduced, mapping = rx.transitive_reduction(g)
    return {
        "status": "ok",
        "dot": reduced.to_dot(),
        "node_map": dict(mapping),
    }
```

Target files
- `src/relspec/rustworkx_graph.py`
- `src/obs/repro.py`
- `tests/unit/test_relspec_graph.py`

Implementation checklist
- [x] Add cycle detection with `is_directed_acyclic_graph` and `simple_cycles`.
- [x] Record SCC groups for fixpoint planning with `strongly_connected_components`.
- [x] Provide `transitive_reduction` and DOT export helpers.
- [x] Normalize rustworkx return types for deterministic serialization.
- [x] Emit diagnostics artifacts in `obs/repro.py`.

## Scope 4: Graph identity and stable snapshots
Objective: formalize a stable rule graph signature that includes nodes, edges, evidence
requirements, and output semantics, and persist snapshots for repro and audits.

Status: Completed.

Representative code
```python
# src/relspec/rustworkx_graph.py
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.io.ipc import payload_hash

_RULE_GRAPH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("label", pa.string()),
        pa.field("nodes", pa.list_(pa.struct([pa.field("id", pa.int32()), pa.field("name", pa.string())]))),
        pa.field("edges", pa.list_(pa.struct([pa.field("source", pa.int32()), pa.field("target", pa.int32()), pa.field("kind", pa.string())]))),
    ]
)


@dataclass(frozen=True)
class RuleGraphSnapshot:
    version: int
    label: str
    nodes: tuple[dict[str, object], ...]
    edges: tuple[dict[str, object], ...]


def rule_graph_signature(snapshot: RuleGraphSnapshot) -> str:
    payload = {
        "version": snapshot.version,
        "label": snapshot.label,
        "nodes": list(snapshot.nodes),
        "edges": list(snapshot.edges),
    }
    return payload_hash(payload, _RULE_GRAPH_SCHEMA)
```

Target files
- `src/relspec/rustworkx_graph.py`
- `src/relspec/registry/snapshot.py`
- `src/relspec/rules/cache.py`
- `src/incremental/invalidations.py`
- `src/obs/repro.py`
- `tests/unit/test_relspec_graph.py`

Implementation checklist
- [x] Define RuleGraphSnapshot and schema for hashing.
- [x] Serialize nodes and edges deterministically (sorted, stable IDs).
- [x] Extend `rule_graph_signature` to include edges and policy metadata.
- [x] Add `obs/repro.py` hooks to persist snapshots and hashes.

## Scope 5: Incremental impact and provenance
Objective: compute impacted rules and evidence via descendants and ancestors, integrate
with incremental config, and expose provenance for debugging and audits.

Status: Partially completed (impact/provenance helpers and artifacts done; incremental integration pending).

Representative code
```python
# src/relspec/rustworkx_schedule.py
import rustworkx as rx

from relspec.rustworkx_graph import RuleGraph


def impacted_rules(graph: RuleGraph, *, evidence_name: str) -> tuple[str, ...]:
    node = graph.evidence_idx.get(evidence_name)
    if node is None:
        return ()
    impacted = rx.descendants(graph.graph, node)
    names = [
        graph.graph[idx][1].name
        for idx in impacted
        if graph.graph[idx][0] == "rule"
    ]
    return tuple(sorted(set(names)))


def provenance_for_rule(graph: RuleGraph, *, rule_name: str) -> tuple[str, ...]:
    node = graph.rule_idx.get(rule_name)
    if node is None:
        return ()
    ancestors = rx.ancestors(graph.graph, node)
    inputs = [
        graph.graph[idx][1].name
        for idx in ancestors
        if graph.graph[idx][0] == "evidence"
    ]
    return tuple(sorted(set(inputs)))
```

Target files
- `src/relspec/rustworkx_schedule.py`
- `src/relspec/incremental.py`
- `src/hamilton_pipeline/modules/incremental.py`
- `tests/relspec/test_rustworkx_incremental.py` (new)

Implementation checklist
- [x] Add impact and provenance helpers (descendants, ancestors).
- [ ] Integrate impact sets with incremental config and state.
- [x] Emit provenance artifacts in `obs/repro.py`.
- [x] Add tests for impact closure and provenance ordering.

## Scope 6: Execution integration inside Hamilton nodes
Objective: keep Hamilton as orchestrator while using rustworkx to order and batch rule
execution, and to expose schedule artifacts as standard pipeline outputs.

Status: Mostly completed (execution integration and artifacts done; schedule metadata in exec events pending).

Representative code
```python
# src/hamilton_pipeline/modules/cpg_build.py
from relspec.rustworkx_graph import build_rule_graph
from relspec.rustworkx_schedule import schedule_rules


@tag(layer="relspec", artifact="relspec_rule_schedule", kind="object")
def relspec_rule_schedule(
    rule_registry_context: RuleRegistryContext,
) -> RuleSchedule:
    rules = rule_registry_context.rule_registry.rules_for_domain("cpg")
    graph = build_rule_graph(rules)
    return schedule_rules(graph)
```

```python
# src/hamilton_pipeline/modules/cpg_build.py
def relationship_tables(
    relationship_execution_context: RelationshipExecutionContext,
    relationship_table_inputs: RelationshipTableInputs,
    relspec_rule_schedule: RuleSchedule,
) -> dict[str, TableLike]:
    # Execute by generation to preserve readiness semantics.
    for generation in relspec_rule_schedule.generations:
        _execute_generation(
            generation=generation,
            inputs=relationship_table_inputs,
            ctx=relationship_execution_context,
        )
    return _finalize_outputs(...)
```

Target files
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/relspec/compiler.py`
- `src/relspec/rustworkx_graph.py`
- `src/relspec/rustworkx_schedule.py`
- `tests/hamilton_pipeline/test_relspec_schedule.py` (new)

Implementation checklist
- [x] Add Hamilton node for rule graph and rule schedule artifacts.
- [x] Execute rules by generation, updating dynamic resolvers per wave.
- [x] Preserve existing compiled output finalization semantics.
- [ ] Record rule execution events with schedule metadata.

## Scope 7: Hamilton DAG synthesis from rustworkx adjacency
Objective: provide an optional path to generate Hamilton-compatible dependency maps
or module stubs from the rustworkx graph for deep alignment and introspection.

Status: Partially completed (dependency map and artifacts emitted; DAG validation pending).

Representative code
```python
# src/hamilton_pipeline/graph_synthesis.py
from __future__ import annotations

from collections.abc import Mapping

from relspec.rustworkx_graph import RuleGraph


def dependency_map(graph: RuleGraph) -> Mapping[str, tuple[str, ...]]:
    mapping: dict[str, tuple[str, ...]] = {}
    for rule_name, node_idx in graph.rule_idx.items():
        preds = graph.graph.predecessor_indices(node_idx)
        deps = tuple(
            graph.graph[idx][1].name
            for idx in preds
            if graph.graph[idx][0] == "evidence"
        )
        mapping[rule_name] = deps
    return mapping
```

Target files
- `src/hamilton_pipeline/graph_synthesis.py` (new)
- `src/hamilton_pipeline/driver_factory.py`
- `src/relspec/rustworkx_graph.py`

Implementation checklist
- [x] Generate a dependency map from the rustworkx graph.
- [ ] Decide how to render map into Hamilton modules or driver inputs.
- [ ] Add a validation step to compare Hamilton DAG vs rustworkx DAG.
- [x] Expose synthesized DAG metadata in diagnostics artifacts.

## Decommission (post-migration cleanup)
Status: Completed.

These are removed only after rustworkx scheduling is the sole path and parity checks pass.

Functions and types to decommission
- [x] `src/relspec/graph.py: order_rules` (replaced by rustworkx TopologicalSorter scheduling)
- [x] `src/relspec/graph.py: order_rules_by_evidence` (replaced by rustworkx scheduling + output semantics)
- [x] `src/relspec/graph.py: RuleSelectors` (replaced by graph builders over RuleDefinition inputs)
- [x] `src/relspec/graph.py: RuleNode` (replaced by rustworkx RuleNode/EvidenceNode types)
- [x] `src/relspec/graph.py: rule_graph_signature` (replaced by rustworkx snapshot signature)
- [x] `src/relspec/graph.py: RULE_GRAPH_SIGNATURE_VERSION` (moved to rustworkx graph signature schema)
- [x] `src/relspec/graph.py: _RULE_GRAPH_SCHEMA` (moved to rustworkx graph signature schema)
- [x] `src/relspec/graph.py: _ready_rules` (replaced by TopologicalSorter readiness)
- [x] `src/relspec/graph.py: _select_by_output` (replaced by all_producers semantics in graph)
- [x] `src/relspec/graph.py: _register_rule_output` (replaced by graph-driven evidence updates)
- [x] `src/relspec/graph.py: _register_output` (replaced by graph-driven evidence updates)
- [x] `src/relspec/graph.py: _central_evidence` (absorbed into EvidenceNode construction)

Modules to delete
- [x] `src/relspec/compiler_graph.py` (compat wrapper for legacy graph helpers)
- [x] `src/relspec/rules/graph.py` (compat wrapper for legacy graph helpers)
