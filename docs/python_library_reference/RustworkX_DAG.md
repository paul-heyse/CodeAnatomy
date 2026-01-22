Below are a few “representative but fairly sophisticated” patterns for using **rustworkx** as the *graph kernel* behind:

1. **relationship rule ordering** (compile-time DAG building + deterministic ordering)
2. **evidence readiness loops** (runtime “what can I run now?”)
3. **dependency-style scheduling** (parallel execution, prioritization, partial rebuilds)

I’ll keep the examples domain-neutral but name things the way a “rules → evidence → relationship edges” system typically looks.

---

## 0) The rustworkx primitives you’ll reach for most

### Graph data structure

* **`rx.PyDiGraph`** for directed dependency graphs. Nodes and edges can hold **arbitrary Python objects** (perfect for storing `Rule` objects, metadata, edge annotations, etc.). ([Rustworkx][1])
* You can enable **runtime cycle checks** via `check_cycle=True` (safer during incremental construction; costs overhead). ([Rustworkx][1])

### Core DAG ordering / readiness APIs

* **`rx.topological_sort(graph)`**: one-shot topological ordering of node indices. ([Rustworkx][2])
* **`rx.lexicographical_topological_sort(dag, key=...)`**: deterministic topo order (tie-breaks using your key). ([Rustworkx][3])
* **`rx.topological_generations(dag)`**: topo order grouped into “generations” (great for parallel stages). ([Rustworkx][4])
* **`rx.TopologicalSorter(graph, ...)`**: stateful / incremental topo scheduling with `get_ready()` + `done()` (this maps *directly* to “evidence readiness”). ([Rustworkx][5])

### Sanity + debugging

* **`rx.is_directed_acyclic_graph(graph)`**: quick DAG check. ([Rustworkx][6])
* **`rx.simple_cycles(graph)`**: enumerate cycles to debug mis-specified dependencies. ([Rustworkx][7])
* **`rx.transitive_reduction(graph)`**: remove redundant edges in a DAG (useful to simplify/visualize dependency logic). ([Rustworkx][8])

### Incremental recompute / explain-why utilities

* **`rx.descendants(graph, node)`**: everything downstream of a changed evidence/rule. ([Rustworkx][9])
* **`rx.ancestors(graph, node)`**: everything upstream explaining why something ran / what it depends on. ([Rustworkx][10])

---

## 1) Example: Build a relationship-rule DAG and compute a deterministic rule order

### Problem shape

You have “rules” that *produce* evidence artifacts and *require* other evidence artifacts. Your job is to derive a **rule dependency DAG**.

### Key rustworkx routines used

* `PyDiGraph` to store rules as node payloads ([Rustworkx][1])
* `is_directed_acyclic_graph` (or `TopologicalSorter(check_cycle=True)`) to detect cycles ([Rustworkx][6])
* `lexicographical_topological_sort` for deterministic ordering ([Rustworkx][3])

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, Iterable, Any

import rustworkx as rx


@dataclass(frozen=True)
class Rule:
    rule_id: str
    requires: FrozenSet[str]   # evidence keys required
    produces: FrozenSet[str]   # evidence keys produced
    run: Callable[[Dict[str, Any]], None]  # (illustrative) side-effectful executor


def build_rule_dag(rules: Iterable[Rule]) -> tuple[rx.PyDiGraph, dict[str, int]]:
    """
    Nodes: Rule objects
    Edges: producer_rule -> consumer_rule
    """
    g = rx.PyDiGraph(multigraph=False)  # nodes & edges can carry arbitrary payloads
    rule_index: dict[str, int] = {}

    # 1) Add rule nodes
    for r in rules:
        rule_index[r.rule_id] = g.add_node(r)

    # 2) Map each evidence key -> producer rule (simple model: single producer)
    producer: dict[str, str] = {}
    for r in rules:
        for e in r.produces:
            producer.setdefault(e, r.rule_id)

    # 3) Add dependency edges based on "requires"
    edges = []
    for r in rules:
        for req in r.requires:
            if req in producer:
                src = rule_index[producer[req]]
                dst = rule_index[r.rule_id]
                edges.append((src, dst))

    g.add_edges_from_no_data(edges)

    # 4) Validate DAG
    if not rx.is_directed_acyclic_graph(g):
        # For debugging: enumerate cycles (can be expensive on huge graphs)
        cycles = rx.simple_cycles(g)
        raise ValueError(f"Rule dependency cycle(s) detected: {cycles}")

    return g, rule_index


def deterministic_rule_order(g: rx.PyDiGraph) -> list[Rule]:
    """
    Deterministic topo order:
    - topo constraints respected
    - ties broken by rule_id
    """
    return rx.lexicographical_topological_sort(g, key=lambda r: r.rule_id)
```

**Notes / wiring advice**

* `PyDiGraph` stores **arbitrary Python objects** as node payloads, so you can keep the full rule object in-graph and not maintain parallel arrays. ([Rustworkx][1])
* A plain `topological_sort()` returns *node indices* and may be non-deterministic for independent nodes; `lexicographical_topological_sort()` gives you a deterministic tie-break using your key function. ([Rustworkx][2])
* For debugging “why did this edge exist?”, use **edge payloads** (instead of `_no_data`) to store which evidence key caused the dependency.

### Variant: simplify the dependency graph for debugging

Once correct, your DAG often has redundant edges (“A→C” exists because “A→B→C”). A **transitive reduction** can make the rule graph easier to interpret/visualize. ([Rustworkx][8])

```python
reduced_g, old_to_new = rx.transitive_reduction(g)
```

---

## 2) Example: Evidence readiness loop (TopologicalSorter as your “ready queue”)

### Problem shape

You’re not just ordering rules—you’re executing them, and a rule becomes runnable only when all prerequisites are satisfied.

### Why `TopologicalSorter` is a perfect fit

`TopologicalSorter` is explicitly designed as:

* loop while `is_active()`
* fetch runnable nodes via `get_ready()`
* mark completion via `done()` ([Rustworkx][5])

That’s basically a ready-queue scheduler.

### Technique: model evidence *and* rules as nodes (bipartite DAG)

This makes “evidence readiness” explicit and lets you start with an **initial set** of evidence you already have, and only schedule what’s reachable from that set.

We’ll build a DAG:

* Evidence node `E:x`
* Rule node `R:my_rule`
* Edges:

  * `E:req` → `R:rule` (rule depends on evidence)
  * `R:rule` → `E:prod` (rule produces evidence)

Then we use `TopologicalSorter(..., initial=seed_evidence_nodes)` to only consider the portion of the graph dominated by the seed. (This is extremely useful for partial runs / partial rebuilds.) ([Rustworkx][5])

```python
from dataclasses import dataclass
from typing import Literal, Set, Tuple

NodeKind = Literal["EVIDENCE", "RULE"]

@dataclass(frozen=True)
class EvidenceNode:
    key: str

@dataclass(frozen=True)
class RuleNode:
    rule_id: str
    requires: FrozenSet[str]
    produces: FrozenSet[str]
    run: Callable[[Dict[str, Any]], None]


GraphNode = Tuple[NodeKind, object]  # ("EVIDENCE", EvidenceNode) or ("RULE", RuleNode)


def build_evidence_rule_graph(rule_nodes: Iterable[RuleNode]) -> tuple[rx.PyDiGraph, dict[str, int], dict[str, int]]:
    """
    Returns:
      g: PyDiGraph
      evidence_idx: evidence_key -> node index
      rule_idx: rule_id -> node index
    """
    g = rx.PyDiGraph(multigraph=False)
    evidence_idx: dict[str, int] = {}
    rule_idx: dict[str, int] = {}

    # Collect all evidence keys mentioned
    all_evidence: Set[str] = set()
    rules_list = list(rule_nodes)
    for r in rules_list:
        all_evidence |= set(r.requires)
        all_evidence |= set(r.produces)

    # Add evidence nodes
    for ek in sorted(all_evidence):
        evidence_idx[ek] = g.add_node(("EVIDENCE", EvidenceNode(ek)))

    # Add rule nodes
    for r in rules_list:
        rule_idx[r.rule_id] = g.add_node(("RULE", r))

    # Add edges evidence->rule (requirements)
    req_edges = []
    for r in rules_list:
        r_i = rule_idx[r.rule_id]
        for ek in r.requires:
            req_edges.append((evidence_idx[ek], r_i))

    # Add edges rule->evidence (products)
    prod_edges = []
    for r in rules_list:
        r_i = rule_idx[r.rule_id]
        for ek in r.produces:
            prod_edges.append((r_i, evidence_idx[ek]))

    g.add_edges_from_no_data(req_edges + prod_edges)

    # Optional safety check (TopologicalSorter can also do cycle checking)
    if not rx.is_directed_acyclic_graph(g):
        raise ValueError(f"Cycle(s) detected in evidence/rule graph: {rx.simple_cycles(g)}")

    return g, evidence_idx, rule_idx


def execute_readiness_loop(
    g: rx.PyDiGraph,
    seed_evidence: Set[str],
    evidence_idx: dict[str, int],
    state: Dict[str, Any],
) -> None:
    """
    - Seed initial evidence as "available"
    - Auto-accept evidence nodes when they become ready
    - Actually execute rule nodes
    """
    seed_nodes = [evidence_idx[k] for k in seed_evidence if k in evidence_idx]

    # TopologicalSorter drives readiness: get_ready() -> done()
    sorter = rx.TopologicalSorter(g, check_cycle=True, initial=seed_nodes)

    available: Set[str] = set(seed_evidence)

    while sorter.is_active():
        ready = sorter.get_ready()
        if not ready:
            # No progress possible: either missing evidence (not in seed),
            # or a logic issue (but check_cycle=True would have raised on cycles)
            break

        # Process ready nodes: evidence nodes auto-commit; rule nodes execute
        done_now = []
        for node_index in ready:
            kind, payload = g[node_index]  # node payload is our ("EVIDENCE"/"RULE", ...)
            if kind == "EVIDENCE":
                available.add(payload.key)
                done_now.append(node_index)
            else:
                rule: RuleNode = payload
                # (illustrative) you can assert prerequisites are present
                missing = set(rule.requires) - available
                if missing:
                    # In a strict model, this shouldn't happen if graph+seed are correct
                    raise RuntimeError(f"Rule {rule.rule_id} became ready but missing evidence: {missing}")

                rule.run(state)
                # We don't add produced evidence here — it will be “released” when the evidence nodes become ready.
                done_now.append(node_index)

        sorter.done(done_now)
```

### What this buys you

* The readiness loop is now *graph-driven*, not bespoke: `get_ready()` produces what can run; `done()` unlocks downstream nodes. ([Rustworkx][5])
* You can do **partial execution** by changing the seed evidence set and using `initial=...`. The docs describe that `initial` restricts the ordering to those nodes and nodes “dominated by them”. ([Rustworkx][5])
* You can choose to turn off argument validation (`check_args=False`) once you trust your scheduler to only `done()` ready nodes, which can reduce overhead. ([Rustworkx][5])

### Variant: reverse-mode (teardown / invalidation order)

`TopologicalSorter(..., reverse=True)` gives you the reverse dependency direction ordering (useful for deletion/teardown or “invalidate-from-root” passes). ([Rustworkx][5])

---

## 3) Example: Dependency scheduling with parallelism + deterministic tie-breaks + priorities

There are two “levels” of sophistication you’ll likely want:

1. **Stage parallelism** (run independent rules together)
2. **Priority scheduling** (run the most “critical” ready work first)

### 3A) Stage parallelism with `topological_generations()`

This gives you “waves” of nodes where each wave’s dependencies are in earlier waves. Perfect for “run each generation in parallel” patterns. ([Rustworkx][4])

```python
def schedule_by_generations(rule_graph: rx.PyDiGraph) -> list[list[int]]:
    # Returns list of generations; each generation is a NodeIndices list of node ids
    generations = rx.topological_generations(rule_graph)
    return [list(gen) for gen in generations]
```

Then your executor might be:

```python
from concurrent.futures import ThreadPoolExecutor

def run_generations(rule_graph: rx.PyDiGraph, generations: list[list[int]]) -> None:
    for gen in generations:
        # run this generation concurrently
        with ThreadPoolExecutor() as ex:
            futures = []
            for idx in gen:
                rule = rule_graph[idx]
                futures.append(ex.submit(rule.run, {}))
            for f in futures:
                f.result()
```

### 3B) Priority scheduling: “critical path first”

If your rules have uneven runtimes, you often want to prioritize work that is on (or near) the critical path.

rustworkx provides:

* `dag_weighted_longest_path(...)` (returns node indices of longest path) ([Rustworkx][11])
* `dag_weighted_longest_path_length(...)` (length) ([Rustworkx][12])

A practical pattern is:

1. compute a “criticality score” per node (longest downstream cost)
2. in the readiness loop, sort ready nodes by that score

Here’s a compact, *actually-usable* criticality computation using a topo order plus `successor_indices` (available on `PyDiGraph`). ([Rustworkx][1])

```python
def compute_bottom_level_costs(rule_graph: rx.PyDiGraph, node_cost) -> dict[int, float]:
    """
    bottom_level[node] = cost(node) + max(bottom_level[succ])   (0 if no succ)
    node_cost: callable(node_payload) -> float
    """
    topo = list(rx.topological_sort(rule_graph))  # node indices in topo order
    bottom: dict[int, float] = {}

    for n in reversed(topo):
        succs = rule_graph.successor_indices(n)  # indices of successors
        best_succ = max((bottom[s] for s in succs), default=0.0)
        bottom[n] = float(node_cost(rule_graph[n])) + best_succ

    return bottom
```

Now combine with `TopologicalSorter` to implement a bounded-concurrency scheduler:

```python
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

def run_with_priorities(rule_graph: rx.PyDiGraph, node_cost, max_workers: int = 8) -> None:
    bottom = compute_bottom_level_costs(rule_graph, node_cost)

    sorter = rx.TopologicalSorter(rule_graph, check_cycle=True, check_args=False)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        pending = {}  # future -> node_index

        while sorter.is_active() or pending:
            # enqueue newly-ready tasks
            newly_ready = sorter.get_ready()
            newly_ready = sorted(newly_ready, key=lambda i: bottom[i], reverse=True)

            for node_index in newly_ready:
                rule = rule_graph[node_index]
                fut = ex.submit(rule.run, {})
                pending[fut] = node_index

            if not pending:
                continue

            done_futs, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            for fut in done_futs:
                node_index = pending.pop(fut)
                fut.result()          # raise errors
                sorter.done([node_index])
```

This is the “classic” graph scheduler loop: `get_ready()` feeds a priority queue; completions call `done()` to unlock more work. ([Rustworkx][5])

---

## 4) “Rule ordering + evidence readiness” extras you’ll likely want immediately

### A) Incremental rebuild / invalidation (downstream closure)

When some evidence or rule output changes, you usually want to recompute downstream rules only.

* `rx.descendants(graph, node)` returns *all downstream nodes reachable from node*, not just direct successors. ([Rustworkx][9])

```python
changed_rule_idx = 17
to_rerun = rx.descendants(rule_graph, changed_rule_idx) | {changed_rule_idx}
```

### B) Explain-why (upstream closure)

When debugging “why did rule X run?”:

* `rx.ancestors(graph, node)` returns everything upstream. ([Rustworkx][10])

```python
why = rx.ancestors(rule_graph, target_rule_idx)
```

### C) Cycle diagnosis workflow

* Quick check: `is_directed_acyclic_graph()` ([Rustworkx][6])
* If false: `simple_cycles()` to print cycles ([Rustworkx][7])
* If you want to fail fast during construction, enable `PyDiGraph(check_cycle=True)` (with overhead). ([Rustworkx][1])
* Many DAG operations (like `topological_sort`) raise `DAGHasCycle` if cycles exist. ([Rustworkx][2])

---

## 5) A quick “which rustworkx routine should I use?” map

* **Just need an ordering:** `topological_sort()` ([Rustworkx][2])
* **Need deterministic ordering across runs:** `lexicographical_topological_sort(key=...)` ([Rustworkx][3])
* **Need parallel stages:** `topological_generations()` ([Rustworkx][4])
* **Need a runtime “ready queue” / scheduler:** `TopologicalSorter.get_ready()` + `done()` ([Rustworkx][5])
* **Need to simplify the graph (debug/visualization):** `transitive_reduction()` ([Rustworkx][8])
* **Need incremental recompute sets:** `descendants()` / `ancestors()` ([Rustworkx][9])
* **Need “critical path” info for prioritization:** `dag_weighted_longest_path*()` ([Rustworkx][11])

---

If you tell me which of these two most resembles your actual “readiness loop”:

1. **rules produce evidence tables** (DataFusion tables/views)
2. **rules produce intermediate relationship edges** (graph edges / adjacency artifacts)

…I can rewrite the examples so that the “run()” bodies look like: “build Ibis expr → compile → register in DataFusion → mark evidence key as present”, and show exactly where you’d store DataFusion plan handles / schema fingerprints on nodes/edges (while still keeping the scheduler purely rustworkx-driven).

[1]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html "PyDiGraph - rustworkx 0.17.1"
[2]: https://www.rustworkx.org/apiref/rustworkx.topological_sort.html "rustworkx.topological_sort - rustworkx 0.17.1"
[3]: https://www.rustworkx.org/apiref/rustworkx.lexicographical_topological_sort.html "rustworkx.lexicographical_topological_sort - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.topological_generations.html "rustworkx.topological_generations - rustworkx 0.17.1"
[5]: https://www.rustworkx.org/apiref/rustworkx.TopologicalSorter.html "TopologicalSorter - rustworkx 0.17.1"
[6]: https://www.rustworkx.org/apiref/rustworkx.is_directed_acyclic_graph.html?utm_source=chatgpt.com "rustworkx.is_directed_acyclic_graph"
[7]: https://www.rustworkx.org/apiref/rustworkx.simple_cycles.html "rustworkx.simple_cycles - rustworkx 0.17.1"
[8]: https://www.rustworkx.org/apiref/rustworkx.transitive_reduction.html "rustworkx.transitive_reduction - rustworkx 0.17.1"
[9]: https://www.rustworkx.org/apiref/rustworkx.descendants.html "rustworkx.descendants - rustworkx 0.17.1"
[10]: https://www.rustworkx.org/apiref/rustworkx.ancestors.html "rustworkx.ancestors - rustworkx 0.17.1"
[11]: https://www.rustworkx.org/apiref/rustworkx.dag_weighted_longest_path.html "rustworkx.dag_weighted_longest_path - rustworkx 0.17.1"
[12]: https://www.rustworkx.org/apiref/rustworkx.dag_weighted_longest_path_length.html "rustworkx.dag_weighted_longest_path_length - rustworkx 0.17.1"

Below are several *representative*, fairly “production-flavored” patterns for using **rustworkx** to drive (1) **relationship rule ordering**, (2) **evidence readiness loops**, and (3) more classic **dependency scheduling**—including the variant you requested where **rules produce intermediate relationship edges** and each rule’s `run()` is effectively:

> build Ibis expr → compile → register in DataFusion → mark evidence key present

I’ll keep the scheduler **purely rustworkx-driven** (topology + readiness), and show exactly where you’d hang **DataFusion plan handles** / **schema fingerprints** on nodes/edges.

---

## The rustworkx building blocks you’ll actually use for schedulers

### 1) `rx.PyDiGraph` as your “scheduler substrate”

* Nodes/edges are identified by stable integer indices; payloads can be *any* Python object, and you can access/update node payloads via `graph[node_index]`. ([Rustworkx][1])
* You can enable `check_cycle=True` to prevent adding edges that would introduce cycles (with a performance penalty). If you add node+edge together, `add_child()`/`add_parent()` avoids some overhead. ([Rustworkx][1])
* Export a debug visualization using `to_dot()` (Graphviz). ([Rustworkx][2])

### 2) Static ordering (simple, deterministic)

* `rx.topological_sort(dag)` → one valid topological order. ([Rustworkx][3])
* `rx.lexicographical_topological_sort(dag, key=..., reverse=...)` → deterministic tie-breaking based on node payload. ([Rustworkx][4])

### 3) Incremental “ready set” scheduling (build-system style)

* `rx.TopologicalSorter(graph)` + `.get_ready()` + `.done(nodes)` + `.is_active()` gives you the classic readiness loop. ([Rustworkx][5])
* (Important nuance) The docs note the underlying graph *can* be mutated and the sorter will pick it up, but it’s **not recommended** because it may produce logical errors. Prefer building the dependency structure up-front. ([Rustworkx][5])

### 4) “Parallel waves”

* `rx.topological_generations(dag)` returns generations where all ancestors are in earlier generations—very natural for batch/parallel execution. ([Rustworkx][6])

### 5) Incremental impact analysis

* `rx.descendants(graph, node)` = everything downstream (reachable). ([Rustworkx][7])
* `rx.ancestors(graph, node)` = everything upstream (providers). ([Rustworkx][8])

### 6) DAG hygiene / debug / optimization

* `rx.is_directed_acyclic_graph(graph)` for a fast DAG check. ([Rustworkx][9])
* `rx.simple_cycles(graph)` to surface cycles when something goes wrong. ([Rustworkx][10])
* `rx.strongly_connected_components(graph)` if you want to *group* cycles into SCCs (useful for “fixpoint subloops”). ([Rustworkx][11])
* `rx.transitive_reduction(dag)` to remove redundant edges and simplify what you visualize/debug. ([Rustworkx][12])
* `rx.dag_longest_path_length(...)` if you want critical-path heuristics. ([Rustworkx][13])

---

## Example A — “Plain” dependency scheduling for rules (rule → rule DAG)

This is the classic “tasks depend on tasks” model. It’s ideal when you can compile your evidence requirements into a **DAG of rules** (even if conceptually you think in evidence keys).

### When this model fits

* Each evidence key has a single producing rule (or you’ve decided which producer “wins”).
* Rule dependencies don’t change at runtime (the most common case).

### Code

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Any
import rustworkx as rx


@dataclass
class RuleTask:
    name: str
    priority: int  # lower runs earlier (tie-break)
    run: Callable[[], Any]


# Build a DAG of rules
dag = rx.PyDiGraph(check_cycle=True)  # prevent accidental cycles

# Add tasks as node payloads
n_parse = dag.add_node(RuleTask("parse_sources", 10, lambda: print("parse")))
n_scips = dag.add_node(RuleTask("load_scip",     10, lambda: print("scip")))
n_edges = dag.add_node(RuleTask("emit_edges",    20, lambda: print("edges")))
n_join  = dag.add_node(RuleTask("resolve",       30, lambda: print("resolve")))

# Dependencies: parse + scip must run before emit_edges, then resolve
dag.add_edge(n_parse, n_edges, {"reason": "needs_ast"})
dag.add_edge(n_scips, n_edges, {"reason": "needs_scip"})
dag.add_edge(n_edges, n_join,  {"reason": "needs_edges"})

# Deterministic ordering with tie-breaking on payload (priority, name)
order = rx.lexicographical_topological_sort(
    dag,
    key=lambda task: f"{task.priority:08d}:{task.name}",
)

for task in order:
    task.run()
```

### Notes / variants

* If you want concurrency, prefer `rx.topological_generations(dag)` and run each generation in parallel (thread pool / asyncio / ray / etc.). ([Rustworkx][6])
* If debugging or visualizing a “why do I have so many edges?” situation, run `rx.transitive_reduction(dag)` and export it to dot for a minimal picture. ([Rustworkx][12])
* If you want a build-system-like loop (ready set, done set), jump to Example B.

---

## Example B — Dependency-style scheduling with a **ready-set loop** (`TopologicalSorter`)

This is the most direct mapping to “dependency-style scheduling patterns”.

### Why it’s useful

* You can pop **multiple ready tasks at once** (parallel execution).
* You can implement “skip if cached” behavior by calling `.done(node)` without running.

### Code

```python
import rustworkx as rx
from dataclasses import dataclass
from typing import Callable, Any


@dataclass
class Task:
    name: str
    run: Callable[[], Any]


g = rx.PyDiGraph(check_cycle=True)

a = g.add_node(Task("A", lambda: print("run A")))
b = g.add_node(Task("B", lambda: print("run B")))
c = g.add_node(Task("C", lambda: print("run C")))
d = g.add_node(Task("D", lambda: print("run D")))

# A,B -> C -> D
g.add_edges_from_no_data([(a, c), (b, c), (c, d)])

sorter = rx.TopologicalSorter(g)

while sorter.is_active():
    ready = sorter.get_ready()       # list[int] node indices ready now
    # You could dispatch these in parallel; shown serially for clarity
    for node in ready:
        task = g[node]
        task.run()
        sorter.done(node)
```

This is exactly the usage pattern shown in rustworkx’s `TopologicalSorter` docs (`get_ready()`, `done()`, `is_active()`). ([Rustworkx][5])

---

## Example C — Your requested model: **Evidence readiness loop** where rules produce **intermediate relationship edges**

This is the closest match to “rules produce intermediate relationship edges”, *and* the `run()` body is:

> build Ibis expr → compile → register in DataFusion → mark evidence key present

### Key idea: a bipartite DAG

Use a single `PyDiGraph` with two node types:

* **Evidence nodes** (keys like `rel.ast_edges`, `rel.symbol_candidates`, …)
* **Rule nodes** (tasks that consume evidence and produce evidence)

Edges:

* `Evidence → Rule` means “rule requires this evidence”
* `Rule → Evidence` means “rule produces this evidence”

Then drive everything with a `TopologicalSorter` over that bipartite graph.

### Why `TopologicalSorter` still works here

If the evidence production graph is acyclic, you can schedule it just like a build graph:

* evidence becomes “done” when present
* rules become runnable when all predecessor evidence is done
* produced evidence becomes present after rule runs

That is *exactly* `get_ready()`/`done()` semantics. ([Rustworkx][5])

---

### DataFusion + Ibis integration points you asked for

* `SessionContext` is the main DataFusion interface (planning/execution engine). ([Apache DataFusion][14])
* DataFusion Python: `ctx.register_view(name, dataframe)` is the documented way to register a view from a DataFrame. ([Apache DataFusion][15])
* Ibis DataFusion backend can be created from an existing `SessionContext` via `from_connection(...)`. ([Ibis][16])
* DataFusion `DataFrame` exposes plan-ish methods you can store (e.g., `logical_plan()`, `optimized_logical_plan()`, `execution_plan()`) as well as `schema()`—handy for fingerprints. ([Apache DataFusion][17])

---

### Code (illustrative but “wireable”)

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union
import hashlib
import rustworkx as rx

# ----- Types you will store as rustworkx node payloads -----

@dataclass(frozen=True)
class EvidenceKey:
    """A logical artifact your engine can reason about."""
    name: str   # e.g. "rel.ast_edges" or "rel.symbol_candidates"

@dataclass
class EvidenceState:
    key: EvidenceKey
    present: bool = False

    # Where you store DataFusion handles / metadata
    view_name: Optional[str] = None
    df: Optional[Any] = None  # datafusion.DataFrame (opaque to scheduler)
    logical_plan: Optional[Any] = None
    optimized_logical_plan: Optional[Any] = None
    execution_plan: Optional[Any] = None
    schema_fingerprint: Optional[str] = None

@dataclass
class RuleState:
    name: str
    requires: Set[EvidenceKey]
    produces: Set[EvidenceKey]

    # A builder that returns ibis expressions per output evidence key
    build_ibis_exprs: Callable[[Dict[EvidenceKey, EvidenceState]], Dict[EvidenceKey, Any]]

    # Observability
    last_error: Optional[str] = None


NodePayload = Union[EvidenceState, RuleState]


def schema_fingerprint_from_arrow_schema(schema: Any) -> str:
    """
    Fingerprint the schema so you can detect downstream invalidation triggers.
    (Exact method depends on the pyarrow schema object you receive.)
    """
    # Many pyarrow schema objects have stable string/serialization forms.
    raw = str(schema).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# ----- The scheduler: pure rustworkx-driven -----

def run_evidence_build(
    g: rx.PyDiGraph,
    evidence_nodes: Dict[EvidenceKey, int],
    rule_nodes: Dict[str, int],
    *,
    ctx: Any,      # datafusion.SessionContext
    ibis_backend: Any,  # ibis DataFusion backend
) -> None:
    """
    Scheduler decisions come only from:
      - graph topology
      - EvidenceState.present
    DataFusion/Ibis objects are payload metadata, not scheduling logic.
    """
    sorter = rx.TopologicalSorter(g)

    while sorter.is_active():
        ready = sorter.get_ready()

        for node_idx in ready:
            payload: NodePayload = g[node_idx]

            # 1) Evidence nodes: mark done only if present
            if isinstance(payload, EvidenceState):
                if not payload.present:
                    raise RuntimeError(f"Missing required input evidence: {payload.key.name}")
                sorter.done(node_idx)
                continue

            # 2) Rule nodes: run once predecessors evidence are done
            rule: RuleState = payload

            # Optional skip/caching behavior:
            # If all products are already present with matching fingerprint, skip.
            # (Here we just check "present", but you can add fingerprint logic.)
            if all(g[evidence_nodes[k]].present for k in rule.produces):
                sorter.done(node_idx)
                continue

            # ---- Your requested run() body shape ----
            # build Ibis expr → compile → register in DataFusion → mark evidence present
            try:
                # Gather current evidence states for inputs
                inputs = {k: g[evidence_nodes[k]] for k in rule.requires}

                # Build Ibis expressions for each output evidence artifact
                out_exprs: Dict[EvidenceKey, Any] = rule.build_ibis_exprs(inputs)

                for out_key, ibis_expr in out_exprs.items():
                    # Compile (often to SQL / SQLGlot underneath)
                    sql = ibis_backend.compile(ibis_expr)

                    # Create a DataFusion DataFrame from SQL
                    df = ctx.sql(sql)

                    # Register as a named view (evidence key -> view name)
                    view_name = out_key.name.replace(".", "__")
                    ctx.register_view(view_name, df)

                    # Store DataFusion handles + schema fingerprint ON THE EVIDENCE NODE
                    ev_node = evidence_nodes[out_key]
                    ev_state: EvidenceState = g[ev_node]

                    ev_state.present = True
                    ev_state.view_name = view_name
                    ev_state.df = df
                    ev_state.logical_plan = df.logical_plan()
                    ev_state.optimized_logical_plan = df.optimized_logical_plan()
                    ev_state.execution_plan = df.execution_plan()

                    schema = df.schema()
                    ev_state.schema_fingerprint = schema_fingerprint_from_arrow_schema(schema)

                sorter.done(node_idx)

            except Exception as e:
                rule.last_error = repr(e)
                raise


# ----- Example graph construction (rules produce intermediate relationship edges) -----

def build_relationship_pipeline_graph() -> tuple[rx.PyDiGraph, Dict[EvidenceKey, int], Dict[str, int]]:
    g = rx.PyDiGraph(check_cycle=True)

    # Evidence keys (relationship-edge artifacts)
    raw_files = EvidenceKey("input.raw_files")
    ast_edges = EvidenceKey("rel.ast_edges")
    scip_occ  = EvidenceKey("evidence.scip_occurrences")
    candidates = EvidenceKey("rel.symbol_candidates")
    resolved  = EvidenceKey("rel.symbol_resolutions")

    evidence_nodes: Dict[EvidenceKey, int] = {}
    for k in [raw_files, ast_edges, scip_occ, candidates, resolved]:
        evidence_nodes[k] = g.add_node(EvidenceState(key=k, present=False))

    # Rules that produce intermediate relationship edges
    def build_ast_edges(inputs):
        # pretend: ibis expr that yields (src, dst, kind, attrs...)
        return {ast_edges: "IBIS_EXPR(ast_edges)"}

    def build_scip_occ(inputs):
        return {scip_occ: "IBIS_EXPR(scip_occurrences)"}

    def build_candidates(inputs):
        # depends on ast_edges + scip_occ; produces candidate edges
        return {candidates: "IBIS_EXPR(symbol_candidates)"}

    def build_resolutions(inputs):
        return {resolved: "IBIS_EXPR(symbol_resolutions)"}

    rules = [
        RuleState("extract_ast_edges", {raw_files}, {ast_edges}, build_ast_edges),
        RuleState("load_scip_occ",     {raw_files}, {scip_occ},  build_scip_occ),
        RuleState("emit_candidates",   {ast_edges, scip_occ}, {candidates}, build_candidates),
        RuleState("resolve_symbols",   {candidates}, {resolved}, build_resolutions),
    ]

    rule_nodes: Dict[str, int] = {}
    for r in rules:
        rule_nodes[r.name] = g.add_node(r)

    # Wire evidence -> rule (requires)
    def requires(ev: EvidenceKey, rule: RuleState):
        g.add_edge(evidence_nodes[ev], rule_nodes[rule.name], {"type": "requires", "key": ev.name})

    # Wire rule -> evidence (produces)
    def produces(rule: RuleState, ev: EvidenceKey):
        g.add_edge(rule_nodes[rule.name], evidence_nodes[ev], {"type": "produces", "key": ev.name})

    for r in rules:
        for ev in r.requires:
            requires(ev, r)
        for ev in r.produces:
            produces(r, ev)

    return g, evidence_nodes, rule_nodes
```

### What to notice (this is the “where do I store it?” part)

**Node payloads carry the *state* you care about:**

* Evidence node stores:

  * `df` (DataFusion DataFrame handle),
  * `logical_plan / optimized_logical_plan / execution_plan`,
  * `schema_fingerprint`,
  * `view_name`,
  * plus the simple readiness boolean `present`.

DataFusion `DataFrame` exposes the plan-ish methods and `schema()` you need for this pattern. ([Apache DataFusion][17])

**Edges can stay lightweight** (just “requires”/“produces” metadata). But if you prefer, you can also store “production metadata” on the **rule → evidence** edge payload and update it later via `update_edge_by_index()` / `update_edge()` (useful if you want *multiple* production attempts or versions per edge). `PyDiGraph` edges carry arbitrary payloads too. ([Rustworkx][1])

**Scheduler remains purely rustworkx-driven:**

* the only gate for evidence nodes is `EvidenceState.present`
* the only gate for rule nodes is “all predecessor evidence nodes have been `.done()`”
* DataFusion/Ibis objects are opaque “artifacts” stored on nodes (not used to decide order)

`ctx.register_view(...)` is your explicit “evidence becomes real” step. ([Apache DataFusion][15])
And Ibis can be attached to the existing DataFusion `SessionContext` via `from_connection`. ([Ibis][16])

---

## Example D — Fast incremental rebuilds: “what breaks if this evidence changes?”

Once you’re storing evidence fingerprints, downstream invalidation becomes a graph query.

### Pattern

* When evidence `E` changes (new fingerprint), re-run **all descendants** (downstream) and clear their produced evidence.

### Code sketch

```python
import rustworkx as rx

def invalidate_downstream(g: rx.PyDiGraph, evidence_node_idx: int):
    impacted = rx.descendants(g, evidence_node_idx)  # downstream closure

    for n in impacted:
        payload = g[n]
        # Clear produced evidence nodes; mark rules dirty, etc.
        # (Exact handling depends on node type.)
```

`descendants()` returns all nodes reachable via directed paths, not just immediate successors—perfect for “impact radius”. ([Rustworkx][7])
If you also want “why is this needed?”, query `ancestors()` for provenance. ([Rustworkx][8])

---

## Example E — If you ever introduce cycles: SCC “fixpoint islands”

If you accidentally create a cycle, topological scheduling can’t proceed. You should *either* (a) forbid it, or (b) deliberately treat it as a “fixpoint group”.

### Detect early

* `rx.is_directed_acyclic_graph(g)` quick DAG check. ([Rustworkx][9])
* `rx.simple_cycles(g)` for explicit cycle output. ([Rustworkx][10])

### If you want to support cycles as “fixpoint groups”

* Compute SCCs: `rx.strongly_connected_components(g)` ([Rustworkx][11])
* Any SCC with size > 1 is a “loop island”. You can:

  1. collapse SCCs into supernodes (conceptually), yielding a DAG of SCCs
  2. schedule SCCs topologically
  3. within an SCC, iterate rules until evidence fingerprints stop changing

That gives you a principled “evidence readiness loop” when the world isn’t a DAG anymore.

---

## Practical add-ons that tend to matter in real schedulers

### Deterministic order in the presence of concurrency

Use `lexicographical_topological_sort(..., key=...)` to make tie-breaking stable across runs. ([Rustworkx][4])

### Batch/parallel planning

Use:

* `topological_generations(dag)` for strict “waves”. ([Rustworkx][6])
* or `layers(dag, first_layer=...)` if you want a greedy layering variant. ([Rustworkx][18])

### Graph simplification for debugging

Run `transitive_reduction(dag)` before rendering/printing. ([Rustworkx][12])

### “Why is this slow?” critical path heuristic

Compute `dag_longest_path_length(...)` using edge weights = estimated runtime cost. ([Rustworkx][13])

### Visualize the schedule graph in CI artifacts

`PyDiGraph.to_dot()` and attach the DOT file as a build artifact. ([Rustworkx][2])

---

## One subtle design choice (worth making explicit)

You have two equally-valid “places” to store DataFusion artifacts:

1. **On Evidence nodes (recommended default)**

   * Evidence is the *thing that exists* (a view/table/edge set), so storing `df`, schema hash, and plan handles there matches your mental model.

2. **On Rule→Evidence edges (useful if you version outputs)**

   * If the same evidence key could be produced in multiple modes (e.g., “fast approximate” vs “exact”), edge payload can store per-production metadata and you pick which edge is “active”.

Either way, rustworkx supports arbitrary payloads on both nodes and edges. ([Rustworkx][1])

---

If you want, I can also rewrite Example C into a **two-graph** model that’s sometimes cleaner:

* Graph #1: *Scheduling graph* (rule DAG only, TopologicalSorter on rules)
* Graph #2: *Relationship graph* (your produced relationship edges as a separate graph/table lineage map)
  …and show how you’d use `descendants()` on the schedule graph but store richer provenance in the relationship graph.

But the bipartite “evidence + rule” DAG above is the closest literal match to your “evidence readiness loop” description while keeping scheduling 100% rustworkx-driven.

[1]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html "https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.html"
[2]: https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.to_dot.html "https://www.rustworkx.org/apiref/rustworkx.PyDiGraph.to_dot.html"
[3]: https://www.rustworkx.org/apiref/rustworkx.topological_sort.html?utm_source=chatgpt.com "rustworkx.topological_sort"
[4]: https://www.rustworkx.org/apiref/rustworkx.lexicographical_topological_sort.html "https://www.rustworkx.org/apiref/rustworkx.lexicographical_topological_sort.html"
[5]: https://www.rustworkx.org/apiref/rustworkx.TopologicalSorter.html "https://www.rustworkx.org/apiref/rustworkx.TopologicalSorter.html"
[6]: https://www.rustworkx.org/apiref/rustworkx.topological_generations.html?utm_source=chatgpt.com "rustworkx.topological_generations"
[7]: https://www.rustworkx.org/apiref/rustworkx.descendants.html?utm_source=chatgpt.com "rustworkx.descendants"
[8]: https://www.rustworkx.org/apiref/rustworkx.ancestors.html?utm_source=chatgpt.com "rustworkx.ancestors"
[9]: https://www.rustworkx.org/apiref/rustworkx.is_directed_acyclic_graph.html?utm_source=chatgpt.com "rustworkx.is_directed_acyclic_graph"
[10]: https://www.rustworkx.org/apiref/rustworkx.simple_cycles.html?utm_source=chatgpt.com "rustworkx.simple_cycles"
[11]: https://www.rustworkx.org/apiref/rustworkx.strongly_connected_components.html?utm_source=chatgpt.com "rustworkx.strongly_connected_components"
[12]: https://www.rustworkx.org/apiref/rustworkx.transitive_reduction.html?utm_source=chatgpt.com "rustworkx.transitive_reduction"
[13]: https://www.rustworkx.org/apiref/rustworkx.dag_longest_path_length.html?utm_source=chatgpt.com "rustworkx.dag_longest_path_length"
[14]: https://datafusion.apache.org/python/user-guide/basics.html?utm_source=chatgpt.com "Concepts — Apache Arrow DataFusion documentation"
[15]: https://datafusion.apache.org/python/user-guide/common-operations/views.html "Registering Views — Apache Arrow DataFusion  documentation"
[16]: https://ibis-project.org/backends/datafusion?utm_source=chatgpt.com "DataFusion - Ibis"
[17]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[18]: https://www.rustworkx.org/apiref/rustworkx.layers.html "https://www.rustworkx.org/apiref/rustworkx.layers.html"

