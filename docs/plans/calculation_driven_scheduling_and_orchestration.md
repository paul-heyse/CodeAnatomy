You’re *very* close to the “views + point-calcs → rustworkx DAG → (optionally) Hamilton DAG” architecture already — you’ve basically implemented the hard part (a bipartite “evidence ↔ rule” DAG and a readiness-loop scheduler) in `src/relspec/rustworkx_graph.py` + `src/relspec/rustworkx_schedule.py`, and you’re using it in both:

* `src/relspec/graph.py` (relationship rules → graph-level plan compilation)
* `src/normalize/runner.py` (normalize rules → ordered execution)

What you *haven’t* done yet (and what you’re describing as the “pivot”) is: **derive the dependency graph from the actual computations** (Ibis/DataFusion plans) instead of from bespoke rule/spec declarations (like the big normalize factories).

Below is a concrete way to make that pivot, *without throwing away what you already built*, and then a “second readiness loop” example that matches your request exactly:

> **run() body = build Ibis expr → compile → register in DataFusion → mark evidence key present**
> while the scheduler remains purely rustworkx-driven.

---

## 1) What I see in your repo right now (the leverage points)

### A. Your scheduler is already the right shape

`RuleGraph` is bipartite: evidence nodes + rule nodes.
`TopologicalSorter(initial=seed_evidence_nodes)` gives you a clean readiness loop in `schedule_rules()`.

That’s exactly what you want for “views produce intermediate relationship edges” because the “edge tables” are just more evidence nodes.

### B. You’re duplicating dependencies in normalize

Example:

* `normalize/ibis_plan_builders.py:type_nodes_plan_ibis()` *implicitly* depends on:

  * `"type_exprs_norm_v1"`
  * `"scip_symbol_information"`
    because it calls `catalog.resolve_expr()` on those names.

But you also hardcode the same dependency list in:

* `normalize/rule_factories.py` → `RuleDefinitionSpec(inputs=(...))`

That duplication is *the* seam to remove.

### C. You already have plan lineage tooling you can reuse

You have multiple ways to extract dependency info from a plan/expression:

* `sqlglot_tools.bridge.sqlglot_diagnostics()` returns `.tables` (and more)
* `sqlglot_tools.lineage.required_columns_by_table()` returns `{table_name: cols_used}`
* `sqlglot_tools.lineage.extract_lineage_payload()` exists too
* `sqlglot_tools.optimizer.plan_fingerprint()` exists (great for caching/signatures)

So: **you don’t need to “parse your own AST” of the builder code**. Just compile the expression and ask sqlglot/lineage what tables were referenced.

---

## 2) The pivot: build the rustworkx DAG from *plans*, not from rule-specs

### The core idea

Replace “RuleDefinition(inputs=…)” as the source of truth with:

1. **A registry of dataset computations** (views + point-calcs)
2. For each computation:

   * build its Ibis expr (using placeholder stubs for missing inputs if needed)
   * compile/diagnose the expr (sqlglot)
   * extract referenced table/view names
3. Build a rustworkx DAG where:

   * evidence nodes = dataset/view names
   * rule nodes = “create view X” or “materialize X”
   * edges = dependencies inferred from the compiled expr

Then your existing readiness-loop scheduler works unchanged (or with a small improvement I’ll recommend below).

---

## 3) A concrete structure that matches your “views + point calculations” framing

### A. Define two task kinds

* **ViewTask**: registers a *named* DataFusion view (lazy)
* **ComputeTask**: actually materializes (collect/write delta/etc.)

They both “produce evidence”.

```python
from dataclasses import dataclass
from typing import Callable, Literal, Sequence, Mapping, Optional

TaskKind = Literal["view", "compute"]

@dataclass(frozen=True)
class TaskSpec:
    name: str                    # stable rule/task name
    output: str                  # evidence key produced (view/table name)
    kind: TaskKind
    build_ibis: Callable[..., "ibis.expr.types.Table"]  # or returns IbisPlan
    # optional: knobs
    priority: int = 100
    tags: tuple[str, ...] = ()
```

### B. Derive dependencies from the compiled expression

Use your existing sqlglot tooling:

* `sqlglot_diagnostics(expr).tables` → upstream evidence dependencies
* `required_columns_by_table(expr)` → per-input required columns (optional, but powerful)

That gives you:

```python
@dataclass(frozen=True)
class InferredDeps:
    inputs: tuple[str, ...]                          # table/view names
    required_cols: Mapping[str, tuple[str, ...]]     # per-input (optional)
    plan_fingerprint: str | None                     # stable cache key
```

### C. Build a RuleGraph (reuse your relspec rustworkx graph shape)

You already have `GraphNode`, `GraphEdge`, `EvidenceNode`, `RuleNode` models — you can either:

* reuse them directly (recommended: less churn), or
* create a parallel “TaskGraph” with the same bipartite structure

Conceptually it’s identical:

* `EvidenceNode(name="type_exprs_norm_v1")`
* `RuleNode(name="normalize.type_nodes", output="type_nodes_v1", inputs=(...), sources=(...))`

**The difference is:** `inputs/sources` are *inferred*, not declared.

---

## 4) One key improvement I strongly recommend for your pivot

Right now, `schedule_rules()` validates evidence using `EvidenceCatalog.satisfies(rule.evidence, inputs=rule.inputs)`.

That model **can’t express per-source required columns** (A needs col `a`, B needs col `b`) without false negatives.

But your `GraphEdge` *already* has `required_columns`. So for plan-derived scheduling, I would do this:

### Upgrade scheduling validation to be edge-based (not EvidenceSpec-based)

For each ready rule node:

* look at predecessor evidence nodes
* read the edge weight (`GraphEdge`) for required columns/types/metadata
* validate those requirements against `EvidenceCatalog`

This is a small, local change and unlocks “derive deps from plan lineage” cleanly.

Pseudo:

```python
def edge_requirements_satisfied(g: RuleGraph, evidence: EvidenceCatalog, rule_idx: int) -> bool:
    for pred in g.graph.predecessor_indices(rule_idx):
        pred_node = g.graph[pred]
        if pred_node.kind != "evidence":
            continue
        edge = g.graph.get_edge_data(pred, rule_idx)  # GraphEdge
        name = pred_node.payload.name
        if edge.required_columns:
            cols = evidence.columns_by_dataset.get(name, set())
            if not set(edge.required_columns).issubset(cols):
                return False
        # same for required_types/required_metadata if you want
    return True
```

Now “required columns by table” becomes a first-class scheduling input.

---

## 5) The “second readiness loop” example you asked for

### “…rules produce intermediate relationship edges”

And:

### “build Ibis expr → compile → register in DataFusion → mark evidence key present”

This example is intentionally “more sophisticated” and matches your repo’s style:

* evidence nodes are view/table names
* rules produce intermediate edge tables
* downstream rules consume those intermediate edge tables
* runtime stores DataFusion handles + schema fingerprints *outside* the graph, keyed by evidence name (scheduler remains rustworkx-driven)

### A. Runtime state (where plan handles + fingerprints live)

Keep the graph deterministic/pure; keep runtime artifacts separate.

```python
from dataclasses import dataclass, field

@dataclass
class RuntimeArtifacts:
    # DataFusion handles (DataFrame/logical plan), keyed by evidence/view name
    df_by_view: dict[str, object] = field(default_factory=dict)

    # fingerprints you can reuse for incremental caching / invalidations
    plan_fp_by_view: dict[str, str] = field(default_factory=dict)
    schema_fp_by_view: dict[str, str] = field(default_factory=dict)
```

### B. Task specs (intermediate edges)

Imagine a mini CPG build subset:

1. `v_norm_spans` view
2. `v_candidate_calls` edge candidates (intermediate)
3. `v_resolved_calls` resolved edges (intermediate)
4. `v_edges_union` final edges view
5. `write_edges_delta` compute task

```python
TASKS = [
  TaskSpec(
    name="view.norm_spans",
    output="v_norm_spans",
    kind="view",
    build_ibis=build_norm_spans_expr,   # joins CST+SCIP nested tables, normalizes spans
    priority=10,
  ),
  TaskSpec(
    name="view.edge_candidates.calls",
    output="v_candidate_calls",
    kind="view",
    build_ibis=build_candidate_calls_expr,  # depends on v_norm_spans, scip_symbol_information
    priority=20,
  ),
  TaskSpec(
    name="view.edge_resolve.calls",
    output="v_resolved_calls",
    kind="view",
    build_ibis=build_resolve_calls_expr,    # depends on v_candidate_calls + symbol table
    priority=30,
  ),
  TaskSpec(
    name="view.edges_union",
    output="v_edges_union",
    kind="view",
    build_ibis=build_edges_union_expr,      # depends on v_resolved_calls + other edge views
    priority=40,
  ),
  TaskSpec(
    name="compute.write_edges_delta",
    output="cpg_edges_delta",               # “evidence” becomes “materialized output exists”
    kind="compute",
    build_ibis=build_edges_union_expr,      # or just reference v_edges_union
    priority=90,
  ),
]
```

### C. Derive dependencies from the expression (sqlglot)

This is the key pivot.

```python
def infer_deps(task: TaskSpec, *, backend) -> InferredDeps:
    expr = task.build_ibis()  # uses catalog.resolve_expr("v_norm_spans") etc.
    diagnostics = sqlglot_diagnostics(expr, backend=backend, options=...)
    deps = tuple(sorted(diagnostics.tables))  # referenced FROM/JOIN tables

    required = required_columns_by_table(expr, backend=backend)  # {table: {cols}}
    required_cols = {t: tuple(sorted(cols)) for t, cols in required.items()}

    fp = plan_fingerprint(expr, backend=backend, policy=...)  # stable cache key
    return InferredDeps(inputs=deps, required_cols=required_cols, plan_fingerprint=fp)
```

Notes:

* In your codebase, you already record/compute these in a few places (compiler + incremental diagnostics). The “pivot” is to reuse them to *build the DAG*.

### D. Build a rustworkx graph (bipartite)

Reuse your existing structure: evidence nodes + rule nodes.

```python
import rustworkx as rx

def build_task_graph(tasks: list[TaskSpec], *, inferred: dict[str, InferredDeps]):
    g = rx.PyDiGraph(multigraph=False, check_cycle=False)

    evidence_idx: dict[str, int] = {}
    rule_idx: dict[str, int] = {}

    # collect evidence names from deps + outputs
    evidence_names = set()
    for t in tasks:
        evidence_names.add(t.output)
        evidence_names.update(inferred[t.name].inputs)

    for name in sorted(evidence_names):
        evidence_idx[name] = g.add_node(GraphNode("evidence", EvidenceNode(name)))

    for t in sorted(tasks, key=lambda x: x.name):
        # store plan fp (stable) on the rule node payload (OK)
        fp = inferred[t.name].plan_fingerprint
        rule_idx[t.name] = g.add_node(
            GraphNode(
                "rule",
                RuleNode(
                    name=t.name,
                    output=t.output,
                    inputs=tuple(inferred[t.name].inputs),
                    sources=tuple(inferred[t.name].inputs),
                    priority=t.priority,
                    evidence=None,  # we’ll validate via edges instead
                ),
            )
        )

    # evidence -> rule edges
    edges = []
    for t in tasks:
        ridx = rule_idx[t.name]
        deps = inferred[t.name].inputs
        req_cols = inferred[t.name].required_cols

        for dep in deps:
            edges.append(
                (evidence_idx[dep], ridx,
                 GraphEdge(kind="requires", name=dep,
                           required_columns=tuple(req_cols.get(dep, ()))) )
            )

        # rule -> evidence produced edge
        edges.append(
            (ridx, evidence_idx[t.output],
             GraphEdge(kind="produces", name=t.output))
        )

    g.add_edges_from(edges)
    return RuleGraph(graph=g, evidence_idx=evidence_idx, rule_idx=rule_idx, output_policy="all_producers")
```

### E. The readiness-loop run() body you asked for

This is the “second loop” version (intermediate edges are evidence):

```python
def run_task_graph(
    task_graph: RuleGraph,
    tasks_by_name: dict[str, TaskSpec],
    *,
    df_ctx,          # DataFusion SessionContext (via ibis_engine.registry.datafusion_context)
    ibis_backend,    # Ibis backend used for compilation
    evidence: EvidenceCatalog,
    artifacts: RuntimeArtifacts,
):
    seed_nodes = [task_graph.evidence_idx[n] for n in evidence.sources if n in task_graph.evidence_idx]
    sorter = rx.TopologicalSorter(task_graph.graph, check_cycle=True, initial=seed_nodes)

    while sorter.is_active():
        ready = list(sorter.get_ready())
        if not ready:
            break

        # Partition ready nodes
        ready_rules = []
        ready_evidence_nodes = []

        for idx in ready:
            node = task_graph.graph[idx]
            if node.kind == "rule":
                ready_rules.append(idx)
            else:
                ready_evidence_nodes.append(idx)

        # Deterministic, priority-aware execution inside a generation
        ready_rules.sort(key=lambda ridx: (
            task_graph.graph[ridx].payload.priority,
            task_graph.graph[ridx].payload.name,
        ))

        # --- Execute READY RULES ---
        for ridx in ready_rules:
            rule_node: RuleNode = task_graph.graph[ridx].payload
            spec = tasks_by_name[rule_node.name]

            # (A) Build Ibis expr
            expr = spec.build_ibis()

            # (B) Compile → SQL (or Substrait) + fingerprint
            # In your repo you already do sqlglot_diagnostics/plan_fingerprint.
            fp = plan_fingerprint(expr, backend=ibis_backend, policy=...)
            artifacts.plan_fp_by_view[spec.output] = fp

            sql = ibis_backend.compile(expr)  # or your ibis→sqlglot→dialect pipeline

            # (C) Register in DataFusion (VIEW tasks: register_view; COMPUTE tasks: execute/write)
            if spec.kind == "view":
                df = df_ctx.sql(sql)
                df_ctx.register_view(spec.output, df)
                artifacts.df_by_view[spec.output] = df

                # (D) Mark evidence present (optionally attach schema fingerprint)
                # You can fetch schema from df or from Ibis expr.schema().
                schema = expr.schema().to_pyarrow()
                evidence.register(spec.output, schema)
                artifacts.schema_fp_by_view[spec.output] = schema_fingerprint(schema)

            else:  # compute/materialize
                df = df_ctx.sql(sql)
                # e.g. write to delta, or collect, etc.
                write_delta(df, path=...)  # your existing finalize/write machinery
                artifacts.df_by_view[spec.output] = df

                # still “evidence becomes present” (materialized)
                schema = expr.schema().to_pyarrow()
                evidence.register(spec.output, schema)
                artifacts.schema_fp_by_view[spec.output] = schema_fingerprint(schema)

            # done(rule node)
            sorter.done([ridx])

        # --- Register READY EVIDENCE NODES ---
        # In this variant, evidence nodes are “made ready” by rule completion.
        # We mark them done after we’ve registered them above, to unlock successors.
        sorter.done(ready_evidence_nodes)
```

**Where do plan handles / schema fingerprints live?**

* **Not in rustworkx nodes/edges** (keeps scheduler deterministic/serializable)
* In `RuntimeArtifacts` keyed by evidence name (view/table), which is stable and easy to persist

That matches your request: *scheduler purely rustworkx-driven*, runtime artifacts stored alongside.

---

## 6) How this directly streamlines `src/relspec` + `src/normalize`

If you adopt the above, you can shrink large parts of the bespoke system:

### A. Normalize: eliminate most of `normalize/rule_factories.py`

Instead of factory-declaring `inputs=(...)`, infer them.

**Migration strategy (low-risk):**

1. Keep producing `RuleDefinition`s, but set `inputs=()` initially.
2. Build the Ibis expr, infer `.tables`, patch the `RuleDefinition.inputs` programmatically.
3. Feed those inferred rules into your existing `build_rule_graph_from_definitions()` and `schedule_rules()`.

This way, you keep your current pipeline contract/policy mechanisms while deleting the duplication.

### B. Relspec: treat “relationship rule” compilation as “view production”

You already compile rules and then `work.register(rule.output_dataset, _plan_schema(plan))` in `relspec/graph.py`.

The change is: instead of building the graph from `RelationshipRule.inputs`, derive it from plan lineage (or keep rule inputs but validate against lineage).

---

## 7) Generating a Hamilton DAG from the rustworkx-derived schedule

You said:

> “have the Hamilton DAG implementation generate based on the rustworkx schedule derivation”

Here’s the clean way to do it:

### A. Treat rustworkx as the source of truth

From the rustworkx graph, for each **rule node**:

* determine upstream **rule outputs** it depends on
  (i.e., predecessor evidence nodes that are *produced by some other rule*)

That gives you a pure function dependency graph.

### B. Generate a module of Hamilton nodes

Hamilton wants Python callables with parameters = dependencies.

You can do this in two practical ways:

#### Option 1: Generate a `.py` file (easiest to debug)

* Write `generated_pipeline.py` into `work_dir`
* Each function calls your shared executor that:

  * builds ibis expr
  * registers view / materializes
  * returns a `ViewReference` or an evidence key

Example generated node:

```python
def view_edge_candidates_calls(v_norm_spans, scip_symbol_information, ctx, runtime):
    return runtime.register_view(
        name="v_candidate_calls",
        ibis_expr=build_candidate_calls_expr(...),
        ctx=ctx,
    )
```

#### Option 2: Create dynamic functions in-memory

* Create a function with a generic `*args, **kwargs`
* Set `fn.__name__`
* Set `fn.__signature__` to the exact dependency arg list
  (many frameworks, including Hamilton-style systems, respect `__signature__`)

### C. What should Hamilton nodes return?

Return something composable and stable:

* For view tasks: return `ViewReference("v_candidate_calls")`
* For compute tasks: return a “materialized artifact descriptor” (delta path + schema fp)

Hamilton then becomes:

* the DAG execution engine
* caching / UI / observability layer
  …while rustworkx remains:
* your deterministic scheduler + impact analysis engine (descendants/ancestors)
* your canonical “graph signature” generator

---

## 8) How to pivot your CPG build specifically (joins/calculations → CPG nodes/edges)

Even though the actual `cpg` python package isn’t present inside this zip (imports exist, but the package directory isn’t in-repo), the scheduling strategy is still concrete:

### Build CPG as a small set of *views* + a small set of *materializations*

1. **Register base nested tables** (single-object CST/SCIP/etc) into DataFusion
2. **Create derived views**:

   * normalize spans
   * normalize ids
   * explode nested arrays into row tables
3. **Create relationship edge views** (intermediate evidence)
4. **Union/aggregate into final CPG edge/node views**
5. **Materialize**: write nodes/edges/props delta

### Your rustworkx DAG nodes become:

* evidence: `cst_nested`, `scip_nested`, `v_norm_spans`, `v_candidate_calls`, `v_resolved_calls`, `v_edges_union`, …
* rules: `view.norm_spans`, `view.edge_candidates.calls`, `view.edge_resolve.calls`, `view.edges_union`, `compute.write_edges_delta`, …

### Why this is *simpler* than propagating `relspec`

Because you stop modeling “rules” as a separate semantic layer and instead model:

* the actual relational computations (views)
* the few materializations that matter

And you can still keep the relspec layer for validation/policies if you want — it just stops being the scheduling source of truth.

---

## 9) rustworkx routines you should lean on (beyond what you’re already using)

You already use (or have implemented wrappers for) the big ones:

* `TopologicalSorter` (dynamic readiness)
* `lexicographical_topological_sort` (deterministic snapshots)
* `is_directed_acyclic_graph`, `simple_cycles`, `strongly_connected_components`
* `transitive_reduction().to_dot()` (great for debugging)
* `dag_longest_path(_length)` (critical path)

For “dependency-style scheduling patterns”, add these patterns:

### A. Impact / incremental rebuild

You already have `impacted_rules()` based on `rx.descendants()`.

Generalize it for datasets:

* changed evidence node → descendants → rule nodes to rerun
* changed rule node → produced evidence nodes → downstream rules

### B. Generation waves for parallel scheduling

You already record “generations” in `RuleSchedule.generations`.

That’s exactly where you’d add:

* concurrency limits
* lane separation (DataFusion vs Python fallback)
* cost heuristics (e.g., big joins later vs earlier)

### C. “Graph signature” as a cache key

You already have:

* `rule_graph_snapshot()` + `rule_graph_signature()`

Extend the signature payload to include:

* plan fingerprints (sqlglot plan_fingerprint per rule node)
* schema fingerprint per evidence output

Then:

* if graph signature unchanged, you can reuse all cached deltas/views
* if only a subgraph changed, use descendants to selectively invalidate

---

## 10) A pragmatic migration plan (so this doesn’t become a big-bang rewrite)

### Phase 1: Derive deps, but don’t change execution

* For each normalize rule / relationship rule:

  * build expr
  * infer `.tables` and compare to declared `inputs`
  * log mismatch (this will immediately show where your current specs drift)

### Phase 2: Let inferred deps drive scheduling (still execute same code)

* Build `RuleGraph` from inferred deps
* Use your existing `schedule_rules()` readiness loop
* Still call the same compilation/execution functions

### Phase 3: Collapse into “views + materializations”

* Convert “cheap” nodes to view registrations (DataFusion)
* Keep only “must materialize” steps as compute tasks
* Let intermediate edges be evidence nodes (second readiness loop)

### Phase 4: Generate Hamilton DAG from the rustworkx graph

* Emit a generated module (file-based) + run Hamilton driver
* Use rustworkx as canonical signature/impact analysis

---

