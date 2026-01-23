# Task/Plan Catalog Pivot — Comprehensive Implementation Plan

> **Goal**: Replace rule‑registry and declared inputs with an inference‑first **Task/Plan Catalog** that derives dependency graphs from Ibis/SQLGlot lineage, builds rustworkx graphs, and emits optional Hamilton DAGs. This plan assumes a full rewrite is acceptable and targets a streamlined, inference‑driven architecture.

---

## 0) Target Architecture Overview

### Core Concepts
- **TaskSpec**: declarative description of *what* to compute (output name + Ibis/SQLGlot builder).
- **PlanArtifact**: compiled Ibis plan + SQLGlot lineage + fingerprints.
- **InferredDeps**: table and column‑level dependencies derived from compiled expressions.
- **TaskGraph**: bipartite rustworkx DAG (evidence ↔ task) built purely from inference.
- **RuntimeArtifacts**: execution‑time state (materialized tables, view refs, fingerprints, schema cache).

### Representative Code Pattern (core flow)
```python
# task_catalog.py (new)
@dataclass(frozen=True)
class TaskSpec:
    name: str
    output: str
    build: Callable[[ExecutionContext], IbisTable]
    kind: Literal["view", "compute", "materialization"] = "view"
    cache_policy: CachePolicy = "none"


# plan_catalog.py (new)
@dataclass(frozen=True)
class PlanArtifact:
    task: TaskSpec
    plan: IbisPlan
    sqlglot_ast: Expression
    deps: InferredDeps
    plan_fingerprint: str


def compile_task_plan(task: TaskSpec, *, backend: IbisCompilerBackend, ctx: ExecutionContext) -> PlanArtifact:
    table = task.build(ctx)
    plan = compile_ibis_plan(table, backend=backend, ctx=ctx)
    sg_expr = ibis_to_sqlglot(plan.expr, backend=backend, params=None)
    deps = infer_deps_from_sqlglot_expr(
        sg_expr,
        rule_name=task.name,
        output=task.output,
    )
    fp = plan_fingerprint(sg_expr)
    return PlanArtifact(task=task, plan=plan, sqlglot_ast=sg_expr, deps=deps, plan_fingerprint=fp)
```

---

## Scope 1 — Introduce Task/Plan Catalog (foundation)

### Why
Establish the canonical inference‑first API: task definitions, plan compilation, lineage extraction, and fingerprints. This replaces rule definitions/spec tables as the source of truth.

### Representative Code Snippets
```python
# src/relspec/task_catalog.py (new)
@dataclass(frozen=True)
class TaskCatalog:
    tasks: tuple[TaskSpec, ...]

    def by_name(self) -> Mapping[str, TaskSpec]:
        return {task.name: task for task in self.tasks}


# src/relspec/plan_catalog.py (new)
@dataclass(frozen=True)
class PlanCatalog:
    artifacts: tuple[PlanArtifact, ...]

    def by_output(self) -> Mapping[str, PlanArtifact]:
        return {artifact.task.output: artifact for artifact in self.artifacts}
```

### Target Files
- **New**: `src/relspec/task_catalog.py`
- **New**: `src/relspec/plan_catalog.py`
- **Update**: `src/relspec/inferred_deps.py` (ensure API supports SQLGlot expressions and Ibis plans)
- **Update**: `src/relspec/runtime_artifacts.py` (continue as the runtime store)

### Implementation Checklist
- [ ] Define `TaskSpec` and `TaskCatalog` (view/compute/materialization + cache policy).
- [ ] Define `PlanArtifact` and `PlanCatalog` (plan, SQLGlot AST, lineage, fingerprints).
- [ ] Implement `compile_task_plan()` helper for Ibis → SQLGlot → lineage.
- [ ] Add unit tests for catalog indexing and plan compilation.

---

## Scope 2 — Inference‑Only Graph Builder

### Why
Replace all rule‑based graph constructors with a single inference path from `PlanCatalog`.

### Representative Code Snippets
```python
# src/relspec/graph_inference.py (new)
@dataclass(frozen=True)
class TaskGraph:
    graph: rx.PyDiGraph
    evidence_idx: dict[str, int]
    task_idx: dict[str, int]


def build_task_graph(artifacts: Sequence[PlanArtifact]) -> TaskGraph:
    deps = [artifact.deps for artifact in artifacts]
    return build_rule_graph_from_inferred_deps(deps)
```

### Target Files
- **New**: `src/relspec/graph_inference.py`
- **Update**: `src/relspec/rustworkx_graph.py` (keep inferred builder, remove legacy paths)

### Implementation Checklist
- [ ] Introduce `TaskGraph` wrapper (optional; or reuse existing `RuleGraph`).
- [ ] Provide `build_task_graph(PlanCatalog)` that uses inferred deps.
- [ ] Remove/disable `build_rule_graph_from_definitions` and other legacy constructors.

---

## Scope 3 — Scheduler & Validation: Edge‑First

### Why
Guarantee readiness based on **inferred column requirements** instead of declared inputs.

### Representative Code Snippets
```python
# src/relspec/rustworkx_schedule.py (updated)
if not validate_edge_requirements(graph, rule_idx, catalog=evidence):
    continue
```

### Target Files
- **Update**: `src/relspec/rustworkx_schedule.py`
- **Update**: `src/relspec/graph_edge_validation.py`

### Implementation Checklist
- [ ] Ensure scheduler only validates through `GraphEdge` requirements.
- [ ] Include required column/types/metadata checks.
- [ ] Provide diagnostics for failed edges.

---

## Scope 4 — Replace Rule Registry with Task Catalog

### Why
Rule registry + spec tables are tightly coupled to declared inputs. Inference pivot requires a task catalog and builder registry instead.

### Representative Code Snippet
```python
# src/relspec/task_registry.py (new)
@dataclass
class TaskRegistry:
    tasks: dict[str, TaskSpec] = field(default_factory=dict)

    def register(self, task: TaskSpec) -> None:
        self.tasks[task.name] = task

    def build_catalog(self) -> TaskCatalog:
        return TaskCatalog(tasks=tuple(self.tasks.values()))
```

### Target Files
- **New**: `src/relspec/task_registry.py`
- **Update**: `src/normalize/*` to emit TaskSpec builders instead of RuleDefinition
- **Update**: `src/cpg/*` to consume TaskCatalog instead of rules
- **Update**: `src/hamilton_pipeline/*` to synthesize from TaskGraph

### Implementation Checklist
- [ ] Create `TaskRegistry` and task discovery hooks.
- [ ] Replace normalize/relationship rule factories with task registration.
- [ ] Update Hamilton orchestration to operate on TaskCatalog/TaskGraph.

---

## Scope 5 — Execution Engine Rewrite

### Why
The current execution pipeline expects rule definitions; inference pivot should compile plans directly from tasks.

### Representative Code Snippets
```python
# src/relspec/execution.py (new)
@dataclass(frozen=True)
class TaskExecutionContext:
    runtime: RuntimeArtifacts
    backend: IbisCompilerBackend


def execute_task_plan(artifact: PlanArtifact, *, ctx: TaskExecutionContext) -> TableLike:
    return materialize_ibis_plan(artifact.plan, execution=ctx.runtime)
```

### Target Files
- **New**: `src/relspec/execution.py`
- **Update**: `src/normalize/runner.py` (use TaskCatalog + PlanCatalog)
- **Update**: `src/hamilton_pipeline/modules/*` (consume PlanArtifacts)

### Implementation Checklist
- [ ] Introduce a minimal execution harness for PlanArtifacts.
- [ ] Replace rule‑compiler pipeline.
- [ ] Align runtime artifacts and evidence catalog updates.

---

## Scope 6 — Incremental + Caching (fingerprint‑based)

### Why
Incremental logic should be based on plan fingerprints and inferred lineage rather than rule definitions.

### Representative Code Snippet
```python
@dataclass(frozen=True)
class IncrementalDiff:
    changed_tasks: tuple[str, ...]


def diff_plan_catalog(prev: PlanCatalog, curr: PlanCatalog) -> IncrementalDiff:
    prev_fp = {a.task.name: a.plan_fingerprint for a in prev.artifacts}
    curr_fp = {a.task.name: a.plan_fingerprint for a in curr.artifacts}
    changed = [name for name, fp in curr_fp.items() if prev_fp.get(name) != fp]
    return IncrementalDiff(changed_tasks=tuple(sorted(changed)))
```

### Target Files
- **Replace**: `src/relspec/incremental.py` with plan‑diff logic
- **Update**: `src/relspec/rules/cache.py` (if retained, rewire to PlanCatalog)

### Implementation Checklist
- [ ] Implement fingerprint‑diff for Task/Plan catalogs.
- [ ] Update incremental scheduling to target changed tasks only.

---

## Scope 7 — Decommission Rule System

### Why
Once all pipelines use Task/Plan catalog, the rule registry, spec tables, and rule handlers are obsolete.

### Files to Decommission/Delete
> **Delete after migration is complete:**
- `src/relspec/rules/`
- `src/relspec/registry/`
- `src/relspec/adapters/`
- `src/relspec/extract/`
- `src/relspec/normalize/` (relspec‑side normalize rule specs)
- `src/relspec/compiler.py`
- `src/relspec/engine.py`
- `src/relspec/plan.py`
- `src/relspec/validate.py`
- `src/relspec/incremental.py` (replace with PlanCatalog diff)
- `src/relspec/graph.py` (rule‑level graph plan path)

### Implementation Checklist
- [ ] Remove all imports of rule registry/spec tables.
- [ ] Remove rule‑definition serialization/contract tables.
- [ ] Delete deprecated graph builders.

---

## Scope 8 — Update Documentation & Invariants

### Target Files
- **Update**: `CLAUDE.md`
- **Update**: `docs/plans/calculation_driven_scheduling_and_orchestration.md`
- **New**: `docs/plans/task_plan_catalog_pivot.md` (this plan)

### Implementation Checklist
- [ ] Replace “rules registry” terminology with “task/plan catalog.”
- [ ] Document TaskSpec lifecycle + compilation + inference.
- [ ] Add diagrams for TaskCatalog → PlanCatalog → TaskGraph → Scheduler.

---

## Cross‑Cutting Decommission List (Functions)

> **Delete once all call sites are migrated**
- `relspec.rustworkx_graph.build_rule_graph_from_definitions`
- `relspec.rustworkx_graph.build_rule_graph_from_relationship_rules`
- `relspec.rustworkx_graph.build_rule_graph_from_normalize_rules`
- `relspec.rules.*` (all rule definition/spec/handler functions)
- `relspec.registry.*` (rule registry / snapshot / rule discovery)
- `relspec.compiler.*` (rule compilation pipeline)
- `relspec.engine.*` (rule execution engine)
- `relspec.plan.*` (rule plan composition)
- `relspec.validate.*` (rule validation pipeline)
- `relspec.incremental.*` (rule‑based incremental)
- `relspec.graph.compile_graph_plan` (rule‑based graph plan)

---

## Migration Order (Recommended)
1. **Scope 1** (Task/Plan catalogs)
2. **Scope 2** (Inference‑only graph)
3. **Scope 3** (Edge‑first scheduling)
4. **Scope 4** (Task registry + adapters rewrite)
5. **Scope 5** (Execution engine rewrite)
6. **Scope 6** (Incremental rewrite)
7. **Scope 7** (Delete rule system)
8. **Scope 8** (Docs + invariants)

---

## Acceptance Criteria
- No manual `inputs=` or rule spec dependency declarations remain.
- All dependency graphs derived from SQLGlot lineage.
- Scheduler only uses edge requirements (columns/types/metadata).
- Plan fingerprints drive caching and incremental diffs.
- Rule registry/spec tables removed from runtime path.
- Hamilton DAGs generated from inferred TaskGraph.

---

## Next Steps (if you want me to proceed)
1. Inventory which subsystems still import `relspec.rules` or `relspec.registry`.
2. Define TaskSpec builders for normalize/relationship/cpg outputs.
3. Implement PlanCatalog + graph inference + scheduler integration.
4. Delete rule system modules and run quality gates.
