# DataFusion Planning Best-in-Class Implementation Plan

Date: 2026-01-28
Owner: Codex (design phase)
Status: design only (breaking changes allowed)

## Goals
- Treat DataFusion planning artifacts as the single source of truth for dependency inference, scheduling, and execution.
- Capture complete and reproducible planning environment state (catalogs, functions, settings, and plan deltas).
- Use physical plan topology and runtime metrics to improve scheduling quality and execution performance.
- Ensure lineage extraction is complete, including subqueries, aliases, and advanced plan variants.
- Upgrade plan artifacts and tests to enable deterministic regression detection and explainability.

## Design principles
- Substrait-first: Substrait bytes are required for plan identity and cross-runtime reproducibility.
- Environment-captured: plan artifacts must include the session config and information_schema snapshots.
- Plan completeness: logical plan traversal must include embedded subplans, not only `inputs()`.
- Profiling-aware scheduling: scheduling should use physical plan topology and metrics, not only static priorities.
- Safety-by-default: SQL planning should be non-mutating unless explicitly allowed.

---

## Scope 1: Complete logical plan traversal (subqueries + embedded plans)

**Objective**
Ensure lineage and dependency extraction includes subqueries and embedded plan fields that are not returned by `LogicalPlan.inputs()`.

**Representative code pattern**
```python
# src/datafusion_engine/plan_walk.py (new helper)

def looks_like_plan(obj: object) -> bool:
    return all(hasattr(obj, name) for name in ("inputs", "to_variant", "display_indent"))


def embedded_plans(variant: object) -> list[object]:
    plans: list[object] = []
    for attr in ("subquery", "plan", "input"):
        value = getattr(variant, attr, None)
        if looks_like_plan(value):
            plans.append(value)
    for attr in getattr(variant, "__dict__", {}):
        value = getattr(variant, attr, None)
        if looks_like_plan(value):
            plans.append(value)
    return plans


def walk_logical_complete(root: object) -> list[object]:
    stack = [root]
    seen: set[int] = set()
    ordered: list[object] = []
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        ordered.append(node)
        variant = getattr(node, "to_variant")()
        stack.extend(reversed(getattr(node, "inputs")()))
        stack.extend(reversed(embedded_plans(variant)))
    return ordered
```

**Target files to modify**
- `src/datafusion_engine/lineage_datafusion.py`
- `src/datafusion_engine/plan_bundle.py`
- New helper: `src/datafusion_engine/plan_walk.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add a reusable plan walker that includes embedded subplans.
- [ ] Update lineage extraction to use the complete walker.
- [ ] Add coverage for Subquery/SubqueryAlias/Explain/Analyze traversal.
- [ ] Add tests for subquery dependency capture.

---

## Scope 2: Expand lineage extraction for missing plan variants

**Objective**
Cover additional plan variants and expression fields to avoid missing required columns, UDFs, and dependencies.

**Representative code pattern**
```python
# src/datafusion_engine/lineage_datafusion.py
_PLAN_EXPR_ATTRS = {
    "Aggregate": ("group_expr", "aggr_expr"),
    "Filter": ("predicate",),
    "Join": ("filter",),
    "Projection": ("expr", "projections"),
    "Sort": ("expr",),
    "TableScan": ("filters",),
    "Window": ("window_expr",),
    "Limit": ("skip", "fetch"),
    "Repartition": ("partitioning_scheme",),
    "Unnest": ("expr",),
    "RecursiveQuery": ("input", "recursive"),
}
```

**Target files to modify**
- `src/datafusion_engine/lineage_datafusion.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add variant fields for Limit/Repartition/Unnest/RecursiveQuery/Explain/Analyze.
- [ ] Ensure join key expressions are captured consistently.
- [ ] Add tests covering each new variant.

---

## Scope 3: Capture EXPLAIN FORMAT TREE and EXPLAIN VERBOSE in plan artifacts

**Objective**
Store deterministic plan deltas and rule-by-rule optimizer provenance.

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py

def _explain_rows(ctx: SessionContext, sql: str) -> list[dict[str, object]]:
    df = ctx.sql(f"EXPLAIN FORMAT TREE {sql}")
    return df.to_arrow_table().to_pylist()


def _explain_verbose_rows(ctx: SessionContext, sql: str) -> list[dict[str, object]]:
    df = ctx.sql(f"EXPLAIN VERBOSE {sql}")
    return df.to_arrow_table().to_pylist()
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_artifact_store.py` schema rows

**Delete list**
- None.

**Implementation checklist**
- [ ] Add tree explain capture for every plan bundle.
- [ ] Add verbose explain capture (rule-by-rule deltas) behind a config flag.
- [ ] Persist explain rows to the plan artifact store.
- [ ] Add plan artifact schema version bump.

---

## Scope 4: Compute physical plan in planning mode

**Objective**
Enable physical plan capture to support scheduling and diagnostics (partitioning, shuffles, repartitions).

**Representative code pattern**
```python
# src/datafusion_engine/planning_pipeline.py
bundle = build_plan_bundle(
    ctx,
    df,
    compute_execution_plan=True,
    session_runtime=session_runtime,
    scan_units=scan_units,
)
partition_count = bundle.execution_plan.partition_count() if bundle.execution_plan else None
```

**Target files to modify**
- `src/datafusion_engine/planning_pipeline.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Compute physical plans by default in plan bundles (breaking change).
- [ ] Capture partition counts and physical plan displays.
- [ ] Record physical plan topology in plan details.

---

## Scope 5: Capture EXPLAIN ANALYZE metrics for scheduling weights

**Objective**
Use runtime metrics to improve scheduling priority, grouping, and critical path estimates.

**Representative code pattern**
```python
# src/datafusion_engine/plan_profiler.py (new)

def explain_analyze(ctx: SessionContext, sql: str) -> list[dict[str, object]]:
    df = ctx.sql(f"EXPLAIN ANALYZE {sql}")
    return df.to_arrow_table().to_pylist()

# src/relspec/execution_plan.py
metrics = plan_profile.metrics_by_task
bottom_costs = compute_bottom_level_costs(task_graph, metrics)
```

**Target files to modify**
- New module: `src/datafusion_engine/plan_profiler.py`
- `src/relspec/execution_plan.py`
- `src/hamilton_pipeline/scheduling_hooks.py`
- `src/hamilton_pipeline/driver_factory.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add a profiling mode that captures EXPLAIN ANALYZE metrics.
- [ ] Persist metrics in plan artifacts.
- [ ] Replace static bottom-level costs with metric-informed weights.

---

## Scope 6: Expand planning environment snapshots (information_schema)

**Objective**
Persist table inventory, column metadata, and function catalog snapshots alongside plan artifacts.

**Representative code pattern**
```python
# src/datafusion_engine/schema_introspection.py
snapshot = {
    "df_settings": introspector.settings_snapshot(),
    "tables": introspector.table_inventory(),
    "columns": introspector.columns_inventory(),
    "routines": introspector.routines_inventory(),
    "parameters": introspector.parameters_inventory(),
}
```

**Target files to modify**
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add full information_schema snapshot capture to plan bundles.
- [ ] Persist snapshots in plan artifact store.
- [ ] Include hashes of snapshots in plan identity.

---

## Scope 7: Enforce SQLOptions safety and parameterization

**Objective**
Ensure planning is non-mutating and plan shapes are stable across dynamic values.

**Representative code pattern**
```python
# src/datafusion_engine/sql_guard.py (new)

def safe_sql(ctx: SessionContext, sql: str, *, options: SQLOptions) -> DataFrame:
    return ctx.sql_with_options(sql, options)

# usage
opts = SQLOptions().with_allow_ddl(False).with_allow_dml(False).with_allow_statements(False)
df = safe_sql(ctx, "SELECT * FROM t WHERE a > $val", options=opts, param_values={"val": 10})
```

**Target files to modify**
- `src/normalize/df_view_builders.py`
- `src/schema_spec/view_specs.py`
- `src/datafusion_engine/sql_options.py`
- New helper: `src/datafusion_engine/sql_guard.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Replace `ctx.sql(...)` with `ctx.sql_with_options(...)` for planning.
- [ ] Standardize SQL parameter usage (`param_values` preferred).
- [ ] Add tests that ensure planning cannot execute DDL/DML.

---

## Scope 8: Substrait strictness and validation

**Objective**
Require Substrait bytes for plan fingerprints and optionally validate them.

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py
if substrait_bytes is None:
    raise ValueError("Substrait bytes are required for plan fingerprinting")

# src/datafusion_engine/execution_helpers.py
validation = validate_substrait_plan(substrait_bytes, df=df)
if not validation["matches"]:
    raise ValueError("Substrait validation failed")
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/runtime.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Enforce Substrait availability for all plan bundles.
- [ ] Add optional Substrait validation (config flag).
- [ ] Record validation results in plan artifacts.

---

## Scope 9: Plan artifact store schema expansion

**Objective**
Persist all new planning artifacts, environment snapshots, and metrics.

**Representative code pattern**
```python
# src/datafusion_engine/plan_artifact_store.py
@dataclass(frozen=True)
class PlanArtifactRow:
    ...
    explain_tree_json: str
    explain_verbose_json: str
    information_schema_json: str
    physical_plan_display: str | None
    explain_analyze_json: str | None
```

**Target files to modify**
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/runtime.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add schema version bump for plan artifacts.
- [ ] Add new fields to row serialization and storage.
- [ ] Write migration notes and backfill tools (design only).

---

## Scope 10: Scheduling upgrades using physical plan + metrics

**Objective**
Use plan topology and runtime metrics to drive task ordering and grouping.

**Representative code pattern**
```python
# src/relspec/execution_plan.py
costs = compute_costs_from_metrics(metrics_by_task, default_priority=priority_for_task)
plan = replace(plan, bottom_level_costs=costs)

# src/hamilton_pipeline/scheduling_hooks.py
bottom_cost = self._bottom_level_costs.get(node_.name, 0.0)
critical_boost = 1.0 if node_.name in self._critical_path_tasks else 0.0
return (-(bottom_cost + critical_boost), node_.name)
```

**Target files to modify**
- `src/relspec/execution_plan.py`
- `src/hamilton_pipeline/scheduling_hooks.py`
- `src/relspec/rustworkx_schedule.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add a cost model based on EXPLAIN ANALYZE metrics.
- [ ] Incorporate physical plan signals (partition count, repartitions).
- [ ] Make dynamic scheduling optional with a config flag.

---

## Scope 11: Golden plan tests and regression gates

**Objective**
Add plan artifact regression tests using tree/verbose explain and environment snapshots.

**Representative code pattern**
```python
# tests/plan_golden/test_plan_artifacts.py
rows = ctx.sql(f"EXPLAIN FORMAT TREE {sql}").to_arrow_table().to_pylist()
write_jsonl("plans/query.tree.jsonl", rows)
assert compare_jsonl("plans/query.tree.jsonl", rows)
```

**Target files to modify**
- New tests: `tests/plan_golden/`
- `src/datafusion_engine/plan_artifact_store.py`

**Delete list**
- None.

**Implementation checklist**
- [ ] Add a golden plan fixture format.
- [ ] Add CI gate for plan artifact drift.
- [ ] Include df_settings and information_schema snapshots in fixtures.

---

## Scope 12: Deferred deletions (after all scopes complete)

**Objective**
Remove legacy or redundant plan-capture paths only after new planning artifacts are fully adopted and validated.

**Candidates for deletion (defer until all scopes are complete)**
- Any legacy plan display fallback paths that are redundant after explain/tree/verbose capture.
- Any legacy dependency extraction paths that bypass DataFusion plan bundles (if still present).
- Any redundant plan artifact fields that are fully superseded by new schema fields.

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/relspec/inferred_deps.py`

**Delete list**
- Defer until the new plan artifacts and tests are fully in place.

**Implementation checklist**
- [ ] Identify all legacy plan capture paths after rollout.
- [ ] Remove redundant fields and simplify plan artifacts.
- [ ] Update tests and migration docs accordingly.

---

## Final design endpoint (best-in-class)
- Complete logical and physical plan capture for every view task.
- Substrait-required plan fingerprints with optional validation.
- Full environment snapshot (df_settings, tables, columns, routines, parameters) in plan artifacts.
- EXPLAIN tree + verbose + analyze captured for deterministic diffing and profiling.
- Scheduling decisions weighted by real metrics and physical plan topology.
- Golden plan tests as regression gates for semantic and performance drift.
