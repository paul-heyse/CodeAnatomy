# DataFusion Planning Best-in-Class Implementation Plan

Date: 2026-01-28
Owner: Codex (design phase)
Status: implemented (breaking changes applied)

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
- [x] Add a reusable plan walker that includes embedded subplans.
- [x] Update lineage extraction to use the complete walker.
- [x] Add coverage for Subquery/SubqueryAlias/Explain/Analyze traversal.
- [x] Add tests for subquery dependency capture.

**Implementation notes**
- Added `src/datafusion_engine/plan_walk.py` and wired `lineage_datafusion.extract_lineage`.
- Added lineage variant coverage in `tests/unit/test_lineage_plan_variants.py`.

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
- [x] Add variant fields for Limit/Repartition/Unnest/RecursiveQuery/Explain/Analyze.
- [x] Ensure join key expressions are captured consistently.
- [x] Add tests covering each new variant.

**Implementation notes**
- Expanded `_PLAN_EXPR_ATTRS` and join key extraction in `src/datafusion_engine/lineage_datafusion.py`.
- Added tests for repartition, unnest, recursive, and join key capture.

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
- [x] Add tree explain capture for every plan bundle.
- [x] Add verbose explain capture (rule-by-rule deltas) behind a config flag.
- [x] Persist explain rows to the plan artifact store.
- [x] Add plan artifact schema version bump.

**Implementation notes**
- Explain outputs are captured via `capture_explain` in `src/datafusion_engine/plan_bundle.py`.
- Verbose explain gated by `DataFusionRuntimeProfile.explain_verbose`.

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
- [x] Compute physical plans by default in plan bundles (breaking change).
- [x] Capture partition counts and physical plan displays.
- [x] Record physical plan topology in plan details.

**Implementation notes**
- Physical plan display + partition/repartition counts recorded in plan details.

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
- [x] Add a profiling mode that captures EXPLAIN ANALYZE metrics.
- [x] Persist metrics in plan artifacts.
- [x] Replace static bottom-level costs with metric-informed weights.

**Implementation notes**
- `src/datafusion_engine/plan_profiler.py` captures explain/analyze output and metrics.
- Scheduling costs incorporate duration/output/partition/repartition metrics with
  opt-out via `ExecutionPlanRequest.enable_metric_scheduling`.

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
- [x] Add full information_schema snapshot capture to plan bundles.
- [x] Persist snapshots in plan artifact store.
- [x] Include hashes of snapshots in plan identity.

**Implementation notes**
- Snapshots now include `df_settings`, `tables`, `schemata`, `columns`, `routines`,
  `parameters`, and `function_catalog`.

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
- [x] Replace `ctx.sql(...)` with `ctx.sql_with_options(...)` for planning.
- [x] Standardize SQL parameter usage (`param_values` preferred).
- [x] Add tests that ensure planning cannot execute DDL/DML.

**Implementation notes**
- Added `src/datafusion_engine/sql_guard.py` and `planning_sql_options` helper.
- Safe SQL guard is exercised in `tests/unit/test_safe_sql.py`.

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
- [x] Enforce Substrait availability for all plan bundles.
- [x] Add optional Substrait validation (config flag).
- [x] Record validation results in plan artifacts.

**Implementation notes**
- `build_plan_bundle` and `plan_fingerprint_from_bundle` now require Substrait bytes.

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
    explain_tree_json: str | None
    explain_verbose_json: str | None
    explain_analyze_json: str | None
    information_schema_json: str
    substrait_validation_json: str | None
```

**Target files to modify**
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/runtime.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Add schema version bump for plan artifacts.
- [x] Add new fields to row serialization and storage.
- [x] Write migration notes and backfill tools (design only).

**Implementation notes**
- Plan artifacts schema bumped to `datafusion_plan_artifacts_v6`.
- Migration notes added in `docs/plans/datafusion_plan_artifacts_v6_migration_notes.md`.

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
- [x] Add a cost model based on EXPLAIN ANALYZE metrics.
- [x] Incorporate physical plan signals (partition count, repartitions).
- [x] Make dynamic scheduling optional with a config flag.

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
- [x] Add a golden plan fixture format.
- [x] Add CI gate for plan artifact drift.
- [x] Include df_settings and information_schema snapshots in fixtures.

**Implementation notes**
- Golden fixtures live in `tests/plan_golden/fixtures/` and are exercised via
  `tests/plan_golden/test_plan_artifacts.py`.

---

## Scope 12: Deferred deletions (completed)

**Objective**
Remove legacy or redundant plan-capture paths now that the new planning artifacts are adopted.

**Candidates for deletion**
- Any legacy plan display fallback paths that are redundant after explain/tree/verbose capture.
- Any legacy dependency extraction paths that bypass DataFusion plan bundles (if still present).
- Any redundant plan artifact fields that are fully superseded by new schema fields.

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/relspec/inferred_deps.py`

**Delete list**
- Legacy plan display columns were removed from plan artifact storage.

**Implementation checklist**
- [x] Identify all legacy plan capture paths after rollout.
- [x] Remove redundant fields and simplify plan artifacts.
- [x] Update tests and migration docs accordingly.

---

## Final design endpoint (best-in-class)
- Complete logical and physical plan capture for every view task.
- Substrait-required plan fingerprints with optional validation.
- Full environment snapshot (df_settings, tables, columns, routines, parameters) in plan artifacts.
- EXPLAIN tree + verbose + analyze captured for deterministic diffing and profiling.
- Scheduling decisions weighted by real metrics and physical plan topology.
- Golden plan tests as regression gates for semantic and performance drift.
