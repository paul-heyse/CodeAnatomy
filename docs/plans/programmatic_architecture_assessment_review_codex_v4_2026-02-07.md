# Programmatic Architecture Assessment Review (Codex v4)

**Date:** 2026-02-07  
**Reviewed document:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md`  
**Focus:** Best-in-class semantic-model-driven architecture with peak performance and fidelity

## 1. Findings (Ordered by Severity)

### 1. Critical: 10.10 validation pseudo-code is still incompatible with `ExecutionPlan` contract

**Plan references:**
- `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2115`
- `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2125`
- `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2134`

**Issue:** The pseudo-code iterates `execution_plan.tasks` and accesses `task.required_udfs`, `task.writes_delta_table`, and `task.input_row_count(...)`, but those fields/functions do not exist on current plan objects.

**Code evidence:**
- `ExecutionPlan` fields are defined at `src/relspec/execution_plan.py:83` and include `view_nodes`, `scan_units`, `task_plan_metrics`, `lineage_by_view`, etc., not `tasks`.
- `TaskPlanMetrics` exists at `src/relspec/execution_plan.py:140` and is keyed by task name in `execution_plan.task_plan_metrics`.
- Scan/write/protocol signals are carried by `ScanUnit` (`src/datafusion_engine/lineage/scan.py:101`).

**Correction:**
- Validate against existing structures:
  - UDF requirements from `execution_plan.view_nodes[*].required_udfs`
  - Delta compatibility from `execution_plan.scan_units[*].protocol_compatibility`
  - size/cost heuristics from `execution_plan.task_plan_metrics`
- Do not invent a parallel task object unless a new `ExecutionTaskView` contract is explicitly added first.

---

### 2. Critical: Delta compatibility property is wrong in 10.10 pseudo-code

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2130`

**Issue:** Pseudo-code checks `compat.is_compatible`; actual field is `compatible`.

**Code evidence:**
- `DeltaProtocolCompatibility.compatible` at `src/datafusion_engine/delta/protocol.py:48`.
- Helper function is `delta_protocol_compatibility(...)` at `src/datafusion_engine/delta/protocol.py:68`.

**Correction:**
- Use `if compat.compatible is False:` and handle `None` (support unconfigured) explicitly according to mode (`error|warn|ignore`).

---

### 3. High: 10.9 stores into a non-existent `ExecutionPlan.scan_policies`

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2050`

**Issue:** `ExecutionPlan` has no `scan_policies` field.

**Code evidence:**
- `ExecutionPlan` contract at `src/relspec/execution_plan.py:83`.
- Existing scan override flow uses `scan_units` and `apply_scan_unit_overrides(...)`:
  - `src/datafusion_engine/plan/pipeline.py:109`
  - `src/datafusion_engine/dataset/resolution.py:183`

**Correction:**
- Keep policy derivation scan-unit-native:
  - either enrich `ScanUnit` metadata,
  - or add a typed `scan_policy_overrides_by_key: Mapping[str, ...]` keyed by scan unit key.

---

### 4. High: Wave 3 and Section 8.7 are internally contradictory

**Plan references:**
- Wave 3 asks to add orchestration fields to `SemanticExecutionContext`: `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:645`
- Section 8.7 asks to keep `SemanticExecutionContext` semantic-only and introduce `ExecutionAuthorityContext`: `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1350`

**Issue:** These are mutually exclusive design choices.

**Code evidence:**
- Current semantic context shape is compact/semantic at `src/semantics/compile_context.py:107`.

**Correction:**
- Adopt 8.7 as authoritative and revise Wave 3 checklist to use a separate orchestration authority object.

---

### 5. High: Wave 3 step 6 puts orchestration validation inside semantic compile boundary

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:679`

**Issue:** It asks for executor-map validation in `compile_semantic_program()`, which is semantic-manifest compilation.

**Code evidence:**
- `compile_semantic_program()` returns `SemanticProgramManifest` only: `src/semantics/compile_context.py:129`.
- Execution planning boundary is `compile_execution_plan(...)`: `src/relspec/execution_plan.py:450`.

**Correction:**
- Move executor availability checks to orchestration/plan compilation boundary (same boundary where plan + runtime policy validation runs).

---

### 6. High: 10.10 checklist still anchors `PolicyValidationResult` in the wrong module

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2150`

**Issue:** Checklist says add `PolicyValidationResult` to `compile_context.py` while 10.10 correctly says validation belongs at execution-plan boundary.

**Code evidence:**
- Semantic compile module scope at `src/semantics/compile_context.py:1`.
- Existing execution-plan compatibility validators live in `src/relspec/execution_plan.py:1283` onward.

**Correction:**
- Define policy validation contracts under `relspec` (or a dedicated `datafusion_engine/plan/policy_validation.py`), not semantic compile context.

---

### 7. Medium: “single compile” decommission target is underspecified for current call graph

**Plan references:**
- Invariant goal: `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1240`
- Decommission statement: `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2412`

**Issue:** The plan calls for single compile but does not concretely define canonical entrypoint ownership across all current callsites.

**Code evidence (CQ calls):**
- `build_semantic_execution_context(...)` has at least two callsites:
  - `src/datafusion_engine/plan/pipeline.py:80`
  - `src/datafusion_engine/session/runtime.py:4572`
- `compile_execution_plan(...)` is already a consolidated boundary from orchestration (`src/hamilton_pipeline/driver_factory.py:527`).

**Correction:**
- Define explicit authority: one compile entrypoint per pipeline run, pass authority object downstream, and enforce with test-level compile counters.

---

### 8. Medium: 10.7 is correct on types but still not fully semantic-authority-first

**Plan references:**
- `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1992`
- `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2006`

**Issue:** It fixes type mismatches but still pulls extract/semantic output locations from runtime profile helpers without a strict semantic-authority contract for non-bootstrap paths.

**Code evidence:**
- `DataSourceConfig` type contract: `src/datafusion_engine/session/runtime.py:3469`
- Manifest bindings API: `src/semantics/program_manifest.py:18`

**Correction:**
- Split builder modes explicitly:
  - semantic-authority mode (manifest + declared output policy),
  - runtime-bootstrap mode (profile-derived fallbacks).
- Fail closed in semantic-authority mode when unresolved.

## 2. Corrections to Apply Before Implementation

1. Replace 10.10 pseudo-code with execution-plan-native iteration over `view_nodes`, `scan_units`, and `task_plan_metrics`.
2. Replace all `compat.is_compatible` checks with `compat.compatible` handling (`True/False/None`).
3. Replace `ExecutionPlan.scan_policies` language with scan-unit-keyed override storage.
4. Make 8.7 authoritative and remove Wave 3 instructions that mutate `SemanticExecutionContext` with orchestration state.
5. Move `PolicyValidationResult` and policy validation implementation into execution-planning modules, not `compile_context.py`.
6. Move executor availability validation out of `compile_semantic_program()`.

## 3. Best-in-Class Expansion Scope (Recommended)

### A. Authority layering: semantic kernel + orchestration kernel

- Keep `SemanticExecutionContext` semantic-only.
- Add `ExecutionAuthorityContext` containing:
  - semantic context,
  - evidence plan,
  - executor map,
  - plan capability snapshot,
  - session runtime fingerprints.
- Version the two contracts independently.

### B. Unify all plan signal consumption behind one typed control plane

- Promote Section 8.8 to mandatory for Waves 4/10.x.
- `PlanSignals` should include:
  - schema (`arrow_schema_from_df`),
  - lineage,
  - normalized stats payload + source provenance,
  - scan-unit protocol compatibility summary,
  - determinism/fingerprint fields.
- All consumers (10.2, 10.3, 10.9, 10.10) read `PlanSignals`, not raw `plan_details`.

### C. Performance architecture track (DataFusion/Delta)

Adopt a formal “perf policy” section tied to runtime config snapshots and plan signatures:
- cache policy tiers:
  - DataFusion listing cache TTL/limit,
  - metadata cache limit,
  - selective `DataFrame.cache()` for fanout-heavy intermediate nodes.
- statistics policy:
  - `collect_statistics` stance,
  - `meta_fetch_concurrency` tuning envelope,
  - explicit fallback behavior when stats unavailable.
- deterministic plan bundle comparison:
  - retain P0/P1/P2 artifacts and diff gates for regressions.

### D. Fidelity and safety track

- Add hard invariant tests for:
  - single semantic compile per run,
  - resolver identity continuity across registration, scan override, and execution,
  - policy validation determinism under fixed runtime hash.
- Add policy-validation rollout modes and a structured artifact schema with machine-readable codes.

## 4. Corrected 10.10 Skeleton (API-Accurate)

```python
# Suggested location: src/relspec/execution_plan.py (or relspec/policy_validation.py)

def validate_policy_bundle(
    execution_plan: ExecutionPlan,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    udf_snapshot: Mapping[str, object],
) -> PolicyValidationResult:
    issues: list[PolicyValidationIssue] = []

    # UDF policy checks from view nodes
    for node in execution_plan.view_nodes:
        required = tuple(node.required_udfs or ())
        if required and not runtime_profile.features.enable_udfs:
            issues.append(error("UDFS_DISABLED", task=node.name, detail=str(required)))
        missing = sorted(name for name in required if name not in udf_snapshot)
        if missing:
            issues.append(error("UDF_MISSING", task=node.name, detail=str(missing)))

    # Delta protocol checks from scan units
    for unit in execution_plan.scan_units:
        compat = unit.protocol_compatibility
        if compat is not None and compat.compatible is False:
            issues.append(error("DELTA_PROTOCOL_INCOMPATIBLE", dataset=unit.dataset_name))

    # Size/policy heuristics from task metrics
    for task_name, metric in execution_plan.task_plan_metrics.items():
        if metric.stats_row_count is not None and metric.stats_row_count < 1000:
            issues.append(warn("SMALL_INPUT_SCAN_POLICY", task=task_name))

    return PolicyValidationResult.from_issues(issues)
```

## 5. Final Assessment

The updated architecture plan is materially improved and close to execution grade. The remaining blockers are now concentrated in a small set of boundary and contract-fit issues (mostly 10.9/10.10 and Wave 3 vs 8.7 consistency). Once those are corrected and the expansion track is added, the plan can support a genuinely best-in-class semantic-authority-first, inference-driven architecture with stronger performance determinism and higher policy fidelity.
