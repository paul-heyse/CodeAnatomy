# Programmatic Architecture Assessment Review (Codex v3)

**Date:** 2026-02-07  
**Reviewed document:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md`

## 1. Review Method

This review re-validates the latest plan revision against current code using:
- `./cq search` / `./cq calls` for symbol and callsite validation.
- targeted source reads for all high-impact pseudo-code paths.
- local DataFusion runtime probe (`uv run python`) and existing repository usage patterns.

Runtime reality check remains unchanged in this environment:
- `datafusion==51.0.0`
- `DataFrame.execution_plan()` exists.
- `ExecutionPlan.statistics()` / `ExecutionPlan.schema()` are not callable in this runtime build.

## 2. Findings (Ordered by Severity)

### 1. Critical: 10.10 uses the wrong config object for UDF gating and an invalid Delta protocol comparison shape

**Plan references:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1992`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2011`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2019`

**What is wrong:**
- `policy_bundle.feature_gates.enable_udfs` is referenced, but `PolicyBundleConfig.feature_gates` is `DataFusionFeatureGates`, which only has optimizer toggles (`enable_dynamic_filter_pushdown`, etc.), not `enable_udfs`.
- `set(policy_bundle.delta_protocol_support)` is invalid because `delta_protocol_support` is a structured object (`DeltaProtocolSupport`), not an iterable of features.

**Code evidence:**
- `DataFusionFeatureGates` fields: `src/datafusion_engine/session/runtime.py:781`
- `FeatureGatesConfig.enable_udfs`: `src/datafusion_engine/session/runtime.py:3508`
- `PolicyBundleConfig.feature_gates` type: `src/datafusion_engine/session/runtime.py:3614`
- proper Delta protocol comparison helper: `src/datafusion_engine/delta/protocol.py:68`

**Correction:**
- Use `runtime_profile.features.enable_udfs` / `runtime_profile.features.enable_async_udfs` for UDF platform gating.
- Use `delta_protocol_compatibility(snapshot, policy_bundle.delta_protocol_support)` instead of `set(...)` arithmetic.

---

### 2. Critical: 10.10 integration point is in the wrong compilation phase

**Plan references:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1976`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2037`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:2066`

**What is wrong:**
- The plan places policy validation inside `compile_semantic_program()`, but this function returns only `SemanticProgramManifest` and has no `ExecutionPlan` task graph semantics.

**Code evidence:**
- `compile_semantic_program()` signature/behavior: `src/semantics/compile_context.py:129`
- `ExecutionPlan` is built at `compile_execution_plan()`: `src/relspec/execution_plan.py:450`

**Correction:**
- Move 10.10 validation to the execution-plan boundary (`compile_execution_plan()` or immediately after it), then attach results to plan diagnostics/artifacts.

---

### 3. High: 10.2 still references a non-existent `DataFusionPlanBundle.output_schema`

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1550`

**What is wrong:**
- `plan_bundle.output_schema` is used, but `DataFusionPlanBundle` has no such property.

**Code evidence:**
- `DataFusionPlanBundle` fields: `src/datafusion_engine/plan/bundle.py:116`
- existing schema extractor utility: `src/datafusion_engine/views/bundle_extraction.py:19`

**Correction:**
- Use `arrow_schema_from_df(plan_bundle.df)` (existing helper) for output schema derivation.

---

### 4. High: Wave 3 contains an API inconsistency (`required_adapter_names` vs `required_executors`)

**Plan references:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:599`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:679`

**What is wrong:**
- Wave 3 correctly proposes net-new `required_adapter_names()`, but a later snippet still calls `evidence_plan.required_executors()` (non-existent).

**Code evidence:**
- current `EvidencePlan` API: `src/extract/coordination/evidence_plan.py:21`

**Correction:**
- Standardize on one new API (`required_adapter_names`) and remove all `required_executors` mentions.

---

### 5. High: 10.7 pseudo-code for `datasource_config_from_manifest()` has type/API mismatches

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1868`

**What is wrong:**
- `DataSourceConfig.extract_output` expects `ExtractOutputConfig`, not a mapping.
- `DataSourceConfig.semantic_output` expects `SemanticOutputConfig`, not a mapping.
- `manifest.dataset_bindings.as_location_map()` is referenced, but no such method exists.

**Code evidence:**
- `DataSourceConfig` type contract: `src/datafusion_engine/session/runtime.py:3469`
- `ManifestDatasetBindings` available API (`locations`, `payload`, `names`, etc.): `src/semantics/program_manifest.py:18`

**Correction:**
- Build `ExtractOutputConfig` / `SemanticOutputConfig` structs explicitly.
- Use `dict(manifest.dataset_bindings.locations)` if a mapping is needed.

---

### 6. Medium: 10.3 reads a non-standard stats key (`stats_row_count`) from `plan_details`

**Plan references:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1697`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:935`

**What is wrong:**
- Proposed extraction uses `stats.get("stats_row_count")`, but existing normalized payload uses `num_rows` / `row_count` (and bytes keys), with optional compatibility coercion downstream.

**Code evidence:**
- stats payload normalization keys: `src/datafusion_engine/plan/bundle.py:2274`
- canonical consumption path: `src/relspec/execution_plan.py:1575`

**Correction:**
- Read `num_rows` first, then `row_count`; avoid introducing new key variants in policy code.

---

### 7. Medium: Wave 2 effort sizing claim (`40+ upstream callers`) is currently unproven

**Plan reference:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:503`

**What is wrong:**
- The claim is used for scope sizing, but current CQ evidence is mixed by function (for example, `materialize_extract_plan` has 25 callsites today; some other functions are wrapped/dynamically surfaced and are not directly measurable by `cq calls` without additional mapping).

**Code evidence:**
- `./cq calls materialize_extract_plan` -> 25 callsites across 13 files.

**Correction:**
- Replace approximate claim with measurable per-function callsite baselines and maintain in section 7 metrics.

---

### 8. Medium: Wave 5 “before current” snippet still uses an outdated sink field shape

**Plan references:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1033`, `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md:1060`

**What is wrong:**
- Snippet uses `profile.diagnostics_sink`; actual access is `profile.diagnostics.diagnostics_sink`.

**Code evidence:**
- `record_artifact()` implementation: `src/datafusion_engine/lineage/diagnostics.py:585`

**Correction:**
- Update pseudo-code to match runtime profile layout.

## 3. Best-in-Class Architecture Enhancements (Beyond Current Plan)

### A. Separate semantic compile authority from orchestration execution authority

Current plan extends `SemanticExecutionContext` with non-semantic fields (`evidence_plan`, executor map). This risks cross-layer coupling.

**Enhancement:**
- Keep `SemanticExecutionContext` focused on semantic compile artifacts.
- Introduce a new orchestration context (for example `ExecutionAuthorityContext`) that composes:
  - semantic manifest/resolver
  - evidence plan
  - executor map
  - capability snapshot
  - runtime/session handle

This preserves clean layer boundaries and allows independent versioning of semantic vs orchestration contracts.

### B. Introduce a shared `PlanSignalExtractor` instead of ad-hoc plan signal reads

Multiple proposals need schema/stats/lineage/predicate signals.

**Enhancement:**
- Add a single utility that derives a typed signal bundle from `DataFusionPlanBundle`:
  - schema (`arrow_schema_from_df(bundle.df)`)
  - lineage (`extract_lineage_from_bundle(bundle)`)
  - physical metrics (`bundle.plan_details` with capability-aware keys)
  - protocol compatibility (for Delta scans)

This avoids repeated plan walking and inconsistent key usage.

### C. Make policy validation an execution-plan diagnostic stage with hard/soft modes

**Enhancement:**
- Run policy validation immediately after `compile_execution_plan()`.
- Persist result in execution-plan diagnostics and artifacts.
- support modes: `error|warn|off` so rollout can start non-blocking.

### D. Reuse existing Delta compatibility machinery end-to-end

**Enhancement:**
- Standardize all Delta protocol support checks on `delta_protocol_compatibility()` and one canonical artifact payload shape.
- Avoid custom feature-set arithmetic in new code paths.

### E. Add phase-level invariants for “single compile, multiple consumers”

**Enhancement:**
- Enforce invariants in tests and optionally runtime assertions:
  - semantic compile invoked once per pipeline run
  - resolver identity reused through registration, scan overrides, CDF, execution
  - execution-plan signature captures capability snapshot and policy validation summary

## 4. Revised Implementation Delta

Apply these deltas to the current plan before implementation starts:

1. Fix 10.10 API/phase issues (Findings 1 and 2).
2. Fix 10.2/10.3 signal access and key usage (Findings 3 and 6).
3. Fix Wave 3/10.7 API consistency bugs (Findings 4 and 5).
4. Correct remaining pseudo-code mismatches and effort sizing assumptions (Findings 7 and 8).
5. Add the best-in-class enhancements A-E as explicit scope items (can run in parallel with Waves 3-5).

## 5. Final Assessment

The updated plan is substantially improved and close to execution-ready. The remaining blockers are no longer broad architectural direction issues; they are now mostly API-fit and phase-boundary correctness issues that should be corrected before implementation to avoid churn.

Once these eight items are addressed, the plan will be aligned for a best-in-class semantically compiling, inference-driven view generation architecture.
