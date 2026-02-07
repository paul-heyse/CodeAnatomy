# Programmatic Architecture Assessment Review (Codex v2)

**Date:** 2026-02-07  
**Reviewed document:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md`

## 1. Scope and Method

This review validates the updated plan against current repository code and runtime behavior.

Validation approach:
- `./cq search` and targeted source inspection for all high-impact claims/snippets.
- DataFusion runtime probe via `uv run python`.

DataFusion probe result in this environment:
- `datafusion_version = 51.0.0`
- `DataFrame.execution_plan()` exists.
- Returned `ExecutionPlan` does not expose callable `statistics()`/`schema()` in this build.

Implication:
- Capability detection remains mandatory for stats-dependent logic.
- New plan APIs must not assume direct plan statistics accessors.

## 2. Corrections Required Before Implementation

## 2.1 Bridge pattern count/state drift remains in Section 2.2

### Issue
The updated plan still states a fallback in `IncrementalRuntime.dataset_resolver` and uses an inconsistent callsite cluster count.

- Plan lines: `108-123` and table row at `116`.
- Plan claims "8 callsite clusters" while listing nine rows and including an incremental runtime fallback that no longer exists.

### Verified state
- `IncrementalRuntime.dataset_resolver` is injected and returned directly: `src/semantics/incremental/runtime.py:71`.
- Registry facade path also uses injected resolver directly: `src/semantics/incremental/runtime.py:91`.
- `dataset_bindings_for_profile()` production callsites outside `compile_context.py` are seven in `src/`:
  - `src/datafusion_engine/registry_facade.py:390`
  - `src/datafusion_engine/session/runtime.py:4081`
  - `src/datafusion_engine/session/runtime.py:4129`
  - `src/datafusion_engine/session/runtime.py:7618`
  - `src/datafusion_engine/session/facade.py:832`
  - `src/datafusion_engine/plan/bundle.py:1909`
  - `src/hamilton_pipeline/driver_factory.py:640`

### Correction
- Remove the incremental runtime fallback row.
- Recompute the baseline counts and keep one counting convention (definition vs consumer-only).

## 2.2 Wave 2 hotspot set still misses a real compile boundary hotspot

### Issue
Wave 2 scope still undercounts direct compile-context instantiation points.

### Verified state
`CompileContext(runtime_profile=...)` callsites in `src/` outside `compile_context.py` are seven:
- `src/engine/materialize_pipeline.py:308`
- `src/hamilton_pipeline/driver_factory.py:465`
- `src/semantics/pipeline.py:1394`
- `src/semantics/pipeline.py:1980`
- `src/extract/coordination/materialization.py:457`
- `src/hamilton_pipeline/modules/task_execution.py:497`
- `src/hamilton_pipeline/modules/task_execution.py:523`

### Correction
- Add `driver_factory.py:465` and `semantics/pipeline.py:1980` to explicit Wave 2 migration inventory.
- Treat Wave 2 as complete only when these seven sites are converged.

## 2.3 Wave 3 implementation snippets use non-existent interfaces/symbols

### Issue A: `EvidencePlan.required_executors()`
Plan proposes adding/using `required_executors()` but treats it as if it already exists.

### Verified state
- `EvidencePlan` currently provides `requires_dataset`, `requires_template`, and `required_columns_for` only: `src/extract/coordination/evidence_plan.py:29`, `src/extract/coordination/evidence_plan.py:41`, `src/extract/coordination/evidence_plan.py:58`.
- `./cq search required_executors` returns no matches.

### Correction
- Either explicitly define `required_executors()` as net-new API, or derive required adapter names from existing `EvidencePlan.sources/requirements` plus extract metadata.

### Issue B: global registration state symbol mismatch
Plan references `_EXTRACT_EXECUTORS_REGISTERED`.

### Verified state
- Actual symbol is `_EXTRACT_EXECUTOR_REGISTRATION_STATE`: `src/hamilton_pipeline/modules/task_execution.py:871`.

### Correction
- Update decommission targets to real symbols.

### Issue C: `_ADAPTERS` import path mismatch
Plan snippets import `_ADAPTERS` from extract execution registry.

### Verified state
- `_ADAPTERS` lives in `src/datafusion_engine/extract/adapter_registry.py:22`.
- `extract_execution_registry.py` stores executor callables in `_EXTRACT_ADAPTER_EXECUTORS`: `src/hamilton_pipeline/modules/extract_execution_registry.py:27`.

### Correction
- Use the correct adapter metadata module, and keep executor storage concerns in orchestration registry.

## 2.4 `SemanticExecutionContext` changes are specified as if fields already exist

### Issue
Wave 3 snippets assume `SemanticExecutionContext` already carries `evidence_plan`/`extract_executor_map` and builder accepts those inputs.

### Verified state
- Current dataclass fields: `manifest`, `dataset_resolver`, `runtime_profile`, `ctx`, `facade`: `src/semantics/compile_context.py:107`.
- `build_semantic_execution_context()` has no `evidence_plan` argument and builds only existing fields: `src/semantics/compile_context.py:152`.

### Correction
- Mark this as an explicit API extension with migration plan (new fields + callsite updates), not as an incremental patch.

## 2.5 Wave 4/10.3 uses non-existent `DataFusionPlanBundle` convenience methods

### Issue
Plan examples call:
- `plan_bundle.output_statistics`
- `plan_bundle.statistics_for_table(...)`
- `plan_bundle.has_filter_on_table(...)`
- `plan_bundle.has_sort_match_for_table(...)`

### Verified state
- `DataFusionPlanBundle` has no such methods/properties: `src/datafusion_engine/plan/bundle.py:116`.
- Existing stats pathway is `plan_details` with `_plan_statistics_section` / `_plan_statistics_payload`: `src/datafusion_engine/plan/bundle.py:2071`, `src/datafusion_engine/plan/bundle.py:2274`.
- Existing lineage extraction already exposes scans/filters/joins via `LineageReport`: `src/datafusion_engine/lineage/datafusion.py:78`.

### Correction
- Base adaptive policy derivation on current `plan_details` + `extract_lineage_from_bundle(...)` (`src/datafusion_engine/views/bundle_extraction.py:43`) instead of inventing absent bundle APIs.

## 2.6 Section 10.2 "before" signature for `_dataset_contract_for` is inaccurate

### Issue
The documented current signature in 10.2 does not match actual code.

### Verified state
Current signature:
- `_dataset_contract_for(name: str, *, dataset_specs: Mapping[str, DatasetSpec]) -> tuple[pa.Schema | None, bool]`
- `src/datafusion_engine/views/registry_specs.py:280`

### Correction
- Update 10.2 examples and migration steps to the actual tuple-returning contract.

## 2.7 Section 10.10 uses a non-existent policy gate API

### Issue
Pseudo-code calls `policy_bundle.feature_gates.enables_udf(udf_name)`.

### Verified state
- No `enables_udf` symbol (`./cq search enables_udf` -> no matches).
- Relevant existing gates are booleans in `FeatureGatesConfig` (`enable_udfs`, `enable_async_udfs`, etc.): `src/datafusion_engine/session/runtime.py:3508`.
- `DataFusionFeatureGates` is optimizer config settings only: `src/datafusion_engine/session/runtime.py:781`.

### Correction
- Rework validation checks to use existing booleans and UDF snapshot presence checks.

## 3. Optimization and Enhancement Recommendations

## 3.1 Recast Wave 4B around existing lineage/stats structures

Instead of adding many methods to `DataFusionPlanBundle`, add a small extraction helper layer that consumes:
- `bundle.plan_details` (`statistics`, `partition_count`, `repartition_count`)
- `extract_lineage_from_bundle(bundle)` (`scans`, `filters`, `joins`, `required_columns_by_dataset`)

Benefits:
- Lower API surface area.
- No duplication of plan-walk logic.
- Reuses already-tested lineage path.

## 3.2 Make Wave 3 executor-map design use existing registry boundaries

Use two-stage derivation:
1. Resolve required adapter names from `EvidencePlan` + extract metadata.
2. Validate against `registered_adapter_names()` and map to executors through `get_extract_executor`.

This avoids leaking `_ADAPTERS` internals across layers and keeps execution registry authority intact.

## 3.3 Keep schema divergence logic at semantic-view node build point

Given current call flow, divergence checks belong in `_build_semantic_view_node()` where both contract and `plan_bundle` are present (`src/datafusion_engine/views/registry_specs.py:410`).

Recommendation:
- Keep `_dataset_contract_for()` focused on spec-contract derivation.
- Add divergence telemetry and optional strict enforcement in node assembly.

## 3.4 Policy validation should integrate with existing async/UDF validators

Before adding new policy validators, reuse and extend:
- `_validate_async_udf_policy`: `src/datafusion_engine/session/runtime.py:4687`
- existing UDF snapshot/install paths in runtime.

This reduces duplicate validation logic and mismatch risk.

## 3.5 Artifact governance should stage via compatibility adapter

Current artifact recording API is string-keyed (`record_artifact(profile, name: str, payload)`): `src/datafusion_engine/lineage/diagnostics.py:585`.

Enhancement path:
- Introduce typed artifact key/spec wrappers first.
- Keep string compatibility during migration.
- Phase migration by subsystem because usage is high-volume (currently ~180 `record_artifact(` references in `src/`).

## 4. Scope Expansions Worth Adding

1. Add resolver identity invariants in integration tests.
- Enforce same resolver object/threading across planning, scan override application, CDF input registration, and execution fallback.

2. Add CQ drift checks as CI guardrails.
- Guard against new direct `CompileContext(runtime_profile=...)` usage outside allowed boundary.
- Guard against reintroduction of `dataset_bindings_for_profile()` outside allowed seam.

3. Add DataFusion capability snapshot artifact.
- Record plan-stat capability flags per runtime startup to explain why adaptive features are enabled/disabled.

4. Add migration-safe API contracts for new execution context fields.
- If extending `SemanticExecutionContext`, version/fingerprint impact should be explicitly documented and tested.

## 5. Revised Milestone Ordering

Recommended execution order:

1. Baseline correction pass in the plan doc (counts, symbols, signatures, pseudo-code API fixes).
2. Wave 1 resolver-threading closure (all seven `dataset_bindings_for_profile` consumers in `src/`).
3. Wave 2 compile-boundary convergence (all seven direct `CompileContext(runtime_profile=...)` callsites outside compile context).
4. Wave 3 executor-map migration with compatibility path.
5. Wave 4/10.3 adaptive policy via existing lineage + plan details (capability-gated).
6. Wave 5 artifact governance phased rollout.
7. 10.10 policy validation extension after Wave 2 and with existing runtime validators.

## 6. Corrected Tracking Metrics

Use these metrics consistently (consumer-only, `src/` only):

```bash
./cq search dataset_bindings_for_profile
./cq search "CompileContext(runtime_profile" --literal
./cq search _EXTRACT_EXECUTOR_REGISTRATION_STATE
./cq search build_semantic_execution_context
./cq search "record_artifact(" --literal
```

Recommended interpretation rules:
- Track production (`src/`) separately from tests/scripts.
- Track helper definitions separately from callsites.
- For removal goals, mark as complete only when both callsites and fallback guards are removed.

## 7. Final Assessment

The updated plan is materially stronger than the prior revision and now has a viable strategic direction. The remaining issues are mainly API-fit and baseline-accounting errors that can cause implementation churn if left uncorrected.

If the corrections above are applied before coding begins, the implementation plan is execution-ready with substantially lower risk.
