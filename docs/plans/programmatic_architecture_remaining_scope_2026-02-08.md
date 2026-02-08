# Programmatic Architecture Remaining Scope (Audit 2026-02-08)

## Source Plan
- `docs/plans/programmatic_architecture_continuation_consolidated_2026-02-07.md`

## Setup Executed
- `bash scripts/bootstrap_codex.sh`
- `uv sync`

## Scope Classification
- **Partially incomplete**: implemented in part (or test-only), but not fully cut over in production flow.
- **Fully incomplete**: no production implementation found for the planned capability.

## Executive Summary
| Plan Area | Status | Remaining Scope |
|---|---|---|
| Phase A | Partially incomplete | Resolver identity has unit coverage but no integration coverage. |
| Phase B | Partially incomplete | Compiled policy wiring is incomplete; cache-policy cutover not fully connected; artifact typing still partial. |
| Phase C | Partially incomplete | Calibrator exists but is not operationally wired into runtime artifact flow. |
| Phase C' | No material remaining gap found | Scheduling hardening appears implemented and tested. |
| Phase D | Partially incomplete | Join inference core is implemented; confidence threading into runtime decision path is incomplete. |
| Phase E | Partially incomplete | Entity model generation is live; 14→6 kind consolidation is not cut over. |
| Phase F | Partially incomplete | Infer phase exists, but inference remains field-heuristic-led and not fully connected to execution policy. |
| Phase G | Partially + fully incomplete | Multiple advanced acceleration tracks are scaffolded but not runtime-wired (or absent). |
| Phase H | Partially + fully incomplete | Schema derivation exists but is not the runtime authority; struct-driven schema generation is missing. |
| Phase I | Partially incomplete | Semantic type routing still includes string heuristics and hardcoded column checks. |
| Phase J | Partially incomplete | Single authority mostly present, but compatibility aliases and static discovery patterns remain. |
| Section 18 decommission | Partially + fully incomplete | Several decommission gates are still open. |

## Detailed Remaining Scope

### Phase A: Correctness and Integration Closure

#### A.2 Resolver identity assertions
**Status:** Partially incomplete  
**Evidence:**
- Resolver identity tracking implementation exists in `src/semantics/resolver_identity.py:1`.
- Unit tests exist in `tests/unit/test_resolver_identity.py:1`.
- No integration references found under `tests/integration` for `resolver_identity` (CQ search in integration scope returned none).

**Remaining implementation scope:**
1. Add integration tests that execute full semantic pipeline paths and assert single resolver identity across registration/scan/CDF/readiness paths.

---

### Phase B: Policy Compilation and Evidence Model

#### B.1 Compiled policy compilation is only partially populated
**Status:** Partially incomplete  
**Evidence:**
- Runtime compilation call does not pass optional compiler inputs (`scan_overrides`, `diagnostics_policy`) in `src/hamilton_pipeline/driver_factory.py:651` and `src/hamilton_pipeline/driver_factory.py:660`.
- Compiler supports those inputs in `src/relspec/policy_compiler.py:32`.
- UDF requirements are currently placeholder empty output in `src/relspec/policy_compiler.py:163`.
- `CompiledExecutionPolicy` includes additional sections (`maintenance_policy_by_dataset`, `materialization_strategy`) in `src/relspec/compiled_policy.py:55` to `src/relspec/compiled_policy.py:60`, but compile path does not populate them.

**Remaining implementation scope:**
1. Thread scan overrides and diagnostics policy into `_compile_authority_policy`.
2. Populate UDF requirements from plan bundle/task metadata.
3. Populate maintenance/materialization sections or explicitly remove them from contract until supported.

#### B.2 Cache-policy cutover is incomplete
**Status:** Partially incomplete  
**Evidence:**
- Naming-heuristic fallback remains active in `src/semantics/pipeline.py:292` and `src/semantics/pipeline.py:1306`.
- Three-tier hierarchy still falls back to naming convention in `src/semantics/pipeline.py:1269`.
- `compiled_cache_policy` is defined/consumed in semantics pipeline (`src/semantics/pipeline.py:119`, `src/semantics/pipeline.py:1328`), but there are no production writers of that option; matches are limited to semantics internals and tests (`rg compiled_cache_policy`).

**Remaining implementation scope:**
1. Inject `compiled_policy.cache_policy_by_view` into semantic build options from orchestration.
2. Remove naming heuristic fallback once policy compiler coverage is complete.

#### B.3 Policy validation artifact typing remains partial
**Status:** Partially incomplete  
**Evidence:**
- `POLICY_VALIDATION_SPEC` has no `payload_type` in `src/serde_artifact_specs.py:333`.
- Artifact model exists as dataclass in `src/relspec/policy_validation.py:106`.
- Builder returns typed artifact in `src/relspec/policy_validation.py:827`.
- Runtime records ad-hoc dict payload in `src/hamilton_pipeline/driver_factory.py:799` to `src/hamilton_pipeline/driver_factory.py:818`.

**Remaining implementation scope:**
1. Register a typed payload for `POLICY_VALIDATION_SPEC`.
2. Emit the typed artifact directly (or canonical typed->builtins conversion) instead of custom dict shape.

#### B.4 Inference confidence model not fully threaded
**Status:** Partially incomplete  
**Evidence:**
- Confidence-capable helper exists in `src/semantics/joins/inference.py:613`.
- Runtime join path uses `require_join_strategy` in `src/semantics/compiler.py:895`.
- `infer_join_strategy_with_confidence` has no production callsites outside inference module exports/tests (`rg infer_join_strategy_with_confidence`).

**Remaining implementation scope:**
1. Wire confidence-aware join decision object into compiler path.
2. Persist/emit confidence in policy/evidence artifacts for runtime explainability.

---

### Phase C: Adaptive Optimization with Bounded Feedback

#### C.2 Calibrator is implemented but not operationally wired
**Status:** Partially incomplete  
**Evidence:**
- Calibrator exists in `src/relspec/policy_calibrator.py:1`.
- Modes are `off|observe|apply` in `src/relspec/policy_calibrator.py:23` (plan text targeted `off|warn|enforce` semantics).
- Calibrator usage is test-centric (`src/relspec/policy_calibrator.py:106`, tests callsites via `rg calibrate_from_execution_metrics`).
- `POLICY_CALIBRATION_RESULT_SPEC` is registered in `src/serde_artifact_specs.py:1294`, but no `record_artifact(POLICY_CALIBRATION_RESULT_SPEC, ...)` callsite found in `src` or `tests`.

**Remaining implementation scope:**
1. Decide and enforce final mode contract for runtime rollout.
2. Invoke calibrator in production post-execution flow with bounded thresholds.
3. Emit `POLICY_CALIBRATION_RESULT_SPEC` artifact in runtime path.

---

### Phase D: Semantic Inference Activation

#### D-phase confidence carry-through
**Status:** Partially incomplete  
**Evidence:**
- Join-key inference when keys are omitted is implemented in `src/semantics/compiler.py:289` to `src/semantics/compiler.py:377`.
- `QualityRelationshipSpec.left_on/right_on` default-empty behavior is present in `src/semantics/quality.py:319` to `src/semantics/quality.py:320`.
- Confidence-aware join strategy output exists but is not used in runtime compiler path (`src/semantics/joins/inference.py:613`, `src/semantics/compiler.py:895`).

**Remaining implementation scope:**
1. Promote inference-confidence from optional metadata to runtime-consumed decision evidence in semantic compilation.

---

### Phase E: Entity-Centric Data Modeling

#### E.2 14→6 kind consolidation not cut over
**Status:** Partially incomplete  
**Evidence:**
- Consolidation map exists in `src/semantics/view_kinds.py:93` to `src/semantics/view_kinds.py:119`.
- Runtime still defines/uses 14 concrete kinds (`src/semantics/view_kinds.py:19` to `src/semantics/view_kinds.py:34`).
- Dispatch table remains 14-kind in `src/semantics/pipeline.py:1120` to `src/semantics/pipeline.py:1137`.

**Remaining implementation scope:**
1. Collapse runtime dispatch to consolidated kinds with per-kind parameterization.
2. Remove residual 14-kind branching once parity is proven.

---

### Phase F: Pipeline-as-Specification

#### F.1 Infer phase exists but remains heuristic-led
**Status:** Partially incomplete  
**Evidence:**
- `infer_semantics` phase exists and is part of build pipeline in `src/semantics/ir_pipeline.py:1150` and `src/semantics/ir_pipeline.py:1225`.
- Join strategy inference in IR phase is still field-name heuristic in `src/semantics/ir_pipeline.py:934`.
- IR cache hint translation helper exists in `src/semantics/ir.py:41`, but it is only referenced in integration tests (`rg ir_cache_hint_to_execution_policy`).

**Remaining implementation scope:**
1. Move IR inference from field-name heuristics toward schema/compatibility semantics.
2. Wire inferred cache/join outputs into compiled execution policy generation.

---

### Phase G: Advanced Acceleration Track

#### G.1 Workload classification/session profiles are not runtime-wired
**Status:** Partially incomplete  
**Evidence:**
- Classifier and session-config functions exist in `src/datafusion_engine/workload/classifier.py:46` and `src/datafusion_engine/workload/classifier.py:75`.
- Callsites are test-only (`rg classify_workload|session_config_for_workload` shows tests + defining module).

**Remaining implementation scope:**
1. Invoke workload classification in plan/runtime orchestration.
2. Apply `session_config_for_workload` output during session/runtime setup.

#### G.2 Pruning metrics artifact track is partial
**Status:** Partially incomplete  
**Evidence:**
- Pruning parser/tracker exist in `src/datafusion_engine/pruning/explain_parser.py:68` and `src/datafusion_engine/pruning/tracker.py:39`.
- `PRUNING_METRICS_SPEC` exists in `src/serde_artifact_specs.py:1270`.
- Runtime currently records `SCAN_UNIT_PRUNING_SPEC` via lineage scan in `src/datafusion_engine/lineage/scan.py:626` to `src/datafusion_engine/lineage/scan.py:641`.
- No `record_artifact(PRUNING_METRICS_SPEC, ...)` callsite found.

**Remaining implementation scope:**
1. Define and emit aggregate pruning metrics artifact (`PRUNING_METRICS_SPEC`) from production execution path.
2. Integrate tracker output with explain-analyze collection path.

#### G.3 Decision provenance graph is scaffolded but not emitted
**Status:** Partially incomplete  
**Evidence:**
- Provenance model + graph builder exist in `src/relspec/decision_provenance.py:299`.
- Mutable recorder exists in `src/relspec/decision_recorder.py:20`.
- Builder usage is test-only (`rg build_provenance_graph` shows tests + defining module).
- `DECISION_PROVENANCE_GRAPH_SPEC` exists in `src/serde_artifact_specs.py:1282`.
- No `record_artifact(DECISION_PROVENANCE_GRAPH_SPEC, ...)` callsite found.

**Remaining implementation scope:**
1. Build provenance graph in runtime compile/execute flow.
2. Emit graph artifact with run linkage and decision outcomes.

#### G.4 / G.5 / G.6 (counterfactual replay, workload-class policy compiler, fallback quarantine)
**Status:** Fully incomplete  
**Evidence:**
- No production matches for these specific plan capabilities (symbol/text search across `src`).
- Existing replay infrastructure is Substrait execution replay (`src/datafusion_engine/plan/execution.py:213`) and does not implement policy counterfactual replay.

**Remaining implementation scope:**
1. Implement policy-counterfactual replay API and storage model.
2. Implement workload-class policy compiler path.
3. Implement fallback quarantine model and diagnostics loop.

#### G.7 External index acceleration
**Status:** Partially incomplete  
**Evidence:**
- Delta add-action file index + pruning exists in `src/storage/deltalake/file_index.py:43` and `src/storage/deltalake/file_pruning.py:192`.
- Integrated into scan planning in `src/datafusion_engine/lineage/scan.py:357` to `src/datafusion_engine/lineage/scan.py:361`.
- No generalized external-index acceleration framework (beyond Delta-file-index pruning) found.

**Remaining implementation scope:**
1. Define external index abstraction and provider contracts.
2. Integrate index-accelerated selection beyond current Delta add-actions path.

---

### Phase H: Schema Derivation

#### H.1 Runtime authority cutover to derived schema is incomplete
**Status:** Partially incomplete  
**Evidence:**
- `derive_extract_schema` exists in `src/datafusion_engine/schema/derivation.py:41`.
- Usage is test-only (`rg derive_extract_schema` shows tests + defining module).
- Runtime schema path still depends on static registry via `extract_schema_for` in `src/datafusion_engine/extract/registry.py:120` to `src/datafusion_engine/extract/registry.py:123`.
- Static schema authorities remain `_BASE_EXTRACT_SCHEMA_BY_NAME` and `NESTED_DATASET_INDEX` in `src/datafusion_engine/schema/registry.py:1739` and `src/datafusion_engine/schema/registry.py:1751`.

**Remaining implementation scope:**
1. Route extract schema resolution through derivation path in production.
2. Keep static registry only as temporary parity fallback until confidence gates pass.

#### H.2 `schema_from_struct()` utility
**Status:** Fully incomplete  
**Evidence:**
- No `schema_from_struct` implementation file under `src/utils` and no code callsites (`rg schema_from_struct` finds plan-doc mentions only).

**Remaining implementation scope:**
1. Implement `src/utils/schema_from_struct.py`.
2. Add unit tests for known struct/schema pairs.
3. Begin replacing manually-declared serialization schemas where planned.

#### H.3/H.4 Unified schema authority + evidence lattice
**Status:** Fully incomplete  
**Evidence:**
- No implementation found for schema-evidence lattice model or dual-authority governance mechanics in production modules.

**Remaining implementation scope:**
1. Implement schema authority resolution policy (derived vs static) with deterministic conflict handling.
2. Add evidence-lattice artifact(s) and enforcement gates.

---

### Phase I: Semantic Type System Activation

#### I.2 Semantic type routing still uses string heuristics
**Status:** Partially incomplete  
**Evidence:**
- Name/join-key heuristics remain in `src/semantics/catalog/tags.py:188` to `src/semantics/catalog/tags.py:189`.
- File identity fallback logic still uses explicit name checks in `src/semantics/catalog/projections.py:83` to `src/semantics/catalog/projections.py:91`.

**Remaining implementation scope:**
1. Replace residual name-based routing with compatibility-group/type-driven routing.
2. Eliminate remaining hardcoded column-name checks where schema semantics exist.

#### I.3 Threshold centralization adoption is partial
**Status:** Partially incomplete  
**Evidence:**
- Canonical tier module exists in `src/relspec/table_size_tiers.py:1`.
- `classify_table_size` usage appears test-only (`rg classify_table_size` shows defining module + tests only).

**Remaining implementation scope:**
1. Replace local threshold checks in runtime policy code with shared `table_size_tiers` classification.

---

### Phase J: Registry Consolidation

#### J.1 Single view-kind authority still carries compatibility aliases
**Status:** Partially incomplete  
**Evidence:**
- Alias in IR module: `SemanticIRKind = ViewKindStr` at `src/semantics/ir.py:20`.
- Alias in spec registry: `SpecKind = ViewKindStr` at `src/semantics/spec_registry.py:37`.
- Compatibility alias for order map remains in `src/semantics/ir_pipeline.py:34`.

**Remaining implementation scope:**
1. Remove transitional aliases after downstream type migrations are complete.
2. Keep `view_kinds.py` as sole runtime/type authority.

#### J.3 Convention-based extractor discovery is still enumerated
**Status:** Partially incomplete  
**Evidence:**
- Discovery helpers exist in `src/datafusion_engine/extract/templates.py:230` and `src/datafusion_engine/extract/templates.py:443`.
- They still enumerate explicit descriptor tuples (`_AST_TEMPLATE`, `_CST_TEMPLATE`, etc.) rather than discovering module descriptors generically.

**Remaining implementation scope:**
1. Replace hardcoded descriptor tuples with convention-based module introspection/registration.

---

### Section 18: Cross-Scope Decommission Items

#### 18.1 CompileContext decommission
**Status:** Fully incomplete  
**Evidence:**
- `CompileContext` still exists and is used in `src/semantics/compile_context.py:155`.

**Remaining implementation scope:**
1. Remove `CompileContext` as runtime dependency after authority/context migration.

#### 18.2 Static schema registry decommission
**Status:** Fully incomplete  
**Evidence:**
- Static extract schema registries remain active (`src/datafusion_engine/schema/registry.py:1739`, `src/datafusion_engine/schema/registry.py:1751`, `src/datafusion_engine/schema/registry.py:2742`).

**Remaining implementation scope:**
1. Decommission static schema maps after H-phase authority cutover gates.

#### 18.3 Triple view-kind definition decommission
**Status:** Partially incomplete  
**Evidence:**
- Single authority introduced in `src/semantics/view_kinds.py:1`, but compatibility aliases remain (`src/semantics/ir.py:20`, `src/semantics/spec_registry.py:37`, `src/semantics/ir_pipeline.py:34`).

**Remaining implementation scope:**
1. Remove residual aliases and migrate all consumers to the single authority directly.

#### 18.4 Naming-convention cache policy decommission
**Status:** Fully incomplete  
**Evidence:**
- Heuristic remains active fallback in `src/semantics/pipeline.py:292` and `src/semantics/pipeline.py:1306`.

**Remaining implementation scope:**
1. Delete heuristic path after compiled cache policy is fully threaded in production.

#### 18.5 Hardcoded column names decommission
**Status:** Partially incomplete  
**Evidence:**
- Remaining hardcoded file/symbol checks in `src/semantics/catalog/tags.py:188` to `src/semantics/catalog/tags.py:189` and `src/semantics/catalog/projections.py:83` to `src/semantics/catalog/projections.py:91`.

**Remaining implementation scope:**
1. Replace remaining literal checks with schema-type and compatibility-group queries.

#### 18.6 Builder dispatch boilerplate decommission
**Status:** Partially incomplete  
**Evidence:**
- Dispatch helpers exist, but 14-kind explicit map still present in `src/semantics/pipeline.py:1120` to `src/semantics/pipeline.py:1137`.

**Remaining implementation scope:**
1. Complete consolidated-kind dispatch to reduce per-kind boilerplate.

#### 18.7 Template registry statics decommission
**Status:** Partially incomplete  
**Evidence:**
- Template/config discovery helpers still enumerate static descriptors in `src/datafusion_engine/extract/templates.py:236` to `src/datafusion_engine/extract/templates.py:246` and `src/datafusion_engine/extract/templates.py:449` to `src/datafusion_engine/extract/templates.py:461`.

**Remaining implementation scope:**
1. Move to convention/introspection-driven registration for template/config discovery.

---

## Priority Order for Closure
1. **Phase B cutover**: compiled policy threading + cache-policy injection + typed policy validation artifact.
2. **Phase H authority cutover**: derived schema as runtime authority; implement `schema_from_struct`.
3. **Phase G runtime wiring**: workload classification/session profile application, pruning/provenance artifact emission.
4. **Phase E/J consolidation**: complete 14→6 kind cutover and remove compatibility aliases.
5. **Section 18 decommission pass**: remove heuristic/static/legacy surfaces once above gates pass.

