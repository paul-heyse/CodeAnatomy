# Semantic-Compiled Aggressive Cutover Master Design v1 (2026-02-06)

## 1. Objective

Deliver an architecture-first consolidation that removes split control planes and makes runtime behavior a direct function of:

1. extraction dataset metadata/schema (`src/extract`, `src/datafusion_engine/extract`),
2. semantic view/model definitions (`src/semantics`),
3. a single compiled program artifact consumed by bootstrap, planning, scheduling, and execution.

This plan is intentionally aggressive. Internal API churn is acceptable. Pytest completeness is a follow-up concern; architecture cutover and legacy removal are primary.

## 2. Evidence Baseline (Current Repo State)

CQ inventory (latest scan in this repo state):

1. `dataset_location` has **41** callsites across **19** files.
2. `semantic_runtime_from_profile` has **11** callsites across **6** files.
3. `build_semantic_ir` has **8** callsites across **7** files (mostly tests plus one production seam).
4. `ensure_view_graph` has **4** callsites across **3** files.
5. `require_semantic_inputs` has **4** callsites across **4** files.

Additional direct code evidence:

1. `src/extract/helpers.py` still exists as a large legacy module and is still imported by many extractor/runtime modules.
2. `_dataset_location_map(...)` still exists in `src/datafusion_engine/plan/bundle.py`.
3. `_v1` canonical leakage remains in runtime-critical files (for example `src/cpg/view_builders_df.py`, `src/hamilton_pipeline/modules/subdags.py`, `src/hamilton_pipeline/io_contracts.py`, `src/semantics/incremental/delta_updates.py`).
4. `src/datafusion_engine/semantics_runtime.py` still bridges profile<->semantic runtime config, creating a parallel config control plane.

## 3. Target Architecture (End State)

## 3.1 Single semantic-compiled program contract

`SemanticProgramManifest` is the sole control-plane artifact. It carries:

1. semantic IR and requested outputs,
2. canonical input mapping and validation result,
3. resolved dataset bindings,
4. deterministic fingerprint and artifact payload.

All orchestration surfaces consume this manifest directly.

## 3.2 Canonical runtime lifecycle

Runtime initialization becomes:

1. compile manifest,
2. optional strict zero-row materialize/register (same control plane, no side path),
3. unified semantic input validation policy,
4. view graph registration/planning/scheduling/execution.

`run_zero_row_bootstrap_validation(...)` remains public but is only a report-mode wrapper around the same canonical initialization pipeline.

## 3.3 Eliminate split config and split validators

Remove parallel adapters and wrappers:

1. no separate profile->semantic runtime bridge layer,
2. one semantic validation engine (`validate_semantic_inputs` policy-driven),
3. no duplicate location-map assembly helpers.

## 3.4 Declarative extract orchestration

Execution uses metadata-driven adapter contracts, not template switch maps or hardcoded dependency maps.

## 3.5 Canonical naming boundary

Business logic uses canonical names only (`cpg_nodes`, `cpg_edges`, etc.). Compatibility aliases, if retained at all, live only at ingress boundary modules.

## 4. DataFusion + Delta Guardrails (Locked)

Based on `datafusion-and-deltalake-stack` references and local patterns:

1. Provider registration must be `register_table(...)` first; compatibility fallback paths are explicit and artifacted.
2. Avoid `register_table_provider(...)` in new code paths (deprecated API surface).
3. Zero-row materialization remains deterministic via:
   - `write_deltalake(..., mode="overwrite", schema_mode="overwrite")`
4. Keep schema authority in Delta metadata; avoid bespoke schema mutation layers.
5. Keep snapshot/scan pin behavior explicit and fingerprinted in manifest/planning artifacts.
6. Keep fallback path observability mandatory (artifact/event emission).
7. Treat `schema_mode="merge"` as non-default and controlled due known nested/merge edge-case risk in delta-rs.

## 5. Non-Negotiable Cutover Policies

1. No dual control-plane after each wave exit.
2. Caller migration and legacy seam deletion happen in the same wave whenever feasible.
3. Prefer file deletion over compatibility shims once migration is complete.
4. Any unavoidable compatibility layer must be isolated and explicitly time-boxed for deletion.
5. CQ-first operations for every seam change (search, calls, impact/sig-impact, post-delete proof).

## 6. Workstream and Wave Plan

## Wave 0: Governance, freeze, and migration guardrails

### Goal
Prevent new drift while migration proceeds aggressively.

### Deliverables
1. Enforce cutover contract as code and docs:
   - `docs/architecture/semantic_compiled_cutover_contract.md`
   - `scripts/check_semantic_compiled_cutover.py`
2. Wire cutover check into CI (currently script/entry exists, pipeline wiring must be completed).

### Files
1. `docs/architecture/semantic_compiled_cutover_contract.md` (tighten prohibitions + allowed entrypoints)
2. `scripts/check_semantic_compiled_cutover.py` (add stricter checks and explicit failure conditions)
3. CI workflow file(s) under `.github/workflows/` (new/updated) to run `codeanatomy-cutover-check`

### Exit
1. New direct calls to banned seams fail CI.
2. Migration exceptions require explicit allowlist entries.

---

## Wave 1: Compile-control-plane hard cutover

### Goal
All runtime/planning orchestration consumes manifest/compile context, not ad hoc IR rebuilds.

### In-scope
1. Replace direct `CompileContext(...).semantic_ir()` orchestration in runtime/planning seams with manifest-first flow.
2. Keep `build_semantic_ir` low-level but limit production usage to compile-context internals only.
3. Eliminate duplicate wrapper confusion between `semantics.ir_pipeline.compile_semantic_program` and compile-context API surface.

### Files to change
1. `src/datafusion_engine/plan/pipeline.py`
2. `src/datafusion_engine/session/facade.py`
3. `src/hamilton_pipeline/driver_factory.py`
4. `src/hamilton_pipeline/modules/task_execution.py`
5. `src/semantics/ir_pipeline.py`
6. `src/semantics/compile_context.py`

### Decommission actions
1. Remove non-internal production direct uses of `build_semantic_ir`.
2. Consolidate compile entrypoint to one authoritative import surface.

### Exit
1. Planning/runtime orchestration takes manifest/context object as input.
2. No ad hoc IR derivation outside approved compile internals.

---

## Wave 2: Unified semantic input validation everywhere

### Goal
All runtime paths use `validate_semantic_inputs(...manifest...)` policy engine semantics.

### In-scope
1. Replace wrapper-style `require_semantic_inputs(...)` calls in production paths with unified manifest policy validation where appropriate.
2. Keep `require_semantic_inputs` only as compatibility shim or remove when no callers remain.

### Files to change
1. `src/semantics/pipeline.py`
2. `src/datafusion_engine/views/registry_specs.py`
3. `src/datafusion_engine/bootstrap/zero_row.py`
4. `src/semantics/validation/policy.py`

### Decommission actions
1. Remove remaining production callers of `require_semantic_inputs`.
2. If no external callers remain, delete shim and export only `validate_semantic_inputs`.

### Exit
1. One validation policy engine active in all runtime flows.
2. Strict zero-row path is schema-first with no hidden row-probe requirement.

---

## Wave 3: Dataset binding unification and bootstrap convergence

### Goal
Remove duplicated dataset-location assembly and make bootstrap canonical init stage.

### In-scope
1. Replace local location-map helpers with manifest dataset bindings.
2. Remove `_dataset_location_map(...)` from plan internals.
3. Move runtime startup to compile->bootstrap(optional)->validate canonical sequence.

### Files to change
1. `src/datafusion_engine/plan/bundle.py`
2. `src/datafusion_engine/plan/pipeline.py`
3. `src/datafusion_engine/session/runtime.py`
4. `src/datafusion_engine/bootstrap/zero_row.py`
5. `src/relspec/execution_plan.py`

### Decommission actions
1. Delete `_dataset_location_map(...)` and callsites in `src/datafusion_engine/plan/bundle.py`.
2. Remove bootstrap-local fallback location derivation not backed by manifest/catalog bindings.

### Exit
1. Bootstrap/plan/runtime resolve locations from one source of truth.
2. `run_zero_row_bootstrap_validation` is a report-mode invocation of canonical init logic.

---

## Wave 4: Delete profile<->semantic runtime bridge control plane

### Goal
Remove `SemanticRuntimeConfig` bridging layer as orchestration authority.

### In-scope
1. Replace `semantic_runtime_from_profile` and `apply_semantic_runtime_config` usage with manifest-driven config resolution.
2. Move any still-needed typed settings into manifest/runtime policy structs directly.

### Files to change
1. `src/datafusion_engine/views/registry_specs.py`
2. `src/engine/session_factory.py`
3. `src/hamilton_pipeline/driver_factory.py`
4. `src/hamilton_pipeline/modules/inputs.py`
5. `src/semantics/__init__.py` (docs/examples)

### Decommission/delete candidates
1. **Delete file**: `src/datafusion_engine/semantics_runtime.py`
2. **Decommission then delete**: `src/semantics/runtime.py` (if no remaining authoritative role)

### Exit
1. No production dependency on semantic runtime bridge adapters.
2. Manifest is the only control-plane config contract.

---

## Wave 5: Extract control-plane consolidation and helper purge

### Goal
Eliminate legacy extract helper monolith and finalize metadata-driven adapter orchestration.

### In-scope
1. Move remaining shared types/functions from `src/extract/helpers.py` into focused coordination modules.
2. Remove legacy import dependency from `src/extract/coordination/materialization.py` back to `extract.helpers`.
3. Remove hardcoded handler maps where adapter registry contract can dispatch cleanly.

### Files to change
1. `src/extract/coordination/materialization.py`
2. `src/extract/coordination/context.py`
3. `src/extract/coordination/evidence_plan.py`
4. `src/extract/coordination/schema_ops.py`
5. `src/extract/coordination/spec_helpers.py`
6. `src/extract/extractors/ast_extract.py`
7. `src/extract/extractors/bytecode_extract.py`
8. `src/extract/extractors/cst_extract.py`
9. `src/extract/extractors/external_scope.py`
10. `src/extract/extractors/file_index/line_index.py`
11. `src/extract/extractors/imports_extract.py`
12. `src/extract/extractors/symtable_extract.py`
13. `src/extract/extractors/tree_sitter/extract.py`
14. `src/extract/git/blobs.py`
15. `src/extract/scanning/repo_scan.py`
16. `src/hamilton_pipeline/io_contracts.py`
17. `src/hamilton_pipeline/modules/task_execution.py`
18. `src/relspec/extract_plan.py`

### Decommission/delete candidates
1. **Delete file**: `src/extract/helpers.py` (after import migration complete)
2. **Delete symbol(s)**: `_EXTRACT_TEMPLATE_HANDLERS`, `_extract_handler_for_adapter` in `src/hamilton_pipeline/modules/task_execution.py` (replace with adapter-driven execution registry)
3. **Delete obsolete adapter helper exports** in `src/datafusion_engine/extract/adapter_registry.py` if unused after migration (`required_inputs_for_template`, `supports_plan_for_template`, etc.)

### Exit
1. No production imports from `extract.helpers`.
2. No template-switch map in task execution.
3. Extract task dependencies derive from metadata/spec contracts only.

---

## Wave 6: Canonical naming enforcement and alias purge

### Goal
No `_v1` runtime branching in core business logic; compatibility boundary only.

### In-scope
1. Convert internal runtime/graph builders to canonical dataset names.
2. Keep optional ingress alias mapping in one boundary module only (or delete if no compatibility needs remain).

### Files to change
1. `src/cpg/view_builders_df.py`
2. `src/hamilton_pipeline/modules/subdags.py`
3. `src/hamilton_pipeline/io_contracts.py`
4. `src/semantics/incremental/delta_updates.py`
5. `src/semantics/catalog/spec_builder.py`
6. `src/semantics/catalog/__init__.py`
7. `src/semantics/__init__.py`
8. `src/semantics/naming_compat.py`

### Decommission actions
1. Remove `_v1` name assumptions from core runtime paths.
2. Delete compatibility translations if no external callers require legacy aliases.

### Exit
1. Core runtime/planning/validation paths run canonical names only.
2. `_v1` remains only in explicit compatibility ingress docs/tests (or fully removed).

---

## Wave 7: DataFusion runtime simplification and obsolete central utilities cleanup

### Goal
Remove now-obsolete centralized utilities in `src/datafusion_engine` that were compensating for split control planes.

### In-scope
1. Collapse duplicate dataset registration/readiness helpers into manifest-driven registration service.
2. Remove redundant CDF/location lookup patterns duplicated across modules.
3. Standardize table provider registration surface and fallback artifacting.

### Files to change (expected)
1. `src/datafusion_engine/dataset/registration.py`
2. `src/datafusion_engine/dataset/resolution.py`
3. `src/datafusion_engine/delta/cdf.py`
4. `src/datafusion_engine/io/write.py`
5. `src/datafusion_engine/views/graph.py`
6. `src/engine/materialize_pipeline.py`
7. `src/extract/coordination/materialization.py`
8. `src/semantics/incremental/cdf_runtime.py`
9. `src/semantics/incremental/delta_context.py`
10. `src/semantics/incremental/plan_bundle_exec.py`
11. `src/semantics/incremental/snapshot.py`

### Decommission actions
1. Remove ad hoc dataset-location discovery fallbacks where manifest bindings are available.
2. Keep only one compatibility fallback path (provider fallback) with explicit artifact output.

### Exit
1. DataFusion centralized utility layer is smaller and manifest-aligned.
2. Location/registration behavior is deterministic and traceable.

---

## Wave 8: Final purge, dead code deletion, and boundary hardening

### Goal
Delete all previously decommissioned symbols/files and enforce future-proof architecture constraints.

### In-scope
1. Delete shims, wrappers, and compatibility code marked in earlier waves.
2. Tighten cutover check script to block reintroduction.
3. Add architectural lint checks for banned seams.

### Exit
1. Legacy ledger is empty.
2. CI blocks regressions into split-control-plane patterns.

## 7. Authoritative Decommission and Deletion Ledger

This ledger is the canonical deletion target set for this migration.

## 7.1 File-level deletions (target state)

1. `src/datafusion_engine/semantics_runtime.py`
   - Replacement: manifest-driven compile/runtime contract.
   - Wave: 4.
2. `src/extract/helpers.py`
   - Replacement: `src/extract/coordination/*` modules.
   - Wave: 5.
3. `src/semantics/runtime.py` (conditional delete)
   - Replacement: manifest policy/config payloads.
   - Wave: 4 or 8 depending on residual use.

## 7.2 Function/symbol deletions

1. `src/datafusion_engine/plan/bundle.py:_dataset_location_map`
   - Replacement: manifest dataset bindings service.
   - Wave: 3.
2. `src/hamilton_pipeline/modules/task_execution.py:_EXTRACT_TEMPLATE_HANDLERS`
   - Replacement: adapter-driven execution registry.
   - Wave: 5.
3. `src/hamilton_pipeline/modules/task_execution.py:_extract_handler_for_adapter`
   - Replacement: adapter-driven execution registry.
   - Wave: 5.
4. `src/semantics/validation/policy.py:require_semantic_inputs` (conditional shim removal)
   - Replacement: direct `validate_semantic_inputs`.
   - Wave: 2 or 8.
5. Any direct `_v1` conditional branches in runtime business logic:
   - Replacement: canonical names + optional ingress-only compatibility layer.
   - Wave: 6.

## 7.3 Modules to decommission (non-delete, major simplification expected)

1. `src/datafusion_engine/session/facade.py` (remove compile-time fallbacks and legacy indirection)
2. `src/datafusion_engine/plan/pipeline.py` (manifest-first planning surface)
3. `src/relspec/execution_plan.py` (consume manifest/context contracts, avoid recompute)
4. `src/datafusion_engine/views/registry_specs.py` (drop semantic runtime bridge + wrapper validation patterns)
5. `src/semantics/pipeline.py` (remove residual local location and wrapper-based validation flow)
6. `src/hamilton_pipeline/driver_factory.py` (stop ad hoc compile/runtime bridge logic)
7. `src/hamilton_pipeline/io_contracts.py` (canonical naming materialization specs)
8. `src/cpg/view_builders_df.py` (canonical naming and compatibility extraction)

## 8. Additional Follow-up Scope Beyond Original Plan

These are high-value expansions to fully realize the semantic-compiled architecture.

1. Manifest-native planning artifacts:
   - include logical/optimized/physical plan fingerprints and settings snapshot in one artifact bundle.
2. Runtime context standardization:
   - one typed initialization struct flowing through DataFusion/hamilton/relspec boundaries.
3. Incremental pipeline unification:
   - remove direct `dataset_location` calls in `src/semantics/incremental/*` and consume manifest bindings.
4. Provider diagnostics unification:
   - single artifact schema for registration path chosen (`register_table` vs fallback), pushdown capability, and pinned snapshot identity.
5. Contract-level migration linting:
   - static checks for banned symbols (`build_semantic_ir` direct calls, `_v1` in runtime logic, `extract.helpers` imports, `_dataset_location_map`-style helpers).

## 9. CQ Operating Protocol for This Migration

Required for every work item:

1. `./cq search <primary_symbol>`
2. `./cq calls <function>`
3. If signature/params change:
   - `./cq impact <function> --param <param>`
   - `./cq sig-impact <function> --to "<new signature>"`
4. After cutover/delete:
   - rerun `./cq calls <function>` and
   - `./cq search <deleted_symbol> --in <path>` (expect no findings).
5. Prefer `./cq run --steps` for related scans to reuse parser work.

Recommended migration verification command packs:

1. Control-plane seams:
   - `./cq run --steps '[{"type":"calls","function":"build_semantic_ir"},{"type":"calls","function":"compile_semantic_program"},{"type":"calls","function":"ensure_view_graph"},{"type":"calls","function":"require_semantic_inputs"}]'`
2. Location and bridge seams:
   - `./cq run --steps '[{"type":"calls","function":"dataset_location"},{"type":"calls","function":"semantic_runtime_from_profile"}]'`
3. Extract legacy seams:
   - `./cq run --steps '[{"type":"calls","function":"materialize_extract_plan"},{"type":"search","query":"from extract.helpers import","in":"src"},{"type":"search","query":"_EXTRACT_TEMPLATE_HANDLERS","in":"src/hamilton_pipeline/modules"}]'`

## 10. Parallel Execution Model

To move aggressively with multiple contributors:

1. Track A (Compile/manifest): Waves 1-3 core control plane.
2. Track B (Extract decommission): Wave 5 imports/symbol deletion.
3. Track C (Naming purge): Wave 6 `_v1` elimination.
4. Track D (DataFusion utility cleanup): Wave 7 centralization cleanup.
5. Track E (Governance/CI): Waves 0 and 8 checks/guards.

Merge order:

1. A first, then B/C in parallel, then D, then E hardening.

## 11. Acceptance Criteria (Program-level)

1. One manifest fingerprint can explain bootstrap, view registration, plan graph, and scheduling outcomes.
2. No production imports of deprecated bridge/helper modules (`datafusion_engine.semantics_runtime`, `extract.helpers`).
3. No direct runtime business-logic dependence on `_v1` names.
4. No duplicate location-map assembly helpers remain.
5. Zero-row strict mode and normal runtime mode share the same control-plane pipeline.
6. CI guardrails prevent reintroduction of split-control-plane seams.

## 12. Testing Strategy (Secondary Priority)

Given architecture-first mandate:

1. Per-wave smoke tests only for touched seams.
2. Full-suite stabilization is deferred to a dedicated follow-up plan.
3. Every deleted seam gets at least one regression assertion proving replacement path coverage.

## 13. Immediate Next Actions

1. Complete Wave 0 CI wiring for cutover check.
2. Start Wave 1 by removing non-internal compile recomputation paths.
3. Start Wave 5 import migration in parallel (extract helpers) to unblock hard deletion.
4. Start Wave 6 canonical naming migration plan using file set already identified in this document.
