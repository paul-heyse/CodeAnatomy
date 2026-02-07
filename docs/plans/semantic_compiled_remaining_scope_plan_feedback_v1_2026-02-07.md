# Semantic-Compiled Remaining Scope Plan: Corrections, Optimizations, and Enhancements (v1, 2026-02-07)

## 1. Purpose

This document reviews `docs/plans/semantic_compiled_remaining_scope_implementation_plan_v1_2026-02-07.md` and proposes:

1. Corrections (factual/path/API/test accuracy)
2. Optimizations (lower-risk, higher-throughput execution strategy)
3. Expansions and enhancements (additional scope that materially improves the final cutover quality)

The intent is to give the author a merge-ready improvement layer without changing the architecture target.

---

## 2. Executive Summary

The plan is directionally strong and aligned to the intended architecture. The biggest changes recommended are:

1. Fix several concrete inaccuracies (path, signatures, test names, and API assumptions).
2. Replace “large signature threading” with one shared execution context object to avoid churn.
3. Narrow and phase the Wave F5 runtime-type deletion to avoid destabilizing unrelated flows.
4. Adjust Wave F3/F4 proposals to current module boundaries and avoid introducing circular dependencies.
5. Harden governance checks with path-aware/AST-aware enforcement instead of broad string matching.

---

## 3. Corrections (High Priority)

## 3.1 File/Path Accuracy

1. `src/datafusion_engine/bootstrap/runtime.py` is referenced in the plan but does not exist.
2. Correct path is `src/datafusion_engine/session/runtime.py` (`run_zero_row_bootstrap_validation` callsite).

Recommendation:
1. Replace all `bootstrap/runtime.py` references with `session/runtime.py`.

## 3.2 Governance Rule Count

Current checker (`scripts/check_semantic_compiled_cutover.py`) has 12 checks, not 13.

Recommendation:
1. Update baseline section to “12 checks”.
2. Include explicit “pending checker enhancements” list instead of stating already-expanded rule count.

## 3.3 `build_cpg` Caller Note

Plan says to update a bootstrap caller for `build_cpg` return handling; bootstrap path does not call `build_cpg`.

Recommendation:
1. Remove that migration step.
2. Limit `build_cpg` return-type updates to real callsites only (`build_cpg_from_inferred_deps`, tests, and any direct consumers in semantics/engine layers).

## 3.4 F3 Listing Registration Signature Mismatch

The plan proposes calling `register_listing_table(ctx, name, location, cache_policy)` from `tables/registration.py`, but current API is context-protocol based (`register_listing_table(context: ListingRegistrationContext)`).

Recommendation:
1. Do not rewrite callsites to a non-existent positional signature.
2. Either:
   - keep dataset-side orchestrator as caller and delegate internally to tables registration with an adapter context object, or
   - refactor `tables/registration.py` first to expose a stable request dataclass API, then migrate callsites.

## 3.5 F4 Extract Executor Proposal Imports Are Incorrect

Plan examples use paths like `extract.extractors.ast.extract` and adapter names like `ast_extract`; current structure uses modules such as `extract.extractors.ast_extract` and template keys like `"ast"`, `"cst"`, etc.

Recommendation:
1. Align to current template keys and module paths.
2. Avoid copying pseudo-import examples that do not match repository layout.

## 3.6 F4 Layering Risk (Potential Cycle)

Moving executor mapping into `src/datafusion_engine/extract/adapter_registry.py` risks coupling `datafusion_engine` to `hamilton_pipeline` execution concerns and can create import cycles.

Recommendation:
1. Keep metadata registry in `datafusion_engine/extract`.
2. Move execution registry to a new orchestration-side module, e.g.:
   - `src/hamilton_pipeline/modules/extract_execution_registry.py`
3. Use adapter template names from `adapter_registry` as the canonical key source.

## 3.7 F5 Runtime Field Assumptions

Plan maps `runtime_config.storage_options` to `runtime_profile.storage_options`, but `DataFusionRuntimeProfile` currently does not expose a direct `storage_options` field in that shape.

Recommendation:
1. Add explicit mapping table to existing profile paths (likely under `data_sources` and policy bundles).
2. Do not introduce new profile top-level fields unless necessary.

## 3.8 Non-Existent Test Targets in Validation Protocol

Several test commands in the plan refer to files that are not present:

1. `tests/integration/datafusion_engine/test_registry_facade.py` (missing)
2. `tests/integration/datafusion_engine/test_session_runtime.py` (missing)
3. `tests/integration/semantics/test_incremental_pipeline.py` (missing)
4. `tests/integration/semantics/test_cdf_processing.py` (missing)

Use existing tests instead:

1. `tests/integration/test_semantic_pipeline.py`
2. `tests/integration/relspec/test_compile_execution_plan.py`
3. `tests/integration/test_zero_row_bootstrap_e2e.py`
4. `tests/integration/extraction/test_materialize_extract_plan.py`
5. `tests/integration/test_incremental_partitioned_updates.py`
6. `tests/integration/test_semantic_incremental_overwrite.py`
7. `tests/integration/storage/test_cdf_cursor_lifecycle.py`
8. `tests/unit/test_registry_facade_rollback.py`

## 3.9 Checker Allowlist Staleness

`scripts/check_semantic_compiled_cutover.py` still contains `src/semantics/naming_compat.py` in allowlist despite file deletion.

Recommendation:
1. Remove stale allowlist entry as part of governance cleanup.

---

## 4. Optimizations (Execution Strategy)

## 4.1 Replace Signature Explosion With a Shared Context Object

Current plan introduces many new function parameters (`dataset_resolver`, `registry_facade`, etc.) across many seams.

Recommendation:
1. Introduce `SemanticExecutionContext` (single object) carrying:
   - `manifest`
   - `dataset_resolver`
   - `registry_facade`
   - `runtime_profile`
   - `ctx` (SessionContext)
2. Thread this object instead of adding N new parameters to each function.

Benefits:
1. Smaller diffs
2. Easier migration rollback
3. Less callsite churn
4. Cleaner API stability

## 4.2 Split F5 Into Two Stages

Deleting `src/semantics/runtime.py` in one wave is high-risk due to broad usage.

Recommendation:
1. F5a: “Bridge neutralization”
   - stop new usages
   - migrate all core callsites off bridge types
   - keep file as thin compatibility shim
2. F5b: “Hard delete”
   - after callsites are zero in `src`
   - remove module and exports

## 4.3 Wave Ordering Adjustment

Proposed order `F1 -> F2 -> {F3,F4,F6} -> F5` is mostly good, but F6 can be sensitive to F5.

Recommended order:
1. F1 compile authority
2. F2 dataset-binding authority
3. F3 registration consolidation
4. F4 extract execution contract
5. F6 incremental convergence
6. F5 runtime-bridge removal (after all consumers moved)

This avoids deleting runtime bridge types before incremental path is truly migrated.

## 4.4 CQ Validation Granularity

Use stable acceptance gates per wave:

1. Track callsite counts before/after with one command bundle per wave.
2. Require “no increase” on unrelated legacy seams in every wave.
3. Capture artifacts path from CQ output into the migration log for reproducibility.

---

## 5. Enhancements / Scope Expansions

## 5.1 Add Explicit “Migration ABI” for Manifest

Add a manifest schema version and compatibility policy now (even if v1 only).

Recommendation:
1. Add `manifest_version` to `SemanticProgramManifest`.
2. Add deterministic serialization tests for backward-compatible evolution.
3. Add one “manifest roundtrip” golden.

## 5.2 Add Resolver Semantics Beyond `location()`

Add explicit APIs to reduce repeated defensive code:

1. `require_location(name) -> DatasetLocation` (raises structured error)
2. `subset(names) -> ManifestDatasetBindings`
3. `to_payload()` canonical deterministic ordering

## 5.3 Introduce Registration Telemetry Contract

To make F3 measurable:

1. Add unified registration artifact with:
   - caller module
   - registration mode
   - fallback_used
   - provider kind
2. Add a simple “fallback budget” metric per run.

## 5.4 Expand Decommission Ledger to “Done/Blocked/Owner”

Current ledger tracks targets but not operational ownership state.

Recommendation:
1. Add columns:
   - status (`done`, `in_progress`, `blocked`)
   - owner
   - blocking dependency
   - proof command

## 5.5 Guardrail Enhancements in Checker

String checks are useful but can be noisy.

Recommendation:
1. Keep string checks for speed.
2. Add path-scoped rule logic for high-risk checks (`dataset_catalog_from_profile` in orchestration).
3. Add optional AST-backed validation for method/function definitions where exactness matters.

## 5.6 Add “No New Direct Catalog Lookup” Policy

Beyond total count reductions, explicitly block new direct lookups in target modules:

1. `src/semantics/pipeline.py`
2. `src/hamilton_pipeline/modules/task_execution.py`
3. `src/engine/materialize_pipeline.py`
4. `src/semantics/incremental/*`

---

## 6. Revised Wave-Specific Adjustments

## 6.1 F1 Adjustments

1. Keep `CpgBuildArtifacts`, but do not force a broad public API break if avoidable.
2. Prefer internal helper:
   - `_build_cpg_artifacts(...) -> CpgBuildArtifacts`
3. Let `build_cpg(...)` preserve external behavior while `build_cpg_from_inferred_deps(...)` consumes shared artifacts.

## 6.2 F2 Adjustments

1. Keep `ManifestDatasetBindings` as concrete authority.
2. Treat protocol introduction as optional unless required for decoupling tests.
3. Replace fixed “4 callsite target” with “compile-boundary module allowlist target” to avoid brittle counting.

## 6.3 F3 Adjustments

1. Do not delete `_register_listing_table` until functionality parity is proven.
2. First refactor it into a delegating wrapper around a single authoritative implementation.
3. Then remove wrapper in follow-up deletion step.

## 6.4 F4 Adjustments

1. Keep executor registry in orchestration layer (Hamilton), not `datafusion_engine`.
2. Keep adapter metadata in `datafusion_engine/extract/adapter_registry.py`.
3. Add one canonical “template -> executor key” bridge function to prevent drift.

## 6.5 F5 Adjustments

1. Convert to staged deprecation and then hard delete.
2. Add explicit migration table per `SemanticRuntimeConfig` field with real profile path.
3. Block deletion until `./cq search "SemanticRuntimeConfig" --in src` is zero.

## 6.6 F6 Adjustments

1. Converge incremental modules on the same execution context object from F2/F3.
2. Ensure incremental registration calls pass through the same facade contract used in non-incremental path.
3. Add one integration test that compares non-incremental vs incremental binding resolution for the same dataset set.

---

## 7. Recommended Replacement Validation Commands

Use commands aligned to existing files:

```bash
# Wave smoke (existing tests)
uv run pytest tests/integration/test_semantic_pipeline.py -q
uv run pytest tests/integration/relspec/test_compile_execution_plan.py -q
uv run pytest tests/integration/test_zero_row_bootstrap_e2e.py -q
uv run pytest tests/integration/extraction/test_materialize_extract_plan.py -q
uv run pytest tests/integration/test_incremental_partitioned_updates.py -q
uv run pytest tests/integration/test_semantic_incremental_overwrite.py -q
uv run pytest tests/integration/storage/test_cdf_cursor_lifecycle.py -q
uv run pytest tests/unit/test_registry_facade_rollback.py -q

# Governance
uv run scripts/check_semantic_compiled_cutover.py --strict

# CQ inventories
./cq calls compile_semantic_program
./cq calls dataset_catalog_from_profile
./cq calls register_dataset_df
./cq search SemanticRuntimeConfig --in src
./cq search _register_listing_table --in src
```

---

## 8. Suggested Plan Delta (What to Change in Author Plan)

1. Fix all incorrect paths and test command references.
2. Replace F3 signature examples with actual current APIs.
3. Move F4 executor centralization to orchestration layer to preserve module boundaries.
4. Stage F5 deletion into compatibility-neutralization then hard delete.
5. Replace brittle numeric callsite goals with allowlist/path-based governance goals.
6. Add execution context object strategy to reduce signature churn and migration risk.
7. Add explicit parity checks before deleting duplicate registration helpers.

---

## 9. Final Recommendation

Proceed with the architecture as planned, but integrate the corrections in Section 3 and optimizations in Sections 4-6 before implementation resumes. This keeps the aggressive cutover posture while materially reducing break risk and rework.
