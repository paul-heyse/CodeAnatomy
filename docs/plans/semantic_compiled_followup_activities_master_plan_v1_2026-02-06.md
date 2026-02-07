# Semantic-Compiled Architecture Follow-Up Master Plan v1 (2026-02-06)

## 1. Purpose

Define the next comprehensive follow-up scope after the initial aggressive cutover so the system becomes fully semantic-compiled and runtime behavior is entirely driven by:

1. extract dataset specs/schemas,
2. semantic view specifications,
3. one compiled manifest and one initialization pipeline consumed by planning, scheduling, execution, and bootstrap.

This plan is architecture-first and deletion-oriented. Broad pytest stabilization remains a follow-up stream.

## 2. Current-State Evidence (Post-Cutover Snapshot)

Evidence source: CQ scans in this repo state.

### 2.1 What is already decommissioned

1. `require_semantic_inputs` callsites: **0**.
2. `semantic_runtime_from_profile` callsites: **0**.
3. `apply_semantic_runtime_config` callsites: **0**.
4. `_dataset_location_map`: no matches.
5. `_extract_outputs_for_template`: no matches.
6. `_REQUIRED_INPUTS`, `_SUPPORTS_PLAN`, `_EXTRACTOR_EXTRA_INPUTS`: no matches.
7. `cpg_nodes_v1` and `cpg_edges_v1`: no matches in `src`.
8. Deleted files already visible in tree:
   - `src/datafusion_engine/semantics_runtime.py`
   - `src/extract/helpers.py`
   - `src/hamilton_pipeline/modules/subdags.py`

### 2.2 Remaining consolidation hotspots

1. `dataset_location_from_catalog`: **31 callsites / 15 files**.
2. `dataset_catalog_from_profile`: **18 callsites / 17 files**.
3. `materialize_extract_plan`: **25 callsites / 13 files**.
4. `register_dataset`: **15 callsites / 12 files**.
5. `register_dataset_df`: **31 callsites / 21 files**.
6. `register_table_provider`: **5 callsites / 4 files**.
7. `ensure_view_graph`: **4 callsites / 3 files**.
8. `build_semantic_ir`: **8 callsites / 7 files** (production path now mainly `src/semantics/compile_context.py`, remaining mostly tests).
9. `compile_semantic_program`: **1 production callsite** (`src/datafusion_engine/session/runtime.py`), indicating adoption is still partial.

### 2.3 Key architectural gaps still present

1. `src/semantics/pipeline.py` still constructs `SemanticProgramManifest` manually inside `build_cpg(...)` and performs local validation; this duplicates compile-context authority.
2. `src/datafusion_engine/views/registry_specs.py` also constructs a local validation manifest (`_validated_semantic_inputs(...)`), creating another policy/control-plane branch.
3. `src/datafusion_engine/bootstrap/zero_row.py` has fallback manifest synthesis in `_validation_errors(...)` when no manifest is provided.
4. `src/hamilton_pipeline/modules/task_execution.py` still dispatches extract handlers via `globals()` naming convention (`_run_extract_adapter(...)`) instead of a fully declarative execution adapter contract.
5. Dataset resolution is still spread across many modules via direct `dataset_location_from_catalog(...)` calls.
6. Registration still carries mixed provider APIs (`register_dataset_df`, `register_table_provider`, wrapper methods in `DataFusionIOAdapter`).

## 3. End-State Contract (Follow-Up Target)

1. **Single semantic compile authority**:
   - one compile path produces one `SemanticProgramManifest`.
2. **Single initialization path**:
   - compile manifest -> optional zero-row bootstrap materialization -> unified semantic validation -> view registration -> planning.
3. **Single dataset-binding authority**:
   - manifest bindings passed downstream; no ad hoc catalog/location lookups in downstream orchestration.
4. **Declarative extract execution authority**:
   - adapter execution registry (metadata-driven), no globals-based dispatch.
5. **Provider-first registration**:
   - DataFusion `register_table(...)` based surface as primary path; compatibility fallbacks explicitly isolated and observable.
6. **Compatibility boundary only at ingress**:
   - alias handling (if any) isolated to `src/semantics/naming_compat.py`, with staged removal.

## 4. Aggressive Follow-Up Workstreams

## Wave F1 — Compile/Validation Authority Collapse

### Objective
Remove remaining manifest/validation duplication and make compile-context output the only control-plane artifact.

### Changes
1. Refactor `src/semantics/pipeline.py` `build_cpg(...)` to consume `compile_semantic_program(...)` result directly.
2. Remove manual `SemanticProgramManifest(...)` construction in `build_cpg(...)`.
3. Refactor `src/datafusion_engine/views/registry_specs.py` `_validated_semantic_inputs(...)` to accept a manifest (or compile context) instead of building a local manifest.
4. Refactor `src/datafusion_engine/bootstrap/zero_row.py` to require manifest input for strict mode; remove fallback manifest synthesis path in `_validation_errors(...)`.
5. Keep `build_semantic_ir(...)` as a low-level IR primitive only for compile internals/tests.

### File targets
1. `src/semantics/pipeline.py`
2. `src/semantics/compile_context.py`
3. `src/datafusion_engine/views/registry_specs.py`
4. `src/datafusion_engine/bootstrap/zero_row.py`
5. `src/semantics/ir_pipeline.py` (remove or de-emphasize duplicate `compile_semantic_program` wrapper export)

### Decommission/deletion outcomes
1. Delete local manifest-construction blocks in `build_cpg(...)` and `_validated_semantic_inputs(...)`.
2. Delete fallback manifest synthesis branch in bootstrap validation.
3. If no external dependency remains, delete `compile_semantic_program(...)` shim in `src/semantics/ir_pipeline.py` and retain compile entrypoint only in `src/semantics/compile_context.py`.

### Exit criteria
1. All runtime semantic validation uses a manifest produced by compile-context.
2. No production code path constructs ad hoc manifests.

---

## Wave F2 — Dataset Binding Authority Cutover

### Objective
Eliminate distributed dataset location lookups in orchestration and planning code.

### Changes
1. Introduce a manifest-bound resolver contract (read-only mapping or service) passed through runtime/planning/execution seams.
2. Replace direct `dataset_location_from_catalog(...)` calls in orchestration layers with manifest-bound access.
3. Restrict direct `dataset_catalog_from_profile(...)` usage to compile/binding construction boundaries.

### High-priority files (current direct usage)
1. `src/datafusion_engine/views/registry_specs.py`
2. `src/extract/infrastructure/worklists.py`
3. `src/hamilton_pipeline/modules/task_execution.py`
4. `src/semantics/incremental/delta_updates.py`
5. `src/semantics/incremental/snapshot.py`
6. `src/datafusion_engine/session/runtime.py`
7. `src/engine/materialize_pipeline.py`
8. `src/semantics/pipeline.py`

### Decommission/deletion outcomes
1. Delete downstream `dataset_location_from_catalog(...)` usage from orchestration modules.
2. Keep `dataset_location_from_catalog(...)` only as compatibility shim at resolver boundary; delete once no callsites remain.

### Exit criteria
1. `dataset_location_from_catalog(...)` callsites drop to compile boundary only.
2. Planner/bootstrap/runtime all read dataset bindings from the same manifest contract.

---

## Wave F3 — Registration API Consolidation (Provider-First)

### Objective
Collapse registration surface area and remove deprecated provider wrappers.

### Changes
1. Standardize on one registration facade method for pipeline/runtime code paths.
2. Minimize direct `register_dataset_df(...)` usage outside the central registry facade.
3. Replace `register_table_provider(...)` wrappers with `register_table(...)` surface where feasible.
4. Keep fallback paths explicit and artifacted.

### High-priority files
1. `src/datafusion_engine/io/adapter.py`
2. `src/datafusion_engine/dataset/registration.py`
3. `src/datafusion_engine/tables/registration.py`
4. `src/datafusion_engine/bootstrap/zero_row.py`
5. `src/datafusion_engine/cache/inventory.py`
6. `src/datafusion_engine/delta/observability.py`

### Decommission/deletion outcomes
1. Delete or deprecate `DataFusionIOAdapter.register_table_provider(...)` once no non-compat production calls remain.
2. Delete `register_delta_table_provider(...)` / `register_delta_cdf_provider(...)` wrappers if they become no-op aliases over unified register path.
3. Merge listing registration duplication between `dataset/registration.py` and `tables/registration.py` (retain one authority module).

### Exit criteria
1. Provider-first registration is explicit and singular.
2. Compatibility fallbacks are isolated, counted, and artifacted.

---

## Wave F4 — Extract Execution Contract Finalization

### Objective
Replace remaining implicit dispatch with explicit metadata-driven adapter execution contracts.

### Changes
1. Replace `_run_extract_adapter(...)` globals-based dispatch with explicit adapter execution registry mapping adapter -> callable.
2. Move adapter execution contract definition next to adapter metadata (single source).
3. Keep extractor modules focused on extraction logic only; orchestration must not encode naming conventions.
4. Reduce `materialize_extract_plan(...)` call fan-out by introducing narrower high-level entrypoint(s) for pipeline orchestration.

### High-priority files
1. `src/hamilton_pipeline/modules/task_execution.py`
2. `src/datafusion_engine/extract/adapter_registry.py`
3. `src/datafusion_engine/extract/extractors.py`
4. `src/relspec/extract_plan.py`
5. `src/extract/coordination/materialization.py`

### Decommission/deletion outcomes
1. Delete globals-based handler discovery in task execution.
2. Delete redundant helper exports in adapter registry after callsite migration:
   - `required_inputs_for_template(...)`
   - `supports_plan_for_template(...)`
   - `additional_required_inputs_for_template(...)`
   (or keep as strict aliases only if an external API contract requires them).

### Exit criteria
1. Extract orchestration is fully declarative and adapter-contract driven.
2. No dynamic handler-name resolution via globals remains.

---

## Wave F5 — Semantic Runtime Type Surface Cleanup

### Objective
Remove now-redundant semantic runtime configuration surfaces and converge on manifest/policy contracts.

### Changes
1. Identify remaining `SemanticRuntimeConfig`/`CachePolicy` dependency surfaces.
2. Move required runtime options into manifest/runtime policy structures where they are actually consumed.
3. Remove compatibility-only runtime structs once migration is complete.

### Candidate files
1. `src/semantics/runtime.py`
2. `src/semantics/pipeline.py`
3. `src/engine/session_factory.py`
4. `src/engine/materialize_pipeline.py`
5. `src/hamilton_pipeline/modules/inputs.py`

### Decommission/deletion outcomes
1. Delete `src/semantics/runtime.py` once its types are no longer authoritative.
2. Remove import/type dependency on `SemanticRuntimeConfig` from orchestration surfaces.

### Exit criteria
1. Manifest + explicit runtime policy structs fully replace semantic runtime bridge types.

---

## Wave F6 — Incremental/CDF Path Convergence

### Objective
Align incremental pipeline with manifest-first control plane and remove remaining duplicated location/registration logic.

### Changes
1. Route incremental modules through manifest dataset bindings where available.
2. Eliminate repeated catalog lookups across incremental modules.
3. Standardize incremental write/read helpers on the same registration and binding contracts as full runtime.

### High-priority files
1. `src/semantics/incremental/cdf_runtime.py`
2. `src/semantics/incremental/delta_context.py`
3. `src/semantics/incremental/delta_updates.py`
4. `src/semantics/incremental/plan_bundle_exec.py`
5. `src/semantics/incremental/snapshot.py`
6. `src/datafusion_engine/delta/cdf.py`

### Exit criteria
1. Incremental and non-incremental paths share dataset binding and registration authority.
2. No incremental-only control-plane duplication remains.

---

## Wave F7 — Compatibility Boundary Reduction to Zero (Optional Hard Cut)

### Objective
Complete canonical-name-only architecture with no runtime aliasing if backward compatibility is not required.

### Changes
1. Keep alias mapping ingress-only during transition.
2. Remove aliasing once external callers are updated.

### Candidate deletion
1. `src/semantics/naming_compat.py` (delete when all ingress aliases are retired).

### Exit criteria
1. Canonical names are the only supported API/runtime names.

## 5. Authoritative Decommission + Deletion Ledger

## 5.1 Completed in current codebase

1. `src/datafusion_engine/semantics_runtime.py` deleted.
2. `src/extract/helpers.py` deleted.
3. `src/hamilton_pipeline/modules/subdags.py` deleted.
4. Legacy extractor hardcoded maps removed.
5. Legacy `_v1` CPG runtime references removed from `src` business logic.

## 5.2 Next deletion candidates (aggressive)

1. `src/semantics/ir_pipeline.py::compile_semantic_program` shim.
2. `src/semantics/runtime.py` (post-type migration).
3. `src/datafusion_engine/io/adapter.py::register_table_provider` wrappers (post-registration consolidation).
4. One of:
   - listing registration logic in `src/datafusion_engine/tables/registration.py`, or
   - duplicate listing registration paths in `src/datafusion_engine/dataset/registration.py`.
5. `src/hamilton_pipeline/modules/task_execution.py::_run_extract_adapter` globals-based dispatch.
6. `src/datafusion_engine/dataset/registry.py::dataset_location_from_catalog` (final stage, once all orchestration callsites are migrated).
7. `src/semantics/naming_compat.py` (if hard cut to canonical-only external API).

## 6. CQ Operating Protocol for Follow-Up Waves

1. Start each scope item with `./cq search <primary_symbol>` and `./cq calls <primary_symbol>`.
2. Before parameter/signature changes, run `./cq impact` and `./cq sig-impact`.
3. For each wave, run one `./cq run --steps` inventory on:
   - compile seams,
   - dataset binding seams,
   - registration seams,
   - extract adapter seams.
4. After each deletion, prove removal with one `./cq search <deleted_symbol>` command.
5. Keep `rg` limited to non-code assets or artifact parsing only.

## 7. DataFusion + Delta Guardrails (Must Remain)

1. Keep deterministic zero-row writes:
   - `write_deltalake(..., mode="overwrite", schema_mode="overwrite")`.
2. Keep provider-first registration and explicit fallback diagnostics.
3. Keep snapshot/scan pin policy explicit and reproducible from manifest payloads.
4. Avoid bespoke schema mutation outside Delta metadata semantics.
5. Keep fallback registration and bootstrap path telemetry artifacts mandatory.

## 8. Minimal Safety Validation During Architecture Cutover

Given architecture-first priority, run narrow smoke checks per wave (not full-suite stabilization):

1. Compile/validation smoke:
   - `tests/integration/test_semantic_pipeline.py`
   - `tests/integration/semantics/test_compiler_type_validation.py`
2. Runtime/bootstrap convergence smoke:
   - `tests/integration/test_zero_row_bootstrap_e2e.py`
   - `tests/integration/runtime/test_runtime_context_smoke.py`
3. Planning/execution smoke:
   - `tests/integration/relspec/test_compile_execution_plan.py`
4. Extract dispatch smoke:
   - `tests/integration/extraction/test_materialize_extract_plan.py`

## 9. Follow-Up Scope Beyond This Plan (After F1–F7)

1. **Manifest-as-ABI**:
   - versioned manifest schema + migration tooling + reproducibility fixtures.
2. **Program graph artifacting**:
   - persist one unified “compiled program graph” artifact for each run (compile + bindings + plan signature + schedule signature).
3. **Static seam governance**:
   - extend `check_semantic_compiled_cutover.py` to ban:
     - new globals-based adapter dispatch,
     - new direct catalog lookups in orchestration modules,
     - new ad hoc manifest construction outside compile-context.
4. **Code ownership boundaries**:
   - define strict ownership modules for compile, bindings, registration, extract orchestration, and incremental runtime.
5. **Hard deletion milestone**:
   - one dedicated wave solely for removing deprecated compatibility code after callsites reach zero.

## 10. Decision Defaults

1. Aggressive deletion remains preferred over prolonged compatibility layers.
2. Manifest-first control plane is mandatory for all new architecture work.
3. Runtime API remains authoritative; CLI work is optional and later.
4. Canonical names remain immediate default behavior.
5. Full-suite stabilization is intentionally deferred until architecture cutover reaches the defined end state.

## 11. Implementation Status Audit (2026-02-07)

This section supersedes the baseline snapshot in Section 2 and reflects the
current codebase state after in-flight implementation work.

### 11.1 Evidence snapshot

1. `uv run scripts/check_semantic_compiled_cutover.py --strict` -> `no findings`.
2. CQ callsite inventory:
   - `compile_semantic_program`: 4 callsites / 3 files.
   - `build_semantic_ir`: 8 callsites / 7 files (production path mostly through compile context; remainder mostly tests).
   - `ensure_view_graph`: 4 callsites / 3 files.
   - `dataset_location_from_catalog`: 0 callsites.
   - `dataset_catalog_from_profile`: 30 callsites / 22 files.
   - `materialize_extract_plan`: 25 callsites / 13 files.
   - `register_dataset_df`: 31 callsites / 21 files.
   - `register_dataset`: 15 callsites / 12 files.
3. Removed legacy seams (CQ verified 0 callsites):
   - `require_semantic_inputs`
   - `semantic_runtime_from_profile`
   - `apply_semantic_runtime_config`
   - `_extract_outputs_for_template`
   - `_REQUIRED_INPUTS`, `_SUPPORTS_PLAN`, `_EXTRACTOR_EXTRA_INPUTS`
4. Removed legacy provider wrappers (CQ verified 0 callsites in `src`):
   - `register_table_provider`
   - `register_delta_table_provider`
   - `register_delta_cdf_provider`
5. Canonical naming hard-cut in `src`:
   - no `cpg_nodes_v1`, `cpg_edges_v1`, or `naming_compat` references in `src`.
   - `src/semantics/naming_compat.py` is deleted.

### 11.2 Wave-by-wave status

#### Wave F1 - Compile/Validation Authority Collapse
Status: `partial (major progress)`

Implemented:
1. `build_cpg(...)` now compiles via `compile_semantic_program(...)`; no manual `SemanticProgramManifest(...)` construction remains in `src/semantics/pipeline.py`.
2. `view_graph_nodes(...)` requires a manifest and no longer builds ad hoc validation manifests in `src/datafusion_engine/views/registry_specs.py`.
3. Bootstrap validation requires manifest input; fallback manifest synthesis path has been removed in `src/datafusion_engine/bootstrap/zero_row.py`.
4. `src/semantics/ir_pipeline.py` no longer exports a `compile_semantic_program` shim.

Remaining:
1. `src/semantics/pipeline.py` still maintains a parallel local view-node build path (`_view_nodes_for_cpg`) and local cache-policy/runtime wiring instead of consuming the same registration path used by `ensure_view_graph(...)`.
2. `build_cpg_from_inferred_deps(...)` recompiles semantic manifests independently, so compile/registration authority is still split across two orchestration paths.

#### Wave F2 - Dataset Binding Authority Cutover
Status: `partial`

Implemented:
1. `dataset_location_from_catalog(...)` is fully removed from callsites.
2. `ManifestDatasetBindings` exists and is consumed in zero-row bootstrap and view-registry cache policy decisions.

Remaining:
1. `dataset_catalog_from_profile(...)` remains heavily distributed (30 callsites across 22 files).
2. High-priority orchestration layers still directly pull catalogs:
   - `src/semantics/pipeline.py`
   - `src/datafusion_engine/plan/pipeline.py`
   - `src/hamilton_pipeline/modules/task_execution.py`
   - `src/extract/infrastructure/worklists.py`
   - `src/engine/materialize_pipeline.py`
   - incremental modules under `src/semantics/incremental/`
3. Manifest bindings are not yet the singular dataset-binding authority across planning/scheduling/execution.

#### Wave F3 - Registration API Consolidation (Provider-First)
Status: `partial`

Implemented:
1. Legacy provider wrapper APIs are removed from active Python callpaths.
2. Core registration paths use `register_table(...)` internally through registration facades.

Remaining:
1. Registration surface is still broad (`register_dataset_df` and `register_dataset` call fan-out remains high).
2. Listing registration logic remains duplicated:
   - `src/datafusion_engine/dataset/registration.py` (`_register_listing_table`)
   - `src/datafusion_engine/tables/registration.py` (`register_listing_table`)
3. One authority module for listing registration is not yet enforced.

#### Wave F4 - Extract Execution Contract Finalization
Status: `partial`

Implemented:
1. Legacy template dispatch helper `_run_extract_adapter` is removed.
2. Legacy hardcoded extractor maps are removed.
3. Duplicate `materialize_extract_plan` implementation is removed (`src/extract/helpers.py` deleted; single definition in `src/extract/coordination/materialization.py`).
4. Adapter metadata is centralized in `src/datafusion_engine/extract/adapter_registry.py`.

Remaining:
1. Execution callable mapping still lives in `src/hamilton_pipeline/modules/task_execution.py` (`_EXTRACT_ADAPTER_EXECUTORS`) instead of being defined/owned by adapter registry metadata.
2. `materialize_extract_plan(...)` still has broad fan-out (25 callsites), so high-level orchestration entrypoint consolidation is not complete.

#### Wave F5 - Semantic Runtime Type Surface Cleanup
Status: `largely unimplemented`

Evidence:
1. `src/semantics/runtime.py` still exists and exports `SemanticRuntimeConfig` and `CachePolicy`.
2. Runtime-bridge types remain widely imported/used in:
   - `src/semantics/pipeline.py`
   - `src/datafusion_engine/session/runtime.py`
   - `src/engine/materialize_pipeline.py`
   - `src/datafusion_engine/views/graph.py`

Remaining:
1. Move required runtime options to manifest/runtime policy structures.
2. Remove orchestration dependency on `SemanticRuntimeConfig`/`CachePolicy`.
3. Delete `src/semantics/runtime.py` after migration.

#### Wave F6 - Incremental/CDF Path Convergence
Status: `largely unimplemented`

Evidence:
1. Incremental modules still perform direct catalog lookup/registration:
   - `src/semantics/incremental/cdf_runtime.py`
   - `src/semantics/incremental/delta_context.py`
   - `src/semantics/incremental/delta_updates.py`
   - `src/semantics/incremental/snapshot.py`
   - `src/semantics/incremental/plan_bundle_exec.py`
2. These paths are not yet aligned to manifest-bound dataset bindings and unified registration authority.

#### Wave F7 - Compatibility Boundary Reduction to Zero
Status: `implemented (hard cut in src)`

Implemented:
1. `src/semantics/naming_compat.py` deleted.
2. Core `src` runtime/semantic paths have no `_v1` CPG naming leaks.

Notes:
1. Legacy `_v1` references still exist in tests/docs artifacts and historical plan docs; this is expected and separate from core runtime cutover.

## 12. Remaining Scope to Implement (Decision-Complete Backlog)

### 12.1 Immediate architectural closures

1. Complete F1 authority collapse by removing parallel view-node build path in `src/semantics/pipeline.py`:
   - decommission `CpgViewNodesRequest` + `_view_nodes_for_cpg(...)` local assembly path.
   - route semantic registration through the same manifest-driven graph path used by `ensure_view_graph(...)`.
2. Eliminate duplicate compile orchestration in `build_cpg_from_inferred_deps(...)`:
   - compile once per operation boundary and share manifest/context for planning/dependency extraction.

### 12.2 Manifest binding hard cut

1. Introduce a manifest-bound dataset resolver interface and thread it through:
   - planning (`src/datafusion_engine/plan/pipeline.py`)
   - task execution (`src/hamilton_pipeline/modules/task_execution.py`)
   - relspec orchestration seams
   - incremental runtime seams.
2. Reduce `dataset_catalog_from_profile(...)` usage to compile/binding boundaries only.
3. Enforce with cutover checker rules that block new direct orchestration lookups.

### 12.3 Registration consolidation and deletion

1. Choose one listing registration authority:
   - keep `src/datafusion_engine/tables/registration.py::register_listing_table(...)` and remove dataset-side duplicate, or invert ownership; do not keep both.
2. Collapse façade usage to one pipeline-facing method (single semantic for register/update).
3. Remove remaining compatibility-only registration indirection once callsites are migrated.

### 12.4 Extract execution contract completion

1. Move execution callables into adapter registry contract (metadata + callable binding in one place).
2. Keep task execution orchestration free of template-name branching beyond adapter lookup.
3. Add a higher-level orchestration entrypoint to reduce direct `materialize_extract_plan(...)` fan-out across extractors.

### 12.5 Semantic runtime bridge removal

1. Design manifest/runtime policy replacements for fields now in `SemanticRuntimeConfig`:
   - output locations
   - cache policy overrides
   - CDF enablement/cursor store
   - storage options
   - schema evolution flags.
2. Migrate callers; then delete:
   - `src/semantics/runtime.py`
   - stale imports in `src/semantics/__init__.py`.

### 12.6 Incremental convergence

1. Make incremental modules consume manifest dataset bindings and registration facade contracts.
2. Remove incremental-specific catalog lookup and ad hoc registration paths.
3. Align incremental write/read policy handling with the same provider-first runtime contracts as non-incremental flows.

### 12.7 Additional aggressive decommission targets

1. `src/semantics/pipeline.py` local orchestration helpers that duplicate view registration/build policy.
2. One duplicate listing registration stack (`dataset/registration.py` or `tables/registration.py`).
3. Compatibility allowances in `scripts/check_semantic_compiled_cutover.py` that reference deleted paths (for example stale allowlist entries) once migration rules are finalized.

### 12.8 Completion definition for this follow-up plan

1. One manifest compile authority.
2. One dataset binding authority.
3. One registration authority.
4. One extract adapter execution authority.
5. Zero orchestration-level direct catalog lookups outside compile/binding boundaries.
6. Zero semantic runtime bridge types in core orchestration paths.
