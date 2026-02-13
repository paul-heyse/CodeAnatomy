# DataFusion Engine Decomposition Assessment

**Date:** 2026-02-12
**Scope:** `src/datafusion_engine/` (73,655 LOC across ~140 files in 20 subpackages)
**Purpose:** Identify dead code, Rust-duplicated functionality, and architectural misalignments

---

## Executive Summary

The `src/datafusion_engine/` module is the largest single module in the Python codebase. It was
designed when Python owned the entire pipeline: session management, plan compilation, execution,
materialization, and scheduling. With the pivot to a Rust execution engine
(`rust/codeanatomy_engine/`), significant portions of this module are now dead, redundant with
Rust implementations, or architecturally misaligned with the three-phase pipeline model.

**Key findings:**

| Category | Files | LOC | % of Module |
|----------|-------|-----|-------------|
| Fully dead (zero production callers) | 5 | ~970 | 1.3% |
| Functionally dead exports (within live files) | ~67 exports | ~700 | 1.0% |
| Rust-duplicated (transition candidates) | 8-12 files | ~6,500 | 8.8% |
| Architecturally misaligned (redesign needed) | 5-8 files | ~17,000 | 23.1% |
| Active and correctly aligned | ~115 files | ~48,500 | 65.8% |

**Total actionable LOC:** ~25,170 (34.2% of the module)

---

## Current Architecture Context

The pipeline has three phases, with Python and Rust owning distinct responsibilities:

```
Phase 1: Python Extraction       Phase 2: Python Semantic IR      Phase 3: Rust Engine
─────────────────────────────    ──────────────────────────────    ─────────────────────
src/extraction/orchestrator.py   src/semantics/compile_context.py  codeanatomy_engine.run_build()
src/extract/extractors/*         src/semantics/ir_pipeline.py      rust/codeanatomy_engine/
  ↓                                ↓                                ↓
Delta tables (work_dir)          SemanticExecutionSpec (JSON)      CPG outputs (output_dir)
```

**Python's legitimate DataFusion usage** is concentrated in two areas:
1. **Extraction** — Extractors build and execute DataFusion queries to transform evidence tables
2. **Semantic IR** — View definitions are assembled (but not executed) for the Rust compiler

**Rust owns:**
- Session construction (`SessionStateBuilder`)
- Plan compilation (`plan_compiler.rs`)
- Rule application (`rules/`)
- Scheduling (`compiler/scheduling.rs`)
- Execution and materialization (`executor/`)
- UDF implementations (`datafusion_ext/udf/`)
- Delta control plane (`datafusion_ext/delta_control_plane.rs`)

---

## Category 1: Fully Dead Code

These files have **zero production callers** and serve no purpose in the current pipeline.

### 1.1 `workload/` subpackage — 307 LOC

| File | LOC | External Importers | Status |
|------|-----|--------------------|--------|
| `workload/classifier.py` | ~170 | 0 (src/), 0 (tests/) | DEAD |
| `workload/session_profiles.py` | ~137 | 0 (src/), 0 (tests/) | DEAD |

**Evidence:** Grep for `from datafusion_engine.workload` across `src/` and `tests/` returns
zero matches. The only references are internal cross-imports between the two files.

**What it was:** A planned feature for dynamic session tuning based on workload classification
(BATCH_INGEST, INTERACTIVE_QUERY, COMPILE_REPLAY, INCREMENTAL_UPDATE). The classifier would
analyze plan signals and select session profiles with tuned batch sizes, memory limits, and
parallelism. This was never wired into the pipeline — sessions use fixed
`DataFusionRuntimeProfile` configurations instead.

**Action:** Delete both files and `workload/__init__.py`.

---

### 1.2 `plan/pipeline_runtime.py` — 541 LOC

| File | LOC | External Importers (src/) | External Importers (tests/) | Status |
|------|-----|---------------------------|-----------------------------| -------|
| `plan/pipeline_runtime.py` | 541 | 0 | 1 | DEAD |

**Evidence:** Grep for `from datafusion_engine.plan.pipeline_runtime` across `src/` returns
zero matches. One test file (`tests/unit/datafusion_engine/plan/test_pipeline_scan_policy_inference_wiring.py`)
imports it.

**What it was:** A two-pass planning pipeline that pinned Delta inputs before scheduling.
Line 25 reveals the critical dependency: `from relspec.inferred_deps import InferredDeps,
infer_deps_from_view_nodes` — this is the old rustworkx scheduling system. The file also
imports `ScanPolicyOverride` from `delta/scan_policy_inference.py` and builds
`PlanningPipelineResult` objects that were consumed by the old Hamilton orchestrator.

**Dependencies to clean up:**
- Delete the test file `tests/unit/datafusion_engine/plan/test_pipeline_scan_policy_inference_wiring.py`
- Verify `delta/scan_policy_inference.py` callers (2 remain: `relspec/policy_compiler.py` and this dead file)

**Action:** Delete `plan/pipeline_runtime.py` and its test file.

---

### 1.3 `delta/plugin_options.py` — ~122 LOC

| File | LOC | External Importers (src/) | External Importers (tests/) | Status |
|------|-----|---------------------------|-----------------------------| -------|
| `delta/plugin_options.py` | ~122 | 0 | 0 | DEAD |

**Evidence:** The file is registered in `delta/__init__.py`'s lazy-load export registry
(`delta_plugin_options_from_session`, `delta_plugin_options_json`) but these exports are never
actually imported by any file in `src/` or `tests/`.

**What it was:** Delta plugin option resolution for session-to-plugin configuration bridging.
This was likely an intermediate step toward the Rust plugin system (`df_plugin_api/`,
`df_plugin_host/`) that is now the sole authority for plugin configuration.

**Action:** Delete `delta/plugin_options.py` and remove its entries from `delta/__init__.py`.

---

## Category 2: Functionally Dead Exports Within Live Files

These files are actively used, but contain significant numbers of dead exports — functions,
classes, or constants that are never called from production code.

### 2.1 `schema/registry.py` — ~56 dead exports of 95 total (4,166 LOC)

This is the second-largest file in the module. Approximately 59% of its exports are dead.

**Dead export categories:**

| Category | Count | Examples |
|----------|-------|---------|
| Schema view-name constants | 6 | `AST_VIEW_NAMES`, `BYTECODE_VIEW_NAMES`, `SCIP_VIEW_NAMES`, `SYMTABLE_VIEW_NAMES`, `TREE_SITTER_VIEW_NAMES`, `CST_VIEW_NAMES` |
| Pipeline schema constants | 4 | `PIPELINE_PLAN_DRIFT_SCHEMA`, `PIPELINE_TASK_EXPANSION_SCHEMA`, `PIPELINE_TASK_GROUPING_SCHEMA`, `PIPELINE_TASK_SUBMISSION_SCHEMA` |
| DataFusion schema constants | 3 | `DATAFUSION_RUNS_SCHEMA`, `DATAFUSION_SQL_INGEST_SCHEMA`, `DATAFUSION_VIEW_ARTIFACTS_SCHEMA` |
| Diagnostics type constants | 3 | `DIAG_DETAILS_TYPE`, `DIAG_DETAIL_STRUCT`, `DIAG_TAGS_TYPE` |
| Validator functions | 11 | `validate_ast_views()`, `validate_bytecode_views()`, `validate_cst_views()`, `validate_scip_views()`, `validate_symtable_views()`, `validate_ts_views()`, etc. |
| Nested dataset functions | 15+ | `extract_nested_context_for()`, `is_extract_nested_dataset()`, `nested_context_for()`, `datasets_for_path()`, etc. |
| Relationship schema map | 1 | `RELATIONSHIP_SCHEMA_BY_NAME` |
| Nested dataset index | 1 | `NESTED_DATASET_INDEX` |

**Live exports (used from 18+ files):**
- `extract_schema_for()` — CRITICAL PATH (extraction, semantics)
- `validate_nested_types()` — session/runtime.py, materialize_pipeline.py
- `nested_view_specs()`, `nested_view_spec()`, `nested_path_for()`
- `registered_table_names()`
- `DATAFUSION_PLAN_ARTIFACTS_SCHEMA`, `DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA`

**What the dead exports were:** A planned-but-abandoned validation layer (validators for each
extractor's views) and an experimental nested dataset extraction system. The pipeline schema
constants (`PIPELINE_PLAN_DRIFT_SCHEMA`, etc.) were for Hamilton pipeline event tracking that
no longer exists.

**Action:** Audit each dead export with `/cq calls` to confirm zero callers, then remove.
Estimated cleanup: ~300-400 lines.

---

### 2.2 `dataset/registration.py` — ~6 dead exports of 11 total (3,425 LOC)

| Dead Export | LOC | Evidence |
|-------------|-----|---------|
| `DataFusionCacheSettings` | ~9 | Zero external importers |
| `DatasetInputSource` | ~71 | Zero external importers |
| `DatasetRegistrationOptions` | ~8 | Zero external importers |
| `DatasetRegistration` | ~9 | Zero external importers |
| `register_dataset_spec()` | ~52 | Zero external importers |
| `resolve_registry_options()` | ~44 | Zero external importers |

**Live exports:** `DataFusionCachePolicy` (18 callers), `register_dataset_df()` (3 callers),
`DataFusionRegistryOptions` (2 callers), `dataset_input_plugin()` (2 callers),
`input_plugin_prefixes()` (2 callers).

**What the dead exports were:** An older input plugin system (`DatasetInputSource`), a
spec-based registration path (`register_dataset_spec`), and configuration objects that were
replaced by simpler alternatives.

**Action:** Remove dead exports. ~193 lines saved.

---

### 2.3 `extract/templates.py` — 5 dead exports of 10 total (1,457 LOC)

| Dead Export | Evidence |
|-------------|---------|
| `CONFIGS` constant | Zero importers |
| `TEMPLATES` constant | Zero importers |
| `DatasetTemplateSpec` type alias | Zero importers |
| `ExtractorConfigSpec` type alias | Zero importers |
| `ExtractorTemplate` type alias | Zero importers |
| `flag_default()` function | Zero importers |

**Live exports:** `config()` (450+ call sites), `template()` (116 call sites),
`dataset_template_specs()`, `expand_dataset_templates()`.

**Action:** Remove dead exports. Minor cleanup (~20-30 lines).

---

## Category 3: Rust-Duplicated Functionality (Transition Candidates)

These modules provide capabilities that now exist in Rust. They are currently active because
the Python extraction phase still needs DataFusion sessions, but their functionality should be
progressively transitioned to Rust as the extraction phase evolves.

### 3.1 Session Factory — `session/factory.py` (433 LOC)

**Python:** Builds `SessionContext` objects with DataFusion configuration, runtime options, and
extension registration for the extraction phase.

**Rust equivalent:** `rust/codeanatomy_engine/src/session/factory.rs` — `SessionStateBuilder`
provides deterministic, reproducible session construction for the execution phase.

**Overlap type:** COMPLEMENTARY, NOT REDUNDANT. Python builds sessions for Phase 1 (extraction
queries), Rust builds sessions for Phase 3 (CPG execution). Both are needed today.

**Transition path:** When extraction moves to Rust, Python session factory becomes dead.
Currently active: imported by `session/runtime.py`, `session/facade.py`, and extraction code.

**Priority:** LOW — Keep until extraction transitions to Rust.

---

### 3.2 Plan Execution Runtime — `plan/execution_runtime.py` (348 LOC)

**Python:** Substrait replay, proto rehydration, plan validation for extraction-phase queries.
Key functions: `replay_substrait_bytes()`, `validate_substrait_plan()`, `execute_plan_artifact()`.

**Rust equivalent:** `rust/codeanatomy_engine/src/executor/pipeline.rs` + `runner.rs` — Plan
execution for CPG materialization.

**Overlap type:** COMPLEMENTARY. Python executes extraction plans; Rust executes CPG plans.
Both use DataFusion but for different pipeline phases.

**Active callers:** `extraction/materialize_pipeline.py`, `extract/coordination/materialization.py`,
`session/facade.py`, `extract/infrastructure/worklists.py`, `semantics/incremental/plan_bundle_exec.py`.

**Transition path:** Moves to Rust when extraction does. Currently essential.

**Priority:** LOW — Keep until extraction transitions to Rust.

---

### 3.3 UDF Metadata Layer — `udf/catalog.py` (1,215 LOC), `udf/runtime.py` (1,677 LOC)

**Python:** Metadata introspection of Rust UDFs. `catalog.py` queries registered UDFs from
DataFusion sessions. `runtime.py` snapshots function/parameter metadata. No Python UDF
implementations exist — all UDFs are Rust-native.

**Rust equivalent:** `rust/datafusion_ext/src/udf_registry.rs` + `udf/` — All UDF
implementations. Rust is the source of truth.

**Overlap type:** ADAPTER. Python reads what Rust registered. Not redundant — Python needs
UDF metadata for extraction-phase query building and validation.

**Transition path:** When extraction moves to Rust, metadata introspection becomes unnecessary
(Rust can access UDFs directly). The `udf/platform.py` (425 LOC) installer could be simplified
to a single Rust call.

**Priority:** MEDIUM — Simplify `platform.py` by removing fallback paths (per prior plans).

---

### 3.4 Delta Control Plane — `delta/control_plane.py` (2,337 LOC)

**Python:** Facade over Rust Delta operations. Marshals Python parameters to Rust calls via
`datafusion._internal`. Handles Delta provider creation, scan configuration, and table loading.

**Rust equivalent:** `rust/datafusion_ext/src/delta_control_plane.rs` — Native Delta provider
construction, scan overrides, predicate parsing.

**Overlap type:** ADAPTER/FACADE. Python marshals data; Rust does the work. Not redundant
in the current architecture — extraction code calls Python Delta APIs.

**Active callers:** `session/runtime.py`, `dataset/registration.py`, `delta/service.py`,
`storage/deltalake/delta.py`, `io/write.py`.

**Transition path:** Progressively thin as Rust capabilities expand. The prior plan identified
Wave 2 (control-plane simplification) as medium priority.

**Priority:** MEDIUM — Follow Wave 2 from existing transition plan.

---

### 3.5 Dataset Registration — `dataset/registration.py` (3,425 LOC)

**Python:** Registers Delta tables, Arrow datasets, and in-memory tables as DataFusion
providers for extraction-phase queries.

**Rust equivalent:** `rust/codeanatomy_engine/src/providers/registration.rs` — Registers
extraction inputs as native Delta providers for CPG execution.

**Overlap type:** COMPLEMENTARY but HEAVY. Python registers tables for extraction; Rust
registers tables for execution. The Python module is significantly larger than necessary for
its extraction-only role.

**Dead exports within:** 6 of 11 public exports are dead (see Category 2.2 above).

**Transition path:** Core registration logic (`register_dataset_df()`) stays until extraction
transitions. Dead exports should be removed immediately. The file could be reduced from
3,425 to ~2,500 lines by removing dead code and simplifying.

**Priority:** MEDIUM — Remove dead exports now; full transition when extraction moves to Rust.

---

## Category 4: Architecturally Misaligned (Redesign Needed)

These modules are active but their design reflects the old architecture. They should be
restructured to align with the Rust-first pipeline model.

### 4.1 `session/runtime.py` — 8,482 LOC (MONOLITH)

**Problem:** This is the largest file in the entire codebase. It serves as a god-object
consolidation point containing 194 top-level definitions spanning:
- Runtime profile management
- Session construction and configuration
- Feature gate resolution
- UDF registration orchestration
- Dataset introspection
- Schema validation helpers
- Delta session integration
- Cache policy resolution
- Extension module resolution
- Encoding and normalization helpers
- Observability instrumentation

**Active callers:** 30+ source modules import from this file.

**Architectural misalignment:** The file conflates extraction-phase concerns (session building,
UDF installation, dataset registration) with responsibilities that now belong to Rust (session
shaping, feature gating, optimization configuration). The monolithic design makes it impossible
to cleanly separate extraction-only logic from Rust-transitional logic.

**Recommended redesign:**
1. Extract `session/config.py` — Configuration classes and profile types (~1,500 LOC)
2. Extract `session/features.py` — Feature gates and capability resolution (~1,000 LOC)
3. Extract `session/introspection.py` — Dataset/schema/UDF introspection helpers (~2,000 LOC)
4. Retain `session/runtime.py` — Core session construction and orchestration (~4,000 LOC)
5. Move Rust-transitional logic to clearly marked sections for future extraction

**Priority:** HIGH — This file's size and scope make every change to the session layer risky.
Decomposition would immediately improve maintainability.

---

### 4.2 `schema/registry.py` — 4,166 LOC (MONOLITH)

**Problem:** Second-largest file. Contains a mix of:
- Active schema definitions used by extraction (live)
- Dead validator functions from the abandoned validation layer (~11 functions)
- Dead schema constants from Hamilton pipeline tracking (~7 constants)
- Dead nested dataset extraction system (~15 functions)
- Live nested view helpers used by session setup

**Architectural misalignment:** Schema validation for CPG outputs is now a Rust responsibility
(`rust/codeanatomy_engine/src/executor/delta_writer.rs` validates schemas during
materialization). Python retains schema DEFINITION authority for extraction tables but should
not contain CPG validation infrastructure.

**Recommended redesign:**
1. Remove all dead exports (56 of 95, ~300-400 LOC)
2. Split remaining live code:
   - `schema/extraction_schemas.py` — Schemas for extraction table outputs
   - `schema/nested_views.py` — Nested view spec helpers
   - `schema/observability_schemas.py` — Pipeline event and artifact schemas
3. Move CPG-related schema definitions to Rust or delete if duplicated

**Priority:** HIGH — Dead code removal is immediate; structural split is medium-term.

---

### 4.3 `lineage/` subpackage — 2,309 LOC

| File | LOC | Role |
|------|-----|------|
| `lineage/datafusion.py` | 570 | Core lineage extraction from DataFusion plans |
| `lineage/diagnostics.py` | 915 | Artifact recording and plan diagnostics |
| `lineage/scan.py` | 824 | Scan unit planning and Delta input analysis |

**Architectural misalignment:** This subpackage was designed primarily for the old rustworkx
dependency inference system (`src/relspec/inferred_deps.py`). With Rust owning scheduling,
the primary consumer of lineage data has moved to Rust.

**Current live usage:**
- `lineage/datafusion.py` — Still used by extraction for `extract_lineage()` and
  `referenced_tables_from_plan()`. Called by `io/write.py`, `views/graph.py`.
- `lineage/diagnostics.py` — Heavily used for artifact recording across dataset, catalog,
  and I/O boundaries. This is observability infrastructure, not scheduling.
- `lineage/scan.py` — Used by `plan/bundle_artifact.py` and `dataset/resolution.py` for
  scan unit planning. Partially scheduling-related.

**Transition path:**
- `diagnostics.py` stays in Python (observability concern)
- `datafusion.py` core lineage extraction could move to Rust
- `scan.py` scan unit planning overlaps with Rust scheduling — candidate for decommission

**Priority:** MEDIUM — `diagnostics.py` is fine. `datafusion.py` and `scan.py` should be
evaluated for Rust transition.

---

### 4.4 `views/graph.py` — 1,460 LOC

**Problem:** Defines the `ViewNode` data structure used across the semantic pipeline to
represent view definitions. The Rust engine has its own view compilation in
`compiler/view_builder.rs`. The Python `ViewNode` model is conceptually duplicated on the
Rust side but they serve different phases.

**Current live usage:** 30+ files import from `views/graph.py`. This is a core data structure
for the semantic IR pipeline.

**Architectural consideration:** The `ViewNode` graph is consumed by the semantic compiler
to build `SemanticExecutionSpec`. Rust then re-compiles from the spec. The Python view model
is not directly serialized — the spec contract (`SemanticExecutionSpec`) is the boundary. So
this is an intermediate representation that could theoretically be eliminated if the semantic
compiler produced the spec directly.

**Transition path:** Long-term — the semantic compiler would need significant restructuring
to bypass `ViewNode`. Keep for now.

**Priority:** LOW — Structural, not a correctness issue.

---

### 4.5 `delta/scan_policy_inference.py` — 499 LOC

**Problem:** One of its two callers (`plan/pipeline_runtime.py`) is dead. The remaining caller
is `relspec/policy_compiler.py` which compiles runtime profiles.

**Current state:** With `pipeline_runtime.py` deleted, this file loses its primary consumer.
The remaining caller should be evaluated for whether scan policy inference is still needed
or whether Rust handles this.

**Action:** After deleting `pipeline_runtime.py`, evaluate whether `scan_policy_inference.py`
can also be removed. If `relspec/policy_compiler.py` is the sole remaining caller, check if
that usage is on a live code path.

**Priority:** MEDIUM — Evaluate after `pipeline_runtime.py` deletion.

---

## Category 5: Cross-Cutting Observations

### 5.1 `__init__.py` Export Surface

`src/datafusion_engine/__init__.py` re-exports many symbols from subpackages. After removing
dead code, this file should be audited to remove stale re-exports. Several lazy-load entries
(like `delta_plugin_options_from_session`) point to dead code.

### 5.2 `generated/delta_types.py` — Internal Only

This file contains auto-generated Delta type definitions used only by `delta/payload.py`,
`delta/control_plane.py`, and `delta/protocol.py`. It's an internal dependency with no
dead code, but should be regenerated if the Rust Delta types change.

### 5.3 `bootstrap/zero_row.py` — 669 LOC, Active

Used by `session/runtime.py` and `session/facade.py` for constructing zero-row bootstrap
tables. Actively used in extraction. Not a transition candidate.

### 5.4 Test Coverage for Dead Code

| Dead Module | Test Files | Action |
|-------------|-----------|--------|
| `workload/` | 0 | N/A |
| `plan/pipeline_runtime.py` | 1 (`test_pipeline_scan_policy_inference_wiring.py`) | Delete test |
| `delta/plugin_options.py` | 0 | N/A |

---

## Recommended Execution Order

### Wave 0: Immediate Deletions (Zero Risk)

Delete files with zero production callers. No callsite changes required.

| Action | File | LOC Removed |
|--------|------|-------------|
| Delete | `workload/__init__.py` | ~10 |
| Delete | `workload/classifier.py` | ~170 |
| Delete | `workload/session_profiles.py` | ~137 |
| Delete | `plan/pipeline_runtime.py` | ~541 |
| Delete | `delta/plugin_options.py` | ~122 |
| Delete test | `tests/unit/datafusion_engine/plan/test_pipeline_scan_policy_inference_wiring.py` | ~50 |
| Update | `delta/__init__.py` — remove `plugin_options` lazy-load entries | ~10 |

**Total: ~1,040 LOC removed, 0 callsite changes**

### Wave 1: Dead Export Pruning (Low Risk)

Remove dead exports from live files. Requires verifying zero callers with `/cq calls`.

| Action | File | Exports Removed | LOC Saved |
|--------|------|-----------------|-----------|
| Prune | `schema/registry.py` | ~56 dead exports | ~300-400 |
| Prune | `dataset/registration.py` | ~6 dead exports | ~193 |
| Prune | `extract/templates.py` | ~5 dead exports | ~20-30 |
| Update | `__init__.py` | Remove stale re-exports | ~20 |

**Total: ~530-640 LOC removed, minimal callsite changes**

### Wave 2: UDF Bridge Simplification (Medium Risk)

Simplify the UDF adapter layer per existing transition plan.

| Action | File | Change |
|--------|------|--------|
| Simplify | `udf/platform.py` | Remove fallback paths, use direct Rust capability snapshot |
| Simplify | `udf/factory.py` | Remove retry loops, use direct Rust registration |
| Simplify | `udf/runtime.py` | Remove Python UDF fallback branches |

**Estimated: ~300-500 LOC simplified**

### Wave 3: Control-Plane Thinning (Medium Risk)

Thin the Python Delta control plane as Rust capabilities expand.

| Action | File | Change |
|--------|------|--------|
| Evaluate | `delta/scan_policy_inference.py` | May be dead after Wave 0 |
| Thin | `delta/control_plane.py` | Collapse validation shims to Rust |
| Thin | `delta/protocol.py` | Move gate validation to Rust |

**Estimated: ~300-500 LOC removed/simplified**

### Wave 4: Monolith Decomposition (Higher Risk)

Restructure the two largest files for maintainability.

| Action | File | Change |
|--------|------|--------|
| Split | `session/runtime.py` (8,482 LOC) | → config.py + features.py + introspection.py + runtime.py |
| Split | `schema/registry.py` (4,166 LOC post-pruning) | → extraction_schemas.py + nested_views.py + observability_schemas.py |

**Estimated: 0 LOC change (restructuring only)**

### Wave 5: Rust Transition (Future, Requires Rust Work)

These transitions depend on extraction moving to Rust.

| Module | Dependency | Estimated LOC |
|--------|-----------|---------------|
| `session/factory.py` | Extraction → Rust | 433 |
| `plan/execution_runtime.py` | Extraction → Rust | 348 |
| `lineage/datafusion.py` | Lineage → Rust | 570 |
| `lineage/scan.py` | Scheduling → Rust | 824 |
| `dataset/registration.py` (remaining) | Registration → Rust | ~2,500 |
| `udf/catalog.py` | UDF metadata → Rust | 1,215 |
| `udf/runtime.py` | UDF runtime → Rust | 1,677 |

**Estimated: ~7,567 LOC removed when extraction transitions to Rust**

---

## Summary by Subpackage

| Subpackage | LOC | Verdict | Priority |
|------------|-----|---------|----------|
| `arrow/` | 5,100 | KEEP — Core Arrow type system, Python-authoritative | — |
| `bootstrap/` | 700 | KEEP — Zero-row table construction for extraction | — |
| `cache/` | 1,700 | KEEP — Active materialization cache layer | — |
| `catalog/` | 1,730 | KEEP — Catalog introspection for extraction | — |
| `compile/` | 230 | KEEP — Compile options used by session/runtime | — |
| `dataset/` | 5,000 | PRUNE + TRANSITION — 6 dead exports; core stays until extraction → Rust | Wave 1, 5 |
| `delta/` | 7,600 | PRUNE + THIN — Delete plugin_options; thin control plane; eval scan_policy | Wave 0, 3 |
| `encoding/` | 200 | KEEP — Encoding policy for extraction | — |
| `errors.py` | 150 | KEEP — Exception types | — |
| `expr/` | 1,800 | KEEP — Expression building for extraction queries | — |
| `extensions/` | 1,600 | KEEP — Runtime capability resolution, extension loading | — |
| `extract/` | 2,700 | PRUNE — 5 dead exports in templates.py | Wave 1 |
| `generated/` | 200 | KEEP — Internal Delta type definitions | — |
| `hashing.py` | 430 | KEEP — Span/ID hashing for extraction | — |
| `identity.py` | 150 | KEEP — Schema identity hashing | — |
| `io/` | 3,200 | KEEP — I/O pipelines for extraction | — |
| `kernels.py` | 1,020 | KEEP — DedupeSpec, SortKey used by semantic pipeline | — |
| `lineage/` | 2,300 | EVALUATE — Diagnostics stays; datafusion.py + scan.py evaluate for Rust | Wave 3, 5 |
| `materialize_policy.py` | 200 | KEEP — Used by extraction | — |
| `plan/` | 6,500 | PRUNE — Delete pipeline_runtime.py; rest is active | Wave 0 |
| `pruning/` | 150 | KEEP — Pruning metrics for Delta | — |
| `registry_facade.py` | 420 | KEEP — Registration orchestration for semantic pipeline | — |
| `schema/` | 9,300 | PRUNE + SPLIT — 56 dead exports; decompose monolith | Wave 1, 4 |
| `session/` | 10,800 | SPLIT — Decompose runtime.py monolith | Wave 4 |
| `sql/` | 400 | KEEP — SQL guard and options | — |
| `symtable/` | 670 | KEEP — 1 live caller (semantics/pipeline.py) | — |
| `tables/` | 900 | KEEP — Table metadata and param tables | — |
| `udf/` | 4,600 | SIMPLIFY + TRANSITION — Remove fallback paths; full transition when extraction → Rust | Wave 2, 5 |
| `views/` | 2,800 | KEEP — Core view graph for semantic IR | — |
| `workload/` | 310 | DELETE — Zero callers | Wave 0 |

---

## Relationship to Prior Plans

This assessment is consistent with and extends the following existing plans:

1. **`comprehensive_src_rust_pivot_review_v1_2026-02-11.md`** — Identified Wave 0 (immediate
   deletes) across all `src/` modules. This document provides the `datafusion_engine`-specific
   deep dive that plan called for.

2. **`cli_datafusion_transition_plan_v1_2026-02-09.md`** — Identified UDF fallback removal
   (Wave 2 here) and control-plane thinning (Wave 3 here).

3. **`datafusion_planning_best_in_class_architecture_plan_v1_2026-02-09.md`** — Confirmed
   all 13 v1 Rust scope items are complete, enabling Python simplification.

4. **`src_rust_pivot_extension_assessment_v2_2026-02-12.md`** — Provides the latest
   cross-module assessment; this document drills into `datafusion_engine/` specifically.

No contradictions with prior plans were found.
