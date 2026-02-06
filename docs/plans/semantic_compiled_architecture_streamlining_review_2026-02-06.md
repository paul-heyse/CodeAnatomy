# Semantic-Compiled Architecture Streamlining Review (2026-02-06)

## Scope and Assessment Method

This review focuses on the architecture path from:

1. extraction dataset definitions and schemas (`src/extract`, `src/datafusion_engine/extract`),
2. semantic view/model definitions (`src/semantics`),
3. planning + scheduling (`src/datafusion_engine/plan`, `src/relspec`),
4. orchestration/execution (`src/hamilton_pipeline`, `src/datafusion_engine/session`).

### Skill usage plan used for this assessment

#### CQ skill plan (code-truth pass)
- Map call graph seams and duplication points with `./cq calls` for:
  - `build_semantic_ir`, `ensure_view_graph`, `register_view_graph`, `require_semantic_inputs`, `materialize_extract_plan`, `compile_execution_plan`, `schedule_tasks`.
- Confirm registry/validation hotspots with targeted reads in:
  - `src/semantics/input_registry.py`, `src/semantics/validation/*`, `src/datafusion_engine/views/registry_specs.py`, `src/datafusion_engine/schema/registry.py`, `src/datafusion_engine/session/runtime.py`, `src/datafusion_engine/bootstrap/zero_row.py`.
- Validate extract/task coupling and hardcoded seams in:
  - `src/relspec/extract_plan.py`, `src/datafusion_engine/extract/extractors.py`, `src/hamilton_pipeline/modules/task_execution.py`.

#### DataFusion + DeltaLake skill plan (platform constraints pass)
- Confirm registration strategy guidance:
  - TableProvider-first, `register_table(...)` preferred, `register_dataset(...)` as compatibility fallback.
- Confirm zero-row / schema behavior guidance:
  - `write_deltalake(..., mode="overwrite", schema_mode="overwrite")` for deterministic schema materialization.
- Confirm snapshot/provider behavior implications:
  - snapshot-scoped schema behavior, fallback tradeoffs, and registration semantics.

## Executive Conclusions

1. The codebase already has a strong semantic core, but compilation output is **not yet a single authoritative runtime program artifact**. Instead, equivalent decisions are recomputed in multiple layers.
2. The largest architectural drag is **duplicated control-plane logic** (input mapping, validation, location resolution, view registration, IR build) across semantics/runtime/planning/execution modules.
3. Zero-row bootstrap is implemented and useful, but currently operates as a **parallel path**; it should be promoted into the same compiled control-plane so bootstrap, plan, and runtime all consume one program contract.
4. Extract metadata/spec intent is declarative, but execution still contains **template-specific imperative dispatch** that prevents full semantic compilation parity.
5. To reach the target vision, the next architecture step is a **semantic-compiled manifest + unified compile context** consumed end-to-end.

## Observed End-to-End Architecture (Current)

### 1) Extract intent and schema surfaces
- Extract metadata is centrally defined (`src/datafusion_engine/extract/metadata.py`) and expanded from templates.
- Extract schema access is split between:
  - schema registry-backed definitions (`extract_schema_for(...)`) and
  - fallback stringly schema assembly (`dataset_schema(...)` in `src/datafusion_engine/extract/registry.py`).
- Extract task specs are partly declarative but include hardcoded maps:
  - `_REQUIRED_INPUTS` and `_SUPPORTS_PLAN` in `src/datafusion_engine/extract/extractors.py`.
  - `_EXTRACTOR_EXTRA_INPUTS` in `src/relspec/extract_plan.py`.

### 2) Semantic IR and view build surfaces
- `build_semantic_ir(...)` compiles/optimizes/emits IR (`src/semantics/ir_pipeline.py`).
- CQ shows `build_semantic_ir` is called across many runtime seams (20 callsites across 16 files), including planning and execution layers.
- Semantic input validation is split across two stacks:
  - table presence mapping (`src/semantics/input_registry.py`),
  - schema/column validation (`src/semantics/validation/catalog_validation.py`).

### 3) View registration and planning surfaces
- `ensure_view_graph(...)` (`src/datafusion_engine/views/registration.py`) orchestrates UDF install, scan overrides, node build, and view registration.
- CQ shows `ensure_view_graph` is called from multiple layers (`plan/pipeline.py`, `session/facade.py`, `driver_factory.py`, `hamilton_pipeline/modules/task_execution.py`).
- `plan_with_delta_pins(...)` (`src/datafusion_engine/plan/pipeline.py`) executes a two-pass planning process with re-registration after scan pinning.

### 4) Scheduling and execution surfaces
- `compile_execution_plan(...)` (`src/relspec/execution_plan.py`) compiles task graph + schedule.
- `schedule_tasks(...)` is correctly centralized in relspec scheduling, but upstream task inputs are assembled through split pipelines.
- Hamilton task execution still uses template handler dispatch (`_extract_outputs_for_template(...)` in `src/hamilton_pipeline/modules/task_execution.py`).

### 5) Zero-row bootstrap surfaces
- Runtime API exists:
  - `DataFusionRuntimeProfile.run_zero_row_bootstrap_validation(...)` (`src/datafusion_engine/session/runtime.py`)
  - facade pass-through (`src/datafusion_engine/session/facade.py`).
- Bootstrap planner/executor exists (`src/datafusion_engine/bootstrap/zero_row.py`), including strict and seeded modes.
- Internal observability/cache tables now bootstrap on missing log (`src/datafusion_engine/cache/inventory.py`, `src/datafusion_engine/delta/observability.py`).

## Core Architecture Gaps

## Gap A: No single compiled control-plane artifact

Symptoms:
- IR compilation, input mapping, and registration are repeatedly recomputed across independent callsites.
- Planner and executor consume semantically equivalent information through different assembly paths.

Impact:
- Drift risk (same logical decision can differ by call path).
- Harder to reason about “what program is running” for a given execution.

## Gap B: Split semantic-input validation stack

Symptoms:
- `semantics.input_registry.require_semantic_inputs(ctx)` validates table presence.
- `semantics.validation.require_semantic_inputs(ctx, input_mapping=...)` validates schema/columns.
- Zero-row bootstrap validation path mixes `semantics.input_registry.require_semantic_inputs` with `validate_semantic_types` directly.

Impact:
- Multiple entrypoints for what should be one contract.
- Zero-row behavior policy has to be duplicated and threaded manually.

## Gap C: Dataset location/catalog resolution duplication

Symptoms:
- Catalog resolution exists in `dataset_catalog_from_profile(...)` (`src/datafusion_engine/dataset/registry.py`).
- Parallel location logic exists in runtime helpers and bootstrap-local helpers (`_semantic_output_locations`, `_semantic_input_location` in `src/datafusion_engine/bootstrap/zero_row.py`).

Impact:
- Increased divergence risk between bootstrap and normal runtime registration paths.

## Gap D: Extract execution remains template-dispatch imperative

Symptoms:
- Handler map in `_extract_outputs_for_template(...)` (`src/hamilton_pipeline/modules/task_execution.py`).
- Hardcoded extractor required inputs in `src/datafusion_engine/extract/extractors.py` and relspec extra input map in `src/relspec/extract_plan.py`.

Impact:
- Extract control plane is partially declarative, partially imperative.
- Limits the system’s ability to auto-adjust solely from metadata/schema changes.

## Gap E: Bootstrap is still an additive runtime feature, not an execution mode in the same program graph

Symptoms:
- Zero-row bootstrap has a dedicated API and report, but is not the canonical first stage of all strict semantic validation flows.
- Schema registry and bootstrap both own some setup/validation concerns.

Impact:
- Parallel control planes (runtime setup vs bootstrap setup).
- Harder to guarantee that pre-run and in-run validation semantics are identical.

## Gap F: Naming/version compatibility debt still leaks into logic seams

Symptoms:
- Canonical outputs (`cpg_nodes`, `cpg_edges`) are in place for semantic validation, but many `_v1` assumptions remain in extract/runtime support surfaces.

Impact:
- The semantic program remains partially constrained by legacy naming and compatibility shims.

## Target Architecture (Recommended)

## 1) Introduce `SemanticProgramManifest` as the single compiled contract

Create a first-class immutable manifest produced once per runtime context and consumed by bootstrap, planner, and executor.

Proposed payload categories:
- Input contracts:
  - required datasets, expected schemas, semantic types, zero-row policy per dataset.
- View program:
  - canonical view DAG, required UDFs/rewrite tags, output contracts.
- Extract task graph:
  - extract task templates, resolved required inputs, output mappings.
- Runtime bindings:
  - dataset locations from a single resolver, Delta policies, scan pinning policy.

Result:
- one source of truth for setup + compile + plan + execution.

## 2) Add a `CompileContext` service and eliminate repeated IR/registration orchestration

`CompileContext` should own:
- cached `SemanticIR`,
- resolved semantic input mapping,
- validated semantic input contract status,
- compiled `view_nodes`,
- resolved dataset catalog snapshot.

Then:
- `ensure_view_graph(...)`, `plan_with_delta_pins(...)`, `compile_execution_plan(...)`, and runtime bootstrap consume this context instead of rebuilding the same pieces.

## 3) Unify semantic-input validation under one policy engine

Consolidate current split validators into a single API with explicit policy levels:
- `schema_only` (strict zero-row),
- `schema_plus_optional_probe` (compat mode),
- `schema_plus_runtime_probe` (legacy fallback mode only).

This unifies:
- table presence,
- required columns,
- semantic type metadata.

## 4) Make dataset location resolution single-path

Adopt `dataset_catalog_from_profile(...)` as the only location resolver used by:
- bootstrap planning,
- registration,
- scan pin planning,
- runtime execution fallback registration.

Eliminate bootstrap-local location derivation helpers once migrated.

## 5) Replace imperative extract task dispatch with registry-driven execution adapters

Refactor from hardcoded template handler map to an adapter registry whose contract is derived from extractor metadata/specs.

Keep imperative code only in extractor implementations, not in pipeline control flow.

## 6) Promote zero-row bootstrap into canonical runtime initialization stage

Treat strict zero-row bootstrap as a standard initialization stage when enabled, not a side API path.

Desired sequence:
1. compile manifest,
2. materialize/register zero-row surfaces,
3. run unified semantic-input validation,
4. proceed to planning/scheduling/execution with the same manifest.

## 7) Introduce explicit compatibility boundary for legacy naming and aliases

Define a compatibility module that maps legacy names to canonical names and keep all aliasing there.

Goal:
- no business logic path should branch on legacy names directly.

## DataFusion/Delta Built-In Leverage (Reduce Bespoke Code)

Use built-ins as architecture constraints rather than reimplementing behavior:

1. Provider registration policy:
- Prefer `register_table(...)` with Delta TableProvider surfaces.
- Use `register_dataset(...)` fallback only for compatibility-degraded paths.

2. Deterministic zero-row table materialization:
- Continue `write_deltalake(..., mode="overwrite", schema_mode="overwrite")` for bootstrap-created tables.
- Keep schema authoritative at Delta metadata layer; avoid custom schema mutation logic for bootstrap.

3. Snapshot-aware planning as first-class policy:
- Keep Delta scan pin/snapshot semantics in planning pipeline, but move pin policy ownership into the compiled manifest so pin behavior is explicit and replayable.

4. Fallback behavior observability:
- Keep artifact emission for fallback registration paths and bootstrap outcomes; these are useful control-plane diagnostics and should remain.

## Prioritized Action Path

## Phase 1: Control-plane unification (highest ROI)
- Introduce `SemanticProgramManifest` + `CompileContext`.
- Migrate view registration, semantic input mapping/validation, and location resolution to consume them.

## Phase 2: Validation and bootstrap convergence
- Merge split semantic input validators.
- Move zero-row bootstrap to canonical init flow behind config mode.

## Phase 3: Extract execution modularization
- Replace hardcoded template dispatch with registry-driven adapter execution.
- Remove hardcoded extractor dependency maps where derivable from metadata/spec contracts.

## Phase 4: Compatibility and cleanup
- Isolate legacy name aliasing.
- Remove duplicate location helpers and duplicated validation paths.
- Prune compatibility shims once callers migrate.

## Acceptance Criteria for “Semantic-Compiled” Maturity

1. One manifest hash can explain and reproduce:
- bootstrap results,
- registered views,
- compiled plan signature,
- scheduled task graph.

2. Any schema/input change in extract metadata or semantic specs updates downstream behavior without hand-editing orchestration code.

3. Strict zero-row mode and non-zero runtime mode execute the same control-plane logic, differing only in data content, not in setup semantics.

4. No duplicated validators for semantic inputs exist outside a single validation policy module.

5. No imperative task-template dispatch remains in orchestration modules for extract control flow.

## Suggested First Refactor Slice

If we optimize for fastest reduction of architectural risk, start with this slice:

1. Create `SemanticProgramManifest` + builder.
2. Route `ensure_view_graph(...)` and `plan_with_delta_pins(...)` to consume the manifest.
3. Replace bootstrap-local location helpers with manifest-derived dataset locations.
4. Fold `semantics.input_registry` + `semantics.validation` entrypoints into a single policy-based validator API.

This slice directly reduces duplication and aligns bootstrap, planning, and runtime validation under one semantic-compiled control plane.
