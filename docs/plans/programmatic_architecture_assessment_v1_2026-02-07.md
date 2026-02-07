# Programmatic Architecture Assessment

**Date:** 2026-02-07
**Context:** Continuation of the semantic-compiled consolidation effort, assessing how to
advance toward a fully programmatic architecture where extracted data and data operations
mapping drive all pipeline behavior with zero static declarations that could drift.
**Revision:** Integrated corrections from programmatic architecture reviews (Codex v1 + v2 + v3 + v4 assessments).

---

## 1. Target Architecture Vision

The target is an architecture where two inputs programmatically determine everything:

1. **Extracted data** - Evidence tables produced by multi-source extractors
2. **Data operations mapping** - DataFusion view definitions that map datasets to CPG outputs

From these two inputs, the system should automatically derive:
- Which datasets exist and where they live (dataset locations)
- What order to process them (scheduling)
- What schema contracts to enforce (validation)
- What caching/materialization to apply (execution policy)
- What UDFs are required (plan introspection)
- What Delta maintenance to run (write policy)

No static declaration should exist that could contradict what the plans actually require.

---

## 2. Current Architectural State

### 2.1 What Already Works Programmatically

**Plan-driven dependency inference is fully operational.** The pipeline:

```
DataFrame (view builder)
  -> build_plan_bundle() -> DataFusionPlanBundle
    -> extract_lineage() -> LineageReport
      -> infer_deps_from_plan_bundle() -> InferredDeps
        -> build_task_graph_from_inferred_deps() -> TaskGraph (rustworkx)
          -> schedule_tasks() -> TaskSchedule
            -> Hamilton DAG execution
```

This chain derives task ordering, column-level dependencies, UDF requirements, and
rewrite tags entirely from DataFusion optimized logical plans. No manual `inputs=`
declarations are used. The `InferredDeps` contract captures:

- `inputs` - tables referenced by scans in the plan
- `required_columns` - per-table column requirements from plan projection
- `required_udfs` - UDFs detected in plan expressions
- `required_rewrite_tags` - rewrite tags from UDF metadata
- `plan_fingerprint` - deterministic hash for cache invalidation

**View graph is the sole registration authority.** All DataFusion view registrations
flow through `ensure_view_graph()` -> `register_view_graph()`, which executes a
topological registration loop with lineage extraction, UDF validation, schema contract
enforcement, and diagnostic recording. Hamilton DAG nodes only consume pre-registered
views.

**Semantic IR drives view generation.** The `SemanticIR` intermediate representation
defines all normalize/relate/union/join operations. View builders are generated from
IR specs, not hand-coded. The IR itself is built from the semantic spec registry which
declares normalization, relationship, and table specs.

**Compiled manifest captures compile-time truth.** `SemanticProgramManifest` bundles:
- `semantic_ir` - the compiled IR
- `dataset_bindings` - resolved dataset locations at compile time
- `validation` - input validation results
- `fingerprint` - deterministic hash for the entire manifest

**`SemanticExecutionContext` is partially adopted.** Already used in planning/bootstrap
paths (`src/datafusion_engine/plan/pipeline.py:78`, `src/datafusion_engine/session/runtime.py:4559`).
The remaining work is extending adoption to compile-context hot spots where context is
still assembled ad hoc.

**Plan bundle statistics extraction is operational.** Physical plan statistics are
already captured via `_plan_statistics_section` and `_plan_statistics_payload`
(`src/datafusion_engine/plan/bundle.py:2071`, `src/datafusion_engine/plan/bundle.py:2274`).
The compatibility strategy for plan statistics should be hardened but the foundation exists.

**Dynamic Hamilton task module generation is active.** `build_task_execution_module()`
at `src/hamilton_pipeline/task_module_builder.py:104` already generates Hamilton DAG
node functions dynamically from the execution plan. This is not a future proposal.

**Scheduling cost signals have existing infrastructure.** `TaskPlanMetrics` already
captures `stats_row_count`, `stats_total_bytes`, `stats_available`, partition counts,
and repartition data (`src/relspec/execution_plan.py:141`, `src/relspec/execution_plan.py:1461`).
Scheduling enhancement is calibration/coverage hardening, not greenfield.

**Normalize dataset locations are partially programmatic.** `normalize_dataset_locations_for_profile()`
(`src/datafusion_engine/session/runtime.py:590`) and catalog integration
(`src/datafusion_engine/dataset/registry.py:332`) already derive normalize locations
from profile + manifest. Only remaining manual data source surfaces need attention.

**Schema contracts already derive from dataset specs.** Contract derivation and
enforcement are present at `src/datafusion_engine/views/registry_specs.py:280` and
`src/datafusion_engine/views/graph.py:353`. The opportunity is contract tightening
and divergence telemetry, not building contracts from scratch.

### 2.2 Remaining Static/Bridge Patterns

Despite the programmatic core, several bridge patterns persist where runtime code
re-derives dataset locations from `DataFusionRuntimeProfile` rather than receiving
them from the compiled manifest. These are the drift surfaces.

#### Bridge Pattern: `dataset_bindings_for_profile()`

**7 consumer callsites** across `src/` (plus 1 definition site), including the
`hamilton_pipeline` path:

| File | Lines | Role |
|------|-------|------|
| `semantics/compile_context.py` | 118-122 | Definition (wraps CompileContext) |
| `datafusion_engine/registry_facade.py` | 388-390 | Building catalog for RegistryFacade |
| `datafusion_engine/session/runtime.py` | 4079-4081 | Session bindings method |
| `datafusion_engine/session/runtime.py` | 4127-4129 | Location resolver method |
| `datafusion_engine/session/runtime.py` | 7616-7618 | Standalone utility |
| `datafusion_engine/session/facade.py` | 830-832 | CDF registration resolver |
| `datafusion_engine/plan/bundle.py` | 1907-1909 | Plan bundle context |
| `hamilton_pipeline/driver_factory.py` | 640 | Driver factory pruning path |

**Note:** `IncrementalRuntime.dataset_resolver` (`src/semantics/incremental/runtime.py:71`)
uses an injected resolver returned directly - there is no fallback to
`dataset_bindings_for_profile()` in the incremental runtime path.

Each of these re-derives bindings from the profile rather than receiving them from the
manifest. This means the same catalog resolution runs multiple times, and if a consumer
adds custom registration logic, it could diverge from what the manifest captured.

#### Bridge Pattern: `dataset_catalog_from_profile()`

Now internal to `CompileContext.dataset_bindings()` (1 callsite at
`compile_context.py:64-66`). Two deprecation markers in `materialize_pipeline.py`
confirm the fallback was intentionally removed there. This pattern is nearly contained.

#### Bridge Pattern: `SemanticRuntimeConfig`

Defined in `semantics/runtime.py` but has no production symbol usage. Only referenced
as a string guardrail in the cutover checker (`scripts/check_semantic_compiled_cutover.py:77`).
This should be treated as a historical/stale reference, not active architecture. No
migration effort is needed - it is already effectively dead code.

---

## 3. Drift Surfaces Analysis

A "drift surface" is any point where a static declaration or re-derivation could produce
a different answer than what the compiled plan actually requires.

### 3.1 Dataset Location Drift

**Risk:** When `dataset_bindings_for_profile()` is called independently by multiple
subsystems (registry facade, session runtime, plan bundle, incremental runtime,
hamilton driver factory), each call re-derives the full catalog from profile data
sources. If any subsystem modifies locations between calls, or if profile mutations
occur, different subsystems see different dataset locations.

**Current mitigation:** `ManifestDatasetBindings` is a frozen dataclass, and
`CompileContext` caches its result. But each `dataset_bindings_for_profile()` call
creates a *new* `CompileContext`, so the caching is per-call, not per-pipeline-run.

**Resolution path:** Thread `ManifestDatasetResolver` from the manifest through all
subsystems. The F6 wave (completed) demonstrated this for `semantics/incremental/`.
The remaining callsites in `datafusion_engine/` and `hamilton_pipeline/` need the
same treatment.

### 3.2 Compile Boundary Drift

**Risk:** Multiple orchestration entrypoints still instantiate `CompileContext(runtime_profile=...)`
directly. The primary drift surface is broader than just `datafusion_engine/*`:

| File | Line | Context |
|------|------|---------|
| `src/engine/materialize_pipeline.py` | 308 | Materialization entry |
| `src/extract/coordination/materialization.py` | 457 | Extract materialization |
| `src/hamilton_pipeline/driver_factory.py` | 465 | Driver factory compile path |
| `src/hamilton_pipeline/modules/task_execution.py` | 497, 523 | Task execution |
| `src/semantics/pipeline.py` | 1394 | Semantic pipeline entry |
| `src/semantics/pipeline.py` | 1980 | Inferred deps pipeline entry |

All seven sites reassemble a compile context instead of receiving the manifest-backed
execution context from the pipeline boundary.

**Resolution path:** Thread a manifest-backed execution context and resolver through
these paths. Add identity assertions for resolver reuse inside a single pipeline run.

### 3.3 Spec Registry Drift

**Risk:** The semantic spec registry (`SEMANTIC_TABLE_SPECS`, `RELATIONSHIP_SPECS`,
`SEMANTIC_NORMALIZATION_SPECS`) uses module-level immutable registries. If specs are
added or modified without updating the IR pipeline, the compiled manifest could reference
specs that produce different views than expected.

**Current mitigation:** `SemanticIR` is built from spec registry at compile time, and
the manifest fingerprint captures the IR hash. Any spec change invalidates the fingerprint.

**Assessment:** Low risk. The spec-to-IR compilation is single-path and deterministic.
The fingerprint mechanism detects drift. No action needed beyond maintaining the
single-path invariant.

### 3.4 UDF Registration Drift

**Risk:** Rust UDFs are installed via the UDF platform snapshot at view graph
registration time. If UDF availability differs between compile time (when plan bundles
capture `required_udfs`) and execution time, plans could fail.

**Current mitigation:** `ensure_view_graph()` installs the UDF platform and captures
the snapshot before view registration. The snapshot is threaded through plan bundle
building and validated at execution time. The `validate_required_udfs()` function
checks UDF availability at registration.

**Assessment:** Well-contained. The UDF snapshot acts as a compile-time contract that
is validated at execution. The key invariant is that UDF installation happens exactly
once per pipeline run, before any plan bundle is built.

### 3.5 Extract Executor Drift

**Risk:** Extract executors are registered via a global mutable registry
(`_EXTRACT_ADAPTER_EXECUTORS` in `extract_execution_registry.py`) with a global
flag `_EXTRACT_EXECUTOR_REGISTRATION_STATE`. If executor registration order changes or
executors are added dynamically, the extract pipeline could behave differently.

**Current mitigation:** `_ensure_extract_executors_registered()` uses a global flag
and registers all 9 executors in a fixed order. The `EvidencePlan` contract validates
which datasets are required before execution begins.

**Assessment:** Moderate risk. The global mutable registry with a boolean guard is
fragile. The fixed registration list is essentially a static declaration that could
drift from what the evidence plan requires. A programmatic approach would derive
required executors from the evidence plan.

### 3.6 Cache Policy Drift

**Risk:** View cache policies (`CachePolicy.NONE`, `DELTA_STAGING`, `DELTA_OUTPUT`)
are assigned at view node construction time in `registry_specs.py`. If the assignment
logic drifts from what the execution plan expects, intermediate results may be
re-computed or stale caches served.

**Current mitigation:** Cache policies are set during view graph construction and
validated at registration. The plan fingerprint includes cache-relevant metadata.

**Assessment:** Low risk for correctness (cache miss = recompute), but moderate risk
for performance. A programmatic approach would derive cache policies from plan
properties (output count, fan-out, reuse frequency).

---

## 4. DataFusion Environment Reality Check

Local probe results:
- `datafusion_version = 51.0.0`
- `DataFrame.execution_plan()` exists.
- Returned `ExecutionPlan` in this environment does **not** expose callable
  `statistics()` or `schema()` directly.

**Implications:**
- Keep defensive stats extraction (already present in `plan/bundle.py`).
- Any new scheduling/stats work must use capability detection, not version assumptions.
- A DataFusion capability adapter (Section 8.2) should gate metric scheduling fields
  and annotate diagnostics with capability status.

---

## 5. Re-Prioritized Convergence Plan

### Wave 0: Reality Alignment (Immediate)

**Goal:** Correct the plan baseline before further design work.

**Actions:**
1. Update drift surface inventory to include `hamilton_pipeline` and `engine`
   compile-context paths.
2. Split items into: implemented, partially implemented, net-new.
3. Replace grep-based metric commands with `./cq` equivalents.

**Exit criteria:**
- A corrected baseline document exists (this revision).
- All future implementation tasks reference corrected statuses.

**Status:** Complete. This document revision represents the Wave 0 deliverable.

### Wave 1: Resolver Threading Closure (Highest Priority)

**Goal:** Remove runtime re-derivation of manifest bindings from production
execution paths.

**Primary targets:**
- `src/datafusion_engine/registry_facade.py:388`
- `src/datafusion_engine/plan/bundle.py:1907`
- `src/datafusion_engine/session/facade.py:830`
- `src/datafusion_engine/session/runtime.py:4079`
- `src/datafusion_engine/session/runtime.py:4127`
- `src/datafusion_engine/session/runtime.py:7616`
- `src/hamilton_pipeline/driver_factory.py:640`

**Design note:** Prefer passing a single resolver reference from boundary context;
avoid opportunistic helper calls. Follow the F6 pattern: add resolver parameter with
fallback, validate with identity assertions, remove fallback after all callsites
migrate.

**Exit criteria:**
- `dataset_bindings_for_profile()` has zero consumer callsites outside `compile_context.py`
  (definition). Note: `incremental/runtime.py` uses injected resolver directly, no fallback.
- All `datafusion_engine/` and `hamilton_pipeline/` code receives dataset locations
  from the manifest chain

**Risk:** Medium. These are well-understood callsite replacements following the F6
pattern. Each can be done with a fallback during transition. Use temporary
compatibility overloads before deleting fallback helpers.

---

#### Implementation Details: Wave 1

**Representative Pattern - Before (Current):**

```python
# src/datafusion_engine/registry_facade.py:373-413
def registry_facade_for_context(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> RegistryFacade:
    # RE-DERIVES bindings from profile every time
    bindings = dataset_bindings_for_profile(runtime_profile)

    catalog = DatasetCatalog()
    for name in bindings.names():
        location = bindings.location(name)
        catalog.register(name, location)

    return RegistryFacade(catalog=catalog, ...)
```

**Representative Pattern - After (Target):**

```python
# src/datafusion_engine/registry_facade.py:373-413
def registry_facade_for_context(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver,  # THREADED from manifest
) -> RegistryFacade:
    # Uses manifest-backed resolver directly
    catalog = DatasetCatalog()
    for name in dataset_resolver.names():
        location = dataset_resolver.location(name)
        catalog.register(name, location)

    return RegistryFacade(catalog=catalog, ...)
```

**Target Files:**
- `src/datafusion_engine/registry_facade.py` (lines 373-413)
- `src/datafusion_engine/plan/bundle.py` (lines 1900-1915)
- `src/datafusion_engine/session/facade.py` (lines 807-838)
- `src/datafusion_engine/session/runtime.py` (lines 4035-4082, 4115-4130, 7606-7640)
- `src/hamilton_pipeline/driver_factory.py` (lines 626-660)

**Implementation Checklist:**

1. **Add resolver parameter with fallback to each target function**
   - [ ] `registry_facade_for_context()` - add `dataset_resolver: ManifestDatasetResolver | None = None`
   - [ ] `_manifest_dataset_locations()` - add `dataset_resolver: ManifestDatasetResolver | None = None`
   - [ ] `DataFusionExecutionFacade.register_cdf_inputs()` - add `dataset_resolver: ManifestDatasetResolver | None = None`
   - [ ] `RuntimeProfileCatalog.dataset_location()` - add `dataset_resolver: ManifestDatasetResolver | None = None`
   - [ ] `_RuntimeProfileCatalogFacadeMixin.dataset_location()` - add `dataset_resolver: ManifestDatasetResolver | None = None`
   - [ ] `record_dataset_readiness()` - add `dataset_resolver: ManifestDatasetResolver | None = None`
   - [ ] `_plan_with_incremental_pruning()` - add `dataset_resolver: ManifestDatasetResolver | None = None`

2. **Update function bodies to use resolver when provided**
   - [ ] Replace `bindings = dataset_bindings_for_profile(profile)` with conditional:
     ```python
     if dataset_resolver is None:
         dataset_resolver = dataset_bindings_for_profile(profile)  # FALLBACK
     ```
   - [ ] Change all `bindings.location(name)` to `dataset_resolver.location(name)`
   - [ ] Change all `bindings.names()` to `dataset_resolver.names()`

3. **Thread resolver from boundary contexts**
   - [ ] Identify all callsites of the 7 target functions
   - [ ] For each callsite, trace back to find the nearest `SemanticExecutionContext` or manifest
   - [ ] Pass `ctx.dataset_resolver` or `manifest.dataset_bindings` as `dataset_resolver=` argument

4. **Add identity assertions for resolver reuse**
   - [ ] In `src/datafusion_engine/views/graph.py`, add assertion that all view registration uses same resolver instance:
     ```python
     assert dataset_resolver is _initial_resolver, "Resolver identity violation"
     ```
   - [ ] In integration tests, add check that resolver identity is preserved across subsystems

5. **Remove fallbacks once all callsites migrated**
   - [ ] Change all `dataset_resolver: ManifestDatasetResolver | None = None` to required parameters
   - [ ] Remove all `if dataset_resolver is None: dataset_resolver = dataset_bindings_for_profile(profile)` blocks
   - [ ] Run full test suite to verify no missing callsites

6. **Mark `dataset_bindings_for_profile()` as internal**
   - [ ] Rename to `_dataset_bindings_for_profile()` (leading underscore)
   - [ ] Add deprecation warning if called from outside `compile_context.py` or `incremental/runtime.py`
   - [ ] Update docstring to indicate it's an internal bootstrap helper

**Decommission Candidates:**
- `dataset_bindings_for_profile()` in `src/semantics/compile_context.py` (after all callsites migrated, keep only as internal bootstrap helper with leading underscore and deprecation warning)

---

### Wave 2: Compile Boundary Convergence (High Priority)

**Goal:** Reduce repeated `CompileContext(runtime_profile=...)` assembly across
orchestration.

**Remaining hotspots (7 total):**
- `src/engine/materialize_pipeline.py:308`
- `src/extract/coordination/materialization.py:457`
- `src/hamilton_pipeline/driver_factory.py:465`
- `src/hamilton_pipeline/modules/task_execution.py:497`
- `src/hamilton_pipeline/modules/task_execution.py:523`
- `src/semantics/pipeline.py:1394`
- `src/semantics/pipeline.py:1980`

Wave 2 is complete only when all seven sites are converged.

**Approach:**
1. Thread a manifest-backed execution context and resolver through these paths
2. Add identity assertions for resolver reuse inside a single pipeline run
3. Remove redundant `compile_semantic_program()` calls that re-derive the same manifest
4. **Design decision (Section 8.7 is authoritative):** Keep `SemanticExecutionContext`
   semantic-only (manifest, dataset_resolver, runtime_profile, ctx, facade). Do NOT add
   `evidence_plan` or `executor_map` to it. Instead, introduce `ExecutionAuthorityContext`
   (see Section 8.7) that composes `SemanticExecutionContext` with orchestration fields.
   Wave 2 focuses on threading the semantic context; Waves 3+ build on top via
   `ExecutionAuthorityContext`.

**Risk:** Compile-boundary convergence can increase object lifetime coupling; keep
context immutable/frozen where possible.

---

#### Implementation Details: Wave 2

**Representative Pattern - Before (Current):**

```python
# src/engine/materialize_pipeline.py:277-308
def build_view_product(
    view_name: str,
    *,
    session_runtime: SessionRuntime,
    policy: ViewCachePolicy,
    view_id: str | None = None,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> PlanProduct:
    profile = session_runtime.profile

    # RE-COMPILES context from profile
    if dataset_resolver is None:
        dataset_resolver = CompileContext(runtime_profile=profile).dataset_bindings()

    # ... rest of function
```

**Representative Pattern - After (Target):**

```python
# src/engine/materialize_pipeline.py:277-308
def build_view_product(
    view_name: str,
    *,
    session_runtime: SessionRuntime,
    policy: ViewCachePolicy,
    view_id: str | None = None,
    execution_context: SemanticExecutionContext,  # THREADED from compile boundary
) -> PlanProduct:
    # Uses pre-compiled context directly
    dataset_resolver = execution_context.dataset_resolver
    manifest = execution_context.manifest
    ctx = execution_context.ctx

    # ... rest of function
```

**Target Files:**
- `src/engine/materialize_pipeline.py` (lines 277-308)
- `src/extract/coordination/materialization.py` (lines 448-457)
- `src/hamilton_pipeline/driver_factory.py` (line 465)
- `src/hamilton_pipeline/modules/task_execution.py` (lines 479-505, 508-549)
- `src/semantics/pipeline.py` (lines 1354-1431, 1971-1990)

**Implementation Checklist:**

1. **Keep `SemanticExecutionContext` semantic-only (Section 8.7 authoritative)**
   - [ ] Do NOT add `evidence_plan` or other orchestration fields to `SemanticExecutionContext`
     - Current fields: `manifest`, `dataset_resolver`, `runtime_profile`, `ctx`, `facade`
     - These are all semantic compile artifacts and should remain so
   - [ ] `build_semantic_execution_context()` signature unchanged for Wave 2
     (`src/semantics/compile_context.py:152`)
   - [ ] Define `ExecutionAuthorityContext` in a new module (see Section 8.7) that
     composes `SemanticExecutionContext` with orchestration fields:
     ```python
     # src/relspec/execution_authority.py (or similar)
     @dataclass(frozen=True)
     class ExecutionAuthorityContext:
         semantic_context: SemanticExecutionContext
         evidence_plan: EvidencePlan | None = None
         extract_executor_map: Mapping[str, Any] | None = None
         capability_snapshot: DataFusionCapabilities | None = None
     ```
   - [ ] Waves 1-2 focus on threading `SemanticExecutionContext`; Waves 3+ adopt
     `ExecutionAuthorityContext` for orchestration-layer fields

2. **Thread execution context through `materialize_pipeline.py`**
   - [ ] Change `build_view_product()` signature to accept `execution_context: SemanticExecutionContext`
   - [ ] Remove `dataset_resolver` parameter (now accessed via `execution_context.dataset_resolver`)
   - [ ] Find all upstream callers (use `./cq calls build_view_product` for actual count) and thread context from nearest compile boundary
   - [ ] Add identity assertion: `assert execution_context.manifest is _compile_manifest`

3. **Thread execution context through `extract/coordination/materialization.py`**
   - [ ] Change `_resolve_dataset_resolver()` to accept `execution_context: SemanticExecutionContext | None`
   - [ ] When context provided, use `execution_context.dataset_resolver` directly
   - [ ] Update `materialize_extract_plan()` signature to accept and pass context
   - [ ] Thread context from all upstream callers (use `./cq calls materialize_extract_plan` for actual count; currently ~25 callsites across 13 files)
   - [ ] Remove fallback `CompileContext(runtime_profile=...)` calls

4. **Thread execution context through `hamilton_pipeline/modules/task_execution.py`**
   - [ ] For `_ensure_scan_overrides()` (line 479), change to accept `execution_context: SemanticExecutionContext`
   - [ ] For `_execute_view()` (line 508), change to accept `execution_context: SemanticExecutionContext`
   - [ ] Both functions currently create `CompileContext(runtime_profile=profile)` and call `.compile(ctx=session)` and `.dataset_bindings()`
   - [ ] Replace with direct use of `execution_context.manifest`, `execution_context.dataset_resolver`, `execution_context.ctx`
   - [ ] Update upstream callers to pass execution context

5. **Consolidate compile calls in `semantics/pipeline.py`**
   - [ ] In `build_cpg()` (line 1394), check if manifest already exists in context
   - [ ] If manifest exists, reuse it; don't call `compile_semantic_program()` again
   - [ ] Same for `build_cpg_from_inferred_deps()` (line 1980)
   - [ ] Add assertion that compile happens exactly once per pipeline run
   - [ ] Create integration test that verifies single compile call

5b. **Thread execution context through `hamilton_pipeline/driver_factory.py`**
   - [ ] At line 465, replace direct `CompileContext(runtime_profile=...)` with threaded context
   - [ ] Ensure `ViewGraphContext` or equivalent carries the resolver from boundary

6. **Add pipeline-level compile-once invariant check**
   - [ ] Create `src/semantics/compile_invariants.py` module
   - [ ] Add `CompileTracker` context manager that enforces single compile per pipeline
   - [ ] Use in all orchestration entry points
   - [ ] Add integration test that fails if multiple compiles occur

**Decommission Candidates:**
- All ad-hoc `CompileContext(runtime_profile=profile)` instantiation patterns outside `compile_context.py` (7 sites: `materialize_pipeline.py:308`, `materialization.py:457`, `driver_factory.py:465`, `task_execution.py:497`, `task_execution.py:523`, `pipeline.py:1394`, `pipeline.py:1980`)
- Redundant `compile_semantic_program()` calls in orchestration (keep only the boundary compile)
- `_resolve_dataset_resolver()` fallback logic in `src/extract/coordination/materialization.py` (after all callers thread context)

---

### Wave 3: Extract Executor Programmatic Binding (Medium Priority)

**Goal:** Replace global mutable registration reliance with plan-derived executor
mapping.

Current registry remains mutable global (`src/hamilton_pipeline/modules/extract_execution_registry.py:27`).

**Approach:**
1. Build immutable executor map from `EvidencePlan` requirements
2. Preserve current registry as compatibility layer behind feature flag until
   migration completes
3. Pass executor map through `TaskExecutionInputs` or a new `ExtractExecutionContext`
4. Replace `get_extract_executor(key)` calls with map lookups
5. Remove `_EXTRACT_EXECUTOR_REGISTRATION_STATE` global flag

**Benefits:**
- No global mutable state
- Executor availability validated at plan time, not lazily at first use
- Missing executors produce compile-time errors rather than runtime KeyError
- The executor map becomes part of the pipeline contract

---

#### Implementation Details: Wave 3

**Representative Pattern - Before (Current):**

```python
# src/hamilton_pipeline/modules/task_execution.py:996-999
ensure_extract_executors_registered()  # GLOBAL MUTABLE REGISTRATION
adapter_key = adapter_executor_key(adapter.name)
handler = get_extract_executor(adapter_key)  # RUNTIME LOOKUP
outputs = handler(inputs, extract_session, profile_name)
```

**Representative Pattern - After (Target):**

```python
# src/hamilton_pipeline/modules/task_execution.py:996-999
# Executor map built at compile time from EvidencePlan
# Note: uses ExecutionAuthorityContext (not SemanticExecutionContext)
executor_map = authority_context.extract_executor_map
adapter_key = adapter_executor_key(adapter.name)
handler = executor_map[adapter_key]  # COMPILE-TIME VALIDATED
outputs = handler(inputs, extract_session, profile_name)
```

**Target Files:**
- `src/hamilton_pipeline/modules/extract_execution_registry.py` (entire module)
- `src/hamilton_pipeline/modules/task_execution.py` (lines 996-999 and upstream)
- `src/semantics/compile_context.py` (or new `src/relspec/execution_authority.py` - define `ExecutionAuthorityContext`)
- `src/extract/coordination/evidence_plan.py` (add executor requirement introspection)

**Implementation Checklist:**

1. **Add executor requirement introspection to `EvidencePlan` (net-new API)**
   - [ ] Add method `required_adapter_names() -> frozenset[str]` to `EvidencePlan` class
   - [ ] **Note:** This is a net-new API addition. `EvidencePlan` currently provides
     `requires_dataset()`, `requires_template()`, and `required_columns_for()` only
     (`src/extract/coordination/evidence_plan.py:29, 41, 58`).
   - [ ] Implementation: derive required adapter names from `EvidencePlan` sources/requirements
     plus extract metadata (two-stage derivation):
     ```python
     def required_adapter_names(self) -> frozenset[str]:
         """Derive required adapter names from plan sources and extract metadata.

         Net-new API: does not exist in current EvidencePlan.
         """
         adapter_names = set()
         for source in self._sources:
             adapter_names.add(source.adapter_name)
         return frozenset(adapter_names)
     ```

2. **Create immutable executor map builder (two-stage derivation)**
   - [ ] Add `src/hamilton_pipeline/modules/extract_executor_map.py` module
   - [ ] Define `ExtractExecutorMap = Mapping[str, _ExtractExecutorFn]` type
   - [ ] Use two-stage derivation that respects existing registry boundaries:
     ```python
     def build_executor_map(required: frozenset[str]) -> ExtractExecutorMap:
         # Stage 1: Resolve required adapter names from EvidencePlan + extract metadata
         # Stage 2: Validate against registered_adapter_names() and map to executors
         from datafusion_engine.extract.adapter_registry import _ADAPTERS  # adapter metadata
         from .extract_execution_registry import get_extract_executor  # executor callables

         executor_map = {}
         for adapter_name in required:
             if adapter_name not in _ADAPTERS:
                 raise ValueError(f"Required adapter '{adapter_name}' not registered")
             executor_key = adapter_executor_key(adapter_name)
             executor_map[executor_key] = get_extract_executor(executor_key)

         return executor_map
     ```
   - [ ] **Note:** `_ADAPTERS` lives in `src/datafusion_engine/extract/adapter_registry.py:22`
     (adapter metadata). Executor callables live in `_EXTRACT_ADAPTER_EXECUTORS` in
     `src/hamilton_pipeline/modules/extract_execution_registry.py:27`. Keep these concerns
     separate: adapter metadata vs. execution registry authority.

3. **Add executor map to `ExecutionAuthorityContext` (NOT `SemanticExecutionContext`)**
   - [ ] **Design decision (Section 8.7 is authoritative):** Keep `SemanticExecutionContext`
     semantic-only (current 5 fields: `manifest`, `dataset_resolver`, `runtime_profile`,
     `ctx`, `facade`). Do NOT add orchestration fields to it.
   - [ ] Instead, compose into `ExecutionAuthorityContext` (see Section 8.7):
     ```python
     @dataclass(frozen=True)
     class ExecutionAuthorityContext:
         semantic_context: SemanticExecutionContext
         evidence_plan: EvidencePlan | None
         extract_executor_map: Mapping[str, Any] | None
         capability_snapshot: DataFusionCapabilities | None
     ```
   - [ ] Build executor map when constructing `ExecutionAuthorityContext`:
     ```python
     executor_map = build_executor_map(evidence_plan.required_adapter_names()) if evidence_plan else None
     authority = ExecutionAuthorityContext(
         semantic_context=semantic_execution_context,
         evidence_plan=evidence_plan,
         extract_executor_map=executor_map,
         capability_snapshot=capabilities,
     )
     ```

4. **Update task execution to use executor map**
   - [ ] In `src/hamilton_pipeline/modules/task_execution.py:996-999`, change from:
     ```python
     ensure_extract_executors_registered()
     handler = get_extract_executor(adapter_key)
     ```
     to:
     ```python
     # Note: uses ExecutionAuthorityContext (not SemanticExecutionContext)
     executor_map = authority_context.extract_executor_map
     handler = executor_map[adapter_key]  # KeyError if missing = compile-time bug
     ```

5. **Add feature flag for compatibility mode**
   - [ ] Add `USE_GLOBAL_EXTRACT_REGISTRY` to `src/relspec/config.py` (default False)
   - [ ] In task execution, check flag and fall back to global registry if enabled
   - [ ] Add deprecation warning when fallback is used

6. **Validate executor availability at orchestration/execution-plan boundary**
   - [ ] **Important:** Do NOT place executor validation inside `compile_semantic_program()`,
     which returns `SemanticProgramManifest` only (`src/semantics/compile_context.py:129`).
     Executor availability is an orchestration concern, not a semantic compile concern.
   - [ ] Validate when building `ExecutionAuthorityContext` (after execution plan is compiled):
     ```python
     # At orchestration boundary (e.g., compile_execution_plan() caller or
     # ExecutionAuthorityContext builder), NOT in compile_semantic_program()
     executor_map = build_executor_map(evidence_plan.required_adapter_names())
     record_artifact(profile, "extract_executor_availability_v1", {
         "required": list(evidence_plan.required_adapter_names()),
         "available": list(executor_map.keys()),
     })
     ```
   - [ ] Add integration test that verifies build fails with clear error if executor missing

7. **Deprecate global mutable registry**
   - [ ] Add deprecation warning to `register_extract_executor()` function
   - [ ] Add deprecation warning to `ensure_extract_executors_registered()` function
   - [ ] Update docstrings to indicate these are legacy compatibility functions

**Decommission Candidates:**
- `_EXTRACT_EXECUTOR_REGISTRATION_STATE` global flag in `src/hamilton_pipeline/modules/task_execution.py:871`
- `ensure_extract_executors_registered()` function (after all callsites migrated to executor map)
- `get_extract_executor()` function (after all callsites migrated to executor map lookup)
- Global mutable `_EXTRACT_ADAPTER_EXECUTORS` dict in `src/hamilton_pipeline/modules/extract_execution_registry.py:27` (keep adapter definitions in `src/datafusion_engine/extract/adapter_registry.py`, remove global registration pattern)

---

### Wave 4: Policy and Maintenance Intelligence (Medium Priority)

**Goal:** Improve policy adaptivity based on execution outcomes and plan signals.

**Keep and refine:**
- Outcome-driven Delta maintenance trigger decisions (augment
  `resolve_delta_maintenance_plan`, `src/datafusion_engine/delta/maintenance.py:68`)
- Plan-informed scan policy tuning via existing scan override infrastructure

**Do not replace:**
- Dataset-level policy declarations should remain authoritative defaults; plan outcomes
  should tune/override where safe

**Approach:**
1. After task execution, capture write metrics (rows written, files created,
   current table version) as part of the execution result
2. Build maintenance decisions from write outcomes:
   - Optimize if files_created > threshold or total_file_count > threshold
   - Vacuum if version_delta > retention_threshold
   - Checkpoint if version_delta > checkpoint_interval
3. Create `resolve_maintenance_from_execution(result: TaskExecutionResult) -> MaintenancePlan | None`
4. Compute per-scan-unit scan policies based on plan signals:
   - Small tables: disable file pruning
   - Tables with pushed predicates: enable Parquet column pruning
   - Tables with sort keys matching join keys: enable file sort order

---

#### Implementation Details: Wave 4

This wave has two distinct tracks: **4A (Maintenance from Execution)** and **4B (Scan Policy from Plan)**.

**Track 4A: Maintenance from Execution Outcomes**

**Representative Pattern - Before (Current):**

```python
# src/datafusion_engine/delta/maintenance.py:68-98
def resolve_delta_maintenance_plan(
    dataset_location: DatasetLocation,
    *,
    delta_version: int | None = None,
    # ... other args
) -> DeltaMaintenancePlanInput | None:
    resolved = dataset_location.resolved
    if resolved is None or resolved.delta_maintenance_policy is None:
        return None  # POLICY-PRESENCE-BASED

    # If policy exists, always return maintenance plan
    return DeltaMaintenancePlanInput(...)
```

**Representative Pattern - After (Target):**

```python
# src/datafusion_engine/delta/maintenance.py:NEW
def resolve_maintenance_from_execution(
    result: TaskExecutionResult,
    *,
    policy: DeltaMaintenancePolicy,
    current_table_version: int,
) -> DeltaMaintenancePlanInput | None:
    # OUTCOME-THRESHOLD-BASED
    needs_optimize = (
        result.files_created > policy.optimize_file_threshold or
        result.total_file_count > policy.total_file_threshold
    )
    needs_vacuum = result.version_delta > policy.vacuum_version_threshold
    needs_checkpoint = result.version_delta > policy.checkpoint_interval

    if not (needs_optimize or needs_vacuum or needs_checkpoint):
        return None

    return DeltaMaintenancePlanInput(
        enable_optimize=needs_optimize,
        enable_vacuum=needs_vacuum,
        enable_checkpoint=needs_checkpoint,
        ...
    )
```

**Track 4A Target Files:**
- `src/datafusion_engine/delta/maintenance.py` (lines 54-207)
- `src/storage/deltalake/config.py` (add threshold fields to `DeltaMaintenancePolicy`)
- `src/relspec/execution_plan.py` (extend `TaskExecutionResult` to capture write metrics)

**Track 4A Implementation Checklist:**

1. **Extend `DeltaMaintenancePolicy` with outcome thresholds**
   - [ ] Add to `src/storage/deltalake/config.py`:
     ```python
     class DeltaMaintenancePolicy(StructBaseStrict, frozen=True):
         optimize_file_threshold: PositiveInt = 10  # Optimize if >N new files created
         total_file_threshold: PositiveInt = 100    # Optimize if total files >N
         vacuum_version_threshold: PositiveInt = 10 # Vacuum if version delta >N
         checkpoint_interval: PositiveInt = 10      # Checkpoint every N versions
         # ... existing fields
     ```

2. **Extend `TaskExecutionResult` to capture write metrics**
   - [ ] Add to `src/relspec/execution_plan.py`:
     ```python
     @dataclass(frozen=True)
     class TaskExecutionResult:
         # ... existing fields
         files_created: int | None = None
         total_file_count: int | None = None
         version_delta: int | None = None  # versions advanced since start
         delta_version: int | None = None  # final table version
     ```

3. **Capture write metrics during task execution**
   - [ ] In write path (`src/datafusion_engine/io/write.py`), after write completes:
     ```python
     table = delta_table(location)
     write_metrics = {
         "files_created": len(write_result.data_files),
         "total_file_count": len(table.files()),
         "version_delta": table.version() - initial_version,
         "delta_version": table.version(),
     }
     ```
   - [ ] Thread write metrics back to `TaskExecutionResult`

4. **Create outcome-based maintenance resolver**
   - [ ] Add `resolve_maintenance_from_execution()` function in `src/datafusion_engine/delta/maintenance.py`
   - [ ] Implementation as shown in pattern above
   - [ ] Add integration test that verifies maintenance triggered only when thresholds exceeded

5. **Integrate outcome-based resolver into execution flow**
   - [ ] After task execution completes, check if Delta write occurred
   - [ ] If yes, call `resolve_maintenance_from_execution(result, policy=location.resolved.delta_maintenance_policy, ...)`
   - [ ] If maintenance plan returned, schedule maintenance task
   - [ ] Record decision as artifact

6. **Add feature flag for outcome-based maintenance**
   - [ ] Add `USE_OUTCOME_BASED_MAINTENANCE` to `src/relspec/config.py` (default True)
   - [ ] When disabled, fall back to policy-presence-based maintenance
   - [ ] Add deprecation warning for policy-presence mode

**Track 4A Decommission Candidates:**
- Policy-presence-based maintenance logic in `resolve_delta_maintenance_plan()` (keep function, change logic to threshold-based)
- `_has_maintenance()` helper if it becomes redundant (check before removing)

---

**Track 4B: Scan Policy from Plan Signals**

**Representative Pattern - Before (Current):**

```python
# src/datafusion_engine/dataset/resolution.py:183-221
def apply_scan_unit_overrides(
    ctx: SessionContext,
    *,
    scan_units: Mapping[str, ScanUnit],
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> None:
    # Applies uniform scan policy to all scan units
    for name, scan_unit in scan_units.items():
        location = dataset_resolver.location(name)
        scan_options = resolve_delta_scan_options(
            location,
            scan_policy=runtime_profile.policies.scan_policy,  # UNIFORM
        )
        # Apply same options to all tables
```

**Representative Pattern - After (Target):**

```python
# src/datafusion_engine/dataset/resolution.py:183-221
def apply_scan_unit_overrides(
    ctx: SessionContext,
    *,
    scan_units: Mapping[str, ScanUnit],
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
    plan_bundle: DataFusionPlanBundle | None = None,  # NEW
) -> None:
    # Derives per-table scan policy from plan signals
    for name, scan_unit in scan_units.items():
        location = dataset_resolver.location(name)

        # Derive adaptive scan policy from plan + table characteristics
        adaptive_policy = derive_scan_policy_for_table(
            table_name=name,
            location=location,
            plan_bundle=plan_bundle,
            base_policy=runtime_profile.policies.scan_policy,
        )

        scan_options = resolve_delta_scan_options(
            location,
            scan_policy=adaptive_policy,  # ADAPTIVE
        )
```

**Track 4B Target Files:**
- `src/datafusion_engine/dataset/resolution.py` (lines 183-221)
- `src/datafusion_engine/delta/scan_config.py` (add adaptive policy derivation)
- `src/datafusion_engine/plan/bundle.py` (expose plan statistics for scan policy decisions)

**Track 4B Implementation Checklist:**

1. **Create scan policy derivation logic using existing plan_details + lineage**
   - [ ] Add `src/datafusion_engine/delta/scan_policy_inference.py` module
   - [ ] **Note:** `DataFusionPlanBundle` does NOT have `statistics_for_table()`,
     `has_filter_on_table()`, or `has_sort_match_for_table()` methods
     (`src/datafusion_engine/plan/bundle.py:116`). Do NOT add these methods.
   - [ ] Instead, derive adaptive signals from two existing structures:
     - `bundle.plan_details` (statistics, partition_count, repartition_count)
       (`src/datafusion_engine/plan/bundle.py:2071, 2274`)
     - `extract_lineage_from_bundle(bundle)` (scans, filters, joins, required_columns_by_dataset)
       (`src/datafusion_engine/views/bundle_extraction.py:43`)
   - [ ] Implement `derive_scan_policy_for_table()`:
     ```python
     from datafusion_engine.views.bundle_extraction import extract_lineage_from_bundle

     def derive_scan_policy_for_table(
         table_name: str,
         location: DatasetLocation,
         plan_bundle: DataFusionPlanBundle | None,
         base_policy: ScanPolicyConfig,
     ) -> ScanPolicyConfig:
         if plan_bundle is None:
             return base_policy

         # Extract signals from existing plan structures (no new bundle methods)
         plan_details = plan_bundle.plan_details
         lineage = extract_lineage_from_bundle(plan_bundle)

         adaptive = base_policy.copy()

         # Small tables: disable file pruning (overhead not worth it)
         stats = plan_details.get("statistics", {}) if plan_details else {}
         # Use canonical stats keys (bundle.py:2274, execution_plan.py:1575)
         row_count = stats.get("num_rows") or stats.get("row_count")
         if row_count is not None and row_count < 10_000:
             adaptive.enable_file_pruning = False

         # Tables with filter predicates from lineage: enable Parquet column pruning
         if lineage is not None and table_name in lineage.filters:
             adaptive.enable_parquet_pushdown = True

         # Tables with join keys from lineage: check sort compatibility
         if lineage is not None and table_name in lineage.joins:
             adaptive.enable_sort_order = True

         return adaptive
     ```

2. **Build extraction helper layer (not new DataFusionPlanBundle methods)**
   - [ ] Create helper functions in `src/datafusion_engine/delta/scan_policy_inference.py`
     that consume existing `bundle.plan_details` and `extract_lineage_from_bundle()`:
     ```python
     def table_has_filter(lineage: LineageReport, table_name: str) -> bool:
         return table_name in lineage.filters

     def table_stats_from_plan(plan_details: dict, table_name: str) -> dict | None:
         return plan_details.get("per_table_stats", {}).get(table_name)
     ```
   - [ ] **Benefits of this approach (per review 3.1):**
     - Lower API surface area (no new methods on DataFusionPlanBundle)
     - No duplication of plan-walk logic (reuses lineage extraction)
     - Reuses already-tested lineage path
   - [ ] Use capability detection (Section 8.2) for DataFusion statistics access

3. **Thread plan bundle through scan override infrastructure**
   - [ ] Update `apply_scan_unit_overrides()` signature to accept `plan_bundle: DataFusionPlanBundle | None = None`
   - [ ] Find all callers and pass plan bundle when available
   - [ ] Add integration test that verifies adaptive scan policies applied correctly

4. **Add feature flag for adaptive scan policies**
   - [ ] Add `USE_ADAPTIVE_SCAN_POLICIES` to `src/relspec/config.py` (default False initially)
   - [ ] When disabled, use uniform base policy
   - [ ] Add telemetry artifact recording adaptive vs. base policy differences

5. **Record scan policy decisions as artifacts**
   - [ ] For each table, record:
     ```python
     record_artifact(profile, "scan_policy_decision_v1", {
         "table_name": name,
         "base_policy": base_policy.to_dict(),
         "adaptive_policy": adaptive_policy.to_dict(),
         "reasons": ["small_table", "has_filter", ...],
     })
     ```

**Track 4B Decommission Candidates:**
- None (this is purely additive; uniform scan policy remains as fallback)

---

### Wave 5: Artifact Contract Governance (Medium-Large)

**Goal:** Centralize artifact naming/version/schema rules without duplicating
existing serde contracts.

**Scale correction:** Current `record_artifact(` usage is much larger than initially
estimated. `./cq search "record_artifact(" --literal` returned 183 matches across
41 files (not 65 as originally estimated).

**Current reusable substrate:**
- `src/serde_schema_registry.py`
- `src/serde_artifacts.py`

**Approach:**
1. Introduce artifact key registry + version governance layered on top of existing
   msgspec schema exports
2. Each artifact callsite references a spec, not a raw string:
   ```python
   record_artifact(profile, DELTA_MAINTENANCE_SPEC, payload)
   ```
3. The registry validates that payloads match the spec's schema fingerprint
4. Migrate by subsystem (planning -> write path -> hamilton lifecycle -> diagnostics)
5. Use an adapter that accepts both legacy string names and typed specs during
   transition, given the large blast radius

**Risk:** 183 matches across 41 files requires staged migration. Should not block
core pipeline modernization; run as parallel track with adapters.

---

#### Implementation Details: Wave 5

**Representative Pattern - Before (Current):**

```python
# src/datafusion_engine/lineage/diagnostics.py:585
def record_artifact(
    profile: DataFusionRuntimeProfile | None,
    name: str,  # RAW STRING
    payload: Mapping[str, Any],
) -> None:
    if profile is None or profile.diagnostics.diagnostics_sink is None:
        return

    # No schema validation
    profile.diagnostics.diagnostics_sink.record_artifact(name, payload)
```

**Representative Pattern - After (Target):**

```python
# src/datafusion_engine/lineage/diagnostics.py:585
def record_artifact(
    profile: DataFusionRuntimeProfile | None,
    spec: ArtifactSpec | str,  # TYPED SPEC or legacy string
    payload: Mapping[str, Any],
) -> None:
    if profile is None or profile.diagnostics.diagnostics_sink is None:
        return

    # Validate against spec schema if typed spec provided
    if isinstance(spec, ArtifactSpec):
        validate_artifact_payload(spec, payload)
        name = spec.canonical_name
    else:
        # Legacy string path (with deprecation warning)
        name = spec

    profile.diagnostics.diagnostics_sink.record_artifact(name, payload)
```

**Target Files:**
- `src/serde_schema_registry.py` (extend with artifact spec registry)
- `src/serde_artifacts.py` (add artifact spec definitions)
- `src/datafusion_engine/lineage/diagnostics.py` (update `record_artifact()` signature)
- All 183 callsites across 41 files (phased migration)

**Implementation Checklist:**

1. **Create `ArtifactSpec` protocol and registry**
   - [ ] Add to `src/serde_schema_registry.py`:
     ```python
     @dataclass(frozen=True)
     class ArtifactSpec:
         canonical_name: str  # e.g., "delta_maintenance_v1"
         schema_fingerprint: str  # Hash of msgspec schema
         payload_type: type  # msgspec.Struct subclass
         description: str

         def validate(self, payload: Mapping[str, Any]) -> None:
             # Validate payload against schema
             msgspec.convert(payload, self.payload_type)

     ARTIFACT_SPECS: dict[str, ArtifactSpec] = {}

     def register_artifact_spec(spec: ArtifactSpec) -> None:
         ARTIFACT_SPECS[spec.canonical_name] = spec
     ```

2. **Define artifact specs for existing artifacts**
   - [ ] Add to `src/serde_artifacts.py`:
     ```python
     # Define msgspec structs for each artifact type
     class DeltaMaintenanceArtifact(msgspec.Struct):
         operation: str
         files_removed: int | None = None
         files_added: int | None = None
         # ... all fields

     # Register specs
     DELTA_MAINTENANCE_SPEC = ArtifactSpec(
         canonical_name="delta_maintenance_v1",
         schema_fingerprint=hash_schema(DeltaMaintenanceArtifact),
         payload_type=DeltaMaintenanceArtifact,
         description="Delta table maintenance operation record",
     )
     register_artifact_spec(DELTA_MAINTENANCE_SPEC)
     ```
   - [ ] Identify top 20 most common artifact names from 183 callsites
   - [ ] Create specs for those 20 first (covers ~80% of usage)

3. **Update `record_artifact()` to support typed specs**
   - [ ] Change signature to accept `spec: ArtifactSpec | str`
   - [ ] Add validation logic when typed spec provided
   - [ ] Add deprecation warning when string name used
   - [ ] Add feature flag `USE_TYPED_ARTIFACT_SPECS` (default False initially)

4. **Create migration helper script**
   - [ ] Add `scripts/migrate_artifact_callsites.py`:
     ```python
     # Script to help migrate callsites from strings to typed specs
     # Scans codebase for record_artifact() calls
     # Suggests spec imports and replacements
     # Generates migration checklist
     ```

5. **Migrate by subsystem (staged rollout)**
   - [ ] **Phase 1: Planning artifacts** (10-15 callsites)
     - `src/datafusion_engine/plan/bundle.py`
     - `src/relspec/execution_plan.py`
   - [ ] **Phase 2: Write path artifacts** (20-25 callsites)
     - `src/datafusion_engine/io/write.py`
     - `src/datafusion_engine/delta/maintenance.py`
   - [ ] **Phase 3: Hamilton lifecycle artifacts** (30-40 callsites)
     - `src/hamilton_pipeline/modules/task_execution.py`
     - `src/hamilton_pipeline/driver_factory.py`
   - [ ] **Phase 4: Diagnostics artifacts** (40-50 callsites)
     - `src/datafusion_engine/views/graph.py`
     - `src/datafusion_engine/session/runtime.py`
   - [ ] **Phase 5: Remaining artifacts** (60-70 callsites)
     - All other files

6. **Add artifact schema evolution support**
   - [ ] Add `ArtifactSpec.version: int` field
   - [ ] Support multiple versions of same artifact (e.g., `delta_maintenance_v1`, `delta_maintenance_v2`)
   - [ ] Add schema migration helpers for version upgrades
   - [ ] Record schema version in artifact payload

7. **Create artifact catalog documentation**
   - [ ] Auto-generate `docs/artifacts/catalog.md` from registered specs
   - [ ] Include: name, version, schema, description, example payload
   - [ ] Add to CI to keep docs in sync with code

**Decommission Candidates:**
- None initially (string names supported during transition)
- After full migration: Remove string name support from `record_artifact()`, make `ArtifactSpec` required

**Scale Management:**
Given 183 callsites:
- **Don't block core waves** - run as parallel track
- **Use adapter pattern** - support both typed specs and strings during transition
- **Migrate incrementally** - 5 phases over multiple milestones
- **Auto-generate migration PRs** - use script to generate replacement code

---

## 6. Architecture Dependency Graph

The convergence waves have the following dependency structure:

```
Wave 0 (Reality Alignment) -- COMPLETE
  |
  v
Wave 1 (Resolver Threading Closure)
  |
  v
Wave 2 (Compile Boundary Convergence)
  |
  +---> Wave 3 (Extract Executor Binding)    [can run after Wave 1]
  |
  +---> Wave 4 (Policy/Maintenance Intelligence)  [can run after Wave 2]
  |
  v
Wave 5 (Artifact Contract Governance)  [parallel track, independent]
```

**Critical path:** Wave 0 -> Wave 1 -> Wave 2 -> Wave 4

**Independent tracks:**
- Wave 3 (extract binding) - can proceed after Wave 1
- Wave 5 (artifact governance) - can proceed any time as staged infrastructure program

---

## 7. Measuring Progress

### Preferred Metrics Commands

Use `./cq`-driven metrics for accuracy and context:

```bash
./cq search dataset_bindings_for_profile
./cq search "CompileContext(runtime_profile" --literal
./cq search _EXTRACT_EXECUTOR_REGISTRATION_STATE
./cq search _EXTRACT_ADAPTER_EXECUTORS
./cq search build_semantic_execution_context
./cq search "record_artifact(" --literal
./cq search build_task_execution_module
./cq search TaskPlanMetrics
```

**Interpretation guidance:**
- Separate `src/` production matches from test/script matches
- Track trend by artifact snapshots in `.cq/artifacts/` between milestones

### Drift Surface Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| `dataset_bindings_for_profile()` consumer callsites in `src/` | 7 | 0 | `./cq search dataset_bindings_for_profile` (filter to src/, exclude compile_context definition) |
| Direct `CompileContext(runtime_profile=` outside compile boundary | 7 | 0 | `./cq search "CompileContext(runtime_profile" --literal` (filter to src/, exclude compile_context.py) |
| Global mutable registries in pipeline | 1 | 0 | `_EXTRACT_ADAPTER_EXECUTORS` |
| Redundant `compile_semantic_program()` calls per pipeline run | ~3 | 1 | Trace compile calls during integration test |
| `record_artifact(` production callsites | ~180 | ~180 (typed) | `./cq search "record_artifact(" --literal` |

**Counting convention:** Consumer-only, `src/` only. Track helper definitions separately
from callsites. For removal goals, mark as complete only when both callsites and fallback
guards are removed.

### Invariant Checks

These should be enforced as assertions or test checks:

1. **Single compilation:** A pipeline run calls `compile_semantic_program()` exactly once.
   **Concrete entrypoint ownership:** `build_semantic_execution_context(...)` is the canonical
   compile entrypoint. Current callsites (`src/datafusion_engine/plan/pipeline.py:80`,
   `src/datafusion_engine/session/runtime.py:4572`) must converge to a single authority call
   per pipeline run, with the resulting context passed downstream. Enforce with test-level
   compile counters (e.g., `CompileTracker` context manager that increments on each compile
   and asserts count == 1 at pipeline end)
2. **Resolver identity:** All subsystems in a pipeline run reference the same
   `ManifestDatasetResolver` instance (identity check, not equality)
3. **Plan-schedule consistency:** Every task in `TaskSchedule.ordered_tasks` has a
   corresponding `InferredDeps` with matching `plan_fingerprint`
4. **UDF availability:** Every `required_udf` in every `InferredDeps` exists in the
   UDF snapshot at registration time
5. **No orphan registrations:** Every view registered in DataFusion appears in the
   view graph and has a corresponding `ViewNode`

---

## 8. Expanded Scope Recommendations

### 8.1 Add Resolver Identity Invariants

The architecture goal is not only value equality but single-authority identity. Add
explicit identity checks in integration tests to ensure one resolver instance is
threaded through view registration, scan overrides, CDF, and readiness diagnostics.

**Implementation:**
- [ ] Create `src/semantics/resolver_identity.py` module
- [ ] Add `ResolverIdentityTracker` context manager
- [ ] Use in integration tests to verify single resolver instance

### 8.2 Add a DataFusion Capability Adapter

Create a small compatibility layer for plan statistics capability detection so code
does not depend on assumed `ExecutionPlan` methods. This should gate metric scheduling
fields and annotate diagnostics with capability status.

Given that DataFusion 51.0.0 does not expose callable `statistics()` or `schema()`
directly on `ExecutionPlan`, capability detection (not version assumptions) is the
correct approach for any stats-dependent code paths.

**Implementation:**
- [ ] Create `src/datafusion_engine/capabilities.py` module
- [ ] Define `DataFusionCapabilities` protocol
- [ ] Implement capability detection:
  ```python
  @dataclass(frozen=True)
  class DataFusionCapabilities:
      has_execution_plan_statistics: bool
      has_execution_plan_schema: bool
      datafusion_version: str

  def detect_capabilities(ctx: SessionContext) -> DataFusionCapabilities:
      # Probe for available methods
      plan = ctx.sql("SELECT 1").execution_plan()
      return DataFusionCapabilities(
          has_execution_plan_statistics=hasattr(plan, "statistics") and callable(getattr(plan, "statistics", None)),
          has_execution_plan_schema=hasattr(plan, "schema") and callable(getattr(plan, "schema", None)),
          datafusion_version=datafusion.__version__,
      )
  ```
- [ ] Use in all stats-dependent code paths (Wave 4B scan policy, schedule cost signals)

### 8.3 Introduce Drift Audits as CI Guardrails

Use `./cq`-based checks for:
- `dataset_bindings_for_profile` production callsites
- Direct `CompileContext(runtime_profile` usage outside compile boundary modules
- Global extract registry usage surfaces

These can run as part of the existing cutover checking script or as standalone CI
assertions.

**Implementation:**
- [ ] Create `scripts/check_drift_surfaces.sh`
- [ ] Add checks for each drift metric from Section 7
- [ ] Add to CI workflow (non-blocking warnings initially)
- [ ] Make blocking after Wave 1 and Wave 2 complete

### 8.4 Add DataFusion Capability Snapshot Artifact

Record plan-stat capability flags per runtime startup to explain why adaptive features
are enabled/disabled. This makes diagnostics self-documenting when stats-dependent
features (Wave 4B scan policy, scheduling cost signals) are capability-gated.

**Implementation:**
- [ ] At runtime startup, after `detect_capabilities()` (Section 8.2), record as artifact:
  ```python
  record_artifact(profile, "datafusion_capabilities_v1", {
      "version": capabilities.datafusion_version,
      "has_execution_plan_statistics": capabilities.has_execution_plan_statistics,
      "has_execution_plan_schema": capabilities.has_execution_plan_schema,
  })
  ```
- [ ] Reference this artifact in adaptive feature decisions (scan policy, write policy, scheduling)

### 8.5 Add Migration-Safe API Contracts for Execution Context Extensions

If extending `SemanticExecutionContext` (Wave 2 adds `evidence_plan`, Wave 3 adds
`extract_executor_map`), version/fingerprint impact should be explicitly documented
and tested. Each new field added to the context changes the execution contract.

**Implementation:**
- [ ] When adding fields to `SemanticExecutionContext`, add corresponding version bump
- [ ] Add integration tests that verify fingerprint changes when new fields added
- [ ] Document field addition in migration notes for each wave
- [ ] Ensure all new optional fields default to `None` for backward compatibility

### 8.6 Stage Artifact Registry by Blast Radius

Given current call volume (183 matches across 41 files), migrate artifact identifiers
in phases with an adapter that accepts both legacy string names and typed specs during
transition. Migrate by subsystem: planning -> write path -> hamilton lifecycle -> diagnostics.

(See Wave 5 implementation details above for full staging plan)

### 8.7 Separate Semantic Compile Authority from Orchestration Execution Authority (AUTHORITATIVE)

**Status: This section is the authoritative design decision for context layering.**
Wave 3 and all subsequent waves must follow this pattern. `SemanticExecutionContext` must
remain semantic-only. All orchestration-layer fields go into `ExecutionAuthorityContext`.

**Rationale:** Extending `SemanticExecutionContext` with non-semantic fields (`evidence_plan`,
`extract_executor_map`) would couple the semantic compile boundary to orchestration execution
concerns, violating clean layer separation and making independent versioning impossible.

**Design:**
- Keep `SemanticExecutionContext` focused on semantic compile artifacts
  (manifest, dataset_resolver, runtime_profile, ctx, facade).
- Introduce `ExecutionAuthorityContext` that composes:
  - `semantic_context: SemanticExecutionContext`
  - `evidence_plan: EvidencePlan | None`
  - `extract_executor_map: Mapping[str, Any] | None`
  - `capability_snapshot: DataFusionCapabilities | None` (from Section 8.2)
  - `session_runtime_fingerprint: str | None`
- Version the two contracts independently.

**Representative Pattern:**

```python
# src/relspec/execution_authority.py (NEW)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.capabilities import DataFusionCapabilities
    from extract.coordination.evidence_plan import EvidencePlan
    from semantics.compile_context import SemanticExecutionContext

@dataclass(frozen=True)
class ExecutionAuthorityContext:
    """Orchestration authority context composing semantic + execution concerns.

    Waves 1-2 thread SemanticExecutionContext (semantic compile artifacts only).
    Waves 3+ build ExecutionAuthorityContext on top for orchestration fields.
    """

    semantic_context: SemanticExecutionContext
    evidence_plan: EvidencePlan | None = None
    extract_executor_map: Mapping[str, Any] | None = None
    capability_snapshot: DataFusionCapabilities | None = None
    session_runtime_fingerprint: str | None = None

    # Convenience delegation to semantic context
    @property
    def manifest(self) -> SemanticProgramManifest:
        return self.semantic_context.manifest

    @property
    def dataset_resolver(self) -> ManifestDatasetResolver:
        return self.semantic_context.dataset_resolver
```

**Target Files:**
- `src/relspec/execution_authority.py` (NEW - define `ExecutionAuthorityContext`)
- `src/semantics/compile_context.py` (UNCHANGED - `SemanticExecutionContext` stays semantic-only)
- `src/hamilton_pipeline/modules/task_execution.py` (Wave 3 - consume `ExecutionAuthorityContext`)
- `src/hamilton_pipeline/driver_factory.py` (Wave 3 - build and thread `ExecutionAuthorityContext`)

**Implementation Checklist:**

1. **Define `ExecutionAuthorityContext`**
   - [ ] Create `src/relspec/execution_authority.py` with frozen dataclass
   - [ ] Compose `SemanticExecutionContext` (not inherit)
   - [ ] Add convenience delegation properties for common access patterns
   - [ ] Add `__post_init__` validation (e.g., executor map non-None when evidence plan present)

2. **Build `ExecutionAuthorityContext` at orchestration boundary**
   - [ ] After `build_semantic_execution_context()` and `compile_execution_plan()`:
     ```python
     semantic_ctx = build_semantic_execution_context(...)
     execution_plan = compile_execution_plan(...)
     authority = ExecutionAuthorityContext(
         semantic_context=semantic_ctx,
         evidence_plan=evidence_plan,
         extract_executor_map=build_executor_map(...) if evidence_plan else None,
         capability_snapshot=detect_capabilities(semantic_ctx.ctx),
     )
     ```

3. **Thread `ExecutionAuthorityContext` through Waves 3+**
   - [ ] Wave 3 task execution consumes `authority_context.extract_executor_map`
   - [ ] Wave 4 policy logic consumes `authority_context.capability_snapshot`
   - [ ] 10.10 validation consumes `authority_context.semantic_context.manifest`

4. **Keep Waves 1-2 focused on `SemanticExecutionContext`**
   - [ ] Wave 1: Thread `dataset_resolver` from `SemanticExecutionContext`
   - [ ] Wave 2: Thread `SemanticExecutionContext` through compile boundary
   - [ ] No orchestration fields added to `SemanticExecutionContext`

**Decommission Candidates:**
- Ad-hoc extraction of orchestration state from `runtime_profile` in task execution paths
  (replaced by `ExecutionAuthorityContext` fields)
- Global mutable executor registry access patterns (replaced by `authority_context.extract_executor_map`)

### 8.8 Unified `PlanSignals` Control Plane (MANDATORY for Waves 4/10.x)

**Status: Mandatory prerequisite for all plan-signal-consuming proposals.**
All consumers (10.2, 10.3, 10.9, 10.10, Wave 4B) must read `PlanSignals`, not raw
`plan_details` or ad-hoc bundle accessors.

Multiple proposals need schema/stats/lineage/predicate signals from `DataFusionPlanBundle`.
Without a shared typed control plane, each proposal invents ad-hoc plan signal reads
with inconsistent key usage and repeated plan walking.

**Representative Pattern:**

```python
# src/datafusion_engine/plan/signals.py (NEW)
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion_engine.lineage.datafusion import LineageReport
    from datafusion_engine.plan.bundle import DataFusionPlanBundle

@dataclass(frozen=True)
class NormalizedPlanStats:
    """Normalized stats payload with source provenance."""

    num_rows: int | None           # canonical key (bundle.py:2274)
    total_bytes: int | None        # canonical key
    partition_count: int | None
    stats_source: str              # "plan_details" | "execution_plan" | "unavailable"

@dataclass(frozen=True)
class ScanUnitCompatSummary:
    """Per-scan-unit protocol compatibility summary."""

    dataset_name: str
    compatible: bool | None        # True/False/None (unconfigured)
    reason: str | None

@dataclass(frozen=True)
class PlanSignals:
    """Typed signal bundle derived from DataFusionPlanBundle.

    Single source of truth for all plan-signal consumers.
    """

    schema: pa.Schema | None                        # arrow_schema_from_df(bundle.df)
    lineage: LineageReport | None                    # extract_lineage_from_bundle(bundle)
    stats: NormalizedPlanStats | None                # normalized from plan_details
    scan_compat: tuple[ScanUnitCompatSummary, ...]   # per-scan-unit Delta compat
    plan_fingerprint: str | None                     # determinism field

def extract_plan_signals(
    bundle: DataFusionPlanBundle,
    *,
    scan_units: Sequence[ScanUnit] | None = None,
) -> PlanSignals:
    """Extract typed signals from a plan bundle.

    Reuses existing helpers; does not add methods to DataFusionPlanBundle.
    """
    from datafusion_engine.views.bundle_extraction import (
        arrow_schema_from_df,
        extract_lineage_from_bundle,
    )

    schema = arrow_schema_from_df(bundle.df) if bundle.df is not None else None
    lineage = extract_lineage_from_bundle(bundle)

    # Normalize stats using canonical keys (bundle.py:2274, execution_plan.py:1575)
    plan_details = bundle.plan_details or {}
    raw_stats = plan_details.get("statistics", {})
    stats = NormalizedPlanStats(
        num_rows=raw_stats.get("num_rows") or raw_stats.get("row_count"),
        total_bytes=raw_stats.get("total_bytes"),
        partition_count=plan_details.get("partition_count"),
        stats_source="plan_details" if raw_stats else "unavailable",
    )

    # Scan unit compatibility summary
    compat_summaries = tuple(
        ScanUnitCompatSummary(
            dataset_name=u.dataset_name,
            compatible=u.protocol_compatibility.compatible if u.protocol_compatibility else None,
            reason=str(u.protocol_compatibility) if u.protocol_compatibility else None,
        )
        for u in (scan_units or ())
    )

    return PlanSignals(
        schema=schema,
        lineage=lineage,
        stats=stats,
        scan_compat=compat_summaries,
        plan_fingerprint=plan_details.get("fingerprint"),
    )
```

**Target Files:**
- `src/datafusion_engine/plan/signals.py` (NEW)
- `src/datafusion_engine/views/bundle_extraction.py` (reuse `arrow_schema_from_df`, `extract_lineage_from_bundle`)
- All consumer proposals (10.2, 10.3, 10.9, 10.10, Wave 4B)

**Implementation Checklist:**

1. **Create `PlanSignals` module**
   - [ ] Create `src/datafusion_engine/plan/signals.py`
   - [ ] Define `NormalizedPlanStats`, `ScanUnitCompatSummary`, `PlanSignals` as frozen dataclasses
   - [ ] Implement `extract_plan_signals()` reusing existing helpers

2. **Normalize all stats access through `PlanSignals.stats`**
   - [ ] Use canonical keys (`num_rows`, `row_count`) from `plan_details`
   - [ ] Include source provenance to distinguish stats origins
   - [ ] Gate with capability detection (Section 8.2) when stats unavailable

3. **Update all consumers to use `PlanSignals`**
   - [ ] 10.2 schema divergence: use `signals.schema` instead of `arrow_schema_from_df(plan_bundle.df)`
   - [ ] 10.3 write policy: use `signals.stats.num_rows` instead of raw plan_details access
   - [ ] 10.9 scan policy: use `signals.lineage` and `signals.stats` instead of ad-hoc reads
   - [ ] 10.10 policy validation: use `signals.scan_compat` for Delta protocol checks
   - [ ] Wave 4B scan policy: use `signals.lineage` and `signals.stats`

4. **Add `PlanSignals` to execution plan artifacts**
   - [ ] Record signals as diagnostic artifact at plan compilation time
   - [ ] Include `plan_fingerprint` for deterministic comparison across runs

**Decommission Candidates:**
- Ad-hoc `plan_details` reads in consumer code (replaced by `PlanSignals` typed access)
- Direct `arrow_schema_from_df()` / `extract_lineage_from_bundle()` calls in individual
  proposals (centralized in `extract_plan_signals()`)

### 8.9 Reuse Existing Delta Compatibility Machinery End-to-End

Standardize all Delta protocol support checks on `delta_protocol_compatibility()`
(`src/datafusion_engine/delta/protocol.py:68`) and one canonical artifact payload shape.
Avoid custom feature-set arithmetic in new code paths.

**Implementation:**
- [ ] All new Delta protocol checks (10.10, Wave 4A, Wave 4B) call
  `delta_protocol_compatibility()` rather than inventing set arithmetic
- [ ] Define a canonical Delta protocol artifact shape for diagnostics

### 8.10 Add Phase-Level Invariants for "Single Compile, Multiple Consumers"

Enforce invariants in tests and optionally runtime assertions:
1. Semantic compile invoked once per pipeline run
2. Resolver identity reused through registration, scan overrides, CDF, execution
3. Execution-plan signature captures capability snapshot and policy validation summary

**Implementation:**
- [ ] Add to integration test suite (extends Section 8.1 resolver identity invariants)
- [ ] Optionally enforce via runtime assertions (can disable via feature flag)
- [ ] Execution-plan fingerprint should include capability snapshot hash

### 8.11 Performance Architecture Track (DataFusion/Delta)

Adopt a formal performance policy section tied to runtime config snapshots and plan
signatures. This ensures performance tuning decisions are traceable and reproducible.

**Representative Pattern:**

```python
# src/datafusion_engine/plan/perf_policy.py (NEW)
from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True)
class CachePolicyTier:
    """Cache policy tier configuration."""

    listing_cache_ttl_seconds: int | None = None
    listing_cache_max_entries: int | None = None
    metadata_cache_max_entries: int | None = None
    enable_dataframe_cache: bool = False  # selective DataFrame.cache() for fanout nodes

@dataclass(frozen=True)
class StatisticsPolicy:
    """Statistics collection stance for plan compilation."""

    collect_statistics: bool = True
    meta_fetch_concurrency: int = 4
    fallback_when_unavailable: str = "skip"  # "skip" | "estimate" | "error"

@dataclass(frozen=True)
class PlanBundleComparisonPolicy:
    """Deterministic plan bundle comparison for regression detection."""

    retain_p0_artifacts: bool = True   # plan structure hash
    retain_p1_artifacts: bool = True   # physical plan details
    retain_p2_artifacts: bool = False  # full plan serialization
    enable_diff_gates: bool = False    # fail on unexpected plan regressions

@dataclass(frozen=True)
class PerformancePolicy:
    """Top-level performance policy tied to runtime config snapshot."""

    cache: CachePolicyTier = CachePolicyTier()
    statistics: StatisticsPolicy = StatisticsPolicy()
    comparison: PlanBundleComparisonPolicy = PlanBundleComparisonPolicy()
```

**Target Files:**
- `src/datafusion_engine/plan/perf_policy.py` (NEW)
- `src/datafusion_engine/session/runtime.py` (integrate `PerformancePolicy` into `DataFusionRuntimeProfile`)
- `src/datafusion_engine/plan/bundle.py` (wire plan bundle comparison)

**Implementation Checklist:**

1. **Define performance policy types**
   - [ ] Create `src/datafusion_engine/plan/perf_policy.py` with frozen dataclasses
   - [ ] Define `CachePolicyTier`, `StatisticsPolicy`, `PlanBundleComparisonPolicy`
   - [ ] Compose into `PerformancePolicy`

2. **Integrate cache policy tiers**
   - [ ] Wire `listing_cache_ttl_seconds` and `listing_cache_max_entries` to DataFusion
     session config where supported
   - [ ] Add selective `DataFrame.cache()` for fanout-heavy intermediate nodes when
     `enable_dataframe_cache` is True
   - [ ] Metadata cache limit for Delta table metadata

3. **Integrate statistics policy**
   - [ ] Wire `collect_statistics` to DataFusion session configuration
   - [ ] Wire `meta_fetch_concurrency` to Delta table scan parallelism
   - [ ] Implement fallback behavior when stats unavailable (per Section 8.2 capability detection)

4. **Implement plan bundle comparison**
   - [ ] Retain P0/P1/P2 artifacts based on policy
   - [ ] Implement diff gates that detect plan structure regressions
   - [ ] Record comparison results as diagnostic artifacts

5. **Record performance policy as artifact**
   - [ ] At runtime startup, record active performance policy:
     ```python
     record_artifact(profile, "performance_policy_v1", {
         "cache": asdict(policy.cache),
         "statistics": asdict(policy.statistics),
         "comparison": asdict(policy.comparison),
     })
     ```

**Decommission Candidates:**
- Ad-hoc cache configuration scattered across session setup (consolidated into `PerformancePolicy`)

---

### 8.12 Fidelity and Safety Track

Add hard invariant tests and structured validation artifacts for correctness guarantees.

**Representative Pattern:**

```python
# src/semantics/compile_invariants.py (extends Section 8.10)
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from semantics.compile_context import SemanticExecutionContext

@dataclass
class CompileTracker:
    """Context manager that enforces single compile per pipeline run.

    Tracks compile invocations and verifies invariants at pipeline end.
    """

    _compile_count: int = 0
    _resolver_ids: list[int] = field(default_factory=list)

    def record_compile(self, ctx: SemanticExecutionContext) -> None:
        self._compile_count += 1
        self._resolver_ids.append(id(ctx.dataset_resolver))

    def verify_invariants(self) -> list[str]:
        violations = []
        if self._compile_count != 1:
            violations.append(
                f"Expected 1 compile call, got {self._compile_count}"
            )
        if len(set(self._resolver_ids)) > 1:
            violations.append(
                f"Resolver identity violation: {len(set(self._resolver_ids))} distinct resolvers"
            )
        return violations

# src/relspec/policy_validation.py (structured artifact schema)
@dataclass(frozen=True)
class PolicyValidationArtifact:
    """Machine-readable policy validation artifact."""

    validation_mode: str                    # "error" | "warn" | "off"
    issue_count: int
    error_codes: tuple[str, ...]            # e.g., ("UDF_MISSING", "DELTA_PROTOCOL_INCOMPATIBLE")
    runtime_hash: str                       # for determinism verification
    is_deterministic: bool                  # True if same result under same runtime hash
```

**Target Files:**
- `src/semantics/compile_invariants.py` (NEW - `CompileTracker` context manager)
- `src/relspec/policy_validation.py` (NEW or extend `execution_plan.py` - structured validation)
- `tests/integration/` (invariant test suites)

**Implementation Checklist:**

1. **Implement `CompileTracker` context manager**
   - [ ] Create `src/semantics/compile_invariants.py`
   - [ ] Track compile invocations and resolver identity
   - [ ] Add `verify_invariants()` that returns violation list
   - [ ] Wire into all orchestration entry points

2. **Add hard invariant tests**
   - [ ] Single semantic compile per run (compile counter == 1)
   - [ ] Resolver identity continuity across registration, scan override, and execution
   - [ ] Policy validation determinism under fixed runtime hash
   - [ ] Add to integration test suite

3. **Add structured policy validation artifacts**
   - [ ] Define `PolicyValidationArtifact` with machine-readable codes
   - [ ] Record after each policy validation run
   - [ ] Include runtime hash for determinism verification
   - [ ] Support `error|warn|off` rollout modes (per 10.10)

4. **Add execution-plan fingerprint extensions**
   - [ ] Include capability snapshot hash in execution-plan fingerprint
   - [ ] Include policy validation summary hash
   - [ ] Use for regression detection across pipeline runs

**Decommission Candidates:**
- None (this is net-new invariant infrastructure)

---

## 9. What NOT to Change

Several patterns appear to be static declarations but are actually programmatic:

1. **Semantic spec registry** (`SEMANTIC_TABLE_SPECS`, `RELATIONSHIP_SPECS`) -
   These are declarative specifications that *define* the data operations mapping.
   They are one of the two core inputs (alongside extracted data) that drive the
   pipeline. Removing them would eliminate the specification layer entirely.

2. **Extract metadata specs** (`extract_metadata_specs()`) - These define what
   extractors produce. They are the contract between the extract layer and the
   semantic layer. They should remain declarative.

3. **Schema specs** (`schema_spec/`) - JSON Schema definitions for CPG contracts.
   These are output contracts, not pipeline configuration.

4. **DataFusion session configuration** (`DataFusionRuntimeProfile.policies`) -
   This is runtime infrastructure configuration (thread counts, memory limits,
   scan policies). It correctly lives outside the semantic manifest.

5. **Dataset-level policy declarations** - Policy presence on dataset locations
   should remain as authoritative defaults. Plan outcomes should tune/override
   where safe, not replace the policy model.

The goal is not to eliminate all configuration, but to ensure that *pipeline
behavior* (what runs, in what order, with what data) is derived from the manifest
rather than independently declared.

---

## 10. Extended Proposals: Corrected Dispositions

The following proposals extend beyond the initial convergence waves. Each has been
reviewed and assigned a disposition reflecting verified code state.

### 10.1 Eliminate Static Output Naming (`SEMANTIC_OUTPUT_NAMES`) - KEEP (Narrow)

**Disposition:** Keep, but narrow scope.

**Current state:** `semantics/naming.py` maintains a static dict `SEMANTIC_OUTPUT_NAMES`
(lines 12-37) that maps internal view names to canonical output names. A static map
exists at `src/semantics/naming.py:12`.

**Programmatic approach:**
1. Derive output names from `SemanticIR.views` at compile time
2. Build the name mapping as part of `SemanticProgramManifest`:
   ```python
   @dataclass(frozen=True)
   class SemanticProgramManifest:
       ...
       output_name_map: Mapping[str, str]  # internal -> canonical
   ```
3. Replace `canonical_output_name()` calls with manifest lookups
4. Migration should be manifest-backed with a compatibility shim during transition

**Scope:** Small. The naming module is 77 lines.

---

#### Implementation Details: 10.1

**Representative Pattern - Before (Current):**

```python
# src/semantics/naming.py:12-37
SEMANTIC_OUTPUT_NAMES: Final[dict[str, str]] = {
    "scip_occurrences_norm": "scip_occurrences_norm",
    "cst_refs_norm": "cst_refs_norm",
    # ... 22+ entries, currently identity mapping
}

def canonical_output_name(internal_name: str) -> str:
    return SEMANTIC_OUTPUT_NAMES.get(internal_name, internal_name)
```

**Representative Pattern - After (Target):**

```python
# src/semantics/compile_context.py:39
@dataclass(frozen=True)
class SemanticProgramManifest:
    semantic_ir: SemanticIR
    dataset_bindings: ManifestDatasetBindings
    validation: InputValidation
    fingerprint: str
    output_name_map: Mapping[str, str]  # NEW: internal -> canonical

# src/semantics/naming.py:12-37
def canonical_output_name(
    internal_name: str,
    *,
    manifest: SemanticProgramManifest | None = None,
) -> str:
    # Prefer manifest mapping when available
    if manifest is not None:
        return manifest.output_name_map.get(internal_name, internal_name)

    # FALLBACK: legacy static map (with deprecation warning)
    return SEMANTIC_OUTPUT_NAMES.get(internal_name, internal_name)
```

**Target Files:**
- `src/semantics/naming.py` (entire module, 92 lines)
- `src/semantics/compile_context.py` (add `output_name_map` field to manifest)
- `src/semantics/compiler.py` (build output name map during IR compilation)

**Implementation Checklist:**

1. **Add output name map to manifest**
   - [ ] Add `output_name_map: Mapping[str, str]` field to `SemanticProgramManifest`
   - [ ] Add to manifest fingerprint calculation

2. **Build output name map during IR compilation**
   - [ ] In `src/semantics/compiler.py`, after building `SemanticIR`:
     ```python
     output_name_map = {}
     for view in semantic_ir.views:
         # Derive canonical name from view metadata
         canonical = _derive_canonical_output_name(view)
         output_name_map[view.internal_name] = canonical
     ```
   - [ ] Currently most names are identity mapping, so start with that
   - [ ] Future: add naming rules/policies to view metadata

3. **Update `canonical_output_name()` to prefer manifest**
   - [ ] Change signature to accept optional manifest parameter
   - [ ] Use manifest mapping when provided
   - [ ] Fall back to static map with deprecation warning

4. **Thread manifest through output naming callsites**
   - [ ] Find all calls to `canonical_output_name()` (scan with `/cq search canonical_output_name`)
   - [ ] For each callsite, pass manifest if available
   - [ ] Mark callsites that can't access manifest for future cleanup

5. **Deprecate static `SEMANTIC_OUTPUT_NAMES` dict**
   - [ ] Add deprecation comment
   - [ ] Add runtime warning when fallback used
   - [ ] After all callsites migrated, remove dict entirely

**Decommission Candidates:**
- `SEMANTIC_OUTPUT_NAMES` static dict in `src/semantics/naming.py` (after all callsites migrated)
- Fallback logic in `canonical_output_name()` (after all callsites thread manifest)

---

### 10.2 Plan-Derived Schema Contracts - REVISE (Tightening + Telemetry)

**Disposition:** Revise. Contract derivation/enforcement already present; focus on
divergence detection and telemetry.

**Verified state:** Contracts already derive from dataset specs and are enforced during
registration (`src/datafusion_engine/views/registry_specs.py:280`,
`src/datafusion_engine/views/graph.py:353`).

**Revised approach:**
1. After `build_plan_bundle()`, compare plan output schema against the dataset spec's
   declared schema
2. Record schema evolution events when plan schema diverges from spec schema
3. Add divergence telemetry: log when plan output columns exceed or differ from spec
4. The contract should validate that the plan produces what the spec *requires*, not
   that it matches exactly - tighten existing checks, don't rebuild

**Integration point:** `_build_semantic_view_node()` at `registry_specs.py:410-468`

---

#### Implementation Details: 10.2

**Representative Pattern - Before (Current):**

```python
# src/datafusion_engine/views/registry_specs.py:280
def _dataset_contract_for(
    name: str,
    *,
    dataset_specs: Mapping[str, DatasetSpec],
) -> tuple[pa.Schema | None, bool]:
    # Derives contract from spec, optional validation
    # Returns (schema, strict_flag)
    ...
```

**Representative Pattern - After (Target):**

Keep `_dataset_contract_for()` focused on spec-contract derivation (per review 3.3).
Add divergence telemetry and optional strict enforcement in `_build_semantic_view_node()`
where both contract and `plan_bundle` are present (`src/datafusion_engine/views/registry_specs.py:410`):

```python
# src/datafusion_engine/views/registry_specs.py:410+
def _build_semantic_view_node(
    name: str,
    *,
    dataset_specs: Mapping[str, DatasetSpec],
    plan_bundle: DataFusionPlanBundle | None = None,  # NEW
    # ... existing params
) -> ViewNode:
    # Existing contract derivation (unchanged)
    contract_schema, strict = _dataset_contract_for(name, dataset_specs=dataset_specs)

    # NEW: Divergence detection at node build point
    # Note: DataFusionPlanBundle has no `output_schema` property (bundle.py:116).
    # Use existing helper: arrow_schema_from_df(plan_bundle.df) (bundle_extraction.py:19)
    if plan_bundle is not None and contract_schema is not None:
        from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
        plan_schema = arrow_schema_from_df(plan_bundle.df)
        divergence = compute_schema_divergence(contract_schema, plan_schema)

        if divergence.has_divergence:
            record_artifact(profile, SCHEMA_DIVERGENCE_SPEC, {
                "view_name": name,
                "spec_columns": list(contract_schema.names),
                "plan_columns": list(plan_schema.names),
                "added_columns": divergence.added_columns,
                "removed_columns": divergence.removed_columns,
                "type_mismatches": divergence.type_mismatches,
            })

            if strict:
                raise SchemaContractViolation(
                    f"Strict schema divergence for {name!r}: {divergence}"
                )

    # ... rest of node construction
```

**Target Files:**
- `src/datafusion_engine/views/registry_specs.py` (lines 280-290 unchanged, 410-468 extended)
- `src/datafusion_engine/views/graph.py` (lines 353-382)
- `src/datafusion_engine/schema/contracts.py` (add divergence computation)

**Implementation Checklist:**

1. **Create schema divergence computation**
   - [ ] Add to `src/datafusion_engine/schema/contracts.py`:
     ```python
     @dataclass(frozen=True)
     class SchemaDivergence:
         has_divergence: bool
         added_columns: list[str]
         removed_columns: list[str]
         type_mismatches: list[tuple[str, str, str]]  # (col, spec_type, plan_type)

     def compute_schema_divergence(
         spec_schema: pa.Schema,
         plan_schema: pa.Schema,
     ) -> SchemaDivergence:
         # Compare schemas and identify differences
         ...
     ```

2. **Add divergence detection in `_build_semantic_view_node()` (not in `_dataset_contract_for`)**
   - [ ] Keep `_dataset_contract_for()` signature and behavior unchanged
     - Current: `_dataset_contract_for(name: str, *, dataset_specs: Mapping[str, DatasetSpec]) -> tuple[pa.Schema | None, bool]`
   - [ ] Add divergence telemetry in `_build_semantic_view_node()` where both contract and plan_bundle are present
   - [ ] Ensure plan bundle available at node build time

3. **Record divergence as structured artifact**
   - [ ] Define `SCHEMA_DIVERGENCE_SPEC` in `src/serde_artifacts.py`
   - [ ] Record divergence when detected
   - [ ] Include severity classification (warning vs error)

4. **Tighten contract validation**
   - [ ] Current validation is optional; make it required for critical outputs
   - [ ] Add `strict_schema_validation: bool` flag to dataset specs
   - [ ] When strict, fail registration if divergence exceeds threshold

5. **Add integration test for schema evolution detection**
   - [ ] Create test that modifies a view to add/remove columns
   - [ ] Verify divergence artifact recorded
   - [ ] Verify validation behavior with strict vs. permissive mode

**Decommission Candidates:**
- None (this tightens existing logic, doesn't remove anything)

---

### 10.3 Manifest-Driven Write Policy Enrichment - REVISE (Enrich, Not Replace)

**Disposition:** Revise. Policy layering already exists; enrich with plan statistics,
do not replace the location policy model.

**Verified state:** Existing `_delta_policy_context` already merges
options + dataset policy + schema/lineage stats columns
(`src/datafusion_engine/io/write.py:288`).

**Revised approach:**
1. During execution plan compilation, extract write-relevant metadata:
   - Stats columns from the plan's output projection
   - Target file size from row count estimates (via physical plan statistics,
     using capability detection per Section 8.2)
2. Enrich the existing `_delta_policy_context` cascade with plan-derived signals
3. Do not replace the dataset-level policy declaration model

**Benefit:** Write policies become more adaptive without disrupting the existing
three-level cascade.

---

#### Implementation Details: 10.3

**Representative Pattern - Before (Current):**

```python
# src/datafusion_engine/io/write.py:288-345
def _delta_policy_context(
    *,
    options: WriteDatasetOptions,
    dataset_location: DatasetLocation,
    request_partition_by: tuple[str, ...],
    schema_columns: Sequence[str] | None = None,
    lineage_columns: Sequence[str] | None = None,
) -> _DeltaPolicyContext:
    # THREE-LEVEL CASCADE:
    # 1. Override from options
    # 2. Location.resolved.delta_write_policy
    # 3. None

    write_policy = options.delta_write_policy or dataset_location.resolved.delta_write_policy

    # Stats columns derived from schema/lineage
    stats_columns = _derive_stats_columns(schema_columns, lineage_columns)

    return _DeltaPolicyContext(
        write_policy=write_policy,
        target_file_size=write_policy.target_file_size if write_policy else None,
        stats_columns=stats_columns,
        # ... other fields
    )
```

**Representative Pattern - After (Target):**

```python
# src/datafusion_engine/io/write.py:288-345
def _delta_policy_context(
    *,
    options: WriteDatasetOptions,
    dataset_location: DatasetLocation,
    request_partition_by: tuple[str, ...],
    schema_columns: Sequence[str] | None = None,
    lineage_columns: Sequence[str] | None = None,
    plan_bundle: DataFusionPlanBundle | None = None,  # NEW
) -> _DeltaPolicyContext:
    # THREE-LEVEL CASCADE (unchanged)
    write_policy = options.delta_write_policy or dataset_location.resolved.delta_write_policy

    # ENRICHMENT: Derive from plan details if available (using existing plan_details,
    # NOT non-existent plan_bundle.output_statistics method)
    if plan_bundle is not None and write_policy is not None:
        plan_details = plan_bundle.plan_details or {}
        stats = plan_details.get("statistics", {})
        # Use canonical stats keys: `num_rows` first, then `row_count` fallback.
        # Do NOT use `stats_row_count` - that is not a normalized payload key.
        # (canonical keys at: bundle.py:2274, consumption at: execution_plan.py:1575)
        row_count = stats.get("num_rows") or stats.get("row_count")

        # Adaptive target file size based on estimated row count
        if row_count and write_policy.target_file_size:
            adaptive_file_size = compute_adaptive_file_size(
                estimated_rows=row_count,
                base_target=write_policy.target_file_size,
            )
        else:
            adaptive_file_size = write_policy.target_file_size

        # Stats columns enriched from plan projection
        plan_stats_columns = extract_stats_columns_from_plan(plan_bundle)
    else:
        adaptive_file_size = write_policy.target_file_size if write_policy else None
        plan_stats_columns = None

    # Merge schema/lineage/plan stats columns
    stats_columns = _merge_stats_columns(schema_columns, lineage_columns, plan_stats_columns)

    return _DeltaPolicyContext(
        write_policy=write_policy,
        target_file_size=adaptive_file_size,
        stats_columns=stats_columns,
        # ... other fields
    )
```

**Target Files:**
- `src/datafusion_engine/io/write.py` (lines 186-248, 288-345)
- `src/datafusion_engine/plan/bundle.py` (expose output statistics)
- `src/storage/deltalake/config.py` (possibly add adaptive policy fields)

**Implementation Checklist:**

1. **Access plan statistics from existing `plan_details` structure**
   - [ ] `DataFusionPlanBundle` already has `plan_details` containing statistics
     via `_plan_statistics_section` and `_plan_statistics_payload`
     (`src/datafusion_engine/plan/bundle.py:2071, 2274`)
   - [ ] Do NOT add new properties/methods to `DataFusionPlanBundle` (`bundle.py:116`)
   - [ ] Create a small extraction helper that reads `bundle.plan_details`:
     ```python
     def plan_stats_from_bundle(bundle: DataFusionPlanBundle) -> dict | None:
         details = bundle.plan_details
         return details.get("statistics") if details else None
     ```
   - [ ] Use capability detection (Section 8.2) for any statistics not yet captured

2. **Create adaptive file size computation**
   - [ ] Add to `src/datafusion_engine/io/write.py`:
     ```python
     def compute_adaptive_file_size(
         estimated_rows: int,
         base_target: int,
     ) -> int:
         # Heuristic: scale file size based on row count
         # Small tables (<10K rows): smaller files
         # Large tables (>1M rows): larger files
         if estimated_rows < 10_000:
             return min(base_target, 32 * 1024 * 1024)  # 32MB max
         elif estimated_rows > 1_000_000:
             return max(base_target, 128 * 1024 * 1024)  # 128MB min
         else:
             return base_target
     ```

3. **Extract stats columns from plan projection**
   - [ ] Add to `src/datafusion_engine/io/write.py`:
     ```python
     def extract_stats_columns_from_plan(
         plan_bundle: DataFusionPlanBundle,
     ) -> frozenset[str] | None:
         # Walk plan projection, identify high-cardinality columns
         # Good stats columns: timestamps, IDs, enums
         # Bad stats columns: free text, large strings
         ...
     ```

4. **Merge stats columns from multiple sources**
   - [ ] Update `_merge_stats_columns()` to accept plan-derived columns
   - [ ] Priority: explicit schema > plan-derived > lineage
   - [ ] Cap total stats columns at policy limit

5. **Thread plan bundle through write path**
   - [ ] Update `_delta_policy_context()` signature
   - [ ] Find all callers and pass plan bundle when available
   - [ ] Add feature flag `USE_PLAN_ENRICHED_WRITE_POLICY` (default False initially)

6. **Record adaptive policy decisions**
   - [ ] When adaptive file size differs from base, record artifact:
     ```python
     record_artifact(profile, ADAPTIVE_WRITE_POLICY_SPEC, {
         "base_target_file_size": base_target,
         "adaptive_target_file_size": adaptive_file_size,
         "estimated_rows": plan_stats.row_count,
         "reason": "small_table" or "large_table",
     })
     ```

**Decommission Candidates:**
- None (this enriches existing cascade, doesn't replace anything)

---

### 10.4 Manifest-Driven Delta Maintenance - KEEP (High Value)

**Disposition:** Keep. High-value gap: currently policy-presence-based, not
outcome-threshold-based.

**Current state:** Delta maintenance is controlled by `DeltaMaintenancePolicy`
configured per-dataset-location. Decisions are presence-based (policy exists ->
run maintenance), not outcome-based (rows written -> decide if maintenance needed).

**Programmatic approach:**
1. After task execution, capture write metrics as part of execution result
2. Build maintenance decisions from write outcomes:
   - Optimize if files_created > threshold
   - Vacuum if version_delta > retention_threshold
3. Create `resolve_maintenance_from_execution()`
4. Dataset policy declares thresholds; write outcomes trigger decisions

(See Wave 4A implementation details above for full implementation)

---

### 10.5 Unified Artifact Schema Registry - KEEP (Expand Scope)

**Disposition:** Keep and expand scope. Real scale is larger (183 matches across
41 files), requiring phased migration and reuse of existing serde registry.

**Approach:**
1. Create `ArtifactSpec` registry layered on existing `serde_schema_registry.py`
   and `serde_artifacts.py`
2. Each artifact callsite references a spec, not a raw string
3. Migrate by subsystem with adapter accepting both legacy and typed specs
4. See Section 8.4 for staging strategy

(See Wave 5 implementation details above for full implementation)

---

### 10.6 Registration Phase Derivation from Execution Plan - DEFER

**Disposition:** Defer. Existing orchestration is stable and partly conditional;
low immediate ROI.

**Rationale:** `_view_graph_phases` is static order but conditionally inserts
`scan_overrides` (`src/datafusion_engine/views/registration.py:125`). The current
phase ordering works correctly and rarely changes. This is an optional flexibility
improvement with lower priority than correctness-focused waves.

---

### 10.7 Programmatic DataSourceConfig Construction - REVISE (Narrow)

**Disposition:** Revise. Normalize and semantic-root derivations already in place;
target only remaining manual seams.

**Verified state:** `normalize_dataset_locations_for_profile()`
(`src/datafusion_engine/session/runtime.py:590`) and catalog integration
(`src/datafusion_engine/dataset/registry.py:332`) already handle the bulk of
programmatic location derivation.

**Narrowed approach:**
1. Identify remaining manual data source surfaces (not covered by existing
   normalize/catalog integration)
2. For those surfaces only, derive from manifest + output roots
3. Do not rebuild `DataSourceConfig.from_manifest()` for what already works

---

#### Implementation Details: 10.7

**Current State Analysis:**

```python
# src/datafusion_engine/session/runtime.py:3469-3475
class DataSourceConfig(StructBaseStrict, frozen=True):
    dataset_templates: Mapping[str, DatasetLocation]
    extract_output: ExtractOutputConfig
    semantic_output: SemanticOutputConfig
    cdf_cursor_store: CdfCursorStore | None

# Helper functions that already exist:
# - normalize_dataset_locations_for_profile() (lines 590-616)
# - extract_output_locations_for_profile() (lines 619-643)
# - semantic_output_locations_for_profile() (lines 646-682)
```

**Target Files:**
- `src/datafusion_engine/session/runtime.py` (lines 3469-3475, 590-682)
- Identify other manual `DataSourceConfig` construction sites

**Implementation Checklist:**

1. **Audit `DataSourceConfig` construction sites**
   - [ ] Use `/cq search DataSourceConfig` to find all construction sites
   - [ ] Classify: already programmatic vs. manual construction
   - [ ] Identify which manual sites are not covered by existing helpers

2. **For manual sites, determine if manifest derivation is possible**
   - [ ] Check if site has access to manifest or can receive it
   - [ ] If yes, add helper function to derive config from manifest
   - [ ] If no, document as legitimate configuration boundary

3. **Create `DataSourceConfig` builder with explicit authority modes**
   - [ ] **Note:** `ManifestDatasetBindings` has no `as_location_map()` method.
     Available API: `locations` (property), `payload`, `names()`, `location()`, `has_location()`
     (`src/semantics/program_manifest.py:18`).
   - [ ] `DataSourceConfig.extract_output` expects `ExtractOutputConfig` (not a mapping);
     `DataSourceConfig.semantic_output` expects `SemanticOutputConfig` (not a mapping)
     (`src/datafusion_engine/session/runtime.py:3469`).
   - [ ] **Split builder into two explicit modes** to enforce semantic-authority-first:
     ```python
     # src/datafusion_engine/session/runtime.py

     def datasource_config_from_manifest(
         manifest: SemanticProgramManifest,
         *,
         runtime_profile: DataFusionRuntimeProfile,
         cdf_cursor_store: CdfCursorStore | None = None,
     ) -> DataSourceConfig:
         """Semantic-authority mode: manifest + declared output policy.

         Fail closed when unresolved - do not silently fall back to profile-derived
         locations in non-bootstrap paths.
         """
         # Use dict() on the locations property (not a non-existent as_location_map())
         locations = dict(manifest.dataset_bindings.locations)
         if not locations:
             msg = "Manifest dataset bindings are empty; cannot build DataSourceConfig in semantic-authority mode."
             raise ValueError(msg)
         return DataSourceConfig(
             dataset_templates=locations,
             extract_output=extract_output_locations_for_profile(runtime_profile),
             semantic_output=semantic_output_locations_for_profile(runtime_profile),
             cdf_cursor_store=cdf_cursor_store,
         )

     def datasource_config_from_profile(
         runtime_profile: DataFusionRuntimeProfile,
         *,
         cdf_cursor_store: CdfCursorStore | None = None,
     ) -> DataSourceConfig:
         """Runtime-bootstrap mode: profile-derived fallbacks.

         Used only for pre-manifest bootstrap phases (e.g., initial session setup
         before semantic compilation). Should NOT be used in post-compile paths.
         """
         return DataSourceConfig(
             dataset_templates=dict(
                 normalize_dataset_locations_for_profile(runtime_profile)
             ),
             extract_output=extract_output_locations_for_profile(runtime_profile),
             semantic_output=semantic_output_locations_for_profile(runtime_profile),
             cdf_cursor_store=cdf_cursor_store,
         )
     ```

4. **Replace manual construction with appropriate builder mode**
   - [ ] For each manual site identified in step 1:
     - Post-compile paths: use `datasource_config_from_manifest()` (semantic-authority)
     - Pre-compile bootstrap paths: use `datasource_config_from_profile()` (runtime-bootstrap)
   - [ ] Thread manifest to post-compile sites if not already available
   - [ ] Add assertion in post-compile paths that manifest is available (fail closed)

5. **Document remaining configuration boundaries**
   - [ ] For sites that legitimately use runtime-bootstrap mode:
     - Add comments explaining why (e.g., pre-manifest bootstrap phase)
     - Ensure these are true configuration boundaries, not drift surfaces
   - [ ] For sites that should use semantic-authority mode:
     - Verify manifest is threaded and builder fails if bindings are empty

**Decommission Candidates:**
- Ad-hoc `DataSourceConfig` construction patterns (after migration to manifest-backed builder)

---

### 10.8 Hamilton DAG Generation from Execution Plan - MARK AS IMPLEMENTED

**Disposition:** Mark as implemented. Convert from proposal to extension/optimization
track.

**Verified state:** Dynamic task module generation is already active via
`build_task_execution_module()` at `src/hamilton_pipeline/task_module_builder.py:104`.
Hamilton DAG node functions are already generated dynamically from the execution plan.

**Future work (optimization track):**
- Performance tuning of generated modules
- Enhanced Hamilton introspection support for generated functions
- Source tracking conventions for dynamic modules

---

### 10.9 Plan-Derived Scan Policy Overrides - KEEP

**Disposition:** Keep. Strong fit with existing scan unit framework.

**Important:** `ExecutionPlan` has no `scan_policies` field (`src/relspec/execution_plan.py:83`).
Scan override flow uses `scan_units` and `apply_scan_unit_overrides(...)`:
- `src/datafusion_engine/plan/pipeline.py:109`
- `src/datafusion_engine/dataset/resolution.py:183`

**Approach:**
1. During execution plan compilation, compute per-scan-unit scan policies based on
   dataset characteristics and plan signals
2. Keep policy derivation scan-unit-native:
   - Either enrich `ScanUnit` metadata with derived policy hints, or
   - Add a typed `scan_policy_overrides_by_key: Mapping[str, ScanPolicyOverride]` keyed
     by scan unit key, stored alongside the execution plan (not inside it)
3. Apply via existing `apply_scan_unit_overrides()` infrastructure
4. Use capability detection (Section 8.2) for any DataFusion statistics-dependent
   policy decisions

(See Wave 4B implementation details above for full implementation)

---

### 10.10 Compile-Time Policy Bundle Validation - KEEP (Promote)

**Disposition:** Keep and promote priority. High leverage once compile-boundary
convergence (Wave 2) lands.

**Approach:**
1. After execution plan compilation, validate policy bundle against plan requirements
2. Check: feature gates enable all required UDFs, Delta protocol support covers write
   ops, scan policy is compatible with dataset sizes
3. Return structured validation result with warnings and errors
4. Run at the execution-plan boundary (`compile_execution_plan()` at
   `src/relspec/execution_plan.py:450`), NOT inside `compile_semantic_program()` which
   returns only `SemanticProgramManifest` and has no execution plan
5. Support validation modes: `error|warn|off` for staged rollout

**Dependency:** Wave 2 (compile boundary convergence) should land first to establish
single compile point.

---

#### Implementation Details: 10.10

**Representative Pattern - After (Target):**

**Important architectural note:** `compile_semantic_program()` returns only
`SemanticProgramManifest` and has no `ExecutionPlan` task graph semantics
(`src/semantics/compile_context.py:129`). Policy validation must run at the
execution-plan boundary (`compile_execution_plan()` at `src/relspec/execution_plan.py:450`),
not inside the semantic compile phase.

**Config object notes:**
- `PolicyBundleConfig.feature_gates` is `DataFusionFeatureGates` (optimizer toggles
  like `enable_dynamic_filter_pushdown`), NOT `FeatureGatesConfig`
  (`src/datafusion_engine/session/runtime.py:781, 3614`).
- UDF platform gating uses `runtime_profile.features.enable_udfs` /
  `runtime_profile.features.enable_async_udfs` (`src/datafusion_engine/session/runtime.py:3508`).
- `DeltaProtocolSupport` is a structured object, not an iterable of features. Use the
  existing `delta_protocol_compatibility()` helper (`src/datafusion_engine/delta/protocol.py:68`).

**Important `ExecutionPlan` contract note:** `ExecutionPlan` fields are defined at
`src/relspec/execution_plan.py:83` and include `view_nodes`, `scan_units`,
`task_plan_metrics`, `lineage_by_view`, etc. There is **no `tasks` attribute**.
- UDF requirements come from `execution_plan.view_nodes[*].required_udfs`
- Delta compatibility comes from `execution_plan.scan_units[*].protocol_compatibility`
- Size/cost heuristics come from `execution_plan.task_plan_metrics` (keyed by task name,
  `TaskPlanMetrics` at `src/relspec/execution_plan.py:140`)
- Scan/write/protocol signals are carried by `ScanUnit` (`src/datafusion_engine/lineage/scan.py:101`)
- `DeltaProtocolCompatibility.compatible` is the correct field name (NOT `is_compatible`)
  (`src/datafusion_engine/delta/protocol.py:48`); handle `None` (unconfigured) explicitly

```python
# Suggested location: src/relspec/execution_plan.py (or relspec/policy_validation.py)
# NOT in compile_context.py (semantic compile scope)

from datafusion_engine.delta.protocol import delta_protocol_compatibility

def validate_policy_bundle(
    execution_plan: ExecutionPlan,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    udf_snapshot: Mapping[str, object],
) -> PolicyValidationResult:
    issues: list[PolicyValidationIssue] = []

    # Check 1: UDF availability and feature gate compatibility from view nodes
    # Use runtime_profile.features (FeatureGatesConfig), NOT
    # policy_bundle.feature_gates (DataFusionFeatureGates = optimizer toggles)
    for node in execution_plan.view_nodes:
        required = tuple(node.required_udfs or ())
        if required and not runtime_profile.features.enable_udfs:
            issues.append(error("UDFS_DISABLED", task=node.name, detail=str(required)))
        missing = sorted(name for name in required if name not in udf_snapshot)
        if missing:
            issues.append(error("UDF_MISSING", task=node.name, detail=str(missing)))

    # Check 2: Delta protocol compatibility from scan units
    # Use compat.compatible (bool | None), NOT compat.is_compatible
    for unit in execution_plan.scan_units:
        compat = unit.protocol_compatibility
        if compat is not None and compat.compatible is False:
            issues.append(error("DELTA_PROTOCOL_INCOMPATIBLE", dataset=unit.dataset_name))

    # Check 3: Size/policy heuristics from task plan metrics
    for task_name, metric in execution_plan.task_plan_metrics.items():
        if metric.stats_row_count is not None and metric.stats_row_count < 1000:
            issues.append(warn("SMALL_INPUT_SCAN_POLICY", task=task_name))

    return PolicyValidationResult.from_issues(issues)
```

**Target Files:**
- `src/relspec/execution_plan.py` (or `src/relspec/policy_validation.py`) - add `PolicyValidationResult`, `PolicyValidationIssue`, and `validate_policy_bundle()` at execution-plan boundary
- `src/datafusion_engine/session/runtime.py` (integrate with existing `_validate_async_udf_policy` at line 4687)
- `src/datafusion_engine/delta/protocol.py` (reuse existing `delta_protocol_compatibility()` at line 68)

**Implementation Checklist:**

1. **Define `PolicyValidationResult` contract**
   - [ ] Add to `src/relspec/execution_plan.py` (or a dedicated `src/relspec/policy_validation.py`),
     NOT `src/semantics/compile_context.py` (which is semantic compile scope, not execution planning).
     Existing execution-plan compatibility validators already live in `src/relspec/execution_plan.py:1283` onward.
     ```python
     @dataclass(frozen=True)
     class PolicyValidationIssue:
         code: str          # Machine-readable code (e.g., "UDF_MISSING", "DELTA_PROTOCOL_INCOMPATIBLE")
         severity: str      # "error" | "warn"
         task: str | None
         detail: str | None

     @dataclass(frozen=True)
     class PolicyValidationResult:
         issues: list[PolicyValidationIssue]

         @property
         def is_valid(self) -> bool:
             return not any(i.severity == "error" for i in self.issues)

         @property
         def warnings(self) -> list[PolicyValidationIssue]:
             return [i for i in self.issues if i.severity == "warn"]

         @property
         def errors(self) -> list[PolicyValidationIssue]:
             return [i for i in self.issues if i.severity == "error"]

         @classmethod
         def from_issues(cls, issues: list[PolicyValidationIssue]) -> PolicyValidationResult:
             return cls(issues=issues)
     ```

2. **Create `validate_policy_bundle()` function**
   - [ ] Place in `src/relspec/execution_plan.py` (execution-plan boundary, NOT `compile_context.py`)
   - [ ] Implement checks as shown in pattern above
   - [ ] **Integrate with existing validators**: Reuse and extend
     `_validate_async_udf_policy` (`src/datafusion_engine/session/runtime.py:4687`)
     and existing UDF snapshot/install paths rather than adding duplicate validation logic
   - [ ] Add check for async UDF timeout/batch size compatibility
   - [ ] Add check for disk cache policy vs. cache-eligible tasks
   - [ ] Use `runtime_profile.features` (which is `FeatureGatesConfig` with `enable_udfs`,
     `enable_async_udfs`, etc. at `src/datafusion_engine/session/runtime.py:3508`),
     NOT `policy_bundle.feature_gates` (which is `DataFusionFeatureGates` = optimizer
     toggles at `src/datafusion_engine/session/runtime.py:781`)
   - [ ] Use `delta_protocol_compatibility()` from `src/datafusion_engine/delta/protocol.py:68`
     for Delta protocol checks, NOT `set()` arithmetic on `DeltaProtocolSupport`

3. **Integrate into execution-plan boundary (NOT compile_semantic_program)**
   - [ ] `compile_semantic_program()` returns `SemanticProgramManifest` only and has no
     `ExecutionPlan` (`src/semantics/compile_context.py:129`)
   - [ ] Run validation in `compile_execution_plan()` (`src/relspec/execution_plan.py:450`)
     or immediately after it:
     ```python
     # In compile_execution_plan() or caller, AFTER execution plan is built:
     validation = validate_policy_bundle(
         execution_plan,
         runtime_profile=runtime_profile,
         udf_snapshot=udf_snapshot,
     )

     if not validation.is_valid:
         raise ExecutionPlanError(
             f"Policy validation failed: {validation.errors}"
         )

     if validation.warnings:
         record_artifact(profile, POLICY_VALIDATION_WARNINGS_SPEC, {
             "warnings": validation.warnings,
         })
     ```
   - [ ] Support validation modes: `error|warn|off` so rollout can start non-blocking

4. **Attach validation to execution-plan diagnostics**
   - [ ] Add `policy_validation: PolicyValidationResult | None` to execution-plan
     diagnostic artifacts (NOT to `SemanticProgramManifest` which has no execution plan)
   - [ ] Record as structured artifact for observability

5. **Create integration test for policy validation**
   - [ ] Test case: missing UDF triggers error
   - [ ] Test case: disabled feature gate triggers error
   - [ ] Test case: scan policy mismatch triggers warning
   - [ ] Verify compilation fails with clear error message

6. **Add feature flag**
   - [ ] Add `ENABLE_POLICY_VALIDATION` to `src/relspec/config.py` (default True)
   - [ ] Allow disabling for debugging/migration

**Decommission Candidates:**
- None (this is net-new validation logic)

**Dependencies:**
- **Wave 2 must complete first** - validation requires single compile point and manifest-backed context

---

## 11. Extended Architecture Dependency Graph

The full set of proposals forms this dependency structure:

```
Wave 0 (Reality Alignment) -- COMPLETE
  |
  v
Wave 1 (Resolver Threading Closure)
  |
  v
Wave 2 (Compile Boundary Convergence)
  |
  +---> 8.7 (ExecutionAuthorityContext)  [prerequisite for Wave 3+]
  |       |
  |       +---> Wave 3 (Extract Executor Binding)
  |       |       |
  |       |       +---> 10.4 (Maintenance from Execution)
  |       |
  |       +---> 8.8 (PlanSignals)  [prerequisite for 10.2/10.3/10.9/10.10]
  |               |
  |               +---> 10.10 (Policy Bundle Validation)
  |               +---> 10.2 (Schema Contract Tightening)
  |               +---> Wave 4 (Policy/Maintenance Intelligence)
  |                       |
  |                       +---> 10.9 (Scan Policy Overrides)
  |                       +---> 10.3 (Write Policy Enrichment)
  |
  +---> 10.1 (Output Naming)
  +---> 10.7 (DataSourceConfig - narrowed)
  |
  v
Wave 5 (Artifact Contract Governance)  [parallel track]
8.11 (Performance Architecture)        [parallel track]
8.12 (Fidelity and Safety)             [parallel track, after Wave 2]
```

**Critical path:** Wave 1 -> Wave 2 -> 8.7 -> {8.8, Wave 3} -> {10.10, Wave 4} -> {10.9, 10.3}

**Authoritative design decisions:**
- 8.7: `ExecutionAuthorityContext` (semantic/orchestration separation)
- 8.8: `PlanSignals` (mandatory typed control plane for all plan signal consumers)

**Implemented (no work):** 10.8 (Hamilton DAG generation)

**Deferred:** 10.6 (Registration phase derivation)

**Independent tracks:**
- Wave 3 (extract binding) - can proceed after 8.7
- Wave 5 (artifact governance) - can proceed any time as staged infrastructure
- 10.5 (artifact schema registry) - parallel track, part of Wave 5
- 8.11 (performance architecture) - can proceed any time
- 8.12 (fidelity and safety) - can proceed after Wave 2

### Priority Tiers (Corrected)

**Tier 1 - Structural Foundation (do first):**
- Wave 1: Resolver threading closure (including `hamilton_pipeline` path)
- Wave 2: Compile boundary convergence across orchestration modules
- 10.1: Output naming from manifest (with compatibility shim)

**Tier 1.5 - Authority Layering (do between Tier 1 and Tier 2):**
- 8.7: Define `ExecutionAuthorityContext` (semantic/orchestration separation)
- 8.8: Implement `PlanSignals` unified control plane (mandatory prerequisite for 10.x)

**Tier 2 - Plan-Driven Policies (do after Tier 1.5):**
- 10.3: Write policy enrichment from plan statistics (not replacement)
- 10.7: DataSourceConfig narrowed with authority-split builder modes
- 10.10: Compile-time policy bundle validation (execution-plan-native)
- Wave 3: Programmatic extract executor binding (via `ExecutionAuthorityContext`)

**Tier 3 - Dynamic Optimization (do after Tier 2):**
- Wave 4: Policy/maintenance intelligence (outcome-driven)
- 10.2: Schema contract tightening and divergence telemetry
- 10.4: Delta maintenance from execution outcomes
- 10.9: Plan-derived scan policy overrides (scan-unit-native, not `ExecutionPlan.scan_policies`)

**Tier 4 - Infrastructure (proceed independently):**
- Wave 5 / 10.5: Artifact schema registry (phased, adapter-mediated)
- 8.11: Performance architecture track (cache/stats/comparison policies)
- 8.12: Fidelity and safety track (invariant tests, structured validation artifacts)

---

## 12. End-State Architecture

When all proposals are implemented, the pipeline flow becomes:

```
INPUTS (only two):
  1. Semantic specs (table specs, relationship specs, normalization specs)
  2. DataFusionRuntimeProfile (infrastructure config: roots, thread counts, policies)

COMPILE PHASE (single call):
  compile_semantic_program()
    -> SemanticIR (from specs)
    -> DatasetBindings (from IR + output roots)
    -> ViewNodes (from IR + plan bundles)
    -> ExecutionPlan (from plan lineage + task graph)
    -> SemanticProgramManifest (bundles all of the above)

DERIVED AT COMPILE TIME (zero static declarations):
  - Output names ............. from SemanticIR.views
  - Dataset locations ........ from IR + output roots
  - Task ordering ............ from plan lineage (already done)
  - Schema contracts ......... tightened from plan output + spec constraints (partial)
  - Cache policies ........... from plan reuse analysis
  - Write policies ........... enriched from plan statistics + existing cascade
  - Scan policies ............ from scan unit characteristics
  - UDF requirements ......... from plan expression analysis (already done)
  - Extract executors ........ from evidence plan requirements
  - Maintenance triggers ..... from write outcome metrics + policy thresholds
  - Policy validation ........ from plan requirements vs. policy bundle
  - Hamilton DAG nodes ....... from execution plan task schedule (already done)
  - Artifact schemas ......... from centralized artifact registry

EXECUTE PHASE:
  Two layered contexts carry everything:
    SemanticExecutionContext (semantic compile artifacts: manifest, resolver, profile)
    ExecutionAuthorityContext (orchestration: semantic_context + evidence_plan + executor_map + capabilities)
  Every subsystem reads from the manifest chain, never re-derives independently.
  The manifest fingerprint is the sole cache key for the entire pipeline.
```

The only configuration surfaces that remain are:
1. **Semantic specs** - the declarative "what to build" input
2. **Output roots** - two paths (extract root, semantic root)
3. **Infrastructure policies** - thread counts, memory limits, codec selection
4. **Feature gates** - enable/disable experimental capabilities

Everything else is derived.

---

## 13. Implementation Risk Notes

1. **Resolver threading** changes affect many integration seams; use temporary
   compatibility overloads before deleting fallback helpers.
2. **Compile-boundary convergence** can increase object lifetime coupling; keep
   context immutable/frozen where possible.
3. **Artifact registry migration** should not block core pipeline modernization;
   run as parallel track with adapters.
4. **DataFusion capability detection** is required for any statistics-dependent
   feature (scheduling signals, plan-derived scan policies, write policy enrichment).
   Do not assume `ExecutionPlan` methods exist without probing.
5. **Authority layering** (`SemanticExecutionContext` vs `ExecutionAuthorityContext`)
   must be enforced from Wave 3 onward. Any PR that adds orchestration fields to
   `SemanticExecutionContext` violates the 8.7 design decision and should be rejected.
6. **`ExecutionPlan` contract fidelity** - validate all plan-consuming code against
   actual `ExecutionPlan` fields (`view_nodes`, `scan_units`, `task_plan_metrics`),
   not assumed fields like `tasks`, `scan_policies`, or convenience methods that
   don't exist. Use `PlanSignals` (8.8) as the typed access layer.

---

## 14. Summary

This ordering maximizes correctness gains first (single manifest authority), then
performance/policy sophistication, while keeping higher-cost platform cleanup
decoupled from core execution correctness:

1. **Wave 1** - Resolver threading closure (including `hamilton_pipeline` path)
2. **Wave 2** - Compile boundary convergence across orchestration modules
3. **8.7 + 8.8** - Authority layering (`ExecutionAuthorityContext`) + unified `PlanSignals`
4. **Waves 3 + 4** - Extract executor binding and policy/maintenance intelligence
   in parallel (via `ExecutionAuthorityContext`)
5. **Wave 5 + 8.11 + 8.12** - Artifact governance, performance architecture, and
   fidelity/safety as staged infrastructure programs

| Tier | Proposals | Theme |
|------|-----------|-------|
| 1 | Wave 1, Wave 2, 10.1 | Structural foundation - single manifest, single context |
| 1.5 | 8.7, 8.8 | Authority layering - ExecutionAuthorityContext + PlanSignals |
| 2 | Wave 3, 10.3, 10.7, 10.10 | Plan-driven policies - write, config, validation |
| 3 | Wave 4, 10.2, 10.4, 10.9 | Dynamic optimization - adapt to actual data |
| 4 | Wave 5 / 10.5, 8.11, 8.12 | Infrastructure - artifacts, performance, fidelity |
| N/A | 10.8 | Already implemented (Hamilton DAG generation) |
| N/A | 10.6 | Deferred (registration phase derivation) |

The key architectural principle: **the compiled manifest is the single source of truth
for pipeline behavior.** Every subsystem should receive its configuration from the
manifest chain, never re-derive it independently.

---

## 15. Cross-Wave Decommission Dependencies

Some code/modules cannot be fully decommissioned until multiple scope items complete.
This section tracks those dependencies.

### `dataset_bindings_for_profile()` Full Removal

**Location:** `src/semantics/compile_context.py:118-126`

**Cannot be removed until:**
- ✅ Wave 1 completes (all 7 consumer callsites in `datafusion_engine/` and `hamilton_pipeline/` migrated)
- ✅ Wave 2 completes (all orchestration compile contexts migrated)

**Note:** `IncrementalRuntime.dataset_resolver` (`src/semantics/incremental/runtime.py:71`)
already uses injected resolver directly - no fallback to remove.

**Final state:** Rename to `_dataset_bindings_for_profile()`, keep as internal bootstrap helper only

---

### `CompileContext` Ad-Hoc Instantiation Pattern

**Pattern:** Direct `CompileContext(runtime_profile=...)` calls outside compile boundary

**Cannot be fully eliminated until:**
- ✅ Wave 2 completes (all orchestration entry points thread execution context)
- ⚠️ Extract coordination paths (`src/extract/coordination/materialization.py`) migrated

**Final state:** All `CompileContext` instantiation happens only in `compile_semantic_program()`.
One canonical compile entrypoint per pipeline run (`build_semantic_execution_context`),
with the resulting `SemanticExecutionContext` passed downstream to all consumers.
`build_semantic_execution_context(...)` callsites (`src/datafusion_engine/plan/pipeline.py:80`,
`src/datafusion_engine/session/runtime.py:4572`) converge to a single authority call.
Enforced via `CompileTracker` context manager with test-level compile counters

---

### Global Extract Executor Registry

**Components:**
- `_EXTRACT_ADAPTER_EXECUTORS` dict (`src/hamilton_pipeline/modules/extract_execution_registry.py:27`)
- `_EXTRACT_EXECUTOR_REGISTRATION_STATE` flag (`src/hamilton_pipeline/modules/task_execution.py:871`)
- `ensure_extract_executors_registered()` function
- `get_extract_executor()` function

**Cannot be removed until:**
- ✅ Wave 3 completes (executor map built from evidence plan)
- ✅ All task execution callsites migrated to executor map
- ⚠️ Feature flag `USE_GLOBAL_EXTRACT_REGISTRY` disabled and removed

**Final state:** Keep adapter definitions, remove global mutable registration pattern

---

### Static `SEMANTIC_OUTPUT_NAMES` Dict

**Location:** `src/semantics/naming.py:12-37`

**Cannot be removed until:**
- ✅ 10.1 completes (output name map added to manifest)
- ⚠️ All `canonical_output_name()` callsites thread manifest
- ⚠️ Fallback logic in `canonical_output_name()` removed

**Final state:** Entire static dict removed, all naming derived from manifest

---

### Policy-Presence-Based Maintenance Logic

**Location:** `src/datafusion_engine/delta/maintenance.py:68-98`

**Cannot be removed until:**
- ✅ Wave 4A completes (outcome-based maintenance resolver created)
- ✅ 10.4 completes (write metrics captured in execution results)
- ⚠️ All maintenance callsites migrated to outcome-based resolver
- ⚠️ Feature flag `USE_OUTCOME_BASED_MAINTENANCE` enabled by default

**Final state:** `resolve_delta_maintenance_plan()` logic changed to threshold-based, policy-presence fallback removed

---

### Untyped Artifact Recording

**Pattern:** `record_artifact(profile, "string_name", payload)`

**Scale:** 183 callsites across 41 files

**Cannot be removed until:**
- ✅ Wave 5 completes (artifact spec registry created)
- ⚠️ All 5 migration phases complete (planning, write path, Hamilton, diagnostics, remaining)
- ⚠️ Feature flag `USE_TYPED_ARTIFACT_SPECS` enabled by default
- ⚠️ Legacy string name support removed from `record_artifact()`

**Final state:** All artifact recording uses typed `ArtifactSpec`, string names rejected

---

### Fallback Compile Calls in Orchestration

**Locations (7 total):**
- `src/engine/materialize_pipeline.py:308`
- `src/extract/coordination/materialization.py:457`
- `src/hamilton_pipeline/driver_factory.py:465`
- `src/hamilton_pipeline/modules/task_execution.py:497, 523`
- `src/semantics/pipeline.py:1394, 1980`

**Cannot be removed until:**
- ✅ Wave 2 completes (execution context threaded through all orchestration)
- ⚠️ Single-compile invariant enforced in integration tests
- ⚠️ `CompileTracker` context manager deployed to all entry points

**Final state:** Only one `compile_semantic_program()` call per pipeline run, all other paths receive pre-compiled manifest

---

### Manual `DataSourceConfig` Construction

**Pattern:** Ad-hoc `DataSourceConfig(dataset_templates={...}, extract_output=..., semantic_output=...)` calls

**Cannot be removed until:**
- ✅ 10.7 completes (manual sites identified and classified)
- ⚠️ `datasource_config_from_manifest()` builder created and adopted
- ⚠️ All manual sites that can derive from manifest migrated

**Final state:** All non-bootstrap `DataSourceConfig` construction uses manifest-backed builder

---

### Schema Contract Divergence Goes Undetected

**Current state:** Contracts derived but divergence not tracked

**Cannot be eliminated until:**
- ✅ 10.2 completes (divergence computation and telemetry added)
- ⚠️ Plan bundle threaded through contract derivation
- ⚠️ Strict schema validation enforced for critical outputs

**Final state:** All schema divergence detected, recorded, and optionally enforced

---

### Summary: Decommission Readiness Matrix

| Component | Wave 1 | Wave 2 | 8.7 | Wave 3 | Wave 4 | Wave 5 | 10.1 | 10.2 | 10.4 | 10.7 | Ready to Remove? |
|-----------|--------|--------|-----|--------|--------|--------|------|------|------|------|------------------|
| `dataset_bindings_for_profile()` | ✅ | ✅ | - | - | - | - | - | - | - | - | ⚠️ (rename, keep as internal) |
| Ad-hoc `CompileContext()` | - | ✅ | - | - | - | - | - | - | - | - | ✅ (after Wave 2) |
| Global extract registry | - | - | ✅ | ✅ | - | - | - | - | - | - | ✅ (after 8.7 + Wave 3) |
| Static `SEMANTIC_OUTPUT_NAMES` | - | - | - | - | - | - | ✅ | - | - | - | ✅ (after 10.1) |
| Policy-presence maintenance | - | - | - | - | ✅ | - | - | - | ✅ | - | ✅ (after Wave 4A + 10.4) |
| Untyped artifact recording | - | - | - | - | - | ✅ | - | - | - | - | ⚠️ (staged, 5 phases) |
| Redundant compile calls | - | ✅ | - | - | - | - | - | - | - | - | ✅ (after Wave 2) |
| Manual `DataSourceConfig` | - | - | - | - | - | - | - | - | - | ✅ | ✅ (after 10.7) |
| Untracked schema divergence | - | - | - | - | - | - | - | ✅ | - | - | ✅ (after 10.2) |
| Ad-hoc plan signal reads | - | - | - | - | - | - | - | - | - | - | ✅ (after 8.8 PlanSignals) |

**Legend:**
- ✅ = Required wave/proposal for decommission
- ⚠️ = Staged removal or keep as internal
- `-` = Not applicable
