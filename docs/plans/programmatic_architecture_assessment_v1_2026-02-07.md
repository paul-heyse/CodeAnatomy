# Programmatic Architecture Assessment

**Date:** 2026-02-07
**Context:** Continuation of the semantic-compiled consolidation effort, assessing how to
advance toward a fully programmatic architecture where extracted data and data operations
mapping drive all pipeline behavior with zero static declarations that could drift.

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

### 2.2 Remaining Static/Bridge Patterns

Despite the programmatic core, several bridge patterns persist where runtime code
re-derives dataset locations from `DataFusionRuntimeProfile` rather than receiving
them from the compiled manifest. These are the drift surfaces.

#### Bridge Pattern: `dataset_bindings_for_profile()`

**7 callsite clusters** across `src/` (definition + 6 consumer locations):

| File | Lines | Role |
|------|-------|------|
| `semantics/compile_context.py` | 118-122 | Definition (wraps CompileContext) |
| `semantics/incremental/runtime.py` | 73-75 | Fallback in `IncrementalRuntime.dataset_resolver` |
| `datafusion_engine/registry_facade.py` | 388-390 | Building catalog for RegistryFacade |
| `datafusion_engine/session/runtime.py` | 4079-4081 | Session bindings method |
| `datafusion_engine/session/runtime.py` | 4127-4129 | Location resolver method |
| `datafusion_engine/session/runtime.py` | 7616-7618 | Standalone utility |
| `datafusion_engine/session/facade.py` | 830-832 | CDF registration resolver |
| `datafusion_engine/plan/bundle.py` | 1907-1909 | Plan bundle context |

Each of these re-derives bindings from the profile rather than receiving them from the
manifest. This means the same catalog resolution runs multiple times, and if a consumer
adds custom registration logic, it could diverge from what the manifest captured.

#### Bridge Pattern: `dataset_catalog_from_profile()`

Now internal to `CompileContext.dataset_bindings()` (1 callsite at
`compile_context.py:64-66`). Two deprecation markers in `materialize_pipeline.py`
confirm the fallback was intentionally removed there. This pattern is nearly contained.

#### Bridge Pattern: `SemanticRuntimeConfig`

Defined in `semantics/runtime.py` but unused in production source code. Only referenced
in test fixtures. The runtime configuration it carries (output locations, cache policies,
CDF settings) should be derived from the manifest or profile policies rather than
maintained as a separate configuration surface.

---

## 3. Drift Surfaces Analysis

A "drift surface" is any point where a static declaration or re-derivation could produce
a different answer than what the compiled plan actually requires.

### 3.1 Dataset Location Drift

**Risk:** When `dataset_bindings_for_profile()` is called independently by multiple
subsystems (registry facade, session runtime, plan bundle, incremental runtime), each
call re-derives the full catalog from profile data sources. If any subsystem modifies
locations between calls, or if profile mutations occur, different subsystems see
different dataset locations.

**Current mitigation:** `ManifestDatasetBindings` is a frozen dataclass, and
`CompileContext` caches its result. But each `dataset_bindings_for_profile()` call
creates a *new* `CompileContext`, so the caching is per-call, not per-pipeline-run.

**Resolution path:** Thread `ManifestDatasetResolver` from the manifest through all
subsystems. The F6 wave (completed) demonstrated this for `semantics/incremental/`.
The remaining callsites in `datafusion_engine/` need the same treatment.

### 3.2 Spec Registry Drift

**Risk:** The semantic spec registry (`SEMANTIC_TABLE_SPECS`, `RELATIONSHIP_SPECS`,
`SEMANTIC_NORMALIZATION_SPECS`) uses module-level immutable registries. If specs are
added or modified without updating the IR pipeline, the compiled manifest could reference
specs that produce different views than expected.

**Current mitigation:** `SemanticIR` is built from spec registry at compile time, and
the manifest fingerprint captures the IR hash. Any spec change invalidates the fingerprint.

**Assessment:** Low risk. The spec-to-IR compilation is single-path and deterministic.
The fingerprint mechanism detects drift. No action needed beyond maintaining the
single-path invariant.

### 3.3 UDF Registration Drift

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

### 3.4 Extract Executor Drift

**Risk:** Extract executors are registered via a global mutable registry
(`_EXTRACT_ADAPTER_EXECUTORS` in `extract_execution_registry.py`) with a global
flag `_EXTRACT_EXECUTORS_REGISTERED`. If executor registration order changes or
executors are added dynamically, the extract pipeline could behave differently.

**Current mitigation:** `_ensure_extract_executors_registered()` uses a global flag
and registers all 9 executors in a fixed order. The `EvidencePlan` contract validates
which datasets are required before execution begins.

**Assessment:** Moderate risk. The global mutable registry with a boolean guard is
fragile. The fixed registration list is essentially a static declaration that could
drift from what the evidence plan requires. A programmatic approach would derive
required executors from the evidence plan.

### 3.5 Cache Policy Drift

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

## 4. Recommended Convergence Waves

### Wave A: Complete Dataset Resolver Threading (High Priority)

**Goal:** Eliminate all `dataset_bindings_for_profile()` bridge calls in
`datafusion_engine/`, replacing them with `ManifestDatasetResolver` passed from the
manifest.

**Scope:**

1. **`datafusion_engine/session/runtime.py`** (3 callsites at 4079, 4127, 7616)
   - Add `_dataset_resolver: ManifestDatasetResolver | None` field to the relevant
     session methods
   - Thread resolver from `SemanticExecutionContext` at pipeline entry
   - Maintain fallback during transition (as done in F6 for IncrementalRuntime)

2. **`datafusion_engine/registry_facade.py`** (1 callsite at 388)
   - `registry_facade_for_context()` should accept `ManifestDatasetResolver`
   - The facade builds a `DatasetCatalog` from bindings - this should receive
     the catalog from the manifest instead of re-deriving it

3. **`datafusion_engine/session/facade.py`** (1 callsite at 830)
   - `register_cdf_inputs()` re-derives bindings for CDF registration
   - Should receive `ManifestDatasetResolver` as parameter

4. **`datafusion_engine/plan/bundle.py`** (1 callsite at 1907)
   - Plan bundle context building re-derives bindings
   - Should receive resolver from the execution context

**Exit criteria:**
- `dataset_bindings_for_profile()` has zero callsites outside `compile_context.py`
  (definition) and `incremental/runtime.py` (fallback)
- All `datafusion_engine/` code receives dataset locations from the manifest chain

**Risk:** Medium. These are well-understood callsite replacements following the F6
pattern. Each can be done with a fallback during transition.

### Wave B: Programmatic Extract Executor Binding (Medium Priority)

**Goal:** Replace the global mutable extract executor registry with executor binding
derived from the evidence plan.

**Approach:**

The current flow:
```
EvidencePlan -> _ensure_extract_executors_registered() -> get_extract_executor(key)
```

The programmatic flow would be:
```
EvidencePlan -> resolve_extract_executors(plan) -> executor_map: {adapter: handler}
```

Where `resolve_extract_executors()` introspects the evidence plan's required datasets,
maps them to adapter keys via the existing `extract_template_adapter()` function, and
returns a frozen mapping of adapter names to executor functions.

**Benefits:**
- No global mutable state
- Executor availability validated at plan time, not lazily at first use
- Missing executors produce compile-time errors rather than runtime KeyError
- The executor map becomes part of the pipeline contract

**Scope:**
1. Create `resolve_extract_executors(plan: EvidencePlan) -> Mapping[str, ExtractExecutorFn]`
2. Pass executor map through `TaskExecutionInputs` or a new `ExtractExecutionContext`
3. Replace `get_extract_executor(key)` calls with map lookups
4. Remove `_EXTRACT_EXECUTORS_REGISTERED` global flag

### Wave C: Manifest-Derived Execution Context (Medium Priority)

**Goal:** Replace scattered context assembly with a single
`SemanticExecutionContext` that carries all compile-time authorities.

`SemanticExecutionContext` already exists at `compile_context.py:107-115` with:
- `manifest: SemanticProgramManifest`
- `dataset_resolver: ManifestDatasetResolver`
- `runtime_profile: DataFusionRuntimeProfile`
- `ctx: SessionContext`
- `facade: DataFusionExecutionFacade | None`

But it is underused. The materialization pipeline, Hamilton driver factory, and
incremental runtime each assemble their own context from separate compile calls.

**Approach:**
1. Compile once at pipeline entry via `build_semantic_execution_context()`
2. Thread `SemanticExecutionContext` through:
   - `materialize_pipeline.py` entry points
   - `driver_factory.py` view graph context
   - `IncrementalRuntime` construction
3. Remove redundant `compile_semantic_program()` calls that re-derive the same manifest
4. Add `evidence_plan: EvidencePlan` to `SemanticExecutionContext` so extract
   requirements are also captured at compile time

**Benefits:**
- Single compilation point eliminates divergence between subsystems
- All runtime decisions reference the same manifest
- Pipeline fingerprint becomes the single cache key for the entire run

### Wave D: Plan-Derived Cache Policy (Low Priority)

**Goal:** Derive view cache policies from plan bundle properties rather than
static assignment in `registry_specs.py`.

**Approach:**
1. Analyze plan bundles during execution plan compilation to determine:
   - Which views are referenced by multiple downstream tasks (candidates for caching)
   - Which views produce final outputs (delta output cache)
   - Which views are referenced once (no cache benefit)
2. Create `resolve_cache_policies(execution_plan: ExecutionPlan) -> Mapping[str, CachePolicy]`
3. Apply policies at view registration time

This is lower priority because the current static assignment is correct (it matches
the spec registry's intent), and cache policy errors are performance issues, not
correctness issues.

### Wave E: DataFusion-Native Scheduling Signals (Low Priority, High Value)

**Goal:** Leverage DataFusion physical plan statistics for scheduling cost estimation.

The scheduling pipeline already computes `bottom_level_costs` and `task_slack_by_task`
from the task graph, but these use uniform task costs. DataFusion physical plans can
expose:

- Estimated row counts from statistics
- Partition counts from physical plan inspection
- Scan file counts from Delta add actions
- Join cardinality estimates

**Approach:**
1. After `build_plan_bundle()`, extract physical plan statistics via
   `execution_plan.display_indent()` or DataFusion's statistics API
2. Map statistics to task costs in `TaskPlanMetrics`
3. Feed into `ScheduleOptions.task_costs` for cost-weighted scheduling

This requires DataFusion 50.1+ statistics support, which is available in the current
environment. The `TaskPlanMetrics` dataclass already has fields for
`stats_row_count`, `stats_total_bytes`, and `stats_available`.

---

## 5. Architecture Dependency Graph

The waves have the following dependency structure:

```
Wave A (Dataset Resolver Threading)
  |
  v
Wave C (Manifest-Derived Execution Context)  <-- depends on A for clean resolver flow
  |
  +---> Wave B (Programmatic Extract Binding)  <-- independent but benefits from C
  |
  v
Wave D (Plan-Derived Cache Policy)  <-- needs execution plan from C
  |
Wave E (DataFusion Scheduling Signals)  <-- independent, uses plan bundles
```

Wave A should be completed first as it unblocks the cleanest version of Wave C.
Waves B and E are independent and can proceed in parallel.

---

## 6. Measuring Progress

### Drift Surface Metrics

Track these counts to measure convergence toward the programmatic target:

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| `dataset_bindings_for_profile()` callsites outside compile_context | 7 | 0 | `grep -r dataset_bindings_for_profile src/ \| grep -v compile_context \| grep -v __all__` |
| `dataset_catalog_from_profile()` callsites outside compile_context | 0 | 0 | Already achieved |
| Global mutable registries in pipeline | 1 | 0 | `_EXTRACT_ADAPTER_EXECUTORS` |
| Redundant `compile_semantic_program()` calls per pipeline run | ~3 | 1 | Trace compile calls during integration test |
| Views with static cache policy assignment | all | 0 | Count in registry_specs |

### Invariant Checks

These should be enforced as assertions or test checks:

1. **Single compilation:** A pipeline run calls `compile_semantic_program()` exactly once
2. **Resolver identity:** All subsystems in a pipeline run reference the same
   `ManifestDatasetResolver` instance (identity check, not equality)
3. **Plan-schedule consistency:** Every task in `TaskSchedule.ordered_tasks` has a
   corresponding `InferredDeps` with matching `plan_fingerprint`
4. **UDF availability:** Every `required_udf` in every `InferredDeps` exists in the
   UDF snapshot at registration time
5. **No orphan registrations:** Every view registered in DataFusion appears in the
   view graph and has a corresponding `ViewNode`

---

## 7. What NOT to Change

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

The goal is not to eliminate all configuration, but to ensure that *pipeline
behavior* (what runs, in what order, with what data) is derived from the manifest
rather than independently declared.

---

## 8. Summary

The architecture is well-advanced toward the programmatic target. The core
chain - plan lineage to dependency inference to scheduling - is fully operational
with zero static declarations. The primary remaining work is:

1. **Wave A** - Thread manifest-derived dataset resolver through all DataFusion
   engine code (6 callsite clusters, following the proven F6 pattern)
2. **Wave B** - Replace global extract executor registry with plan-derived binding
3. **Wave C** - Consolidate scattered context assembly into single
   `SemanticExecutionContext` flow
4. **Waves D/E** - Opportunistic improvements to cache policy and scheduling
   that leverage the programmatic foundation

The key architectural principle to maintain: **the compiled manifest is the single
source of truth for pipeline behavior.** Every subsystem should receive its
configuration from the manifest chain, never re-derive it independently.
