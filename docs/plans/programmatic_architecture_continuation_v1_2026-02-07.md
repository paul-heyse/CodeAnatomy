# Programmatic Architecture: Continuation Assessment

**Date:** 2026-02-07
**Updated:** 2026-02-07 (integrated review feedback from `programmatic_architecture_continuation_review_v2_2026-02-07.md`)
**Context:** Assessment of progress toward the fully programmatic architecture where
extracted data and data operations mapping drive all pipeline behavior, and
identification of the highest-leverage next steps to continue convergence.
**Prerequisite:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md`

> **Review reframing:** The continuation plan is directionally strong, but the key
> correction is not "foundations missing" but "foundations exist; integration coverage
> is uneven." Most leverage comes from closing compile-boundary fallbacks, wiring
> already-implemented outcome/capability components into write/runtime paths, and
> upgrading policy inference from static heuristics to a measured closed loop.

---

## 1. Architecture Progress Summary

The target architecture has two core inputs that programmatically determine everything:

1. **Extracted data** - Evidence tables from multi-source extractors
2. **Data operations mapping** - DataFusion view definitions mapping datasets to CPG outputs

From these two inputs, the system derives scheduling, validation, caching, UDF
requirements, and maintenance decisions with zero static declarations that could drift.

### 1.1 What Is Operational Today

The inference-driven core is fully functional:

```
SemanticIR (declarative view graph)
  -> compile_semantic_program() -> SemanticProgramManifest (immutable contract)
    -> build_plan_bundle() -> DataFusionPlanBundle (per-view plan artifact)
      -> extract_lineage() -> LineageReport (scans, joins, UDFs, columns)
        -> infer_deps_from_plan_bundle() -> InferredDeps (typed dependency contract)
          -> build_task_graph_from_inferred_deps() -> TaskGraph (rustworkx)
            -> schedule_tasks() -> TaskSchedule (cost-aware ordering)
              -> build_task_execution_module() -> Hamilton DAG (dynamic codegen)
```

Every link in this chain derives its output from the plan, not from static declarations.
No manual `inputs=` declarations exist. Dependencies come from DataFusion optimized
logical plan lineage. UDF requirements come from plan expression walking. Column
requirements come from plan projection analysis.

The `PlanSignals` typed control plane (`src/datafusion_engine/plan/signals.py`) captures
schema, lineage, normalized stats, scan compatibility, and plan fingerprint in a single
structured artifact. Write policy adaptation already consumes `PlanSignals.stats` to
derive adaptive `target_file_size`.

The `ExecutionAuthorityContext` (`src/relspec/execution_authority.py`) is fully
implemented with validation enforcement, composing `SemanticExecutionContext` with
`EvidencePlan`, `extract_executor_map`, and `capability_snapshot`.

### 1.2 Wave Completion Status

| Wave | Goal | Status | Remaining |
|------|------|--------|-----------|
| **Wave 0** | Reality alignment | **Complete** | - |
| **Wave 1** | Resolver threading closure | **90%** | 6 bridge fallbacks still use `CompileContext` (CQ-verified: 7 pattern matches, 6 outside boundary) |
| **Wave 2** | Compile boundary convergence | **Not started** | 6 production `CompileContext(runtime_profile=...)` sites outside `compile_context.py` |
| **Wave 3** | Extract executor binding | **95%** | Deprecated global registry still functional; production dependence reduced to test-oriented paths |
| **Wave 4A** | Maintenance from execution | **Implemented, not integrated** | `build_write_outcome_metrics` + `resolve_maintenance_from_execution` exist (`maintenance.py:153`, `:244`) but have no production callsites; write path still uses `resolve_delta_maintenance_plan` |
| **Wave 4B** | Scan policy from plan | **Infrastructure exists** | `derive_scan_policy_overrides` + `_apply_inferred_scan_policy_overrides` wired in `plan/pipeline.py`; next step is richer signals + measured payoff |
| **Wave 5** | Artifact contract governance | **Phase 1 complete** | ~180 callsites still use raw strings |
| **8.7** | ExecutionAuthorityContext | **Complete** | - |
| **8.8** | PlanSignals | **Complete** | - |
| **8.9** | Delta compatibility reuse | **Complete** | - |
| **10.1** | Output naming from manifest | **Complete** | Identity naming deployed |
| **10.8** | Hamilton DAG generation | **Complete** | Dynamic module builder operational |
| **10.10** | Policy bundle validation | **Operational** | `validate_policy_bundle()` active in `driver_factory.py:2003`; expand rule quality and artifact enforcement |

> **Note (from review):** V1 originally referenced `build_cpg_streaming`; current code
> path is `build_cpg_from_inferred_deps` in `src/semantics/pipeline.py:2012`.

### 1.3 Drift Surface Scorecard

Current measurements against the target metrics from the original assessment
(CQ-verified counts where noted):

| Metric | Original | Current | Target |
|--------|----------|---------|--------|
| `CompileContext` callsites in `src/` (CQ) | - | 9 | boundary-only |
| `CompileContext(runtime_profile=...)` outside boundary (CQ) | 7 | 6 | 0 |
| `dataset_bindings_for_profile()` consumer callsites in `src/` (CQ) | 7 | 5 | 0 |
| Global mutable registries | 1 | 1 (deprecated, test-oriented) | 0 |
| `record_artifact(` with raw strings | ~180 | ~165 | 0 |
| `SemanticExecutionContext` threading sites | 0 | 4 | all entry points |
| `ExecutionAuthorityContext` adoption | 0 | 1 (driver_factory) | all orchestration |
| `detect_plan_capabilities()` wired | - | Yes (`runtime_capabilities.py:78`) | hardened |
| `validate_policy_bundle()` active | - | Yes (`driver_factory.py:2003`) | expanded |

---

## 2. Architectural Analysis: Where the Boundary Is Today

### 2.1 The Compile/Runtime Boundary

The architecture follows a clean compile-then-execute model:

**Compile time** (produces immutable manifest):
- Build IR from semantic model (`compile_semantics` / `optimize_semantics` / `emit_semantics`)
- Validate inputs against IR requirements
- Resolve dataset bindings from catalog
- Return `SemanticProgramManifest` with fingerprint

**Runtime** (consumes manifest + profile):
- Generate DataFusion builders from IR view specs
- Build plan bundles (captures DataFusion logical plans)
- Extract dependencies from plan lineage
- Register views in topological order
- Materialize outputs with derived cache policies

The boundary is clean but **runtime re-derivation persists** in five key locations where
`CompileContext(runtime_profile=...)` is constructed instead of receiving the pre-compiled
`SemanticExecutionContext`. These are the Wave 2 targets.

### 2.2 What Flows Programmatically vs. What Is Still Bridged

**Fully programmatic** (derived from plans, no static declarations):
- Task dependencies (from DataFusion plan lineage)
- Column-level requirements (from plan projection)
- UDF requirements (from plan expression walking)
- Required rewrite tags (from UDF metadata)
- Plan fingerprints (from plan content hashing)
- Task scheduling order (from rustworkx graph analysis)
- Hamilton DAG topology (from execution plan)
- Schema contracts (from DataFusion schema introspection)
- Write policy adaptation (from plan statistics via PlanSignals)
- Scan unit file pruning (from Delta add-actions + pushed predicates)
- Protocol compatibility (from `delta_protocol_compatibility()`)

**Still bridged** (re-derived from runtime profile, not from manifest):
- Dataset locations (6 sites re-derive via `CompileContext`, CQ-verified)
- Extract executor availability (deprecated global registry still functional, test-oriented)
- Delta maintenance decisions (policy-presence-based; outcome-based resolver `resolve_maintenance_from_execution` exists but is not wired into write path)
- Cache policy assignment (derived from naming conventions, not from plan properties)

**Partially bridged** (infrastructure exists, integration coverage uneven):
- Scan policy per table (`derive_scan_policy_overrides` wired in `plan/pipeline.py`, but signal richness and payoff measurement incomplete)
- DataFusion capability detection (`detect_plan_capabilities()` exists at `runtime_capabilities.py:78` and is wired into `driver_factory.py:601`, but not all consumers gate on it)

### 2.3 DataFusion Plan Introspection Depth

The current plan introspection stack is comprehensive:

```
DataFusionPlanBundle
  ├── logical_plan (unoptimized)
  ├── optimized_logical_plan
  ├── execution_plan (physical)
  ├── substrait_bytes (serialized, cross-system)
  ├── plan_fingerprint (deterministic hash)
  ├── delta_input_pins (version-locked inputs)
  ├── udf_requirements (from expression walking)
  ├── scan_units (file-level, per-dataset)
  └── plan_details (statistics, partitioning, explain trees)
```

`PlanSignals` (`src/datafusion_engine/plan/signals.py`) distills this into an
actionable control plane:

```
PlanSignals
  ├── schema: pa.Schema
  ├── lineage: LineageReport (scans, joins, filters, UDFs, columns)
  ├── stats: NormalizedPlanStats (num_rows, total_bytes, partition_count)
  ├── scan_compat: tuple[ScanUnitCompatSummary, ...]
  ├── plan_fingerprint: str
  └── repartition_count: int
```

**DataFusion version consideration:** DataFusion 51.0.0 (Python bindings) does not
expose `ExecutionPlan.statistics()` or `ExecutionPlan.schema()` as callable methods.
The existing plan statistics extraction in `bundle.py` uses defensive extraction from
`plan_details` and `explain_analyze` output. Any new stats-dependent code must use
capability detection, not version assumptions.

> **Update (from review):** The capability adapter is no longer unimplemented.
> `detect_plan_capabilities()` exists at `runtime_capabilities.py:78`, a snapshot
> builder includes plan capabilities at `:213`, and execution authority wiring exists
> in `driver_factory.py:601`. The remaining work is hardening and coverage
> expansion, not greenfield construction.

---

## 3. Highest-Leverage Next Steps

The analysis identifies five convergence opportunities ordered by architectural leverage,
considering both the dependency graph and the blast radius of each change.

### 3.1 Complete Wave 1+2: Resolver and Compile Boundary Closure

**Leverage:** Highest. This eliminates the primary drift surface and is a prerequisite
for all downstream work.

**Current state:** 6 production sites still construct `CompileContext(runtime_profile=...)`
as a fallback (CQ-verified: 7 pattern matches in `src/`, 6 outside `compile_context.py`):

| File | Line | Function |
|------|------|----------|
| `engine/materialize_pipeline.py` | 315 | `materialize_view_to_location()` |
| `semantics/pipeline.py` | 1406 | `build_cpg()` |
| `semantics/pipeline.py` | 2012 | `build_cpg_from_inferred_deps()` |
| `hamilton_pipeline/modules/task_execution.py` | 510 | `_semantic_view_registration()` |
| `hamilton_pipeline/modules/task_execution.py` | 546 | `_execute_view()` |
| `extract/coordination/materialization.py` | 461 | `_resolve_dataset_resolver()` |

All six already accept `execution_context: SemanticExecutionContext | None` and prefer
it when provided. The remaining work is:

1. **Ensure all upstream callers pass `execution_context`** rather than `None`
2. **Remove the `CompileContext` fallback** once all paths provide context
3. **Add resolver identity assertions** to verify single-instance threading
4. **Add compile-once invariant** (the `CompileTracker` from section 8.12)

**Pattern:**
```python
# BEFORE (current fallback)
if execution_context is not None:
    resolver = execution_context.dataset_resolver
else:
    resolver = CompileContext(runtime_profile=profile).dataset_bindings()

# AFTER (context required)
resolver = execution_context.dataset_resolver
```

**Risk:** Low. Each site already has the parameter; this is removing fallbacks, not
adding new plumbing. Test coverage exists for all paths.

**Exit criteria:**
- Zero `CompileContext(runtime_profile=...)` outside `compile_context.py`
- Zero `dataset_bindings_for_profile()` consumer callsites outside `compile_context.py`
- Identity assertion passes in integration tests

### 3.2 Harden DataFusion Capability Adapter (Section 8.2)

**Leverage:** High. Consistent capability gating is a prerequisite for reliable
stats-dependent scheduling and adaptive scan policies.

> **Update (from review):** The capability adapter is already implemented and wired
> in primary paths. This is now a hardening and coverage task, not greenfield.

**Current state:** Capability detection infrastructure exists:
- `detect_plan_capabilities()`: `src/datafusion_engine/extensions/runtime_capabilities.py:78`
- Snapshot builder includes plan capabilities: `runtime_capabilities.py:213`
- Execution authority wiring: `src/hamilton_pipeline/driver_factory.py:601`
- UDF availability: checked via snapshot validation
- Protocol compatibility: checked via `delta_protocol_compatibility()`
- Plan statistics: defensively extracted from `plan_details` dict

**Remaining work (hardening):**
- Ensure all stats-dependent consumers gate on capability snapshot, not version assumptions
- Populate `ExecutionAuthorityContext.capability_snapshot` consistently (currently `None` in some paths)
- Add self-documenting diagnostics explaining why features are enabled/disabled
- Record capability snapshot as typed artifact at startup: `"datafusion_capabilities_v1"`

**Risk:** Very low. Infrastructure exists; this is coverage expansion.

### 3.3 Integrate Outcome-Based Delta Maintenance (Wave 4A)

**Leverage:** Medium-high. Transitions the last major static declaration (maintenance
policy presence) to a programmatic, outcome-driven model.

> **Update (from review):** Outcome-based maintenance logic already exists but is not
> integrated into the write flow. This is an integration priority, not a new feature.

**Current state:**
- **Exists but unwired:**
  - `build_write_outcome_metrics`: `src/datafusion_engine/delta/maintenance.py:153`
  - `resolve_maintenance_from_execution`: `src/datafusion_engine/delta/maintenance.py:244`
  - CQ shows **no production callsites** for these two functions.
- **Still in use:** `resolve_delta_maintenance_plan` at:
  - `src/datafusion_engine/io/write.py:1677`
  - `src/semantics/incremental/delta_context.py:215`

**Target:** Maintenance decisions driven by execution outcomes:
- Optimize if `files_created > threshold` or `total_file_count > threshold`
- Vacuum if `version_delta > retention_threshold`
- Checkpoint if `version_delta > checkpoint_interval`

**Integration steps** (the resolver and metrics builder already exist):
1. Wire `build_write_outcome_metrics` into write result capture in `io/write.py`
2. Replace `resolve_delta_maintenance_plan` calls with `resolve_maintenance_from_execution`
3. Persist outcome-driven maintenance decisions as artifacts with cause fields
4. Thread write metrics from `WritePipeline.write()` result back to execution layer

**Key DataFusion/DeltaLake integration point:** After `write_deltalake()` completes,
the `DeltaTable` instance provides `version()` and `files()` which supply the
metrics needed for threshold-based decisions. The existing write pipeline
(`src/datafusion_engine/io/write.py`) already captures `delta_result.version` -
extending to capture file counts is mechanical.

**Risk:** Low-medium. The resolver exists; this is wiring + validation, not design.

### 3.4 Enrich Adaptive Scan Policy Signals (Wave 4B)

**Leverage:** Medium. Extends existing adaptive scan infrastructure with richer
signals and measured payoff tracking.

> **Update (from review):** Adaptive scan-policy infrastructure already exists and
> is wired. This is a signal enrichment and payoff measurement task, not initial
> construction.

**Current state:**
- **Already wired:**
  - Inference function: `src/datafusion_engine/delta/scan_policy_inference.py:42`
  - Pipeline application: `src/datafusion_engine/plan/pipeline.py:131` and `:173`
  - CQ confirms call path (`derive_scan_policy_overrides`, `_apply_inferred_scan_policy_overrides`)
- `apply_scan_unit_overrides()` in `src/datafusion_engine/dataset/resolution.py`
  applies scan policy with override support

**Target:** Richer per-table adaptive scan policy derived from plan signals:
- Small tables (low row count): disable file pruning overhead
- Tables with pushed predicates: enable Parquet column/page pruning
- Tables with sort keys matching join keys: enable sort-order exploitation
- Measured payoff: track scan volume reduction as runtime artifact

**Enrichment approach:** Use existing `PlanSignals.lineage` and
`PlanSignals.stats` to add richer signal sources:

```python
def derive_scan_policy_for_table(
    table_name: str,
    plan_signals: PlanSignals,
    base_policy: ScanPolicyConfig,
) -> ScanPolicyConfig:
    # Derive from lineage.scans, lineage.filters, stats.num_rows
```

This follows the architecture principle: plan introspection products (PlanSignals)
drive all downstream decisions.

**Risk:** Low-medium. Infrastructure exists; this is enrichment with measured
payoff gates.

### 3.5 Complete Artifact Governance Migration (Wave 5 Phases 2-5)

**Leverage:** Medium. Eliminates raw-string artifact names across the remaining ~165
callsites. The infrastructure (ArtifactSpec, ArtifactSpecRegistry, adapter pattern)
is complete from Phase 1.

**Current state:**
- 14 typed specs registered with `payload_type` (msgspec Struct validation)
- 9 untyped specs registered (canonical name only, no schema validation)
- ~165 callsites still use raw string names
- Migration scripts exist (`scripts/migrate_artifact_callsites.py`,
  `scripts/audit_artifact_callsites.py`)

**Phased migration order** (from original plan, adjusted for current state):
1. **Phase 2: Write path** (~20-25 callsites) - `io/write.py`, `delta/maintenance.py`
2. **Phase 3: Hamilton lifecycle** (~30-40 callsites) - `task_execution.py`, `driver_factory.py`
3. **Phase 4: Diagnostics** (~40-50 callsites) - `views/graph.py`, `session/runtime.py`
4. **Phase 5: Remaining** (~60-70 callsites) - all other files

**Risk:** Low per-phase. Adapter pattern supports both typed specs and legacy strings.
Each phase is independently testable.

---

## 4. Architectural Opportunities Beyond the Original Plan

The codebase exploration reveals several opportunities that extend the programmatic
architecture beyond the original assessment scope.

### 4.1 Cache Policy Derivation from Plan Properties

**Current state:** Cache policies (`CachePolicy.NONE`, `DELTA_STAGING`,
`DELTA_OUTPUT`) are assigned at view node construction time based on naming
conventions and dataset location type. This is a static declaration pattern.

**Opportunity:** Derive cache policy from plan properties:
- Views with high fan-out (many downstream consumers) -> `DELTA_STAGING`
- Views that are final outputs -> `DELTA_OUTPUT`
- Views with single consumer and low cost -> `NONE`

The information needed is already available in the `TaskGraph` (rustworkx
`out_degree` per node) and `TaskPlanMetrics` (estimated cost). A
`derive_cache_policy(task_graph, node, metrics)` function could replace the
current static assignment.

### 4.2 DataFusion Caching Policy and Telemetry

> **Update (from review):** `DataFrame.cache()` is already integrated at
> `src/datafusion_engine/dataset/registration.py:3176`. The opportunity is
> policy/telemetry quality, not first-time integration.

For normalized datasets consumed by many relationship rules (high fan-out),
the existing `cache()` integration can be extended with policy-driven selection:
if the cache policy is `DELTA_STAGING` and the view's estimated size fits in
memory, prefer `DataFrame.cache()` over Delta staging writes, eliminating
I/O overhead while maintaining the same logical pipeline.

DataFusion also provides `RuntimeEnv.CacheManager` for engine-level cache lifecycle
management, and `list_files_cache_*` / `metadata_cache_limit` settings for
reducing repeated object-store and Parquet metadata overhead across rule evaluation.

### 4.3 Expand Compile-Time Policy Bundle Validation (10.10)

> **Update (from review):** Compile-time policy bundle validation is already
> operational. `validate_policy_bundle()` exists at `relspec/policy_validation.py:155`
> and is called in plan-context build at `driver_factory.py:2003`. The remaining
> work is expanding rule quality and artifact enforcement.

With `PlanSignals` (8.8) and `ExecutionAuthorityContext` (8.7) both complete,
the existing validation can be extended to cover:

- Feature gates enable all required UDFs
- Delta protocol support covers write operations
- Scan policy is compatible with dataset characteristics
- Validation results emitted as structured `PolicyValidationArtifact` with
  machine-readable error codes

The validation runs at `compile_execution_plan()` time, NOT inside
`compile_semantic_program()` (which is semantic-only).

### 4.4 Resolver Identity and Single-Compile Invariants (8.1 + 8.12)

The infrastructure for identity tracking is ready but not enforced:

1. **Resolver identity:** Add `ResolverIdentityTracker` that asserts all
   subsystems in a pipeline run reference the same `ManifestDatasetResolver`
   instance (identity check, not equality). Wire into integration tests.

2. **Single-compile invariant:** Add `CompileTracker` context manager that
   enforces `compile_semantic_program()` is called exactly once per pipeline run.
   Current code has 2-3 redundant compile calls in orchestration paths.

Both are low-risk, high-value invariant enforcement that prevents regression
as the architecture evolves.

### 4.5 Plan-Statistics-Driven Scheduling Enhancement

The scheduling infrastructure (`ScheduleCostContext` with `bottom_level_costs`,
`task_slack_by_task`, `betweenness_centrality`, `articulation_tasks`,
`bridge_tasks`) is complete. The opportunity is feeding actual plan statistics
into the cost model:

- `TaskPlanMetrics.stats_row_count` -> task duration estimate
- `TaskPlanMetrics.stats_total_bytes` -> I/O cost estimate
- `ScanUnit.total_files` / `candidate_file_count` -> scan cost estimate

Currently, scheduling uses uniform costs. With plan-statistics-driven costs,
the scheduler would prioritize large, critical-path tasks and schedule small
tasks to fill gaps - standard HEFT-style scheduling.

**Prerequisite:** Section 8.2 (capability adapter) for reliable stats access.

### 4.6 Policy-as-Data Compiled Artifact

*(From review: V2 §4.1)*

Add a canonical `CompiledExecutionPolicy` artifact generated at compile time and
threaded into runtime unchanged. Runtime consumes the policy artifact; it does not
re-derive policy heuristically.

Suggested sections:
- `scan_policy_by_dataset`
- `cache_policy_by_view`
- `maintenance_policy_by_dataset`
- `udf_requirements_by_view`
- `validation_mode` and enforcement level

This is the structural mechanism that converts all remaining heuristic-based runtime
decisions into compile-time-resolved, artifact-backed decisions.

### 4.7 Plan-Evidence Confidence Model

*(From review: V2 §4.2)*

Attach confidence and rationale to inferred decisions:
- `confidence_score` (0-1)
- `evidence_sources` (lineage/stats/runtime outcomes/capabilities)
- `fallback_reason` (when confidence insufficient)

Use this for:
1. Conservative fallback behavior when confidence is insufficient
2. Drift diagnostics identifying where inference is weakest
3. Governance dashboards for policy maturity

### 4.8 Closed-Loop Optimization from Artifacts

*(From review: V2 §4.3)*

Move from one-way planning to a learning loop:
1. Compile emits baseline policy
2. Runtime captures execution outcomes and DataFusion metrics
3. Post-run calibrator updates policy defaults for future runs (bounded,
   deterministic update rules)

This is the path to "inference-driven" without unsafe online adaptation.

### 4.9 DataFusion Pruning-Ladder Optimization Program

*(From review: V2 §4.4)*

Treat pruning efficacy as a first-class product metric. Programmatically tune and
track:
1. Row-group pruning effectiveness
2. Page-index pruning effectiveness
3. Filter pushdown efficacy
4. Statistics availability and freshness

Use explain/metrics artifacts to measure actual pruning gains per query shape.

### 4.10 Runtime Cache Stack Observability

*(From review: V2 §4.5)*

Beyond `df.cache()`, add explicit observability over cache layers:
1. Listing/file-metadata/statistics cache configuration at session build
2. Cache hit/miss telemetry as runtime artifacts
3. Per-workload cache profile selection (batch ingest, interactive query,
   compile-heavy replay)

This prevents blind cache tuning and supports reproducible performance.

### 4.11 External Index Acceleration for Semantic Workloads

*(From review: V2 §4.6)*

For dominant semantic query shapes (span-range, symbol lookup, file-scoped
traversals), evaluate external index assisted file pruning:
1. Coarse file-level pruning by external index/provider
2. Handoff to DataFusion parquet row-group/page pruning for in-file refinement

This can materially reduce scan volume for large code corpora while preserving
declarative query semantics.

### 4.12 Semantic Object Model and Relation Templates as Compiled Schema

*(From review: V2 §4.7)*

Elevate relationship/object templates to a typed, compile-validated schema layer:
1. Template registry with versioned contracts
2. Static validation at compile stage
3. Generated relation builders and tests from templates

Result: fewer bespoke relation implementations and better evolution safety.
This aligns directly with the entity-centric data modeling vision in section 9.4
and relationship spec templates in section 9.8.

### 4.13 Reproducible Execution Package

*(From review: V2 §4.8)*

Define a replayable "execution package" keyed by fingerprint:
- Semantic manifest hash
- Execution policy artifact hash
- Runtime capability snapshot hash
- Plan bundle fingerprints
- Session configuration hash

This becomes the primitive for:
1. Deterministic replay
2. Performance regression bisecting
3. Policy-impact attribution

---

## 5. Recommended Execution Order

Based on dependency analysis, architectural leverage, and risk.

> **Reframing (from review):** The roadmap should be framed around **integration
> closure and control-loop maturity**, not broad new foundations. Most components
> exist; the work is wiring, hardening, and closing feedback loops.

### Phase A: Correctness and Integration Closure (Eliminates drift surfaces)

1. **Wave 1+2 completion** - Remove `CompileContext` fallbacks (6 sites, CQ-verified)
2. **Section 8.2** - Harden DataFusion capability adapter (exists, needs coverage)
3. **Section 8.1** - Resolver identity assertions
4. **Wave 4A** - Wire outcome-based maintenance into write path (functions exist,
   need integration)
5. **Invariant tests** - Single compile per run, resolver identity reuse, zero
   fallback compile-context callsites in target modules

These create a solid foundation where the manifest-backed execution context flows
through all production paths, capability detection is consistent, identity
invariants are enforced, and maintenance is outcome-driven.

**Outcome:** Zero drift surfaces for dataset locations, compile boundaries, and
maintenance triggers. All existing infrastructure wired into production paths.

### Phase B: Policy Compilation and Evidence Model

6. **4.6** - Introduce `CompiledExecutionPolicy` artifact
7. **10.10** - Expand compile-time policy bundle validation (already operational,
   extend rules)
8. **4.7** - Add confidence/rationale fields to inference decisions
9. Replace cache naming heuristics with compiled policy derivation from IR view
   kind, lineage, cardinality, and volatility markers

These extend the programmatic model from "derive decisions from plans" to
"compile all policy into a single artifact consumed unchanged at runtime."

**Outcome:** Runtime consumes a compiled policy artifact. No heuristic-based
re-derivation. Inference decisions include confidence and evidence sources.

### Phase C: Adaptive Optimization with Bounded Feedback

10. **Wave 4B** - Enrich adaptive scan policy signals (infrastructure exists,
    add richer signals + payoff measurement)
11. **4.8** - Build artifact calibrator for scan/maintenance/cache thresholds
12. Add rollout controls (off/warn/enforce) by policy domain
13. Require measurable payoff gates (latency, bytes scanned, maintenance cost)

**Outcome:** Optimization behavior is measurable and self-improving under strict
invariants. Bounded, deterministic feedback loop.

### Phase C' (Parallel): Infrastructure Hardening

14. **Wave 5 Phases 2-5** - Artifact governance migration (~165 callsites)
15. **Section 8.12** - Fidelity and safety invariants
16. **Plan-statistics scheduling** - Cost-aware task ordering
17. Deprecation path completion for extract registry (low production risk)

These can proceed independently and don't block the critical path.

**Outcome:** All artifact callsites use typed specs, invariants are enforced
in CI, and runtime policies are fully plan-derived.

---

## 6. Dependency Graph (Updated)

```
Phase A: Correctness and Integration Closure
  Wave 1+2 (Resolver + Compile Boundary, 6 sites)
    |
    +---> 8.2 Harden (Capability Adapter coverage)
    |       |
    +---> 8.1 (Resolver Identity Assertions)
    |       |
    +---> Wave 4A Wire (Outcome Maintenance → write path)
    |       |
    +---> Invariant Tests (compile-once, identity, zero fallbacks)
    |
    v
Phase B: Policy Compilation and Evidence Model
    |
    +---> 4.6 (CompiledExecutionPolicy artifact)
    |       |
    +---> 10.10 Expand (Policy Bundle Validation rules)
    |       |
    +---> 4.7 (Confidence + Rationale fields)
    |       |
    +---> Cache policy from compiled artifact (replaces naming heuristic)
    |
    v
Phase C: Adaptive Optimization with Bounded Feedback
    |
    +---> Wave 4B Enrich (Scan policy signals + payoff measurement)
    |
    +---> 4.8 (Artifact calibrator, closed-loop)
    |
    +---> Rollout controls + payoff gates
    |
    v
Phase C' (parallel): Infrastructure Hardening
    |
    +---> Wave 5 Phases 2-5 (Artifact Migration)
    +---> 8.12 (Fidelity Invariants)
    +---> Plan-Statistics Scheduling
    +---> Extract registry deprecation completion
```

**Critical path:** Wave 1+2 → 8.2 harden → Phase B (CompiledExecutionPolicy) → Phase C

**Independent tracks:**
- Wave 4A wire (after Wave 1+2, low risk)
- Wave 5 (any time)
- 8.12 (after 10.10)
- Extract registry deprecation (any time, low risk)

---

## 7. Measuring Progress (Updated Metrics)

### Drift Surface Metrics

| Metric | Current (CQ-verified) | After Phase A | After Phase B | Final |
|--------|----------------------|---------------|---------------|-------|
| `CompileContext(runtime_profile=...)` outside boundary | 6 | 0 | 0 | 0 |
| `dataset_bindings_for_profile()` consumers in `src/` | 5 | 0 | 0 | 0 |
| Global mutable registries | 1 (deprecated, test-oriented) | 0 | 0 | 0 |
| `record_artifact(` with raw strings | ~165 | ~165 | ~165 | 0 |
| Capability detection coverage | partial (wired, uneven) | consistent | consistent | consistent |
| Outcome-based maintenance in write path | 0 (exists, unwired) | wired | wired | wired |
| Scan policy signal richness | basic | basic | enriched | enriched + feedback |

### Programmatic Coverage Metrics

| Aspect | Current | After Phase A | After Phase B |
|--------|---------|---------------|---------------|
| Task dependencies | Programmatic | Programmatic | Programmatic |
| UDF requirements | Programmatic | Programmatic | Programmatic |
| Column requirements | Programmatic | Programmatic | Programmatic |
| Scheduling order | Programmatic | Programmatic | Programmatic + cost-aware |
| Dataset locations | 90% (6 fallbacks) | 100% | 100% |
| Write policy | Partially adaptive | Fully adaptive | Compiled artifact |
| Scan policy | Partially adaptive (wired) | Enriched signals | Feedback-calibrated |
| Maintenance decisions | Policy-presence (outcome resolver exists but unwired) | Outcome-threshold | Compiled policy |
| Cache policy | Static naming | Static naming | Compiled artifact |
| Policy validation | Operational (basic rules) | Expanded rules | Confidence-scored |

### Hard Migration KPIs (from review)

Track these as the definitive "entirely programmatic" indicators:

| KPI | Current | Target |
|-----|---------|--------|
| `compile_context_fallback_callsites_in_src` | 6 | 0 outside compile boundary |
| `dataset_bindings_profile_fallback_callsites_in_src` | 5 | 0 outside compile boundary |
| `runtime_policy_derivation_points` | multiple heuristics | 0 unmanaged heuristics |
| `inference_decisions_with_confidence` | 0% | 100% |
| `writes_using_outcome_based_maintenance` | 0% | 100% for Delta writes |
| `artifact_replay_success_rate` | unmeasured | >99% deterministic replay parity |
| `pruning_effectiveness_score` by workload class | unmeasured | monotonic improvement |

---

## 8. Key Architectural Principle

The unifying principle across all phases is:

> **The DataFusion plan is the single source of truth for what the pipeline needs.**
> Everything else - dependencies, scheduling, UDFs, schema contracts, maintenance,
> scan policies, cache strategies - is derived from plan introspection products.
> Runtime configuration sets boundaries and thresholds; the plan determines behavior
> within those boundaries.

The two core inputs (extracted data + data operations mapping) produce DataFusion
plans. Plans produce `PlanSignals`. Signals drive all downstream decisions. No
static declaration should exist that could contradict what the plans actually require.

The remaining convergence work is eliminating the last bridge patterns that
re-derive information the plan already provides, and extending the plan-driven
model into maintenance, scan policy, and validation domains where static
declarations still persist.

### 8.1 Refined Assessment (from review)

The architectural direction remains correct, but the continuation plan should be
reframed around **integration closure and control-loop maturity**, not broad new
foundations. The repo is now at the stage where best-in-class trajectory depends on:

1. Making compile artifacts the sole source of runtime truth
2. Making policy decisions evidence-backed and replayable
3. Making optimization behavior measurable and self-improving under strict invariants

The shift in framing is from "add more static declarations" to "compile policy and
behavior from plan/capability evidence." Most of the components needed for this
already exist - the work is integration, coverage, and closing feedback loops.

### 8.2 Immediate Implementation Notes (from review)

Low-risk, high-leverage starting points:

1. Start with `src/hamilton_pipeline/modules/task_execution.py` fallbacks
   (`_ensure_scan_overrides`, `_execute_view`) because they are central bridge
   points and already accept `execution_context`
2. Update write pipeline integration in `src/datafusion_engine/io/write.py` to
   use outcome-based maintenance resolver and emit decision artifacts
3. Replace `_default_semantic_cache_policy` in `src/semantics/pipeline.py` with
   manifest/IR-driven policy selection
4. Keep deprecated extract registry APIs in compatibility mode but remove
   remaining production initialization reliance

---

## 9. Deeper Review: Toward Minimal-Statement View Definitions

The architecture has a clear target state: view creation should be expressible in
the minimum number of statements that are truly necessary to fully specify a view.
Anything beyond that is a failure to formalize operations that are intrinsically
part of the process. This section identifies the specific consolidation
opportunities that move toward that ideal.

### 9.1 The Current View Specification Gap

A view in the CPG pipeline requires specifying these concerns:

1. **What to compute** (normalize / relate / union / aggregate / dedupe)
2. **Which inputs** (source tables or upstream views)
3. **How to join** (join keys, join strategy, filter predicates)
4. **What quality signals to apply** (hard predicates, feature scoring, ranking)
5. **What to output** (projection columns, naming)
6. **How to cache** (none, delta_staging, delta_output)
7. **What schema contract to enforce** (dedupe keys, sort order, constraints)

Of these, only concerns 1, 2, and 4 are genuinely unique per view. Concerns
3, 5, 6, and 7 can be derived from the semantic type system and the view's
position in the dependency graph. Yet today all seven are specified explicitly,
creating substantial boilerplate that obscures the essential specification.

**Evidence of redundancy in the current relationship specs:**

| Field | Declarations | Unique Values | Redundancy |
|-------|-------------|---------------|------------|
| `left_on` / `right_on` | 18 (9 specs x 2) | 1 (`["file_id"]`) | 94% |
| `how` | 9 | 1 (`"inner"`) | 89% |
| `rank.keep` | 9 | 1 (`"best"`) | 89% |
| `rank.top_k` | 9 | 1 (`1`) | 89% |
| Output projection pattern | 9 | 2 templates | 78% |
| Ranking partition key | 9 | Derivable from left entity | 100% |

### 9.2 Underutilized Inference Infrastructure

The codebase already contains the infrastructure to infer most of these
redundant declarations, but its coverage depth and calibration remain improvable:

> **Correction (from review):** `infer_join_strategy()` is used in production via
> `require_join_strategy()` at `src/semantics/compiler.py:806`. CQ confirms the
> call chain. The original "zero callsites" claim was incorrect.

**`infer_join_strategy()`** (`semantics/joins/inference.py`): A complete,
priority-ordered join strategy inference system that examines `AnnotatedSchema`
to determine the correct join type (span overlap, span contains, foreign key,
symbol match, file equi-join). This function **is called in production** via
`require_join_strategy()` in the `SemanticCompiler`, but coverage depth and
calibration remain improvable - many relationship specs still declare join
strategies manually rather than relying on inference.

**`AnnotatedSchema.infer_join_keys()`** (`semantics/types/annotated_schema.py`):
A method that finds compatible join key pairs between two schemas using the
`CompatibilityGroup` system. It exists but is **never called**. All join keys
are currently hard-coded as `["file_id"]` in every relationship spec.

**`JoinCapabilities.from_schema()`** (`semantics/joins/inference.py`): Extracts
structural capabilities (has_file_identity, has_spans, has_entity_id,
has_symbol, fk_columns) from an `AnnotatedSchema`. This is the foundation
for automatic join strategy selection but is not used in the compilation path.

**`SemanticSchema` classification** (`semantics/schema.py`): Already infers
table type hierarchy (RAW → EVIDENCE → ENTITY → RELATION) and column roles
(PATH, SPAN_START, SPAN_END, ENTITY_ID, SYMBOL, TEXT). This classification
determines *what operations are valid* on a table, but the information is not
used to *select* operations automatically.

### 9.3 The Minimal-Statement View Definition Model

With the inference infrastructure activated, view definitions reduce to their
essential, irreducible content:

**Normalization** (currently ~15 lines per spec → 1 statement):
```python
# Current: 15 lines of SemanticTableSpec + SemanticNormalizationSpec
# Essential: "normalize cst_refs, producing entity IDs in the cst_ref namespace"
normalize("cst_refs", namespace="cst_ref")
# Everything else inferred: span columns, path column, entity ID derivation,
# foreign keys, text columns, output naming, canonical projections
```

**Relationship** (currently ~40 lines per QualityRelationshipSpec → 3-5 statements):
```python
# Current: 40 lines specifying join keys, strategy, signals, ranking, projection
# Essential: "relate refs to SCIP occurrences by span overlap with these quality signals"
relate(
    "cst_refs_norm", "scip_occurrences_norm",
    strategy="span_overlap",                    # Or inferred from schemas
    hard=[span_overlaps("l__span", "r__span"), truthy("r__is_read")],
    features=[exact_span_match(weight=20.0), exact_end_match(weight=10.0)],
)
# Inferred: join keys (file_id), join type (inner), ranking (partition by
# left entity_id, order by score desc, keep best), output projection
# (entity_id, symbol, path, bstart, bend), provider, origin
```

**Union** (currently ~10 lines → 1 statement):
```python
# Essential: "union these relationship tables into edges"
union_edges(["rel_name_symbol", "rel_def_symbol", ...])
# Inferred: discriminator column, canonical edge schema, projection
```

### 9.4 Semantic Data Modeling: Objects as Concept Representations

The current `SemanticTableSpec` treats extracted tables as schemas with columns.
The deeper opportunity is treating them as *concept representations* where the
schema encodes the concept's identity, spatial location, and relationships:

**Current model** (schema-centric):
```python
SemanticTableSpec(
    table="cst_refs",
    primary_span=SpanBinding("bstart", "bend"),
    entity_id=IdDerivation(out_col="ref_id", namespace="cst_ref",
                           start_col="bstart", end_col="bend"),
    text_cols=("ref_text",),
)
```

**Concept model** (entity-centric):
```python
# A "reference" is a concept with:
# - Identity: derived from (path, bstart, bend) in the "cst_ref" namespace
# - Location: byte span [bstart, bend] in a source file
# - Content: the reference text
# - Relationships: can be related to symbols via span overlap
#
# Everything else follows from these concept properties:
# - Normalization applies stable_id + span_make automatically
# - Join strategy with SCIP tables uses span overlap (both are span-located)
# - Output projection includes entity_id, symbol, path, bstart, bend
# - Ranking partitions by entity_id (the concept identity)
```

The key insight is that `SemanticType` and `CompatibilityGroup` already model
these concept properties at the column level. The missing piece is a
higher-level abstraction that describes the *entity as a whole*:

```
Entity Declaration:
  name: "cst_ref"
  identity: stable_id(path, bstart, bend)
  location: byte_span(bstart, bend)
  content: [ref_text]
  source: "cst_refs"

→ Normalization, join strategy, ranking key, output schema all follow
  from this single declaration.
```

### 9.5 Builder Dispatch Consolidation

The pipeline currently has 15 `_builder_for_*` dispatch functions in
`semantics/pipeline.py` that route `SemanticIRView.kind` to builder
construction. Analysis reveals:

| Pattern | Functions | Core Logic |
|---------|-----------|------------|
| Dictionary dispatch (identical structure) | 4 | Lookup in dict, raise if missing |
| Simple compiler wrapper | 3 | Call SemanticCompiler method |
| Configuration-dependent | 4 | Lookup + apply config |
| Truly unique logic | 4 | Specific to SCIP/bytecode/projection/relationship |

**Opportunity:** The 4 dictionary-dispatch functions and 3 simple wrappers
(7 of 15 functions) follow identical patterns. A generic dispatch factory
eliminates ~80 lines of boilerplate:

```python
# Replace 7 functions with a single factory:
def _dispatch_from_registry(
    registry: Mapping[str, DataFrameBuilder],
    context_label: str,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    def _handler(spec, context):
        builder = registry.get(spec.name)
        if builder is None:
            raise KeyError(f"Missing {context_label} for {spec.name!r}")
        return builder
    return _handler
```

### 9.6 Cache Policy: From Naming Convention to Graph Property

The current `_default_semantic_cache_policy()` assigns cache policies based on
syntactic name patterns (`cpg_*` → delta_output, `rel_*` → delta_staging,
`*_norm` → delta_staging). This is a static declaration pattern that drifts
when naming conventions change.

**Target:** Derive cache policy from the view's position in the dependency
graph, which is already fully computed:

| Graph Property | Inferred Cache Policy |
|----------------|----------------------|
| Terminal node (no downstream consumers) + in output_locations | `delta_output` |
| Terminal node + not in output_locations | `none` |
| High fan-out (out_degree > 2) | `delta_staging` |
| Single consumer + low estimated cost | `none` |
| All other intermediate nodes | `delta_staging` |

The `TaskGraph` (rustworkx) already computes `out_degree`, `betweenness_centrality`,
and `articulation_tasks`. A `derive_cache_policy(task_graph, node)` function
replaces the entire naming-convention heuristic with a graph-derived property,
making it impossible for cache policy to drift from actual pipeline structure.

### 9.7 Eliminating Bespoke Protocols per Dataset

Several places in the pipeline have dataset-specific conditionals that should
be formalized as intrinsic operations:

**1. SCIP normalization special-casing:**
The IR compiler (`ir_pipeline.py:559-567`) manually constructs a
`SemanticIRView(kind="scip_normalize")` because SCIP occurrences require
line-index-to-byte-offset conversion. This is currently a bespoke protocol
but is actually an instance of a general pattern: "normalize an evidence
table whose spans are in line/column units, not byte units."

**Formalization:** Add a `span_unit` property to the entity declaration.
When `span_unit="line_column"`, the normalization pipeline automatically
inserts a line-index join before byte-span derivation. No special IR kind
needed.

**2. Bytecode line table special-casing:**
Similar to SCIP: `SemanticIRView(kind="bytecode_line_index")` exists because
bytecode line tables need line-to-byte conversion. Same formalization applies.

**3. Span unnest special-casing:**
Four `SemanticIRView(kind="span_unnest")` entries exist for AST, tree-sitter,
symtable, and bytecode instruction span unnesting. These all perform the same
operation (unnest nested span fields into flat columns) on different source
tables. A single parameterized "unnest spans from nested struct" operation
replaces four bespoke kinds.

**4. Symtable view special-casing:**
Five `SemanticIRView(kind="symtable")` entries map to five builder functions.
These are cross-table derivations (symtable_scopes × symtable_symbols → bindings,
etc.) that could be expressed as relationship specs with the appropriate
join strategy.

**Consolidation path:** Reduce the 14 `SemanticIRKind` values to ~6 by
formalizing the patterns:

| Current Kinds | Consolidated Kind | Parameterized By |
|---------------|------------------|------------------|
| normalize, scip_normalize, bytecode_line_index | `normalize` | `span_unit` (byte/line_column) |
| span_unnest | `normalize` | `structure` (flat/nested) |
| symtable | `derive` | derivation spec |
| join_group, relate | `relate` | join strategy + quality signals |
| union_nodes, union_edges | `union` | target schema |
| projection, finalize, export | `project` | output contract |
| diagnostic | `diagnostic` | (unchanged) |
| artifact | (remove) | (unused stub) |

### 9.8 Relationship Spec Template System

The `quality_specs.py` file contains 504 lines defining 9 relationship specs.
Analysis shows that 5 of the 9 follow an identical structural template
(span-based matching between a CST entity and SCIP occurrences):

```
Pattern: Entity-to-Symbol via Span Geometry
- Join on file_id (always)
- Hard predicate: span overlap or span contains
- Features: exact span start match, exact span end match
- Rank: partition by left entity ID, order by score desc, keep best 1
- Output: entity_id, symbol, path, bstart, bend
```

**Template factory** (reduces each spec from ~40 lines to ~5):

```python
def entity_symbol_relationship(
    name: str,
    left_view: str,
    right_view: str,
    *,
    span_strategy: Literal["overlap", "contains"],
    additional_hard: Sequence[HardPredicate] = (),
    entity_id_col: str = "entity_id",
    base_score: float = 2000,
    base_confidence: float = 0.95,
) -> QualityRelationshipSpec:
    # All boilerplate (join keys, ranking, projection) derived from:
    # - left schema's entity_id column
    # - right schema's symbol column
    # - span strategy selection
    ...
```

This is not hypothetical generalization - it is the formalization of a pattern
that already repeats 5 times with only the entity_id column name and span
strategy varying.

### 9.9 The Compile-Optimize-Emit Pipeline Enhancement

The current `compile_semantics() → optimize_semantics() → emit_semantics()`
pipeline builds the IR from static model declarations. The optimization phase
performs join-group fusion and dead-code pruning. The emit phase adds dataset
rows and fingerprints.

**Missing phase: Schema-Aware Inference**

Between compile and optimize, a new phase should:

1. **Resolve schemas** for all views in the IR (from AnnotatedSchema)
2. **Infer join strategies** using `infer_join_strategy()` for all join groups
3. **Validate declared join keys** against inferred compatible columns
4. **Derive cache policies** from graph properties instead of naming conventions
5. **Derive output projections** from the standard output templates when not
   explicitly specified

This phase transforms the IR from "declared intent" to "resolved plan" by
filling in everything that can be inferred. The result is that view
specifications need only contain the irreducible minimum: which tables to
connect and what quality signals to apply.

### 9.10 Summary: Statements Required Per View Kind

| View Kind | Current Statements | Minimal Statements | What's Eliminated |
|-----------|-------------------|-------------------|-------------------|
| Normalization | 15-20 lines | 1 (entity declaration) | Column mappings, span bindings, ID derivation defaults |
| Span-based relationship | 35-45 lines | 3-5 (tables + signals) | Join keys, join type, ranking, projection |
| FK-based relationship | 25-35 lines | 3-5 (tables + signals) | Same as above |
| Union (nodes/edges) | 8-10 lines | 1 (input list) | Discriminator, schema, projection |
| CPG output finalization | 10-15 lines | 0 (derived from IR) | Builder dispatch, contract wrapping |

**Total pipeline specification:**
- Current: ~800 lines across `registry.py`, `quality_specs.py`, `ir_pipeline.py`
- Minimal: ~120 lines of entity declarations + quality signal specs
- Reduction: ~85%

---

## 10. Execution Roadmap for Deeper Consolidation

### Phase D: Semantic Inference Activation (Highest leverage for consolidation)

11. **Expand `infer_join_strategy()` coverage in IR compilation** - The inference
    function is already called in production via `require_join_strategy()`
    (`compiler.py:806`), but many relationship specs still declare join strategies
    manually. Expand coverage so the compiler consistently uses schema-inferred
    strategies, removing redundant manual join key declarations
12. **Implement relationship spec templates** - Factory functions for the
    5 repeated entity-to-symbol relationship patterns. Align with the compile-
    validated template registry proposed in section 4.12
13. **Derive cache policy from graph properties** - Replace naming-convention
    heuristic with `TaskGraph` fan-out analysis. Feed into `CompiledExecutionPolicy`
    artifact (section 4.6) for compile-time resolution

**Outcome:** Relationship specs shrink from ~40 lines to ~5 lines. Cache
policy becomes self-maintaining. Redundant manual join declarations eliminated.

### Phase E: Entity-Centric Data Modeling (Deeper abstraction)

14. **Define entity declaration format** - High-level entity specs that
    encode identity, location, content, and relationship capabilities
15. **Consolidate IR view kinds** - Reduce from 14 to ~6 by parameterizing
    the bespoke protocols (SCIP normalization, span unnest, symtable)
16. **Builder dispatch factory** - Replace 7 boilerplate dispatch functions
    with a single generic factory

**Outcome:** The semantic model becomes a set of entity declarations and
relationship intents. The compilation pipeline infers everything else.

### Phase F: Pipeline-as-Specification (Long-term vision)

17. **Schema-aware inference phase** in compile-optimize-emit pipeline
18. **Auto-generated output projections** from standard templates
19. **Declarative quality signal composition** - Composable signal primitives
    that replace per-spec boilerplate
20. **Reproducible execution package** (section 4.13) - Fingerprint-keyed
    replayable execution for deterministic replay and regression bisecting

**Outcome:** Adding a new evidence source requires only an entity
declaration and (optionally) relationship quality signals. All normalization,
join strategy, caching, scheduling, and output projection are derived.
Execution is reproducible and replayable by fingerprint.

### Phase G: Advanced Acceleration Track (from review)

21. **Query-shape profiling** and workload classes
22. **External-index file pruning** provider for semantic-heavy workloads
    (section 4.11)
23. **Workload-specific DataFusion sessions** - Expand config/profile
    strategy for batch ingest vs. interactive query vs. compile-heavy replay
24. **Pruning-ladder optimization program** (section 4.9) - Track pruning
    efficacy as a first-class product metric

**Outcome:** Performance optimization is workload-aware, measured, and
self-improving. Scan volume reduction is tracked per query shape.

---

## 11. Updated Dependency Graph (Including Deeper Consolidation)

```
Phase A: Correctness and Integration Closure (from section 5)
  Wave 1+2 → 8.2 harden → 8.1 → Wave 4A wire → Invariant tests
    |
    v
Phase B: Policy Compilation and Evidence Model (from section 5)
  4.6 CompiledExecutionPolicy → 10.10 expand → 4.7 Confidence → Cache from policy
    |
    v
Phase C: Adaptive Optimization (from section 5)
  Wave 4B enrich → 4.8 Calibrator → Rollout controls
    |
Phase C': Infrastructure Hardening (parallel, from section 5)
  Wave 5 + 8.12 + Plan-stats scheduling + Extract registry deprecation
    |
    v
Phase D: Semantic Inference Activation
  Expand infer_join_strategy() coverage → Spec templates (+ 4.12) → Graph-derived cache
    |
    v
Phase E: Entity-Centric Data Modeling
  Entity declarations → Consolidate IR kinds → Builder dispatch factory
    |
    v
Phase F: Pipeline-as-Specification (long-term)
  Schema-aware inference → Auto projections → Declarative signals → 4.13 Execution package
    |
    v
Phase G: Advanced Acceleration Track (from review)
  Query-shape profiling → External-index pruning → Workload sessions → Pruning-ladder
```

**Critical insight:** Phases D and E can start in parallel with Phase B.
They have no dependency on the policy compilation work. Phase D specifically
has no prerequisites beyond what exists today - the inference infrastructure
is already built and used in production via `require_join_strategy()`, it
just needs expanded coverage.

**Phase G** is fully independent and can start any time, but benefits from
Phase C's adaptive optimization infrastructure for measured payoff tracking.

---

## 12. Measuring Consolidation Progress

### Specification Density Metrics

| Metric | Current | After Phase D | After Phase E | After Phase F |
|--------|---------|---------------|---------------|---------------|
| Lines per relationship spec | ~40 | ~5 | ~3 | ~3 |
| Lines per normalization spec | ~15 | ~10 | ~1 | ~1 |
| `_builder_for_*` functions | 15 | 15 | 8 | 4 |
| `SemanticIRKind` values | 14 | 14 | 6 | 6 |
| Manual join key declarations | 18 | 0 | 0 | 0 |
| Naming-convention cache policies | 5 patterns | 0 | 0 | 0 |
| Total pipeline spec lines | ~800 | ~350 | ~200 | ~120 |

### Inference Coverage Metrics

| Decision | Currently Inferred | After Consolidation |
|----------|-------------------|-------------------|
| Task dependencies | Yes (plan lineage) | Yes |
| Join keys | Partially (via `require_join_strategy`, but many manual) | Yes (schema inference, expanded coverage) |
| Join strategy | Partially (active via `require_join_strategy()`, but many manual overrides) | Yes (AnnotatedSchema + JoinCapabilities, full coverage) |
| Cache policy | No (naming convention) | Yes (graph properties → CompiledExecutionPolicy) |
| Output projection | No (explicit) | Yes (standard templates) |
| Ranking partition key | No (explicit) | Yes (left entity ID) |
| Normalization columns | Partially (schema) | Yes (entity declaration) |
| Span unit conversion | No (bespoke IR kind) | Yes (entity span_unit property) |
| Maintenance policy | No (policy-presence; outcome resolver exists but unwired) | Yes (outcome-based, compiled) |
| Scan policy | Partially (infrastructure wired, signals basic) | Yes (enriched signals, feedback-calibrated) |

---

## 13. Static Declaration Audit: Schemas, Columns, Registries, and Thresholds

A comprehensive code-level review reveals a large surface of static declarations
that duplicate information derivable from the compilation pipeline, the semantic
type system, or DataFusion plan introspection. This section catalogs those
surfaces, quantifies them, and proposes derivation strategies.

### 13.1 Schema Declaration Surface (~2,500 LOC of Derivable Schemas)

The single largest static declaration surface is PyArrow schema constants.
`src/datafusion_engine/schema/registry.py` alone contains ~1,400 lines of
`pa.schema([...])` and `pa.field(...)` declarations. These fall into three
categories with very different derivability profiles:

| Category | Files | LOC | Derivable? | Source of Truth |
|----------|-------|-----|------------|-----------------|
| **Extraction output schemas** (13+ base, 30+ nested) | `schema/registry.py` | ~800 | Yes | Extractor output DataFrames (`df.schema()`) |
| **Relationship/view output schemas** (8+) | `schema/registry.py` | ~200 | Yes | View builder return schemas |
| **Serialization schemas** (hash, telemetry, etc.) | `runtime.py`, `rustworkx_graph.py`, `session/runtime.py`, `obs/metrics.py`, incremental modules | ~400 | Yes | msgspec StructBase class definitions |
| **Extract schema overrides** | `extract/registry.py` | ~100 | Yes | `ExtractMetadata.fields` + `.row_fields` |
| **Pipeline output contracts** | `hamilton_pipeline/io_contracts.py` | ~100 | External boundary | Keep explicit |
| **CPG property enums/specs** | `cpg/prop_catalog.py` | ~200 | External boundary | Keep explicit |
| **Schema spec contracts** | `schema_spec/system.py` | ~100 | Partial | Boundary contracts, but metadata derivable |

**Key finding: ~1,500 LOC of schemas duplicate what DataFusion, extractors, or
struct definitions already know.** Only ~400 LOC represent legitimate external
boundaries that should remain explicit.

**The extraction schema problem is the most acute.** The codebase maintains a
parallel schema registry that manually redeclares what extractors produce. When
an extractor adds a column, both the extractor AND `schema/registry.py` must be
updated in lockstep. This is precisely the drift surface the architecture is
designed to eliminate.

**Derivation strategy for extraction schemas:**
1. Extractors are already self-describing via `ExtractMetadata` (fields, row_fields,
   row_extras). Make this the sole schema source.
2. For nested datasets (e.g., LibCST file → `cst_nodes`, `cst_refs`), derive nested
   schemas by introspecting the struct field types in the parent schema.
3. Register derived schemas in the DataFusion `information_schema` catalog at
   extraction time, eliminating the static registry entirely.

**Derivation strategy for serialization schemas:**
Each `_*_SCHEMA` constant (e.g., `_PROFILE_HASH_SCHEMA`, `_TASK_GRAPH_SCHEMA`,
`_SESSION_RUNTIME_HASH_SCHEMA`, `_TELEMETRY_SCHEMA`) mirrors a msgspec
`StructBase` class. A `schema_from_struct(StructType) -> pa.Schema` utility
generates the PyArrow schema from the struct definition, eliminating manual
duplication. When the struct evolves, the schema follows automatically.

**Competing schema sources today (should be one):**

| Source | Location | Coverage |
|--------|----------|----------|
| Static registry | `schema/registry.py` | All extraction + semantic schemas |
| Extract metadata | `ExtractMetadata` objects | Extract datasets only |
| DataFusion catalog | `information_schema.*` views | Registered tables only |
| Semantic dataset rows | `semantics/catalog/dataset_rows.py` | Semantic datasets only |
| View builder returns | DataFrame builder functions | View outputs only |

**Target:** A single schema authority where DataFusion's `information_schema` is
the canonical source, populated by extractors and view builders at registration
time. The static registry becomes unnecessary.

### 13.2 Hardcoded Column Names vs. Semantic Type System

The codebase has a mature semantic type system (`SemanticType`, `AnnotatedSchema`,
`CompatibilityGroup`) that classifies columns by role (FILE_IDENTITY, SPAN_POSITION,
ENTITY_IDENTITY, SYMBOL_IDENTITY). It provides methods to discover columns by role
rather than by name. **This system is asymmetrically deployed:** join strategy
inference uses it correctly, but most other code hardcodes column names.

**Concentration of hardcoded column names:**

| Location | Pattern | Count | Semantic Role Known? |
|----------|---------|-------|---------------------|
| `quality_specs.py` | `left_on=["file_id"], right_on=["file_id"]` | 9 specs | Yes (FILE_IDENTITY) |
| `symtable/views.py` | Join key tuples (`["path", "name"]`, etc.) | 11 joins | Yes (FILE_IDENTITY, ENTITY_IDENTITY) |
| `catalog/projections.py` | `col("path")`, `col("bstart")`, `col("bend")` | 6+ selects | Yes (FILE_IDENTITY, SPAN_POSITION) |
| `catalog/tags.py` | `"file_id" in join_keys or "file" in name` | 4 routing rules | Yes (type-queryable) |
| `incremental/deltas.py` | `if "symbol" in schema.names` | 3 conditionals | Yes (SYMBOL_IDENTITY) |
| `span_normalize.py` | `col("file_id")`, `col("path")`, `col("line_no")` | 8+ references | Yes (config-driven, partially) |

**What already exists but is underused:**

| Method | Location | What It Does | Used? |
|--------|----------|-------------|-------|
| `AnnotatedSchema.infer_join_keys(other)` | `types/annotated_schema.py` | Find compatible join pairs from compatibility groups | No (manual join keys everywhere) |
| `AnnotatedSchema.columns_by_compatibility_group(group)` | `types/annotated_schema.py` | Find columns by semantic role | No (hardcoded names) |
| `SemanticSchema.path_name()` | `semantics/schema.py` | Discover path column name | Rarely |
| `SemanticSchema.span_start_name()` | `semantics/schema.py` | Discover span start column | Rarely |
| `SemanticSchema.entity_id_name()` | `semantics/schema.py` | Discover entity ID column | Rarely |
| `_preferred_column(schema, group, preferred)` | `joins/inference.py` | Best-effort column by role | Only in join inference |

**Impact of a column rename (e.g., `file_id` → `file_identity`):**
- 9 relationship specs break (quality_specs.py)
- 11 symtable join definitions break
- Multiple filter/select points in incremental modules break
- Join inference (`joins/inference.py`) would adapt automatically

**Derivation strategy:**
Replace hardcoded column name references with semantic role queries. The pattern
from `joins/inference.py` is already best-practice:

```python
# Current (fragile):
left_on=["file_id"], right_on=["file_id"]

# Target (schema-driven):
left_on = schema.columns_by_compatibility_group(CompatibilityGroup.FILE_IDENTITY)
right_on = other_schema.columns_by_compatibility_group(CompatibilityGroup.FILE_IDENTITY)

# Current (fragile routing):
if "symbol" in schema.names: ...

# Target (semantic query):
if schema.has_semantic_type(SemanticType.SYMBOL): ...
```

### 13.3 Duplicate and Redundant Registries

Several registries exist in parallel that encode the same information:

**1. View kind ordering duplicated across files:**
- `_KIND_ORDER` dict in `ir_pipeline.py` (14 entries with numeric ordering)
- `SpecKind` Literal type in `spec_registry.py` (same 14 values)
- Builder dispatch in `pipeline.py` (15 `_builder_for_*` functions keyed by kind)

These are three representations of the same concept. A single authoritative
definition should generate both the type and the ordering.

**2. Output naming identity mapping:**
`SEMANTIC_OUTPUT_NAMES` in `naming.py` maps each view name to itself (identity
mapping). The forward lookup `canonical_output_name()` and reverse lookup
`internal_name()` are no-ops. This entire module could be eliminated if code
used canonical names directly, or reduced to a single validation function.

**3. Extractor template registry:**
`TEMPLATES` and `CONFIGS` dicts in `extract/templates.py` hardcode extractor
names, evidence ranks, and metadata. These could be discovered from actual
extractor modules via convention-over-configuration (each extractor module
exports a standard `TEMPLATE` and `CONFIG` attribute).

**4. View name constants in `relspec/view_defs.py`:**
`REL_NAME_SYMBOL_OUTPUT`, `CST_REFS_NORM_OUTPUT`, etc. are string constants
that duplicate names already present in `SEMANTIC_OUTPUT_NAMES`,
`QUALITY_RELATIONSHIP_SPECS`, and `SEMANTIC_NORMALIZATION_SPECS`. A single
source (the semantic spec itself) should be authoritative.

### 13.4 Thresholds and Policies That Should Be Plan-Signal-Derived

The codebase contains ~15 module-level threshold constants that encode
assumptions about pipeline behavior. These represent static guesses about
values that the plan introspection pipeline already measures:

| Constant | File | Value | Better Source |
|----------|------|-------|---------------|
| `_STREAMING_ROW_THRESHOLD` | `incremental/delta_updates.py` | 100,000 | `PlanSignals.stats.estimated_row_count` |
| `_SMALL_TABLE_ROW_THRESHOLD` | `scan_policy_inference.py` | 10,000 | `PlanSignals.stats.estimated_row_count` |
| `_ADAPTIVE_SMALL_TABLE_THRESHOLD` | `io/write.py` | 10,000 | `NormalizedPlanStats` |
| `_ADAPTIVE_LARGE_TABLE_THRESHOLD` | `io/write.py` | 1,000,000 | `NormalizedPlanStats` |
| `target_file_size` | `storage/deltalake/config.py` | 96 MiB | Plan parallelism + column cardinality |
| `DEFAULT_CACHE_POLICY` sizes | `session/cache_policy.py` | 128/256/64 MiB | Runtime capability snapshot |
| `DEFAULT_MAX_ISSUE_ROWS` | `diagnostics.py` | 200 | Manifest configuration |
| `FILE_QUALITY_SCORE_THRESHOLD` | `diagnostics.py` | 800.0 | Quality relationship specs |
| `default_merge_strategy` | `incremental/config.py` | UPSERT | Relationship spec characteristics |
| `stats_max_columns` | `storage/deltalake/config.py` | 32 | Schema width from DatasetSpec |
| `_MIN_RETENTION_HOURS` | `delta/maintenance.py` | 168 (7 days) | DeltaMutationPolicy |

**Pattern:** Each of these is a reasonable default, but each also represents a
static assumption that could drift from actual pipeline characteristics. The
`CompiledExecutionPolicy` artifact (section 4.6) is the natural home for
plan-signal-derived versions of all these thresholds.

**Highest-leverage subset:** The row count thresholds (4 constants across 3 files)
all serve the same purpose: classify tables as small/medium/large to select
different execution strategies. These should be a single configurable tier
definition consumed from `NormalizedPlanStats`:

```python
# Current (scattered constants):
_SMALL_TABLE_ROW_THRESHOLD = 10_000   # scan_policy_inference.py
_ADAPTIVE_SMALL_TABLE_THRESHOLD = 10_000   # io/write.py
_STREAMING_ROW_THRESHOLD = 100_000   # delta_updates.py
_ADAPTIVE_LARGE_TABLE_THRESHOLD = 1_000_000   # io/write.py

# Target (single tier definition in compiled policy):
class TableSizeTier(StrEnum):
    SMALL = "small"      # < small_threshold rows
    MEDIUM = "medium"    # < large_threshold rows
    LARGE = "large"      # >= large_threshold rows
```

### 13.5 Feature Flags Masquerading as Configuration

Several boolean flags gate behavior that could be inferred from runtime state:

| Flag | File | Current | Better Source |
|------|------|---------|---------------|
| `USE_GLOBAL_EXTRACT_REGISTRY` | `relspec/feature_flags.py` | Env var | Detect from `ExecutionAuthorityContext.extract_executor_map` availability |
| `CpgBuildOptions.use_cdf` | `semantics/pipeline.py` | Explicit `bool \| None` | `SemanticIncrementalConfig.enabled` |
| `DiagnosticsPolicy.*` (7 booleans) | `relspec/pipeline_policy.py` | All `True` | Execution tier (dev/staging/prod) |
| `StatisticsPolicy.collect_statistics` | `plan/perf_policy.py` | `True` | Execution tier |

The `DiagnosticsPolicy` is the most acute example: 7 boolean fields are all
`True` by default, producing comprehensive but expensive diagnostics in every
run. An execution-tier-aware default would be:
- **Dev:** All enabled (current behavior)
- **Production:** Only `capture_datafusion_metrics` and `emit_semantic_quality_diagnostics`
- **Debug:** All enabled + verbose explain

### 13.6 Naming Conventions Used as Semantic Signals

Several code paths use string name patterns to infer properties that should
come from structured metadata:

| Pattern | Location | What It Infers | Better Source |
|---------|----------|---------------|---------------|
| `"cpg_*"` prefix | `_default_semantic_cache_policy` | Output view → `delta_output` | IR view kind + output locations |
| `"rel_*"` prefix | `_default_semantic_cache_policy` | Relationship → `delta_staging` | IR view kind |
| `"*_norm"` suffix | `_default_semantic_cache_policy` | Normalized → `delta_staging` | IR view kind |
| `"file_id" in join_keys` | `catalog/tags.py` | File-scoped entity | `CompatibilityGroup.FILE_IDENTITY` |
| `"symbol" in name` | `catalog/tags.py` | Symbol-related entity | `SemanticType.SYMBOL` |
| `"_v1"` suffix | `naming.py` | Version indicator | Manifest version field |
| `"file" in name` | `catalog/tags.py` | File-level grain | Schema type classification |

The `catalog/tags.py` case is particularly instructive: it combines column
name presence checks (`"file_id" in join_keys`) with substring matching on
view names (`"file" in name`) to infer entity type and grain. Both could be
replaced by querying the `AnnotatedSchema` for compatibility groups and the
`SemanticIRView` for its declared kind.

### 13.7 Quantified Impact

| Static Declaration Surface | Current LOC | After Derivation | Reduction |
|---------------------------|-------------|-----------------|-----------|
| Schema registry (`schema/registry.py`) | ~1,400 | ~200 (external boundaries only) | 85% |
| Serialization schemas (scattered) | ~400 | 0 (generated from structs) | 100% |
| Extract schema overrides | ~100 | 0 (from ExtractMetadata) | 100% |
| Hardcoded column name references | ~60 call sites | ~5 (genuine exceptions) | 92% |
| Threshold constants | ~15 constants across 8 files | 1 tier definition | 93% |
| Duplicate view kind definitions | 3 locations | 1 authoritative source | 67% |
| Output naming identity mapping | ~50 LOC | 0 (eliminated) | 100% |
| View name string constants | ~30 constants | 0 (from spec names) | 100% |

**Total estimated LOC reduction: ~2,000 lines** of static declarations replaced
by derivation from existing infrastructure.

---

## 14. Execution Roadmap: Static Declaration Elimination

### Phase H: Schema Derivation (Highest LOC impact)

25. **Extract schema from ExtractMetadata** - Replace `schema/registry.py`
    extraction schemas with runtime derivation from `ExtractMetadata.fields`
    and nested struct introspection. Eliminate the ~800 LOC static registry.
26. **Generate serialization schemas from structs** - Implement
    `schema_from_struct()` utility that derives PyArrow schemas from
    msgspec StructBase definitions. Replace ~400 LOC of `_*_SCHEMA` constants.
27. **Unify schema authority** - Make DataFusion `information_schema` the single
    schema source. Extractors and view builders register schemas at construction
    time; no parallel static registry.

**Outcome:** Schema declarations drop from ~2,500 LOC to ~400 LOC (external
boundaries only). Schema evolution is automatic when struct or extractor
definitions change.

### Phase I: Semantic Type System Activation (Eliminates column name fragility)

28. **Deploy `AnnotatedSchema` for join key selection** - Replace all 9
    hardcoded `left_on=["file_id"]` declarations in `quality_specs.py` with
    `AnnotatedSchema.infer_join_keys()` or
    `columns_by_compatibility_group(FILE_IDENTITY)`
29. **Deploy semantic type queries for routing** - Replace `"symbol" in
    schema.names` conditionals with `schema.has_semantic_type(SemanticType.SYMBOL)`.
    Replace name-substring routing in `catalog/tags.py` with compatibility
    group queries.
30. **Consolidate threshold constants** - Define a single `TableSizeTier`
    classification derived from `NormalizedPlanStats`, replacing 4 scattered
    row-count thresholds.

**Outcome:** Column name changes require zero manual updates. Routing logic
queries structural properties, not string patterns.

### Phase J: Registry Consolidation (Eliminates duplication)

31. **Single view kind authority** - Merge `_KIND_ORDER`, `SpecKind`, and
    builder dispatch into a single `ViewKindRegistry` that generates the
    type, ordering, and dispatch table from one definition.
32. **Eliminate output naming identity map** - Remove `SEMANTIC_OUTPUT_NAMES`
    module; use spec names directly.
33. **Convention-based extractor discovery** - Replace hardcoded `TEMPLATES`
    and `CONFIGS` dicts with module-level attribute discovery from extractor
    packages.

**Outcome:** Each concept has exactly one authoritative definition. No
duplicate registries to maintain.

---

## 15. Updated Dependency Graph (Including Static Declaration Elimination)

```
Phase A: Correctness and Integration Closure (from section 5)
  Wave 1+2 → 8.2 harden → 8.1 → Wave 4A wire → Invariant tests
    |
    v
Phase B: Policy Compilation and Evidence Model (from section 5)
  4.6 CompiledExecutionPolicy → 10.10 expand → 4.7 Confidence → Cache from policy
    |
Phase C/C': Adaptive Optimization + Infrastructure Hardening (from section 5)
    |
    v
Phase D: Semantic Inference Activation (from section 10)
  Expand infer_join_strategy() → Spec templates → Graph-derived cache
    |
    v
Phase E: Entity-Centric Data Modeling (from section 10)
  Entity declarations → Consolidate IR kinds → Builder dispatch factory
    |
    v
Phase F: Pipeline-as-Specification (from section 10)
  Schema-aware inference → Auto projections → Declarative signals → Execution package
    |
    v
Phase G: Advanced Acceleration Track (from section 10)
  Query-shape profiling → External-index pruning → Workload sessions
    |
    ======== NEW PHASES (from section 14) ========
    |
Phase H: Schema Derivation (can start in parallel with Phase D)
  Extract schema from metadata → Struct-to-schema utility → Unified schema authority
    |
Phase I: Semantic Type System Activation (depends on Phase D partially)
  AnnotatedSchema for joins → Semantic type routing → Threshold consolidation
    |
Phase J: Registry Consolidation (can start in parallel with Phase H)
  Single view kind authority → Eliminate naming map → Convention-based discovery
```

**Parallelism opportunities:**
- Phase H (schema derivation) has no dependencies on Phases A-C and can start
  immediately. The `ExtractMetadata` infrastructure it depends on already exists.
- Phase I depends partially on Phase D (semantic inference activation) because
  both touch the `AnnotatedSchema` → join key inference path.
- Phase J (registry consolidation) has no dependencies and can start immediately.

---

## 16. Measuring Static Declaration Elimination

### Static Surface Metrics

| Metric | Current | After Phase H | After Phase I | After Phase J |
|--------|---------|---------------|---------------|---------------|
| Schema registry LOC | ~1,400 | ~200 | ~200 | ~200 |
| Serialization schema constants | ~15 | 0 | 0 | 0 |
| Hardcoded column name call sites | ~60 | ~60 | ~5 | ~5 |
| Duplicate view kind definitions | 3 | 3 | 3 | 1 |
| Row-count threshold constants | 4 (in 3 files) | 4 | 1 | 1 |
| Output naming map entries | ~25 | ~25 | ~25 | 0 |
| View name string constants | ~30 | ~30 | ~30 | 0 |

### Derivation Authority Metrics

| Schema Source | Current Status | After Phase H |
|--------------|---------------|---------------|
| Static `pa.schema()` in registry.py | Primary (fragile) | Eliminated for internals |
| `ExtractMetadata` fields | Secondary | Primary for extraction |
| DataFusion `information_schema` | Underused | Primary for all registered tables |
| View builder `df.schema()` | Not captured | Primary for view outputs |
| msgspec StructBase definitions | Not used for schema | Primary for serialization schemas |
| Explicit boundary contracts | Mixed with internal | Clearly separated (only external) |
