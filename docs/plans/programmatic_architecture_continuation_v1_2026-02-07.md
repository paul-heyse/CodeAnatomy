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

---

## 17. Detailed Implementation Plans

This section expands each scope item with representative code snippets, target file lists, implementation checklists, and decommission targets.

### PHASE A: Correctness and Integration Closure

#### A.1 Remove CompileContext Fallbacks (Wave 1+2)

**Current Pattern (6 sites):**

Each site follows this identical fallback structure:

```python
# BEFORE (current - all 6 sites)
if execution_context is not None:
    resolver = execution_context.dataset_resolver
else:
    from semantics.compile_context import CompileContext
    resolver = CompileContext(runtime_profile=profile).dataset_bindings()

# AFTER (target)
resolver = execution_context.dataset_resolver
```

**Key architectural element - SemanticExecutionContext:**

```python
# src/semantics/compile_context.py:109-118
@dataclass(frozen=True)
class SemanticExecutionContext:
    manifest: SemanticProgramManifest
    dataset_resolver: ManifestDatasetResolver
    runtime_profile: DataFusionRuntimeProfile
    ctx: SessionContext
    facade: DataFusionExecutionFacade | None = None
```

**Key architectural element - ManifestDatasetResolver protocol:**

```python
# src/semantics/program_manifest.py:118-160
class ManifestDatasetResolver(Protocol):
    def location(self, name: str) -> DatasetLocation | None: ...
    def has_location(self, name: str) -> bool: ...
    def names(self) -> Sequence[str]: ...
```

**Fallback site details:**

| # | File | Line | Function | Available Context |
|---|------|------|----------|-------------------|
| 1 | `src/engine/materialize_pipeline.py` | 310-315 | `materialize_view_to_location()` | `execution_context` param exists |
| 2 | `src/hamilton_pipeline/modules/task_execution.py` | 504-512 | `_ensure_scan_overrides()` | `execution_context` param exists |
| 3 | `src/hamilton_pipeline/modules/task_execution.py` | 540-548 | `_execute_view()` | `execution_context` param exists |
| 4 | `src/extract/coordination/materialization.py` | 455-461 | `_ensure_dataset_resolver()` | `execution_context` param exists |
| 5 | `src/semantics/pipeline.py` | ~1406 | `build_cpg()` | Entry point, constructs context |
| 6 | `src/semantics/pipeline.py` | ~2012 | `build_cpg_from_inferred_deps()` | Entry point, constructs context |

**Target files:**

| File | Action |
|------|--------|
| `src/engine/materialize_pipeline.py` | Edit: remove fallback, make `execution_context` required |
| `src/hamilton_pipeline/modules/task_execution.py` | Edit: remove 2 fallbacks, make `execution_context` required |
| `src/extract/coordination/materialization.py` | Edit: remove fallback, simplify `_ensure_dataset_resolver()` |
| `src/semantics/pipeline.py` | Edit: ensure `build_cpg()` and `build_cpg_from_inferred_deps()` always construct and pass context |
| `src/semantics/compile_context.py` | Edit: add deprecation warnings to `CompileContext.dataset_bindings()` |
| `tests/integration/test_runtime_semantic_locations.py` | Edit: update tests to always pass `execution_context` |
| `tests/unit/hamilton_pipeline/modules/test_extract_execution_registry.py` | Edit: update test fixtures |

**Implementation checklist:**

- [ ] Audit all callers of `materialize_view_to_location()` to ensure they pass `execution_context`
- [ ] Change `execution_context` parameter from `| None` to required in sites 1-4
- [ ] Remove `CompileContext` import and fallback block in each site
- [ ] Ensure `build_cpg()` constructs `SemanticExecutionContext` via `build_semantic_execution_context()` and passes it to all downstream calls
- [ ] Ensure `build_cpg_from_inferred_deps()` does the same
- [ ] Add assertion: `assert execution_context is not None` at each former fallback site during transition
- [ ] Update integration tests to always provide `execution_context`
- [ ] Run: `uv run pytest tests/unit/ tests/integration/ -m "not e2e" -q`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `CompileContext.dataset_bindings()` method | `src/semantics/compile_context.py:83-95` | No longer called from production paths |
| `dataset_bindings_for_profile()` function | `src/semantics/compile_context.py:134-153` | Wrapper around `CompileContext.dataset_bindings()` |
| `_ensure_dataset_resolver()` function | `src/extract/coordination/materialization.py:450-461` | Simplified to direct context access; function body becomes trivial |

---

#### A.2 Resolver Identity Assertions (Section 8.1)

**Key architectural element - ResolverIdentityTracker (already exists):**

```python
# src/semantics/resolver_identity.py
@dataclass
class ResolverIdentityTracker:
    label: str = "pipeline"

    def record_resolver(self, resolver: object, *, label: str = "unknown") -> None: ...
    def distinct_resolvers(self) -> int: ...
    def verify_identity(self) -> list[str]: ...
    def assert_identity(self) -> None: ...

# Context manager for scoped tracking:
@contextmanager
def resolver_identity_tracking(*, label: str = "pipeline", strict: bool = False) -> Iterator[ResolverIdentityTracker]: ...

# Thread-safe registration from any subsystem:
def record_resolver_if_tracking(resolver: object, *, label: str = "unknown") -> None: ...
```

**Deployment pattern:**

```python
# In pipeline entry point (build_cpg_from_inferred_deps):
with resolver_identity_tracking(label="cpg_pipeline", strict=True) as tracker:
    # All subsystems call record_resolver_if_tracking(resolver, label="subsystem")
    # At exit, tracker.assert_identity() raises if multiple resolver instances found
    ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/pipeline.py` | Edit: wrap `build_cpg_from_inferred_deps()` with `resolver_identity_tracking()` |
| `src/hamilton_pipeline/modules/task_execution.py` | Edit: call `record_resolver_if_tracking()` at resolver access points |
| `src/engine/materialize_pipeline.py` | Edit: call `record_resolver_if_tracking()` at resolver access |
| `tests/integration/test_resolver_identity.py` | Create: integration test for resolver identity invariant |

**Implementation checklist:**

- [ ] Wrap `build_cpg_from_inferred_deps()` main body with `resolver_identity_tracking(strict=True)`
- [ ] Add `record_resolver_if_tracking(resolver, label="task_execution")` in `_ensure_scan_overrides` and `_execute_view`
- [ ] Add `record_resolver_if_tracking(resolver, label="materialize_pipeline")` in `materialize_view_to_location`
- [ ] Create integration test asserting `tracker.distinct_resolvers() == 1` after a full pipeline run
- [ ] Run: `uv run pytest tests/integration/test_resolver_identity.py -v`

**Decommission after completion:** None (additive change).

---

#### A.3 Harden DataFusion Capability Adapter (Section 8.2)

**Key architectural elements (already exist):**

```python
# src/datafusion_engine/extensions/runtime_capabilities.py:42-60
@dataclass(frozen=True)
class DataFusionPlanCapabilities:
    has_execution_plan_statistics: bool
    has_execution_plan_schema: bool
    datafusion_version: str
    has_dataframe_execution_plan: bool

# src/datafusion_engine/extensions/runtime_capabilities.py:63-76
@dataclass(frozen=True)
class RuntimeCapabilitiesSnapshot:
    event_time_unix_ms: int
    profile_name: str | None
    settings_hash: str
    strict_native_provider_enabled: bool
    delta: DeltaCompatibilitySnapshot
    extension_capabilities: Mapping[str, object]
    plugin: ExtensionPluginSnapshot
    execution_metrics: Mapping[str, object] | None
    plan_capabilities: DataFusionPlanCapabilities | None = None

# src/relspec/execution_authority.py:34-63
@dataclass(frozen=True)
class ExecutionAuthorityContext:
    semantic_context: SemanticExecutionContext
    evidence_plan: EvidencePlan | None = None
    extract_executor_map: Mapping[str, Any] | None = None
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None
    session_runtime_fingerprint: str | None = None
    enforcement_mode: ExecutionAuthorityEnforcement = "warn"
```

**Target files:**

| File | Action |
|------|--------|
| `src/hamilton_pipeline/driver_factory.py` | Edit: ensure `capability_snapshot` is populated in all `ExecutionAuthorityContext` constructions |
| `src/datafusion_engine/plan/pipeline.py` | Edit: gate stats-dependent code paths on `plan_capabilities` |
| `src/datafusion_engine/extensions/runtime_capabilities.py` | Edit: add diagnostic artifact recording |
| `src/serde_artifact_specs.py` | Edit: add `CAPABILITIES_SNAPSHOT_SPEC` typed artifact spec |
| `src/serde_artifacts.py` | Edit: add `CapabilitiesSnapshotArtifact` msgspec struct |

**Implementation checklist:**

- [ ] Ensure `build_runtime_capabilities_snapshot()` is called at session startup and result threaded to `ExecutionAuthorityContext`
- [ ] Verify `capability_snapshot` is non-None in all `ExecutionAuthorityContext` constructions
- [ ] Add `record_artifact(CAPABILITIES_SNAPSHOT_SPEC, snapshot_payload)` at startup
- [ ] Gate `PlanSignals.stats` consumers on `plan_capabilities.has_execution_plan_statistics`
- [ ] Add diagnostic log entries explaining why features are enabled/disabled
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission after completion:** None (hardening change).

---

#### A.4 Wire Outcome-Based Maintenance (Wave 4A)

**Key architectural elements (exist but unwired):**

```python
# src/datafusion_engine/delta/maintenance.py:108-128
@dataclass(frozen=True)
class WriteOutcomeMetrics:
    files_created: int | None = None
    total_file_count: int | None = None
    version_delta: int | None = None
    final_version: int | None = None

# src/datafusion_engine/delta/maintenance.py:153-192
def build_write_outcome_metrics(
    write_result: Mapping[str, object],
    *, initial_version: int | None = None,
) -> WriteOutcomeMetrics: ...

# src/datafusion_engine/delta/maintenance.py:244-321
def resolve_maintenance_from_execution(
    metrics: WriteOutcomeMetrics,
    *, plan_input: DeltaMaintenancePlanInput,
) -> DeltaMaintenancePlan | None: ...
```

**Current maintenance call to replace:**

```python
# src/datafusion_engine/io/write.py:1733-1762
def _run_post_write_maintenance(self, *, spec, delta_version):
    # CURRENTLY uses:
    plan = resolve_delta_maintenance_plan(DeltaMaintenancePlanInput(...))

    # SHOULD use:
    metrics = build_write_outcome_metrics(write_result, initial_version=initial_version)
    plan = resolve_maintenance_from_execution(metrics, plan_input=DeltaMaintenancePlanInput(...))
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/io/write.py` | Edit: capture `initial_version` before write, call `build_write_outcome_metrics()` after write, replace `resolve_delta_maintenance_plan()` with `resolve_maintenance_from_execution()` |
| `src/semantics/incremental/delta_context.py` | Edit: replace `resolve_delta_maintenance_plan()` call at line 215 with outcome-based resolver |
| `src/serde_artifact_specs.py` | Edit: add `MAINTENANCE_DECISION_SPEC` typed spec |
| `src/serde_artifacts.py` | Edit: add `MaintenanceDecisionArtifact` with cause fields |
| `tests/unit/test_write_delta_table_streaming.py` | Edit: update tests for new maintenance path |

**Implementation checklist:**

- [ ] In `_run_post_write_maintenance()`: capture `initial_version = delta_table.version()` before write
- [ ] After write: call `build_write_outcome_metrics(write_result_dict, initial_version=initial_version)`
- [ ] Replace `resolve_delta_maintenance_plan(plan_input)` with `resolve_maintenance_from_execution(metrics, plan_input=plan_input)`
- [ ] Record maintenance decision as typed artifact with cause fields (which threshold was exceeded)
- [ ] Update `delta_context.py` to use the same pattern
- [ ] Add unit tests for threshold-based maintenance decisions
- [ ] Run: `uv run pytest tests/unit/test_write_delta_table_streaming.py -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `resolve_delta_maintenance_plan()` production callsites | `io/write.py:1747`, `delta_context.py:215` | Replaced by `resolve_maintenance_from_execution()` |

*Note: `resolve_delta_maintenance_plan()` function itself remains as the internal fallback within `resolve_maintenance_from_execution()` when no outcome thresholds are configured.*

---

### PHASE B: Policy Compilation and Evidence Model

#### B.1 Introduce CompiledExecutionPolicy Artifact (Section 4.6)

**Key architectural elements - existing policy objects to consolidate:**

```python
# src/storage/deltalake/config.py:88-142
class DeltaWritePolicy(StructBaseStrict, frozen=True):
    target_file_size: int | None = None
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    stats_policy: DeltaStatsPolicy | None = None
    # ... 6 more fields

# src/relspec/pipeline_policy.py:14-51
class DiagnosticsPolicy(StructBaseStrict, frozen=True):
    capture_datafusion_metrics: bool = True
    capture_datafusion_traces: bool = True
    # ... 5 more boolean fields

# src/datafusion_engine/materialize_policy.py:15-49
class MaterializationPolicy(FingerprintableConfig, frozen=True):
    prefer_streaming: bool = False
    determinism_tier: DeterminismTier = DeterminismTier.STRICT
    writer_strategy: Literal["arrow", "datafusion"] = "arrow"

# src/schema_spec/system.py:507-536
class ScanPolicyConfig(StructBaseStrict, frozen=True):
    listing: ListingPolicyConfig
    delta_listing: DeltaListingPolicyConfig
    delta_scan: DeltaScanOptions
```

**New artifact structure:**

```python
# src/relspec/compiled_policy.py (NEW FILE)
from serde_msgspec import StructBaseStrict

class CompiledExecutionPolicy(StructBaseStrict, frozen=True):
    """Compile-time-resolved execution policy artifact.

    Runtime consumes this artifact unchanged; it does not
    re-derive policy heuristically.
    """
    scan_policy_by_dataset: Mapping[str, ScanPolicyConfig] = {}
    cache_policy_by_view: Mapping[str, CachePolicy] = {}
    maintenance_policy_by_dataset: Mapping[str, DeltaMaintenancePolicy | None] = {}
    udf_requirements_by_view: Mapping[str, tuple[str, ...]] = {}
    materialization_policy: MaterializationPolicy | None = None
    diagnostics_policy: DiagnosticsPolicy | None = None
    validation_mode: Literal["off", "warn", "error"] = "warn"
    policy_fingerprint: str | None = None
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/compiled_policy.py` | Create: `CompiledExecutionPolicy` struct |
| `src/relspec/policy_compiler.py` | Create: `compile_execution_policy()` function that builds the artifact from plan signals + manifest |
| `src/relspec/execution_authority.py` | Edit: add `compiled_policy: CompiledExecutionPolicy | None = None` field to `ExecutionAuthorityContext` |
| `src/hamilton_pipeline/driver_factory.py` | Edit: call `compile_execution_policy()` during plan-context build, thread result into execution authority |
| `src/serde_artifact_specs.py` | Edit: register `COMPILED_EXECUTION_POLICY_SPEC` |
| `src/serde_artifacts.py` | Edit: add `CompiledExecutionPolicyArtifact` payload type |
| `tests/unit/relspec/test_compiled_policy.py` | Create: unit tests for policy compilation |

**Implementation checklist:**

- [ ] Define `CompiledExecutionPolicy` as frozen `StructBaseStrict` with per-dataset and per-view policy sections
- [ ] Implement `compile_execution_policy(manifest, plan_bundle, plan_signals)` that populates each section
- [ ] Add `compiled_policy` field to `ExecutionAuthorityContext`
- [ ] Wire `compile_execution_policy()` into `driver_factory.py` plan-context build phase
- [ ] Record compiled policy as typed artifact
- [ ] Add fingerprint computation (`policy_fingerprint` field)
- [ ] Create unit tests validating policy compilation from test manifests
- [ ] Run: `uv run pytest tests/unit/relspec/ -v`

**Decommission after completion:** None immediately (existing policy objects remain as components; decommission is cross-scope with Phase D cache policy work).

---

#### B.2 Expand Policy Bundle Validation (Section 10.10)

**Key architectural element (already operational):**

```python
# src/relspec/policy_validation.py:155-198
def validate_policy_bundle(
    execution_plan: ExecutionPlan,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    udf_snapshot: Mapping[str, object],
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None,
    semantic_manifest: SemanticProgramManifest | None = None,
) -> PolicyValidationResult: ...

# Current validation rules (6 validators):
# 1. _udf_feature_gate_issues()     - UDFs enabled when required
# 2. _udf_availability_issues()     - Required UDFs exist
# 3. _delta_protocol_issues()       - Delta protocol compatibility
# 4. _manifest_alignment_issues()   - Scan datasets in manifest
# 5. _small_scan_policy_issues()    - Small input scan warnings
# 6. _capability_issues()           - Runtime capability validation
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/policy_validation.py` | Edit: add new validators for compiled policy consistency, scan policy override validation, statistics availability gating |
| `src/serde_artifact_specs.py` | Edit: add `POLICY_VALIDATION_RESULT_SPEC` typed spec |
| `src/serde_artifacts.py` | Edit: add `PolicyValidationResultArtifact` with machine-readable error codes |
| `tests/unit/relspec/test_policy_validation.py` | Edit: add tests for new validation rules |

**Implementation checklist:**

- [ ] Add `_compiled_policy_consistency_issues()` validator: verify compiled policy sections match manifest datasets
- [ ] Add `_scan_policy_compatibility_issues()` validator: verify scan overrides are compatible with dataset characteristics
- [ ] Add `_statistics_availability_issues()` validator: warn when stats-dependent policies lack plan statistics
- [ ] Emit validation results as typed `PolicyValidationResultArtifact` with machine-readable codes
- [ ] Wire new validators into `validate_policy_bundle()` pipeline
- [ ] Run: `uv run pytest tests/unit/relspec/test_policy_validation.py -v`

**Decommission after completion:** None (additive expansion).

---

#### B.3 Add Confidence Model to Inference Decisions (Section 4.7)

**Key architectural elements - existing partial infrastructure:**

```python
# src/semantics/quality.py:115-142
class Feature:
    name: str
    expr: str
    weight: float
    kind: Literal["evidence", "quality"] = "evidence"

# src/semantics/quality.py:145-188
class SignalsSpec:
    base_score: int = 1000
    base_confidence: float = 0.5    # <-- confidence exists here
    hard: tuple[HardPredicate, ...] = ()
    features: tuple[Feature, ...] = ()
    quality_score_column: str = "quality_score"
    quality_weight: float = 0.0001
```

**New confidence structure:**

```python
# src/relspec/inference_confidence.py (NEW FILE)
class InferenceConfidence(StructBaseStrict, frozen=True):
    """Confidence and rationale for an inferred decision."""
    confidence_score: float          # 0.0-1.0
    evidence_sources: tuple[str, ...]  # ("lineage", "stats", "capabilities")
    fallback_reason: str | None = None  # When confidence insufficient
    decision_type: str               # "scan_policy", "cache_policy", "join_strategy"
    decision_value: str              # The actual decision made
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/inference_confidence.py` | Create: `InferenceConfidence` struct and helpers |
| `src/datafusion_engine/delta/scan_policy_inference.py` | Edit: attach `InferenceConfidence` to scan policy overrides |
| `src/semantics/joins/inference.py` | Edit: attach confidence to `JoinStrategy` results |
| `src/relspec/compiled_policy.py` | Edit: add `confidence_by_decision: Mapping[str, InferenceConfidence]` to `CompiledExecutionPolicy` |
| `tests/unit/relspec/test_inference_confidence.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Define `InferenceConfidence` struct with `confidence_score`, `evidence_sources`, `fallback_reason`
- [ ] Add confidence scoring to `derive_scan_policy_overrides()` based on evidence completeness
- [ ] Add confidence scoring to `infer_join_strategy()` based on schema match quality
- [ ] Thread confidence into `CompiledExecutionPolicy` artifact
- [ ] Record confidence as part of typed policy artifacts
- [ ] Run: `uv run pytest tests/unit/relspec/ -v`

**Decommission after completion:** None (additive).

---

#### B.4 Replace Cache Naming Heuristic with Compiled Policy (from Section 4.1 + 9.6)

**Current pattern to replace:**

```python
# src/semantics/pipeline.py:275-301
def _default_semantic_cache_policy(
    *, view_names, output_locations
) -> dict[str, CachePolicy]:
    # Uses naming conventions:
    # cpg_* -> delta_output
    # rel_* -> delta_staging
    # *_norm -> delta_staging
    # Everything else -> none
```

**Target pattern:**

```python
def _compiled_cache_policy(
    *, compiled_policy: CompiledExecutionPolicy,
) -> dict[str, CachePolicy]:
    return dict(compiled_policy.cache_policy_by_view)
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/pipeline.py` | Edit: replace `_default_semantic_cache_policy()` with compiled policy consumption |
| `src/relspec/policy_compiler.py` | Edit: add cache policy compilation from graph properties |
| `tests/unit/semantics/test_cache_policy.py` | Create: test graph-derived cache policy |

**Implementation checklist:**

- [ ] In `compile_execution_policy()`: derive cache policy from `TaskGraph` out-degree and output locations
- [ ] Terminal nodes in output_locations → `delta_output`
- [ ] High fan-out nodes (out_degree > 2) → `delta_staging`
- [ ] Single consumer + low cost → `none`
- [ ] Replace `_default_semantic_cache_policy()` callers with compiled policy lookup
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `_default_semantic_cache_policy()` | `src/semantics/pipeline.py:275-301` | Replaced by compiled policy derivation |
## PHASE C: Adaptive Optimization with Bounded Feedback

### C.1 Enrich Adaptive Scan Policy Signals (Wave 4B)

**Key architectural elements (already wired):**

```python
# src/datafusion_engine/delta/scan_policy_inference.py:42-86
def derive_scan_policy_overrides(
    signals: PlanSignals,
    *,
    base_policy: ScanPolicyConfig | None = None,
    capability_snapshot: RuntimeCapabilitiesSnapshot | Mapping[str, object] | None = None,
) -> tuple[ScanPolicyOverride, ...]: ...

# Current signal sources (lines 88-157):
# 1. Small table detection: stats.num_rows < 10_000 → disable file pruning
# 2. Pushed filter detection: scan.pushed_filters → enable parquet pushdown
```

**New signal sources to add:**

```python
# Enrichment targets within _infer_override_for_scan():
# 3. Sort-order exploitation: If table sort keys match join keys → enable sort-merge
# 4. Predicate selectivity: If pushed predicates have high selectivity → enable page-index pruning
# 5. Column pruning efficiency: If projection is narrow relative to table width → flag for pushdown
# 6. Partition pruning: If filter on partition column → optimize partition listing
```

**Application site:**

```python
# src/datafusion_engine/plan/pipeline.py:175-201
def _apply_inferred_scan_policy_overrides(
    profile, *, plan_bundle, capability_snapshot, base_policy
) -> None:
    # Calls derive_scan_policy_overrides() per view
    # Records decisions via record_scan_policy_decisions()
    # Applies overrides via apply_scan_unit_overrides()
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/delta/scan_policy_inference.py` | Edit: add new signal handlers in `_infer_override_for_scan()` for sort-order, selectivity, column pruning |
| `src/datafusion_engine/plan/signals.py` | Edit: add `sort_keys`, `predicate_selectivity_estimate`, `projection_ratio` fields to `PlanSignals` |
| `src/datafusion_engine/plan/pipeline.py` | Edit: pass enriched signals through to scan policy inference |
| `tests/unit/datafusion_engine/test_scan_policy_inference.py` | Edit: add tests for new signal-based overrides |

**Implementation checklist:**

- [ ] Add `sort_keys: tuple[str, ...] = ()` to `PlanSignals`
- [ ] Add `predicate_selectivity_estimate: float | None = None` to `PlanSignals`
- [ ] Extract sort-key information from DataFusion plan's `SortExec` nodes
- [ ] Implement sort-order match detection in `_infer_override_for_scan()`
- [ ] Add column-pruning ratio calculation from plan projection vs full schema
- [ ] Record enriched signal sources in scan policy override artifacts
- [ ] Add payoff measurement: record scan volume before/after overrides as artifact
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission after completion:** None (enrichment of existing infrastructure).

---

### C.2 Build Artifact Calibrator (Section 4.8)

**Key concept:** Post-run calibrator reads execution outcome artifacts and updates policy defaults for future runs using bounded, deterministic update rules.

**Calibration loop:**

```python
# src/relspec/policy_calibrator.py (NEW FILE)
class PolicyCalibrationResult(StructBaseStrict, frozen=True):
    """Result of post-run policy calibration."""
    policy_domain: str           # "scan", "cache", "maintenance"
    previous_threshold: float
    calibrated_threshold: float
    evidence_count: int
    confidence: float
    bounded: bool                # True if bounded by min/max constraints

def calibrate_scan_thresholds(
    outcome_artifacts: Sequence[ScanPolicyOutcomeArtifact],
    *,
    current_thresholds: ScanThresholds,
    bounds: CalibrationBounds,
) -> PolicyCalibrationResult: ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/policy_calibrator.py` | Create: calibration logic with bounded update rules |
| `src/relspec/calibration_bounds.py` | Create: min/max bounds for each policy domain |
| `src/serde_artifacts.py` | Edit: add `PolicyCalibrationArtifact` payload type |
| `src/serde_artifact_specs.py` | Edit: register calibration spec |
| `tests/unit/relspec/test_policy_calibrator.py` | Create: unit tests for calibration |

**Implementation checklist:**

- [ ] Define `CalibrationBounds` struct with min/max for each tunable parameter
- [ ] Implement `calibrate_scan_thresholds()` using exponential moving average bounded by min/max
- [ ] Implement `calibrate_maintenance_thresholds()` for maintenance interval calibration
- [ ] Record calibration results as typed artifacts
- [ ] Add rollout controls: `CalibrationMode = Literal["off", "warn", "enforce"]`
- [ ] Require measurable payoff gates: reject calibration if no improvement measured
- [ ] Run: `uv run pytest tests/unit/relspec/test_policy_calibrator.py -v`

**Decommission after completion:** None (new capability).

---

## PHASE C' (Parallel): Infrastructure Hardening

### C'.1 Artifact Governance Migration (Wave 5 Phases 2-5)

**Key pattern - typed artifact registration:**

```python
# src/serde_artifact_specs.py (existing pattern)
VIEW_CACHE_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_cache_artifact_v1",
        description="Cache materialization artifact.",
        payload_type=ViewCacheArtifact,    # <- msgspec Struct validation
    )
)

# Migration target for raw string callsites:
# BEFORE:
profile.record_artifact("some_artifact_name", {"key": "value"})
# AFTER:
from serde_artifact_specs import SOME_ARTIFACT_SPEC
from serde_artifacts import SomeArtifact
payload = SomeArtifact(key="value")
profile.record_artifact(SOME_ARTIFACT_SPEC, payload)
```

**Phased migration targets:**

| Phase | Scope | File Targets | Estimated Callsites |
|-------|-------|-------------|---------------------|
| Wave 5.2 | Write path | `io/write.py`, `delta/maintenance.py` | ~20-25 |
| Wave 5.3 | Hamilton lifecycle | `task_execution.py`, `driver_factory.py` | ~30-40 |
| Wave 5.4 | Diagnostics | `views/graph.py`, `session/runtime.py` | ~40-50 |
| Wave 5.5 | Remaining | All other files | ~60-70 |

**Target files (per phase):**

| File | Action |
|------|--------|
| `src/serde_artifacts.py` | Edit: add new msgspec artifact payload types per phase |
| `src/serde_artifact_specs.py` | Edit: register new typed specs per phase |
| `src/datafusion_engine/io/write.py` | Edit (Wave 5.2): replace raw string artifact calls |
| `src/hamilton_pipeline/modules/task_execution.py` | Edit (Wave 5.3): replace raw string artifact calls |
| `src/hamilton_pipeline/driver_factory.py` | Edit (Wave 5.3): replace raw string artifact calls |
| `scripts/audit_artifact_callsites.py` | Run: audit remaining raw string callsites between phases |

**Implementation checklist (per sub-phase):**

- [ ] Run `scripts/audit_artifact_callsites.py` to identify remaining raw-string callsites in scope
- [ ] For each callsite group, define a `msgspec.Struct` payload type in `serde_artifacts.py`
- [ ] Register the typed `ArtifactSpec` with `payload_type` in `serde_artifact_specs.py`
- [ ] Replace `profile.record_artifact("raw_name", dict)` with `profile.record_artifact(TYPED_SPEC, TypedPayload(...))`
- [ ] Run: `uv run pytest tests/msgspec_contract/ -v` to verify round-trip serialization
- [ ] Run: `uv run pytest tests/unit/ -q` after each sub-phase

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| Raw string artifact name constants | Various files | Replaced by typed `ArtifactSpec` references |
| `scripts/migrate_artifact_callsites.py` | `scripts/` | Migration complete |

---

### C'.2 Plan-Statistics-Driven Scheduling

**Key architectural elements (cost model exists but uses uniform defaults):**

```python
# src/relspec/execution_plan.py:37-45
class ScheduleCostContext(StructBaseStrict, frozen=True):
    task_costs: Mapping[str, float] | None = None
    bottom_level_costs: Mapping[str, float] | None = None
    slack_by_task: Mapping[str, float] | None = None
    betweenness_centrality: Mapping[str, float] | None = None
    articulation_tasks: frozenset[str] | None = None
    bridge_tasks: frozenset[str] | None = None

# src/relspec/execution_plan.py:934-963
def bottom_level_costs(graph, *, task_costs=None) -> dict[str, float]:
    # When task_costs is None, defaults to priority or 1.0 per task
    # THIS IS THE UNIFORM COST DEFAULT
```

**Statistics-based cost estimation pattern:**

```python
# Target: derive task_costs from PlanSignals
def derive_task_costs_from_plan(
    plan_metrics: Mapping[str, TaskPlanMetrics],
) -> dict[str, float]:
    costs: dict[str, float] = {}
    for task_name, metrics in plan_metrics.items():
        row_cost = metrics.stats_row_count or 1000
        byte_cost = metrics.stats_total_bytes or 1_000_000
        # Normalize to relative cost units
        costs[task_name] = (row_cost / 1000) + (byte_cost / 1_000_000)
    return costs
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/execution_plan.py` | Edit: add `derive_task_costs_from_plan()` function |
| `src/relspec/rustworkx_schedule.py` | Edit: thread `task_costs` from plan metrics into `ScheduleOptions.cost_context` |
| `src/hamilton_pipeline/driver_factory.py` | Edit: compute task costs from plan metrics and pass to scheduler |
| `tests/unit/relspec/test_execution_plan.py` | Edit: add tests for plan-statistics-derived costs |

**Implementation checklist:**

- [ ] Implement `derive_task_costs_from_plan()` using `stats_row_count` and `stats_total_bytes`
- [ ] Thread derived costs into `ScheduleCostContext.task_costs`
- [ ] Verify `bottom_level_costs()` uses the derived costs instead of uniform defaults
- [ ] Verify `task_slack_by_task()` uses the derived costs
- [ ] Add integration test comparing schedule order with uniform vs statistics-based costs
- [ ] Run: `uv run pytest tests/unit/relspec/ tests/integration/relspec/ -v`

**Decommission after completion:** None (enrichment of existing infrastructure).

---

## PHASE D: Semantic Inference Activation

### D.1 Expand `infer_join_strategy()` Coverage in IR Compilation

**Key architectural elements:**

```python
# src/semantics/joins/inference.py:372-440
def infer_join_strategy(
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
    *,
    hint: JoinStrategyType | None = None,
) -> JoinStrategy | None: ...

# src/semantics/joins/inference.py:458-499
def require_join_strategy(
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
    *,
    hint: JoinStrategyType | None = None,
    left_name: str = "left",
    right_name: str = "right",
) -> JoinStrategy: ...

# src/semantics/joins/inference.py:36-94
@dataclass(frozen=True)
class JoinCapabilities:
    has_file_identity: bool
    has_spans: bool
    has_entity_id: bool
    has_symbol: bool
    fk_columns: tuple[str, ...]

    @classmethod
    def from_schema(cls, schema: AnnotatedSchema) -> JoinCapabilities: ...
```

**Current compiler usage (limited):**

```python
# src/semantics/compiler.py:800-812
# Only called when join_type hint exists:
if hint is None and options.join_type is not None:
    hint = (JoinStrategyType.SPAN_CONTAINS if options.join_type == "contains"
            else JoinStrategyType.SPAN_OVERLAP)
strategy = require_join_strategy(left_info.annotated, right_info.annotated, hint=hint)
```

**Target expansion - remove hardcoded join keys from quality specs:**

```python
# BEFORE (quality_specs.py - all 9 specs):
QualityRelationshipSpec(
    left_on=["file_id"],
    right_on=["file_id"],
    ...
)

# AFTER (join keys inferred at compile time):
QualityRelationshipSpec(
    # left_on and right_on omitted - derived from schemas
    ...
)
# At compile time, compiler calls:
# join_keys = left_schema.infer_join_keys(right_schema)
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/compiler.py` | Edit: call `require_join_strategy()` for all relationship compilation, not just hinted joins; use `AnnotatedSchema.infer_join_keys()` when `left_on`/`right_on` are empty |
| `src/semantics/quality.py` | Edit: make `left_on` and `right_on` optional (default to `()`) to signal "infer from schemas" |
| `src/semantics/quality_specs.py` | Edit: remove hardcoded `left_on=["file_id"], right_on=["file_id"]` from all 9 specs |
| `src/semantics/types/annotated_schema.py` | Edit: ensure `infer_join_keys()` is robust for all schema pairs |
| `tests/unit/semantics/test_join_inference.py` | Edit: add tests for schema-inferred join keys |

**Implementation checklist:**

- [ ] Make `left_on` and `right_on` default to `()` in `QualityRelationshipSpec`
- [ ] In compiler `relate()`: when `left_on` is empty, call `left_schema.infer_join_keys(right_schema)` to derive join keys
- [ ] Verify inferred keys match current hardcoded keys (`["file_id"]`) for all 9 specs
- [ ] Remove hardcoded join keys from all 9 quality specs
- [ ] Add assertion: inferred keys must be non-empty for relationship compilation to succeed
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| Hardcoded `left_on=["file_id"]` | `src/semantics/quality_specs.py` (9 occurrences) | Replaced by schema inference |
| Hardcoded `right_on=["file_id"]` | `src/semantics/quality_specs.py` (9 occurrences) | Replaced by schema inference |

---

### D.2 Implement Relationship Spec Templates

**Current pattern (repeated 5 times across ~200 lines):**

```python
# src/semantics/quality_specs.py - Entity-to-Symbol template pattern
QualityRelationshipSpec(
    name="rel_name_symbol",
    left_view="cst_refs_norm",
    right_view="scip_occurrences_norm",
    left_on=["file_id"],               # CONSTANT across 5 specs
    right_on=["file_id"],              # CONSTANT
    how="inner",                        # CONSTANT
    join_file_quality=True,             # CONSTANT
    signals=SignalsSpec(
        base_score=2000,                # VARIES
        base_confidence=0.95,           # VARIES
        hard=[                          # VARIES
            HardPredicate(span_overlaps_expr(...)),
            HardPredicate(truthy_expr("r__is_read")),
        ],
        features=[                      # VARIES
            Feature("exact_span", ..., weight=20.0),
        ],
    ),
    rank=RankSpec(                      # CONSTANT pattern
        partition_by=["l__entity_id"],  # Always left entity_id
        order_by=["quality_score DESC"],
        keep="best",
        top_k=1,
    ),
)
```

**Template factory:**

```python
# src/semantics/quality_templates.py (NEW FILE)
def entity_symbol_relationship(
    name: str,
    left_view: str,
    right_view: str,
    *,
    span_strategy: Literal["overlap", "contains"] = "overlap",
    additional_hard: Sequence[HardPredicate] = (),
    base_score: int = 2000,
    base_confidence: float = 0.95,
    additional_features: Sequence[Feature] = (),
    provider: str = "unknown",
    origin: str = "semantic_compiler",
) -> QualityRelationshipSpec:
    """Build an entity-to-symbol relationship spec from minimal parameters.

    Infers: join keys, join type, ranking, file quality join, output projection.
    """
    hard = [HardPredicate(_span_predicate(span_strategy)), *additional_hard]
    return QualityRelationshipSpec(
        name=name,
        left_view=left_view,
        right_view=right_view,
        # left_on/right_on omitted - inferred from schemas (Phase D.1)
        how="inner",
        signals=SignalsSpec(
            base_score=base_score,
            base_confidence=base_confidence,
            hard=tuple(hard),
            features=tuple(additional_features),
        ),
        provider=provider,
        origin=origin,
    )
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/quality_templates.py` | Create: `entity_symbol_relationship()` template factory |
| `src/semantics/quality_specs.py` | Edit: replace 5 entity-to-symbol specs with template calls (~200 lines → ~50 lines) |
| `tests/unit/semantics/test_quality_templates.py` | Create: unit tests for template factory |

**Implementation checklist:**

- [ ] Implement `entity_symbol_relationship()` factory that encapsulates all shared fields
- [ ] Define `_span_predicate()` helper for span_overlaps vs span_contains selection
- [ ] Replace `REL_NAME_SYMBOL`, `REL_DEF_SYMBOL`, `REL_IMPORT_SYMBOL`, `REL_CALLSITE_SYMBOL`, and one more spec with template calls
- [ ] Verify output specs are structurally identical to current specs
- [ ] Keep non-template specs (e.g., `REL_CST_DOCSTRING_OWNER_BY_ID` which uses ID-based matching) as-is
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| ~150 lines of boilerplate in 5 relationship specs | `src/semantics/quality_specs.py` | Replaced by template factory calls |

---

### D.3 Derive Cache Policy from Graph Properties

**Current heuristic to replace:**

```python
# src/semantics/pipeline.py:275-301
def _default_semantic_cache_policy(*, view_names, output_locations):
    for name in view_names:
        if name.startswith("cpg_"):           # Naming convention
            resolved[name] = "delta_output"
        elif name.startswith("rel_"):          # Naming convention
            resolved[name] = "delta_staging"
        elif name.endswith("_norm"):           # Naming convention
            resolved[name] = "delta_staging"
```

**Graph-derived replacement:**

```python
# src/relspec/policy_compiler.py (addition to compile_execution_policy)
def _derive_cache_policies(
    task_graph: TaskGraph,
    output_locations: Mapping[str, DatasetLocation],
) -> dict[str, CachePolicy]:
    """Derive cache policy from graph topology, not naming conventions."""
    policies: dict[str, CachePolicy] = {}
    for node_idx in task_graph.graph.node_indices():
        node = task_graph.graph[node_idx]
        name = node.task_name
        out_degree = task_graph.graph.out_degree(node_idx)

        if name in output_locations:
            policies[name] = "delta_output"
        elif out_degree > 2:
            policies[name] = "delta_staging"
        elif out_degree == 0 and name not in output_locations:
            policies[name] = "none"
        else:
            policies[name] = "delta_staging" if out_degree > 0 else "none"
    return policies
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/policy_compiler.py` | Edit: add `_derive_cache_policies()` using `TaskGraph` topology |
| `src/semantics/pipeline.py` | Edit: replace `_default_semantic_cache_policy()` with compiled policy consumption |
| `tests/unit/relspec/test_cache_policy_derivation.py` | Create: test graph-derived cache policies |

**Implementation checklist:**

- [ ] Implement `_derive_cache_policies()` using rustworkx graph `out_degree()`
- [ ] Wire into `compile_execution_policy()` to populate `cache_policy_by_view`
- [ ] Replace `_default_semantic_cache_policy()` callers with `compiled_policy.cache_policy_by_view`
- [ ] Verify cache policy assignments match current naming-convention heuristic for existing views
- [ ] Add edge case handling for disconnected nodes
- [ ] Run: `uv run pytest tests/unit/relspec/ tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `_default_semantic_cache_policy()` | `src/semantics/pipeline.py:275-301` | Replaced by graph-derived policy in `CompiledExecutionPolicy` |
---

## PHASE E: Entity-Centric Data Modeling

### E.1 Define Entity Declaration Format

**Current model (schema-centric, ~15 lines per entity):**

```python
# src/semantics/specs.py:62-72
class SemanticTableSpec(StructBaseStrict, frozen=True):
    table: str
    path_col: str = "path"
    primary_span: SpanBinding = SpanBinding("bstart", "bend")
    entity_id: IdDerivation = IdDerivation("entity_id", "entity")
    foreign_keys: tuple[ForeignKeyDerivation, ...] = ()
    text_cols: tuple[str, ...] = ()

# src/semantics/specs.py:27-35
class SpanBinding(StructBaseStrict, frozen=True):
    start_col: str
    end_col: str
    unit: SpanUnit = "byte"
    canonical_start: str = "bstart"
    canonical_end: str = "bend"
    canonical_span: str = "span"

# src/semantics/specs.py:38-47
class IdDerivation(StructBaseStrict, frozen=True):
    out_col: str
    namespace: str
    path_col: str = "path"
    start_col: str = "bstart"
    end_col: str = "bend"
    null_if_any_null: bool = True
    canonical_entity_id: str | None = "entity_id"
```

**Current registry (7 entries, src/semantics/registry.py:31-139):**

```python
SEMANTIC_TABLE_SPECS: Final[dict[str, SemanticTableSpec]] = {
    "cst_refs": SemanticTableSpec(
        table="cst_refs",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(out_col="ref_id", namespace="cst_ref"),
        text_cols=("ref_text",),
    ),
    "cst_defs": SemanticTableSpec(
        table="cst_defs",
        primary_span=SpanBinding("def_bstart", "def_bend"),
        entity_id=IdDerivation(out_col="def_id", namespace="cst_def",
                               start_col="def_bstart", end_col="def_bend"),
        foreign_keys=(ForeignKeyDerivation(
            out_col="container_def_id", target_namespace="cst_def",
            start_col="container_def_bstart", end_col="container_def_bend",
            guard_null_if=("container_def_kind",),
        ),),
    ),
    # ... 5 more entries
}
```

**Target model (entity-centric declaration):**

```python
# src/semantics/entity_model.py (NEW FILE)
class EntityDeclaration(StructBaseStrict, frozen=True):
    """High-level entity declaration encoding concept identity and location.

    All normalization, join strategy, ranking, and output schema
    follow from these properties.
    """
    name: str                            # "cst_ref"
    source_table: str                    # "cst_refs"
    identity: IdentitySpec               # How entity_id is derived
    location: LocationSpec               # Span or point location
    content: tuple[str, ...] = ()        # Content columns ("ref_text",)
    relationships: tuple[str, ...] = ()  # Relationship capabilities
    span_unit: SpanUnit = "byte"         # "byte" or "line_column"

class IdentitySpec(StructBaseStrict, frozen=True):
    namespace: str                       # "cst_ref"
    id_column: str = "entity_id"        # Output column name
    from_columns: tuple[str, ...] = ("path", "bstart", "bend")

class LocationSpec(StructBaseStrict, frozen=True):
    kind: Literal["span", "point", "none"] = "span"
    start_col: str = "bstart"
    end_col: str = "bend"
    path_col: str = "path"
```

**Example migration:**

```python
# BEFORE (SemanticTableSpec - 10 lines):
"cst_refs": SemanticTableSpec(
    table="cst_refs",
    primary_span=SpanBinding("bstart", "bend"),
    entity_id=IdDerivation(out_col="ref_id", namespace="cst_ref"),
    text_cols=("ref_text",),
)

# AFTER (EntityDeclaration - 5 lines):
EntityDeclaration(
    name="cst_ref",
    source_table="cst_refs",
    identity=IdentitySpec(namespace="cst_ref", id_column="ref_id"),
    location=LocationSpec(start_col="bstart", end_col="bend"),
    content=("ref_text",),
)
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/entity_model.py` | Create: `EntityDeclaration`, `IdentitySpec`, `LocationSpec` definitions |
| `src/semantics/entity_registry.py` | Create: registry of entity declarations; generate `SemanticTableSpec` from declarations for backward compat |
| `src/semantics/registry.py` | Edit: replace `SEMANTIC_TABLE_SPECS` with generated versions from entity declarations |
| `src/semantics/specs.py` | Retain but deprecate: `SemanticTableSpec` becomes generated output |
| `tests/unit/semantics/test_entity_model.py` | Create: test entity declarations and SemanticTableSpec generation |

**Implementation checklist:**

- [ ] Define `EntityDeclaration`, `IdentitySpec`, `LocationSpec` structs
- [ ] Implement `entity_to_table_spec(decl: EntityDeclaration) -> SemanticTableSpec` converter
- [ ] Create entity declarations for all 7 current `SEMANTIC_TABLE_SPECS` entries
- [ ] Verify generated `SemanticTableSpec` matches current static versions exactly
- [ ] Replace `SEMANTIC_TABLE_SPECS` dict with generated versions
- [ ] Add `span_unit` property for SCIP/bytecode entities (enables IR kind consolidation)
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| Manual `SEMANTIC_TABLE_SPECS` entries | `src/semantics/registry.py:31-139` | Generated from entity declarations |
| Manual `SEMANTIC_NORMALIZATION_SPECS` entries | `src/semantics/registry.py:157-200` | Generated from entity declarations |

---

### E.2 Consolidate IR View Kinds (14 → ~6)

**Current duplication:**

```python
# src/semantics/ir.py:13-28 (14 kinds)
SemanticIRKind = Literal[
    "normalize", "scip_normalize", "bytecode_line_index", "span_unnest",
    "symtable", "diagnostic", "export", "projection", "finalize",
    "artifact", "join_group", "relate", "union_nodes", "union_edges",
]

# src/semantics/spec_registry.py:33-48 (IDENTICAL 14 values)
SpecKind = Literal[
    "normalize", "scip_normalize", "bytecode_line_index", "span_unnest",
    "symtable", "diagnostic", "export", "projection", "finalize",
    "artifact", "join_group", "relate", "union_nodes", "union_edges",
]

# src/semantics/ir_pipeline.py:21-36 (ordering for same 14 values)
_KIND_ORDER: Mapping[str, int] = {
    "normalize": 0, "scip_normalize": 1, "bytecode_line_index": 2,
    "span_unnest": 2, "symtable": 2, "join_group": 3, "relate": 4,
    # ...
}
```

**Consolidation target:**

```python
# src/semantics/view_kinds.py (NEW FILE - single authority)
class ViewKind(StrEnum):
    NORMALIZE = "normalize"     # Subsumes: normalize, scip_normalize, bytecode_line_index
    DERIVE = "derive"           # Subsumes: symtable, span_unnest
    RELATE = "relate"           # Subsumes: relate, join_group
    UNION = "union"             # Subsumes: union_nodes, union_edges
    PROJECT = "project"         # Subsumes: projection, finalize, export
    DIAGNOSTIC = "diagnostic"   # Unchanged

VIEW_KIND_ORDER: Final[dict[ViewKind, int]] = {
    ViewKind.NORMALIZE: 0,
    ViewKind.DERIVE: 1,
    ViewKind.RELATE: 2,
    ViewKind.UNION: 3,
    ViewKind.PROJECT: 4,
    ViewKind.DIAGNOSTIC: 5,
}

# Parameterization for subkinds:
class ViewKindParams(StructBaseStrict, frozen=True):
    span_unit: SpanUnit = "byte"              # normalize: byte vs line_column
    structure: Literal["flat", "nested"] = "flat"  # normalize: flat vs nested (span_unnest)
    derivation_spec: str | None = None        # derive: which derivation
    union_target: Literal["nodes", "edges"] | None = None  # union: target type
    output_contract: str | None = None        # project: contract name
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/view_kinds.py` | Create: single `ViewKind` enum with ordering and parameterization |
| `src/semantics/ir.py` | Edit: replace `SemanticIRKind` Literal with `ViewKind` import |
| `src/semantics/spec_registry.py` | Edit: replace `SpecKind` Literal with `ViewKind` import |
| `src/semantics/ir_pipeline.py` | Edit: replace `_KIND_ORDER` with `VIEW_KIND_ORDER` import |
| `src/semantics/pipeline.py` | Edit: update builder dispatch to use consolidated `ViewKind` |
| `tests/unit/semantics/test_view_kinds.py` | Create: test consolidation correctness |

**Implementation checklist:**

- [ ] Define `ViewKind` enum with 6 consolidated values
- [ ] Define `ViewKindParams` for subkind parameterization
- [ ] Create backward-compat mapping: `old_kind_to_new(old: str) -> tuple[ViewKind, ViewKindParams]`
- [ ] Replace `SemanticIRKind` Literal type with `ViewKind`
- [ ] Replace `SpecKind` Literal type with `ViewKind`
- [ ] Replace `_KIND_ORDER` dict with `VIEW_KIND_ORDER`
- [ ] Update `_builder_for_semantic_spec()` dispatch to use consolidated kinds
- [ ] Verify pipeline produces identical outputs with consolidated kinds
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `SemanticIRKind` Literal | `src/semantics/ir.py:13-28` | Replaced by `ViewKind` enum |
| `SpecKind` Literal | `src/semantics/spec_registry.py:33-48` | Replaced by `ViewKind` enum |
| `_KIND_ORDER` dict | `src/semantics/ir_pipeline.py:21-36` | Replaced by `VIEW_KIND_ORDER` |

---

### E.3 Builder Dispatch Factory

**Current pattern (14 dispatch functions, ~200 lines):**

```python
# src/semantics/pipeline.py:1064-1091
def _builder_for_semantic_spec(spec, *, context):
    handlers = {
        "normalize": _builder_for_normalize_spec,       # line 1070
        "scip_normalize": _builder_for_scip_normalize_spec,  # 1071
        # ... 12 more entries
    }
    handler = handlers.get(spec.kind)
    if handler is None:
        raise ValueError(...)
    return handler(spec, context)

# 14 individual builder functions (lines 878-1061):
# _builder_for_normalize_spec (878-892) - dict lookup
# _builder_for_scip_normalize_spec (895-903) - dict lookup
# _builder_for_span_unnest_spec (918-940) - dict lookup
# _builder_for_symtable_spec (943-961) - dict lookup
# _builder_for_join_group_spec (964-972) - dict lookup
# _builder_for_relate_spec (975-989) - dict lookup
# _builder_for_union_edges_spec (992-996) - direct call
# _builder_for_union_nodes_spec (999-1003) - direct call
# _builder_for_projection_spec (1006-1014) - direct call
# _builder_for_diagnostic_spec (1017-1027) - dict lookup
# _builder_for_export_spec (1030-1040) - conditional
# _builder_for_finalize_spec (1043-1052) - dict lookup
# _builder_for_artifact_spec (1055-1061) - raises error
# _builder_for_bytecode_line_index_spec (906-915) - dict lookup
```

**Factory replacement (for the 7 dict-lookup functions):**

```python
# src/semantics/pipeline.py (refactored)
def _dispatch_from_registry(
    registry: Mapping[str, DataFrameBuilder],
    context_label: str,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    """Generic dispatch factory for builder lookups."""
    def _handler(spec: SemanticSpecIndex, context: _SemanticSpecContext) -> DataFrameBuilder:
        builder = registry.get(spec.name)
        if builder is None:
            msg = f"Missing {context_label} builder for {spec.name!r}."
            raise KeyError(msg)
        return builder
    return _handler
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/pipeline.py` | Edit: replace 7 dict-lookup builder functions with `_dispatch_from_registry()` factory; keep 4 unique-logic functions |
| `tests/unit/semantics/test_builder_dispatch.py` | Create: test dispatch factory |

**Implementation checklist:**

- [ ] Implement `_dispatch_from_registry()` factory
- [ ] Replace `_builder_for_normalize_spec`, `_builder_for_scip_normalize_spec`, `_builder_for_bytecode_line_index_spec`, `_builder_for_span_unnest_spec`, `_builder_for_symtable_spec`, `_builder_for_join_group_spec`, `_builder_for_relate_spec` with factory instances
- [ ] Keep `_builder_for_union_edges_spec`, `_builder_for_union_nodes_spec`, `_builder_for_projection_spec`, `_builder_for_export_spec`, `_builder_for_finalize_spec`, `_builder_for_diagnostic_spec`, `_builder_for_artifact_spec` (unique logic)
- [ ] Update `_builder_for_semantic_spec()` dispatch table
- [ ] Verify all builder functions produce identical outputs
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| 7 dict-lookup `_builder_for_*` functions | `src/semantics/pipeline.py:878-989` | Replaced by generic factory instances |

---

## PHASE F: Pipeline-as-Specification

### F.1 Schema-Aware Inference Phase in Compile-Optimize-Emit Pipeline

**Current pipeline:**

```python
# src/semantics/ir_pipeline.py
def compile_semantics(model: SemanticModel) -> SemanticIR: ...    # Builds IR from model
def optimize_semantics(ir: SemanticIR) -> SemanticIR: ...         # Join fusion, dead code pruning
def emit_semantics(ir: SemanticIR) -> SemanticIR: ...             # Dataset rows, fingerprints
def build_semantic_ir(outputs=None) -> SemanticIR: ...            # compose: compile → optimize → emit
```

**New phase (between compile and optimize):**

```python
# src/semantics/ir_pipeline.py (addition)
def infer_semantics(ir: SemanticIR) -> SemanticIR:
    """Schema-aware inference phase: resolve derived properties.

    Between compile and optimize, this phase:
    1. Resolves AnnotatedSchemas for all views
    2. Infers join strategies using infer_join_strategy()
    3. Validates declared join keys against inferred compatible columns
    4. Derives cache policies from graph properties
    5. Derives output projections from standard templates

    Result: IR views contain only irreducible specifications;
    everything derivable has been resolved.
    """
    # For each relate/join_group view:
    #   1. Build AnnotatedSchema from left/right inputs
    #   2. Call infer_join_strategy(left_schema, right_schema)
    #   3. Validate declared join keys if present
    #   4. Attach inferred properties as view metadata

    # For each view:
    #   1. Derive cache policy from graph topology
    #   2. Derive output projection from standard template
    return ir  # Modified with inferred properties
```

**Updated pipeline:**

```python
def build_semantic_ir(outputs=None) -> SemanticIR:
    ir = compile_semantics(model)
    ir = infer_semantics(ir)      # NEW PHASE
    ir = optimize_semantics(ir)
    ir = emit_semantics(ir)
    return ir
```

**Key function - AnnotatedSchema.infer_join_keys() (already exists but unused):**

```python
# src/semantics/types/annotated_schema.py:268-298
def infer_join_keys(self, other: AnnotatedSchema) -> list[tuple[str, str]]:
    """Infer potential join keys between two schemas.
    Uses compatibility groups to find matching columns.
    Prefers exact name matches over position-based pairing."""
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/ir_pipeline.py` | Edit: add `infer_semantics()` phase between compile and optimize |
| `src/semantics/ir.py` | Edit: add `inferred_properties: Mapping[str, object] | None = None` field to `SemanticIRView` |
| `tests/unit/semantics/test_ir_inference_phase.py` | Create: test the inference phase |

**Implementation checklist:**

- [ ] Implement `infer_semantics()` as a new IR pipeline phase
- [ ] For each relate/join_group view: resolve schemas, infer join strategy, validate keys
- [ ] For each view: compute graph-derived cache policy
- [ ] For terminal views: derive output projection from standard template
- [ ] Attach inferred properties to `SemanticIRView` metadata
- [ ] Update `build_semantic_ir()` to include the new phase
- [ ] Verify pipeline produces identical outputs before and after inference phase
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

**Decommission after completion:** None (additive pipeline phase).

---

### F.2 Reproducible Execution Package (Section 4.13)

**Execution package structure:**

```python
# src/relspec/execution_package.py (NEW FILE)
class ExecutionPackage(StructBaseStrict, frozen=True):
    """Fingerprint-keyed replayable execution package."""
    package_fingerprint: str
    manifest_hash: str
    policy_artifact_hash: str
    capability_snapshot_hash: str
    plan_bundle_fingerprints: Mapping[str, str]
    session_config_hash: str
    created_at_unix_ms: int

    @classmethod
    def from_execution(
        cls,
        *,
        manifest: SemanticProgramManifest,
        compiled_policy: CompiledExecutionPolicy,
        capability_snapshot: RuntimeCapabilitiesSnapshot,
        plan_bundles: Mapping[str, DataFusionPlanBundle],
        session_config: DataFusionRuntimeProfile,
    ) -> ExecutionPackage: ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/execution_package.py` | Create: `ExecutionPackage` struct and `from_execution()` factory |
| `src/serde_artifacts.py` | Edit: add `ExecutionPackageArtifact` payload type |
| `src/serde_artifact_specs.py` | Edit: register execution package spec |
| `src/hamilton_pipeline/driver_factory.py` | Edit: build and record execution package at pipeline start |
| `tests/unit/relspec/test_execution_package.py` | Create: unit tests for deterministic fingerprinting |

**Implementation checklist:**

- [ ] Define `ExecutionPackage` struct with hash fields
- [ ] Implement `from_execution()` that computes deterministic hashes for each component
- [ ] Compute `package_fingerprint` as composite hash of all component hashes
- [ ] Record execution package as typed artifact at pipeline start
- [ ] Add replay comparison: compare current package fingerprint with previous run
- [ ] Run: `uv run pytest tests/unit/relspec/ -v`

**Decommission after completion:** None (new capability).

---

## PHASE G: Advanced Acceleration Track

### G.1 Query-Shape Profiling and Workload Classes

**Target workload classification:**

```python
# src/datafusion_engine/workload/classifier.py (NEW FILE)
class WorkloadClass(StrEnum):
    BATCH_INGEST = "batch_ingest"           # Large writes, sequential
    INTERACTIVE_QUERY = "interactive_query"  # Ad-hoc reads, latency-sensitive
    COMPILE_REPLAY = "compile_replay"       # Deterministic re-execution
    INCREMENTAL_UPDATE = "incremental_update"  # CDF-based updates

class QueryShapeProfile(StructBaseStrict, frozen=True):
    dominant_join_types: tuple[str, ...]    # "span_overlap", "file_equi"
    scan_volume_bytes: int
    output_volume_bytes: int
    filter_selectivity: float
    workload_class: WorkloadClass
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/workload/classifier.py` | Create: workload classification from plan signals |
| `src/datafusion_engine/workload/__init__.py` | Create: module init |
| `src/datafusion_engine/session/runtime.py` | Edit: select session configuration based on workload class |
| `tests/unit/datafusion_engine/test_workload_classifier.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Define `WorkloadClass` enum and `QueryShapeProfile` struct
- [ ] Implement `classify_workload(plan_signals: PlanSignals) -> WorkloadClass`
- [ ] Map workload classes to DataFusion session configurations (memory limits, parallelism, cache sizes)
- [ ] Wire classification into session builder
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission after completion:** None (new capability).

---

### G.2 Pruning-Ladder Optimization Program (Section 4.9)

**Target pruning metrics:**

```python
# src/datafusion_engine/pruning/metrics.py (NEW FILE)
class PruningMetrics(StructBaseStrict, frozen=True):
    row_groups_total: int
    row_groups_pruned: int
    pages_total: int
    pages_pruned: int
    filters_pushed: int
    statistics_available: bool
    pruning_effectiveness: float  # 0.0-1.0
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/pruning/metrics.py` | Create: pruning metrics collection |
| `src/datafusion_engine/pruning/__init__.py` | Create: module init |
| `src/serde_artifacts.py` | Edit: add `PruningMetricsArtifact` payload type |
| `src/serde_artifact_specs.py` | Edit: register pruning metrics spec |
| `src/datafusion_engine/plan/pipeline.py` | Edit: collect pruning metrics after query execution |
| `tests/unit/datafusion_engine/test_pruning_metrics.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Define `PruningMetrics` struct
- [ ] Extract pruning metrics from DataFusion `explain analyze` output
- [ ] Record metrics as typed artifact per query execution
- [ ] Add monotonic improvement tracking across runs
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission after completion:** None (new capability).
---

## PHASE H: Schema Derivation

### H.1 Extract Schema from ExtractMetadata

**Current static registry (~800 LOC):**

```python
# src/datafusion_engine/schema/registry.py:1739-1749
_BASE_EXTRACT_SCHEMA_BY_NAME: ImmutableRegistry[str, pa.Schema] = ImmutableRegistry.from_dict({
    "repo_files_v1": REPO_FILES_SCHEMA,      # ~10 fields, lines 576-586
    "libcst_files_v1": LIBCST_FILES_SCHEMA,   # ~18 fields, lines 554-574
    "ast_files_v1": AST_FILES_SCHEMA,         # ~14 fields, lines 682-698
    "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
    "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
    "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
    "scip_index_v1": SCIP_INDEX_SCHEMA,
})

# Nested dataset lookup:
# src/datafusion_engine/schema/registry.py:1751-2120+
NESTED_DATASET_INDEX: ImmutableRegistry[str, NestedDatasetSpec] = ImmutableRegistry.from_dict({
    "cst_nodes": {"root": "libcst_files_v1", "path": "nodes", "role": "intrinsic"},
    "cst_refs": {"root": "libcst_files_v1", "path": "refs", "role": "intrinsic"},
    # ... 17+ more entries
})
```

**ExtractMetadata as alternative schema source:**

```python
# src/datafusion_engine/extract/metadata.py:32-50
@dataclass(frozen=True)
class ExtractMetadata:
    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]         # <-- defines column names
    row_fields: tuple[str, ...] = ()  # <-- defines nested struct fields
    row_extras: tuple[str, ...] = ()
    template: str | None = None
    # ... more fields

# src/datafusion_engine/extract/metadata.py:158-167
@cache
def extract_metadata_specs() -> tuple[ExtractMetadata, ...]: ...

# src/datafusion_engine/extract/metadata.py:171-183
@cache
def extract_metadata_by_name() -> Mapping[str, ExtractMetadata]: ...
```

**Schema derivation utility:**

```python
# src/datafusion_engine/schema/derivation.py (NEW FILE)
def derive_extract_schema(metadata: ExtractMetadata) -> pa.Schema:
    """Derive PyArrow schema from ExtractMetadata field definitions.

    Uses metadata.fields for top-level columns and metadata.row_fields
    for nested struct fields. Column types are inferred from:
    1. Standard field registry (file_id -> utf8, bstart -> int64, etc.)
    2. Nested struct introspection from parent schema
    3. Convention-based type inference for remaining fields
    """
    fields = []
    for field_name in metadata.fields:
        arrow_type = _infer_field_type(field_name)
        fields.append(pa.field(field_name, arrow_type))
    return pa.schema(fields)
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/schema/derivation.py` | Create: `derive_extract_schema()`, `derive_nested_schema()` utilities |
| `src/datafusion_engine/schema/field_types.py` | Create: standard field type registry (`file_id` → `pa.utf8()`, `bstart` → `pa.int64()`, etc.) |
| `src/datafusion_engine/schema/registry.py` | Edit: replace static `_BASE_EXTRACT_SCHEMA_BY_NAME` with derived schemas; keep as validation target |
| `src/datafusion_engine/extract/metadata.py` | Edit: add `schema() -> pa.Schema` method to `ExtractMetadata` that delegates to `derive_extract_schema()` |
| `tests/unit/datafusion_engine/test_schema_derivation.py` | Create: test that derived schemas match current static schemas exactly |

**Implementation checklist:**

- [ ] Build standard field type registry mapping known field names to PyArrow types
- [ ] Implement `derive_extract_schema()` using metadata fields + type registry
- [ ] Implement `derive_nested_schema()` for nested datasets using parent struct introspection
- [ ] Add `schema()` method to `ExtractMetadata`
- [ ] Write comparison test: `assert derive_extract_schema(metadata) == static_schema` for all 7 base schemas
- [ ] Once comparison passes, replace `_BASE_EXTRACT_SCHEMA_BY_NAME` entries with derived versions
- [ ] Update `extract_schema_for()` to prefer metadata-derived schemas
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `REPO_FILES_SCHEMA` constant | `src/datafusion_engine/schema/registry.py:576-586` | Derived from ExtractMetadata |
| `LIBCST_FILES_SCHEMA` constant | `src/datafusion_engine/schema/registry.py:554-574` | Derived from ExtractMetadata |
| `AST_FILES_SCHEMA` constant | `src/datafusion_engine/schema/registry.py:682-698` | Derived from ExtractMetadata |
| `SYMTABLE_FILES_SCHEMA` constant | `src/datafusion_engine/schema/registry.py` | Derived from ExtractMetadata |
| `TREE_SITTER_FILES_SCHEMA` constant | `src/datafusion_engine/schema/registry.py` | Derived from ExtractMetadata |
| `BYTECODE_FILES_SCHEMA` constant | `src/datafusion_engine/schema/registry.py` | Derived from ExtractMetadata |
| `SCIP_INDEX_SCHEMA` constant | `src/datafusion_engine/schema/registry.py` | Derived from ExtractMetadata |
| ~30 nested type constants (`CST_NODE_T`, `CST_REF_T`, etc.) | `src/datafusion_engine/schema/registry.py` | Derived from parent struct introspection |

---

### H.2 Generate Serialization Schemas from Structs

**Current pattern (manual duplication):**

```python
# Example: Profile hash schema mirrors a struct
# Schema constant (manual):
_PROFILE_HASH_SCHEMA = pa.schema([
    ("profile_name", pa.utf8()),
    ("settings_hash", pa.utf8()),
    # ... mirrors a StructBase class
])

# Struct definition (authoritative):
class ProfileHashPayload(StructBaseStrict, frozen=True):
    profile_name: str | None = None
    settings_hash: str = ""
```

**Schema generation utility:**

```python
# src/utils/schema_from_struct.py (NEW FILE)
import msgspec
import pyarrow as pa

_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    str: pa.utf8(),
    int: pa.int64(),
    float: pa.float64(),
    bool: pa.bool_(),
}

def schema_from_struct(struct_type: type[msgspec.Struct]) -> pa.Schema:
    """Generate PyArrow schema from a msgspec Struct definition.

    Maps struct field types to PyArrow types. Handles:
    - Primitive types (str, int, float, bool)
    - Optional types (T | None -> nullable field)
    - Tuple types (tuple[str, ...] -> list(utf8))
    - Nested struct types (recursive)
    """
    fields = []
    for field_info in msgspec.structs.fields(struct_type):
        arrow_type = _resolve_arrow_type(field_info.type)
        nullable = _is_optional(field_info.type)
        fields.append(pa.field(field_info.name, arrow_type, nullable=nullable))
    return pa.schema(fields)
```

**Target files:**

| File | Action |
|------|--------|
| `src/utils/schema_from_struct.py` | Create: `schema_from_struct()` utility |
| `src/datafusion_engine/schema/registry.py` | Edit: replace `_*_SCHEMA` serialization constants with `schema_from_struct()` calls |
| `tests/unit/utils/test_schema_from_struct.py` | Create: test schema generation against known structs |

**Implementation checklist:**

- [ ] Implement `schema_from_struct()` with type mapping for primitives, optionals, tuples, nested structs
- [ ] Handle `Mapping[str, ...]` fields (map type in PyArrow)
- [ ] Handle `Literal[...]` fields (utf8 with value constraints)
- [ ] Test against 3-4 existing struct/schema pairs to verify exact match
- [ ] Replace serialization schema constants with generated versions
- [ ] Run: `uv run pytest tests/unit/utils/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `_PROFILE_HASH_SCHEMA` and similar constants | Various files | Generated from struct definitions |
| ~15 serialization schema constants | Scattered across `schema/registry.py`, `session/runtime.py`, `obs/metrics.py` | Auto-generated from structs |

---

### H.3 Unify Schema Authority

**Current competing sources:**

| Source | Location | Coverage |
|--------|----------|----------|
| Static registry | `schema/registry.py` | All extraction + semantic |
| ExtractMetadata | `extract/metadata.py` objects | Extract datasets |
| DataFusion catalog | `information_schema.*` views | Registered tables |
| View builder returns | DataFrame builder functions | View outputs |

**Target: DataFusion `information_schema` as single authority:**

```python
# At extraction time:
ctx.register_record_batches("cst_refs", [batch])
# Schema now available via: ctx.sql("SELECT * FROM information_schema.columns WHERE table_name = 'cst_refs'")

# At view registration time:
ctx.register_dataframe("rel_name_symbol", df)
# Schema now available via information_schema
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/schema/registry.py` | Edit: remove internal schema constants; keep only external boundary schemas |
| `src/datafusion_engine/dataset/registration.py` | Edit: register schemas in DataFusion catalog at registration time |
| `tests/unit/datafusion_engine/test_schema_authority.py` | Create: test schema authority unification |

**Implementation checklist:**

- [ ] Ensure all table registrations populate DataFusion's `information_schema`
- [ ] Replace `extract_schema_for(name)` internal callers with catalog lookups where possible
- [ ] Keep explicit schemas only for external boundary contracts (CPG output, pipeline IO)
- [ ] Separate internal schemas (derivable) from external schemas (must be explicit) in registry
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ tests/integration/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| Internal extraction schema constants | `src/datafusion_engine/schema/registry.py` (~800 LOC) | Derived from ExtractMetadata or catalog |
| `_BASE_EXTRACT_SCHEMA_BY_NAME` registry | `src/datafusion_engine/schema/registry.py:1739-1749` | Replaced by metadata-derived schemas |

---

## PHASE I: Semantic Type System Activation

### I.1 Deploy AnnotatedSchema for Join Key Selection

**Current pattern (hardcoded, 9 specs):**

```python
# src/semantics/quality_specs.py (all 9 specs):
QualityRelationshipSpec(
    left_on=["file_id"],    # Hardcoded column name
    right_on=["file_id"],   # Hardcoded column name
    ...
)
```

**Schema-driven replacement:**

```python
# Using existing AnnotatedSchema.infer_join_keys():
# src/semantics/types/annotated_schema.py:268-298
def infer_join_keys(self, other: AnnotatedSchema) -> list[tuple[str, str]]:
    """Uses CompatibilityGroup to find joinable column pairs."""
    common_groups = self.common_compatibility_groups(other)
    # Prefers exact name matches (e.g., both "file_id")
    # Falls back to position-based pairing
    ...

# Or using direct group query:
# src/semantics/types/annotated_schema.py:166-181
def columns_by_compatibility_group(self, group: CompatibilityGroup) -> tuple[AnnotatedColumn, ...]:
    return tuple(c for c in self.columns if group in c.compatibility_groups)
```

**Deployment in compiler:**

```python
# In src/semantics/compiler.py relate() method:
# When left_on/right_on are empty (Phase D.1), use:
left_file_cols = left_schema.columns_by_compatibility_group(CompatibilityGroup.FILE_IDENTITY)
right_file_cols = right_schema.columns_by_compatibility_group(CompatibilityGroup.FILE_IDENTITY)
# Or:
join_pairs = left_schema.infer_join_keys(right_schema)
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/compiler.py` | Edit: use `infer_join_keys()` when `left_on`/`right_on` are empty |
| `src/semantics/quality_specs.py` | Edit: remove hardcoded `left_on=["file_id"]` from all 9 specs (depends on D.1) |
| `tests/unit/semantics/test_annotated_schema_deployment.py` | Create: test inferred join keys match expected values |

**Implementation checklist:**

- [ ] Verify `AnnotatedSchema.infer_join_keys()` returns `[("file_id", "file_id")]` for all 9 spec pairs
- [ ] In compiler: call `infer_join_keys()` when `left_on` and `right_on` are empty
- [ ] Add assertion: inferred keys must match any explicitly declared keys (transitional validation)
- [ ] Remove `left_on=["file_id"]` and `right_on=["file_id"]` from all 9 quality specs
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| 18 hardcoded `left_on`/`right_on` entries | `src/semantics/quality_specs.py` | Inferred from AnnotatedSchema |

---

### I.2 Deploy Semantic Type Queries for Routing

**Current pattern (string-based routing):**

```python
# src/semantics/catalog/tags.py:
if "file_id" in join_keys:     # String name check
    ...
if "symbol" in name:            # Substring check
    ...

# src/semantics/incremental/deltas.py:65-69
def _export_key_columns(schema: pa.Schema) -> list[str]:
    key_cols = ["file_id", "qname_id"]
    if "symbol" in schema.names:    # String presence check
        key_cols.append("symbol")
    return key_cols

# src/semantics/catalog/projections.py:81-88
if "file_id" in names:          # String name check
    edge_owner = col("file_id")
```

**Schema-driven replacement:**

```python
# Using existing AnnotatedSchema methods:
# src/semantics/types/annotated_schema.py:215-228
def has_semantic_type(self, sem_type: SemanticType) -> bool:
    return any(c.semantic_type == sem_type for c in self.columns)

# BEFORE:
if "symbol" in schema.names:
    key_cols.append("symbol")

# AFTER:
annotated = AnnotatedSchema.from_arrow_schema(schema)
if annotated.has_semantic_type(SemanticType.SYMBOL):
    symbol_cols = annotated.columns_by_semantic_type(SemanticType.SYMBOL)
    key_cols.extend(c.name for c in symbol_cols)
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/catalog/tags.py` | Edit: replace `"file_id" in join_keys` with compatibility group query |
| `src/semantics/incremental/deltas.py` | Edit: replace `"symbol" in schema.names` with `has_semantic_type()` |
| `src/semantics/catalog/projections.py` | Edit: replace `col("file_id")` lookups with semantic type queries |
| `tests/unit/semantics/test_semantic_routing.py` | Create: test semantic-type-based routing |

**Implementation checklist:**

- [ ] Replace `"file_id" in join_keys` patterns with `annotated.has_compatibility_group(FILE_IDENTITY)`
- [ ] Replace `"symbol" in schema.names` with `annotated.has_semantic_type(SemanticType.SYMBOL)`
- [ ] Replace `"file" in name` substring checks with `SemanticIRView.kind` or schema classification
- [ ] Replace hardcoded `col("file_id")` with `col(schema.file_identity_column_name())`
- [ ] Add helper: `AnnotatedSchema.file_identity_column() -> str | None` for common case
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| ~40 hardcoded column name references | `tags.py`, `deltas.py`, `projections.py` | Replaced by semantic type queries |

---

### I.3 Consolidate Threshold Constants

**Current scattered constants:**

```python
# src/datafusion_engine/delta/scan_policy_inference.py:20
_SMALL_TABLE_ROW_THRESHOLD = 10_000

# src/datafusion_engine/io/write.py
_ADAPTIVE_SMALL_TABLE_THRESHOLD = 10_000
_ADAPTIVE_LARGE_TABLE_THRESHOLD = 1_000_000

# src/semantics/incremental/delta_updates.py
_STREAMING_ROW_THRESHOLD = 100_000
```

**Consolidated tier definition:**

```python
# src/relspec/table_size_tiers.py (NEW FILE)
class TableSizeTier(StrEnum):
    SMALL = "small"      # < small_threshold
    MEDIUM = "medium"    # < large_threshold
    LARGE = "large"      # >= large_threshold

class TableSizeThresholds(StructBaseStrict, frozen=True):
    small_threshold: int = 10_000
    large_threshold: int = 1_000_000

def classify_table_size(
    row_count: int | None,
    thresholds: TableSizeThresholds = TableSizeThresholds(),
) -> TableSizeTier:
    if row_count is None or row_count < thresholds.small_threshold:
        return TableSizeTier.SMALL
    if row_count < thresholds.large_threshold:
        return TableSizeTier.MEDIUM
    return TableSizeTier.LARGE
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/table_size_tiers.py` | Create: `TableSizeTier`, `TableSizeThresholds`, `classify_table_size()` |
| `src/datafusion_engine/delta/scan_policy_inference.py` | Edit: replace `_SMALL_TABLE_ROW_THRESHOLD` with `classify_table_size()` |
| `src/datafusion_engine/io/write.py` | Edit: replace `_ADAPTIVE_SMALL_TABLE_THRESHOLD` and `_ADAPTIVE_LARGE_TABLE_THRESHOLD` with `classify_table_size()` |
| `src/semantics/incremental/delta_updates.py` | Edit: replace `_STREAMING_ROW_THRESHOLD` with tier-based check |
| `tests/unit/relspec/test_table_size_tiers.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Define `TableSizeTier` enum and `TableSizeThresholds` config struct
- [ ] Implement `classify_table_size()` function
- [ ] Replace 4 scattered threshold constants with `classify_table_size()` calls
- [ ] Wire `TableSizeThresholds` into `CompiledExecutionPolicy` for runtime configurability
- [ ] Run: `uv run pytest tests/unit/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `_SMALL_TABLE_ROW_THRESHOLD` | `src/datafusion_engine/delta/scan_policy_inference.py:20` | Consolidated into `TableSizeThresholds` |
| `_ADAPTIVE_SMALL_TABLE_THRESHOLD` | `src/datafusion_engine/io/write.py` | Consolidated |
| `_ADAPTIVE_LARGE_TABLE_THRESHOLD` | `src/datafusion_engine/io/write.py` | Consolidated |
| `_STREAMING_ROW_THRESHOLD` | `src/semantics/incremental/delta_updates.py` | Consolidated |

---

## PHASE J: Registry Consolidation

### J.1 Single View Kind Authority

**Current triple definition:**

```python
# 1. src/semantics/ir.py:13-28 (SemanticIRKind Literal)
# 2. src/semantics/spec_registry.py:33-48 (SpecKind Literal - IDENTICAL)
# 3. src/semantics/ir_pipeline.py:21-36 (_KIND_ORDER Mapping)
```

**Consolidated authority:**

```python
# src/semantics/view_kinds.py (from Phase E.2)
# After E.2 consolidation, both ir.py and spec_registry.py import ViewKind
# _KIND_ORDER is replaced by VIEW_KIND_ORDER
# Single source generates type, ordering, and dispatch table
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/view_kinds.py` | Already created in E.2; ensure it's the sole import source |
| `src/semantics/ir.py` | Edit: remove `SemanticIRKind`, import `ViewKind` from `view_kinds` |
| `src/semantics/spec_registry.py` | Edit: remove `SpecKind`, import `ViewKind` from `view_kinds` |
| `src/semantics/ir_pipeline.py` | Edit: remove `_KIND_ORDER`, import `VIEW_KIND_ORDER` from `view_kinds` |

**Implementation checklist:**

- [ ] Verify `ViewKind` from E.2 is the single authority
- [ ] Remove `SemanticIRKind` Literal from `ir.py`, replace with `ViewKind` import
- [ ] Remove `SpecKind` Literal from `spec_registry.py`, replace with `ViewKind` import
- [ ] Remove `_KIND_ORDER` from `ir_pipeline.py`, replace with `VIEW_KIND_ORDER` import
- [ ] Update all import sites
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `SemanticIRKind` Literal | `src/semantics/ir.py:13-28` | Replaced by `ViewKind` |
| `SpecKind` Literal | `src/semantics/spec_registry.py:33-48` | Replaced by `ViewKind` |
| `_KIND_ORDER` dict | `src/semantics/ir_pipeline.py:21-36` | Replaced by `VIEW_KIND_ORDER` |

---

### J.2 Eliminate Output Naming Identity Map

**Current identity mapping:**

```python
# src/semantics/naming.py:23-48
SEMANTIC_OUTPUT_NAMES: Final[dict[str, str]] = {
    "scip_occurrences_norm": "scip_occurrences_norm",  # Identity!
    "cst_refs_norm": "cst_refs_norm",                  # Identity!
    # ... all 22 entries are identity mappings (key == value)
}

# src/semantics/naming.py:51-85
def canonical_output_name(internal_name, *, manifest=None) -> str:
    # When manifest has output_name_map, use it (correct path)
    # Otherwise fall back to SEMANTIC_OUTPUT_NAMES (identity no-op)
    ...

# src/semantics/naming.py:88-104
def internal_name(output_name) -> str:
    reverse = {v: k for k, v in SEMANTIC_OUTPUT_NAMES.items()}
    return reverse.get(output_name, output_name)  # Also identity no-op
```

**Simplification:**

```python
# src/semantics/naming.py (simplified)
def canonical_output_name(internal_name: str, *, manifest: object | None = None) -> str:
    """Get canonical output name. Manifest map is authoritative when available."""
    if manifest is not None:
        output_map = getattr(manifest, "output_name_map", None)
        if output_map is not None:
            return output_map.get(internal_name, internal_name)
    return internal_name  # No static dict needed

def internal_name(output_name: str) -> str:
    return output_name  # Identity without the reverse lookup overhead
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/naming.py` | Edit: remove `SEMANTIC_OUTPUT_NAMES` dict, simplify `canonical_output_name()` and `internal_name()` |
| `tests/unit/semantics/test_naming.py` | Edit: update tests |

**Implementation checklist:**

- [ ] Remove `SEMANTIC_OUTPUT_NAMES` dict (22 entries)
- [ ] Simplify `canonical_output_name()` to use manifest map only, fall back to identity
- [ ] Simplify `internal_name()` to return input unchanged
- [ ] Verify all callers still get correct names
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| `SEMANTIC_OUTPUT_NAMES` dict | `src/semantics/naming.py:23-48` | Identity mapping provides no value |

---

### J.3 Convention-Based Extractor Discovery

**Current hardcoded registries:**

```python
# src/datafusion_engine/extract/templates.py:80-212
TEMPLATES: dict[str, ExtractorTemplate] = {
    "ast": ExtractorTemplate(extractor_name="ast", evidence_rank=4, ...),
    "cst": ExtractorTemplate(extractor_name="cst", evidence_rank=3, ...),
    # ... 7+ more entries
}

# src/datafusion_engine/extract/templates.py:215-390
CONFIGS: dict[str, ExtractorConfigSpec] = {
    "ast": ExtractorConfigSpec(extractor_name="ast", defaults={...}),
    # ... more entries
}

# src/datafusion_engine/extract/templates.py:772-787
_DATASET_TEMPLATE_REGISTRY: ImmutableRegistry[str, Callable] = ImmutableRegistry.from_dict({
    "repo_scan": _repo_scan_records,
    "ast": _ast_records,
    "cst": _cst_records,
    # ... 6 more entries
})
```

**Convention-based discovery:**

```python
# Each extractor module exports standard attributes:
# src/extract/ast_extractor.py:
TEMPLATE = ExtractorTemplate(extractor_name="ast", evidence_rank=4, ...)
CONFIG = ExtractorConfigSpec(extractor_name="ast", defaults={...})

# Discovery function:
# src/datafusion_engine/extract/discovery.py (NEW FILE)
def discover_extractor_templates() -> dict[str, ExtractorTemplate]:
    """Discover templates from extractor modules via convention."""
    import importlib
    templates = {}
    for module_name in EXTRACTOR_MODULES:
        mod = importlib.import_module(module_name)
        if hasattr(mod, "TEMPLATE"):
            templates[mod.TEMPLATE.extractor_name] = mod.TEMPLATE
    return templates
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/extract/discovery.py` | Create: convention-based template discovery |
| `src/datafusion_engine/extract/templates.py` | Edit: replace `TEMPLATES` and `CONFIGS` dicts with discovery-based loading |
| Each extractor module | Edit: export `TEMPLATE` and `CONFIG` attributes |
| `tests/unit/datafusion_engine/test_extractor_discovery.py` | Create: test convention-based discovery |

**Implementation checklist:**

- [ ] Add `TEMPLATE` and `CONFIG` attributes to each extractor module
- [ ] Implement `discover_extractor_templates()` function
- [ ] Replace static `TEMPLATES` dict with discovered templates
- [ ] Replace static `CONFIGS` dict with discovered configs
- [ ] Verify discovered templates match current static versions
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission after completion:**

| Target | Location | Reason |
|--------|----------|--------|
| Static `TEMPLATES` dict | `src/datafusion_engine/extract/templates.py:80-212` | Replaced by convention-based discovery |
| Static `CONFIGS` dict | `src/datafusion_engine/extract/templates.py:215-390` | Replaced by convention-based discovery |

---

## 18. Cross-Scope Decommission Items

Items that can only be decommissioned after multiple scope items are completed:

### 18.1 CompileContext Class (After A.1 + A.4)

| Item | Location | Depends On |
|------|----------|------------|
| `CompileContext` class | `src/semantics/compile_context.py:38-106` | A.1 (fallback removal) + all callers migrated |
| `CompileContext.compile()` method | `src/semantics/compile_context.py:96-106` | All compile paths use `build_semantic_execution_context()` |
| `CompileContext.semantic_ir()` method | `src/semantics/compile_context.py:64-68` | All IR paths use `semantic_ir_for_outputs()` directly |

**Condition:** Zero production callers of `CompileContext()`. Can verify via `/cq calls CompileContext`.

---

### 18.2 Static Schema Registry (After H.1 + H.2 + H.3)

| Item | Location | Depends On |
|------|----------|------------|
| ~800 LOC of extraction schema constants | `src/datafusion_engine/schema/registry.py:200-700` | H.1 (metadata derivation) fully validated |
| ~400 LOC of serialization schema constants | Various files | H.2 (struct-to-schema generation) fully validated |
| ~30 nested type constants | `src/datafusion_engine/schema/registry.py` | H.1 nested schema derivation |
| `_BASE_EXTRACT_SCHEMA_BY_NAME` registry | `src/datafusion_engine/schema/registry.py:1739-1749` | H.3 (unified schema authority) |

**Condition:** All derived schemas pass exact-match comparison with current static schemas.

---

### 18.3 SemanticIRKind + SpecKind + _KIND_ORDER (After E.2 + J.1)

| Item | Location | Depends On |
|------|----------|------------|
| `SemanticIRKind` Literal | `src/semantics/ir.py:13-28` | E.2 (ViewKind consolidation) |
| `SpecKind` Literal | `src/semantics/spec_registry.py:33-48` | E.2 + J.1 (single authority) |
| `_KIND_ORDER` dict | `src/semantics/ir_pipeline.py:21-36` | E.2 (VIEW_KIND_ORDER) |

**Condition:** All 3 definitions removed, `ViewKind` from `view_kinds.py` is sole import.

---

### 18.4 Naming Convention Cache Policy (After B.4 + D.3)

| Item | Location | Depends On |
|------|----------|------------|
| `_default_semantic_cache_policy()` | `src/semantics/pipeline.py:275-301` | B.4 (compiled policy) + D.3 (graph derivation) |
| Name pattern matching logic | Same function | Both compiled policy and graph derivation must be validated |

**Condition:** `CompiledExecutionPolicy.cache_policy_by_view` is populated and consumed by all cache policy consumers.

---

### 18.5 resolve_delta_maintenance_plan() Production Usage (After A.4)

| Item | Location | Depends On |
|------|----------|------------|
| `resolve_delta_maintenance_plan()` production callsites | `io/write.py:1747`, `delta_context.py:215` | A.4 (outcome-based wiring) |

**Condition:** All write paths use `resolve_maintenance_from_execution()`. The function itself remains as internal fallback when no outcome thresholds are configured.

---

### 18.6 Hardcoded Column Name References (After D.1 + I.1 + I.2)

| Item | Location | Depends On |
|------|----------|------------|
| 18 `left_on`/`right_on` entries | `src/semantics/quality_specs.py` | D.1 (schema-inferred join keys) |
| ~40 hardcoded column name references | `tags.py`, `deltas.py`, `projections.py` | I.2 (semantic type routing) |
| 4 scattered threshold constants | `scan_policy_inference.py`, `write.py`, `delta_updates.py` | I.3 (tier consolidation) |

**Condition:** All routing uses `AnnotatedSchema` methods; all join keys are schema-inferred; all thresholds use `TableSizeTier`.

---

### 18.7 Builder Dispatch Boilerplate (After E.2 + E.3)

| Item | Location | Depends On |
|------|----------|------------|
| 7 `_builder_for_*` dict-lookup functions | `src/semantics/pipeline.py:878-989` | E.3 (factory pattern) |
| 14-entry dispatch table | `src/semantics/pipeline.py:1064-1091` | E.2 (consolidated ViewKind) + E.3 |

**Condition:** Builder dispatch uses consolidated `ViewKind` with generic factory for dict-lookup patterns.

---

### 18.8 Template Registry Statics (After D.2 + J.3)

| Item | Location | Depends On |
|------|----------|------------|
| ~150 lines of boilerplate in 5 specs | `src/semantics/quality_specs.py` | D.2 (template factory) |
| `TEMPLATES` dict (~130 lines) | `src/datafusion_engine/extract/templates.py` | J.3 (convention discovery) |
| `CONFIGS` dict (~175 lines) | `src/datafusion_engine/extract/templates.py` | J.3 (convention discovery) |
| `SEMANTIC_OUTPUT_NAMES` dict | `src/semantics/naming.py:23-48` | J.2 (identity map elimination) |

**Condition:** Template factory replaces boilerplate specs; convention-based discovery replaces static dicts.

---

## End of Document
