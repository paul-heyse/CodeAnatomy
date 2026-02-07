# Programmatic Architecture Continuation Review (V2)

**Date:** 2026-02-07  
**Scope:** Deep review of `docs/plans/programmatic_architecture_continuation_v1_2026-02-07.md` with CQ-verified code reality and DataFusion/Delta optimization opportunities.  
**Goal:** Drive toward an entirely programmatic, semantically-compiling, inference-driven database architecture with minimal drift surfaces.

---

## 1. Executive Outcome

The continuation document is directionally strong, but several baseline assumptions are now stale.  
The biggest correction is not "foundations missing", but "foundations exist; integration coverage is uneven."

Most leverage now comes from:

1. Closing compile-boundary and resolver fallbacks that still instantiate ad hoc `CompileContext`.
2. Wiring already-implemented outcome/capability components into write/runtime paths.
3. Upgrading policy inference from static heuristics to a measured closed loop (plan signals + execution outcomes + artifact feedback).
4. Shifting from "add more static declarations" to "compile policy and behavior from plan/capability evidence."

---

## 2. Corrections to Continuation V1 (Code-Verified)

### 2.1 Compile boundary and resolver surface counts

**Correction:** Current counts differ from V1 and should be refreshed.

- `CompileContext` callsites in `src/`: **9** (`./cq q "entity=callsite name=CompileContext in=src"`).
- `CompileContext(runtime_profile=...)` pattern matches in `src/`: **7** (`./cq q "pattern='CompileContext(runtime_profile=\\$X)' in=src"`).
- Of those 7 pattern matches, outside `src/semantics/compile_context.py`: **6**.
- `dataset_bindings_for_profile()` callsites in `src/`: **5** (`./cq q "entity=callsite name=dataset_bindings_for_profile in=src"`).

Notable callsite previously omitted in V1 text:

- `src/hamilton_pipeline/modules/task_execution.py:546` (`_execute_view`) still constructs `CompileContext(runtime_profile=profile)`.

Also, V1 references `build_cpg_streaming`; current code path is `build_cpg_from_inferred_deps` in `src/semantics/pipeline.py:2012`.

### 2.2 Runtime capability adapter status

**Correction:** Capability adapter is already implemented and wired in primary paths.

- `detect_plan_capabilities()` exists: `src/datafusion_engine/extensions/runtime_capabilities.py:78`.
- Snapshot builder includes plan capabilities: `src/datafusion_engine/extensions/runtime_capabilities.py:213`.
- Execution authority wiring exists: `src/hamilton_pipeline/driver_factory.py:601`.

Implication: this is now a hardening and coverage task, not a greenfield Wave.

### 2.3 Policy validation status

**Correction:** Compile-time policy bundle validation is operational in driver construction.

- `validate_policy_bundle()` exists: `src/relspec/policy_validation.py:155`.
- Called in plan-context build: `src/hamilton_pipeline/driver_factory.py:2003`.
- CQ confirms active callsites (`./cq calls validate_policy_bundle`).

Implication: expand rule quality and artifact enforcement, not "implement validation from scratch."

### 2.4 Adaptive scan policy status (Wave 4B framing)

**Correction:** Adaptive scan-policy infrastructure already exists and is wired.

- Inference function: `src/datafusion_engine/delta/scan_policy_inference.py:42`.
- Pipeline application: `src/datafusion_engine/plan/pipeline.py:131` and `src/datafusion_engine/plan/pipeline.py:173`.
- CQ confirms call path (`./cq calls derive_scan_policy_overrides`, `./cq calls _apply_inferred_scan_policy_overrides`).

Implication: next step is richer signals + measured payoff, not initial construction.

### 2.5 Outcome-based maintenance status (Wave 4A framing)

**Correction:** Outcome-based maintenance logic exists but is not integrated into write flow.

- Exists:
  - `build_write_outcome_metrics`: `src/datafusion_engine/delta/maintenance.py:153`
  - `resolve_maintenance_from_execution`: `src/datafusion_engine/delta/maintenance.py:244`
- CQ shows no production callsites for these two functions.
- Current write path still uses `resolve_delta_maintenance_plan`:
  - `src/datafusion_engine/io/write.py:1677`
  - `src/semantics/incremental/delta_context.py:215`

Implication: this should move to "integration priority" rather than "new feature."

### 2.6 Join inference "unused infrastructure" claim

**Correction:** `infer_join_strategy()` is used in production via `require_join_strategy()`.

- Production call: `src/semantics/compiler.py:806` (`require_join_strategy`).
- CQ confirms:
  - `./cq calls require_join_strategy` -> includes production caller.
  - `./cq calls infer_join_strategy` -> called by `require_join_strategy` plus tests.

Refined conclusion: join inference is active, but coverage depth and calibration remain improvable.

### 2.7 DataFrame cache status

**Correction:** `DataFrame.cache()` is already integrated.

- `src/datafusion_engine/dataset/registration.py:3176` uses `df.cache()`.

Refined conclusion: opportunity is policy/telemetry quality, not first-time integration.

### 2.8 Extract registry status

**Correction:** Legacy global registry still exists, but production dependence is reduced.

- Deprecated initializer remains:
  - `src/hamilton_pipeline/modules/task_execution.py:897`
  - `src/hamilton_pipeline/modules/task_execution.py:914`
- CQ shows deprecated wrapper and registry lookup are test-oriented (`./cq calls ensure_extract_executors_registered`, `./cq calls get_extract_executor`).

Refined conclusion: deprecation path can be completed with low production risk.

---

## 3. Updated Priority Model (What Actually Moves the Architecture)

### 3.1 Priority A: Single-compile / single-resolver invariants

Target invariants per pipeline run:

1. Exactly one semantic compile (manifest + resolver) per execution boundary.
2. Resolver identity is stable across all downstream consumers.
3. No fallback `CompileContext(...)` instantiation outside compile boundary module.
4. No `dataset_bindings_for_profile()` consumers outside compile boundary compatibility shims.

Why first: this removes the highest-probability drift class at low conceptual complexity.

### 3.2 Priority B: Integrate existing outcome/capability machinery

Promote implemented-but-unused components to first-class runtime behavior:

1. Use `build_write_outcome_metrics` + `resolve_maintenance_from_execution` in write pipeline.
2. Persist outcome-driven maintenance decisions as artifacts with cause fields.
3. Gate scan-policy decisions with capability snapshots consistently (already partly done).

Why second: converts static policy into evidence-driven policy with limited new surface area.

### 3.3 Priority C: Replace naming heuristics with plan/manifest evidence

Current drift-prone example:

- `_default_semantic_cache_policy` uses name pattern heuristics (`src/semantics/pipeline.py:193`).

Move to compile-time policy derivation from:

1. IR view kind (`SemanticIRKind`)
2. inferred lineage/required columns
3. estimated cardinality/statistics confidence
4. volatility markers (incremental/CDF usage)

Why third: directly advances "entirely programmatic" behavior.

---

## 4. Expanded Scope Beyond Continuation V1

### 4.1 Policy-as-Data compiled artifact

Add a canonical `CompiledExecutionPolicy` artifact generated at compile time and threaded into runtime unchanged.

Suggested sections:

- `scan_policy_by_dataset`
- `cache_policy_by_view`
- `maintenance_policy_by_dataset`
- `udf_requirements_by_view`
- `validation_mode` and enforcement level

Runtime consumes policy artifact; it does not re-derive policy heuristically.

### 4.2 Plan-evidence confidence model

Attach confidence and rationale to inferred decisions:

- `confidence_score` (0-1)
- `evidence_sources` (lineage/stats/runtime outcomes/capabilities)
- `fallback_reason` (when confidence insufficient)

Use this for:

1. conservative fallback behavior,
2. drift diagnostics,
3. governance dashboards for policy maturity.

### 4.3 Closed-loop optimization from artifacts

Move from one-way planning to a learning loop:

1. Compile emits baseline policy.
2. Runtime captures execution outcomes and DataFusion metrics.
3. Post-run calibrator updates policy defaults for future runs (bounded, deterministic update rules).

This is the path to "inference-driven" without unsafe online adaptation.

### 4.4 DataFusion pruning-ladder optimization program

Treat pruning efficacy as a first-class product metric.  
Programmatically tune and track:

1. row-group pruning effectiveness,
2. page-index pruning effectiveness,
3. filter pushdown efficacy,
4. statistics availability and freshness.

Use explain/metrics artifacts to measure actual pruning gains per query shape.

### 4.5 Runtime cache stack observability

Beyond `df.cache()`, add explicit observability over cache layers:

1. listing/file-metadata/statistics cache configuration at session build,
2. cache hit/miss telemetry as runtime artifacts,
3. per-workload cache profile selection (batch ingest, interactive query, compile-heavy replay).

This prevents blind cache tuning and supports reproducible performance.

### 4.6 External index acceleration for semantic workloads

For dominant semantic query shapes (span-range, symbol lookup, file-scoped traversals), evaluate external index assisted file pruning:

1. coarse file-level pruning by external index/provider,
2. handoff to DataFusion parquet row-group/page pruning for in-file refinement.

This can materially reduce scan volume for large code corpora while preserving declarative query semantics.

### 4.7 Semantic object model and relation templates as compiled schema

Elevate relationship/object templates to a typed, compile-validated schema layer:

1. template registry with versioned contracts,
2. static validation at compile stage,
3. generated relation builders and tests from templates.

Result: fewer bespoke relation implementations and better evolution safety.

### 4.8 Reproducible execution package

Define a replayable "execution package" keyed by fingerprint:

- semantic manifest hash
- execution policy artifact hash
- runtime capability snapshot hash
- plan bundle fingerprints
- session configuration hash

This becomes the primitive for:

1. deterministic replay,
2. perf regression bisecting,
3. policy-impact attribution.

---

## 5. Concrete Roadmap Revision

### 5.1 Phase 1 (Immediate): Correctness and integration closure

1. Remove remaining `CompileContext(runtime_profile=...)` fallbacks outside boundary.
2. Eliminate runtime `dataset_bindings_for_profile()` consumers by threading `SemanticExecutionContext`.
3. Wire outcome-based maintenance in write path.
4. Add invariant tests:
   - single compile per run,
   - resolver identity reuse,
   - zero fallback compile-context callsites in target modules.

### 5.2 Phase 2: Policy compilation and evidence model

1. Introduce `CompiledExecutionPolicy` artifact.
2. Replace cache naming heuristics with compiled policy derivation.
3. Add confidence/rationale fields to inference decisions and artifacts.
4. Enforce policy artifact consumption in runtime modules.

### 5.3 Phase 3: Adaptive optimization with bounded feedback

1. Build artifact calibrator for scan/maintenance/cache thresholds.
2. Add rollout controls (off/warn/enforce) by policy domain.
3. Require measurable payoff gates (latency, bytes scanned, maintenance cost).

### 5.4 Phase 4: Advanced acceleration track

1. Introduce query-shape profiling and workload classes.
2. Add optional external-index file pruning provider for semantic-heavy workloads.
3. Expand DataFusion config/profile strategy for workload-specific sessions.

---

## 6. Metrics That Better Reflect "Entirely Programmatic"

Track these as hard migration KPIs:

1. `compile_context_fallback_callsites_in_src` (target: 0 outside compile boundary).
2. `dataset_bindings_profile_fallback_callsites_in_src` (target: 0 outside compile boundary).
3. `runtime_policy_derivation_points` (target: 0 unmanaged heuristics).
4. `inference_decisions_with_confidence` (target: 100%).
5. `writes_using_outcome_based_maintenance` (target: 100% for Delta writes).
6. `artifact_replay_success_rate` (target: >99% deterministic replay parity).
7. `pruning_effectiveness_score` by workload class (target: monotonic improvement).

---

## 7. Immediate Implementation Notes (Low-Risk, High-Leverage)

1. Start with `src/hamilton_pipeline/modules/task_execution.py` fallbacks (`_ensure_scan_overrides`, `_execute_view`) because they are central bridge points and already accept `execution_context`.
2. Update write pipeline integration in `src/datafusion_engine/io/write.py` to use outcome-based maintenance resolver and emit decision artifacts.
3. Replace `_default_semantic_cache_policy` in `src/semantics/pipeline.py` with manifest/IR-driven policy selection.
4. Keep deprecated extract registry APIs in compatibility mode but remove remaining production initialization reliance.

---

## 8. Final Assessment

The architectural direction remains correct, but the continuation plan should be reframed around **integration closure and control-loop maturity**, not broad new foundations.

The repo is now at the stage where best-in-class trajectory depends on:

1. making compile artifacts the sole source of runtime truth,
2. making policy decisions evidence-backed and replayable,
3. making optimization behavior measurable and self-improving under strict invariants.
