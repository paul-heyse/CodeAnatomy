# DataFusion Planning Optimization v2: Design Review and Corrective Recommendations (2026-02-06)

## Scope Reviewed
- Proposal: `docs/plans/datafusion_planning_optimization_best_in_class_plan_v2.md`
- Current implementation surfaces:
  - `src/datafusion_engine/plan/profiler.py`
  - `src/datafusion_engine/plan/bundle.py`
  - `src/datafusion_engine/plan/diagnostics.py`
  - `src/datafusion_engine/lineage/datafusion.py`
  - `src/obs/diagnostics_report.py`
  - `src/datafusion_engine/session/runtime.py`
  - `src/datafusion_engine/delta/*`
- Reference capability docs:
  - `.codex/skills/datafusion-and-deltalake-stack/reference/datafusion_planning.md`
  - `.codex/skills/datafusion-and-deltalake-stack/reference/deltalake_datafusion_integration.md`
  - `.codex/skills/datafusion-and-deltalake-stack/reference/deltalake_datafusionmixins.md`
  - `.codex/skills/datafusion-and-deltalake-stack/reference/datafusion_deltalake_advanced_rust_integration.md`
  - `.codex/skills/datafusion-and-deltalake-stack/reference/datafusion_rust_UDFs.md`
  - `.codex/skills/datafusion-and-deltalake-stack/reference/datafusion_schema.md`

## Executive Conclusions
- The v2 proposal is directionally strong, but several items need correction to avoid fragile diagnostics work that does not improve query outcomes.
- Highest-value path is to shift from "record more plan text" to a closed-loop optimization contract:
  - Observe stable planner/runtime signals
  - Decide policy adjustments
  - Re-plan only when expected benefit is material
  - Validate with runtime counters (bytes/rows pruned, cache hits, memory)
- The strongest immediate opportunities are:
  - Replace dead EXPLAIN capture path
  - Add pushdown effectiveness classification that distinguishes expected residual filters vs missed pushdown
  - Promote adaptive parquet/scan knobs driven by measured selectivity
  - Keep physical-plan regression checks, but avoid text-only parsing as the primary mechanism

## Corrections to the v2 Proposal

### 1) Scope Item 3 (EXPLAIN capture) is valid, but SQL-only assumption is incomplete
- Correct: current `capture_explain()` is effectively disabled (`CODEANATOMY_DISABLE_DF_EXPLAIN` defaults to `"1"`).
- Gap: the proposal assumes every plan has an originating SQL string. Current bundle path primarily has `DataFrame`, not canonical SQL.
- Correction:
  - Keep DataFrame-first capture path.
  - Use SQL EXPLAIN when SQL text is known.
  - Otherwise use a temporary-view wrapper path (register dataframe as temp view and explain `SELECT * FROM <view>`), or retain a guarded DataFrame `.explain()` fallback until SQL explain parity is guaranteed.

### 2) Scope Item 5 (statistics quality) has a stale premise
- Proposal claims Python `ExecutionPlan` has no statistics method.
- Current implementation already probes `execution_plan.statistics()` in `bundle.py`.
- Correction:
  - Use `execution_plan.statistics()` as primary source when available.
  - Use EXPLAIN statistics parsing as fallback, not primary.
  - Add source labeling (`statistics_source = execution_plan|explain|none`) to avoid ambiguity.

### 3) Scope Item 7 (physical property extraction) should not rely only on `display_indent()` parsing
- Text parsing is brittle across DataFusion minor releases and display formatting changes.
- Correction:
  - Keep text parsing as a compatibility fallback.
  - Prefer proto-backed extraction (from existing `execution_plan_proto`) via a Rust helper that emits stable operator topology payload.

### 4) Scope Item 8 (topology hash) needs stronger invariants
- Counting operators is insufficient for regression detection; misses key behavior shifts.
- Correction:
  - Include operator kinds, exchange/distribution strategy, partition counts, and scan-node options that affect pruning/pushdown.
  - Keep topology hash separate from plan fingerprint and classify mismatch severity (`warn` vs `error`) by expected determinism tier.

### 5) Scope Item 9 (pushdown verification) needs false-positive controls
- Residual filters are not always failure; some are expected (`Inexact` pushdown, post-scan semantics).
- Correction:
  - Combine three signals:
    - Provider pushdown capability hints (already recorded in registration artifacts)
    - Logical scan pushed filters
    - Runtime pruning counters / scan metrics
  - Classify outcomes as `expected_residual`, `partial_pushdown`, `pushdown_missed`, not a single boolean.

### 6) Scope Item 13 threshold is too simplistic
- Fixed threshold (`candidate/total > 0.5`) can flag legitimate scans.
- Correction:
  - Use workload-aware thresholds by dataset class and predicate family.
  - Emit actionable diagnostics only when low pruning correlates with avoidable knobs (partition key mismatch, missing stats, disabled pushdown).

## Additional Optimization Opportunities Missing from v2

### A) Adaptive "pruning ladder" policy controller
- Goal: convert diagnostics into measurable runtime improvements.
- Implement per-query policy decisions for:
  - `datafusion.execution.parquet.pushdown_filters`
  - page index/bloom toggles
  - filter reorder toggles
- Decision inputs:
  - projected column width
  - predicate selectivity history
  - prior `pushdown_rows_pruned` vs `pushdown_rows_matched`
- This directly aligns with DataFusion pruning guidance and avoids one-size-fits-all defaults.

### B) Delta scan config adaptation loop
- Current stack already carries `DeltaScanConfig` knobs (`wrap_partition_values`, `enable_parquet_pushdown`, view-type forcing).
- Add policy feedback:
  - when partition-column-heavy workloads dominate, validate `wrap_partition_values` benefit;
  - when schema evolution friction appears, toggle `schema_force_view_types` intentionally with telemetry impact checks.

### C) Runtime metrics as optimizer feedback, not just reporting
- Runtime capabilities already expose structured execution metrics.
- Use them to drive policy changes:
  - metadata/list-files/statistics cache hit rates
  - memory pressure trends
  - scan/pruning efficiency
- Add "changed-policy reason" artifacts so tuning decisions are auditable and reproducible.

### D) Maintenance-aware planning checks
- Planning quality depends on Delta table maintenance state (optimize/vacuum/checkpoint health).
- Add pre-plan readiness signals:
  - stale snapshot/log conditions
  - retention hazards that can invalidate pinned versions
  - compaction debt hints for heavily fragmented datasets

### E) Optional external index lane (deferred)
- For high-selectivity workloads, external index + `Inexact` pushdown provider path can outperform baseline file skipping.
- Keep this as a gated R&D track after core adaptive policy loop is stable.

## Revised Implementation Sequence (Outcome-Oriented)

### Phase 1: Explain and stats foundation (correctness first)
1. Replace dead EXPLAIN gate in `plan/profiler.py`.
2. Implement dual-path explain capture (SQL-first, DataFrame-safe fallback).
3. Standardize statistics source precedence (`execution_plan.statistics()` then EXPLAIN fallback).

### Phase 2: Pushdown truth model
1. Add structured pushdown outcome classification in `lineage/datafusion.py`.
2. Join lineage signals with provider capability artifacts and runtime scan metrics.
3. Surface outcome classes in `plan_details` and diagnostics.

### Phase 3: Stable physical topology regression signal
1. Add proto-first physical analysis (Rust helper + Python adapter).
2. Keep text parsing fallback for compatibility.
3. Add topology hash with richer invariants and determinism-tier severity.

### Phase 4: Adaptive policy controller
1. Introduce decision rules for pushdown/page-index/filter-reorder toggles.
2. Emit decision artifacts (`policy_before`, `policy_after`, `reason`, `expected_gain`).
3. Re-plan only when expected gain threshold is met.

### Phase 5: Delta-specific optimization loop
1. Feed pruning/pushdown outcomes into `DeltaScanConfig` defaults by dataset/workload profile.
2. Add maintenance/readiness checks to avoid costly low-yield scans.
3. Validate no regression in strict-native provider path.

## Test Scope Updates (Must-Have)

### Unit
- `tests/unit/test_plan_bundle_artifacts.py`
  - explain dual-path capture coverage
  - stats source precedence and fallback behavior
  - topology hash stability for same proto payload
- `tests/unit/test_diagnostics_report.py`
  - pushdown outcome classes
  - adaptive-policy decision payload summary fields
- New/expanded:
  - `tests/unit/datafusion_engine/test_plan_profiler.py`
  - `tests/unit/datafusion_engine/test_physical_analysis.py`
  - `tests/unit/datafusion_engine/test_pushdown_outcomes.py`

### Integration
- `tests/integration/test_df_delta_smoke.py`
  - strict-native path still canonical
  - runtime metrics emitted and consumed by policy loop
- runtime/object-store suites:
  - verify no policy drift causes registration incompatibility
  - verify policy loop does not relax strict-native constraints

### Regression/Golden
- Update plan golden fixtures to include:
  - `statistics_source`
  - `pushdown_outcomes`
  - `physical_topology_hash`
  - adaptive-policy decision metadata

## Acceptance Criteria (Optimization-Focused)
- No functional regression in strict-native Delta provider paths.
- Deterministic plan artifacts remain stable for identical inputs/config.
- For selective queries, measured scan efficiency improves (bytes/files/rows pruned) versus pre-change baseline.
- Adaptive decisions are explainable and reproducible from persisted artifacts.
- No new warning noise from expected residual filter behavior.

## Risks and Mitigations
- Risk: overfitting policy heuristics to one workload.
  - Mitigation: start with conservative defaults and dataset/profile allowlist.
- Risk: proto shape drift across DataFusion updates.
  - Mitigation: version-tag topology extractor and keep display parser fallback.
- Risk: diagnostics overhead affecting latency.
  - Mitigation: sample heavy diagnostics and separate "always-on" vs "debug" capture modes.

## Recommended Disposition of v2 Scope Items
- Keep with corrections: 3, 4, 5, 7, 8, 9, 13.
- Add new execution-critical scope: adaptive policy controller (new P0/P1 deliverable).
- Defer: external index integration until core loop and regressions are stable.

