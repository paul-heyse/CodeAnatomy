# DataFusion Planning & Optimization: Best-in-Class Implementation Plan

**Date:** 2026-02-06
**Scope:** Review of DataFusion planning capabilities vs CodeAnatomy implementation, gap analysis, and improvement roadmap

---

## Executive Summary

CodeAnatomy has a strong DataFusion planning foundation: plan bundles capture three plan layers (logical, optimized logical, physical), lineage extraction drives inference-based scheduling, plan fingerprints enable caching and determinism validation, and Delta Lake integration operates through a mature Rust control plane. However, comparing the implementation against the full DataFusion planning surface reveals significant untapped optimization capabilities. The codebase treats DataFusion planning primarily as a **lineage extraction source** rather than as a **full planning compiler pipeline** whose configuration, statistics, optimizer behavior, and physical properties are all first-class planning artifacts.

This plan identifies 14 gap areas grouped into 5 workstreams, ordered by impact and dependency.

---

## Current State: What We Do Well

| Capability | Location | Status |
|---|---|---|
| Plan Bundle (P0/P1/P2 + Substrait) | `datafusion_engine/plan/bundle.py` | Solid |
| Lineage extraction from optimized logical plan | `datafusion_engine/lineage/datafusion.py` | Solid |
| Dependency inference from plan lineage | `relspec/inferred_deps.py` | Solid |
| Plan fingerprinting (hash-based cache keys) | `semantics/plans/fingerprints.py` | Solid |
| Plan artifact persistence to Delta | `datafusion_engine/plan/artifact_store.py` | Solid |
| Plan determinism validation | `artifact_store.py::validate_plan_determinism()` | Solid |
| Delta table provider via Rust control plane | `datafusion_engine/delta/control_plane.py` + Rust | Solid |
| UDF system (Rust native + Python fallback) | `datafusion_engine/udf/` + `rust/datafusion_ext/` | Solid |
| Schema contracts and validation | `datafusion_engine/schema/contracts.py` | Solid |
| Custom physical optimizer rules (Rust) | `rust/datafusion_ext/src/physical_rules.rs` | Foundation exists |
| Dynamic filter pushdown config | `session/runtime.py::enable_dynamic_filter_pushdown` | Configured |
| Statistics collection config | `tables/registration.py` / `session/runtime.py` | Configured |

---

## Gap Analysis: 14 Improvement Areas

### Workstream 1: Plan Environment Determinism (Highest Priority)

**Rationale:** The reference docs establish that a DataFusion plan is only reproducible when the *full session environment* (config, functions, catalogs) is captured alongside the plan. Currently, plan fingerprints hash the plan text but not the session settings that produced it.

#### Gap 1: Session Configuration Not Captured as Plan Artifact

**Current state:** `PlanArtifacts` contains `ddl_fingerprint` and `policy_hash` (which captures some runtime profile settings) but does NOT capture the effective DataFusion `df_settings` that the optimizer actually used.

**Impact:** Two sessions with different `target_partitions`, `repartition_joins`, `parquet.pushdown_filters`, or `sql_parser.enable_ident_normalization` settings will produce different plans from identical SQL, but our fingerprint cannot distinguish them. Plan regression attribution is impossible without knowing what changed: the query or the config.

**Target state:** Every `DataFusionPlanBundle` includes a `session_settings_hash` computed from the full `information_schema.df_settings` snapshot, plus the raw settings are persisted alongside plan artifacts for diffing.

**Implementation:**
1. Add `capture_session_settings(ctx: SessionContext) -> dict[str, str]` that executes `SELECT * FROM information_schema.df_settings` (or `SHOW ALL` fallback) and returns the key-value mapping.
2. Compute `session_settings_hash = hash_sha256_hex(sorted(settings.items()))`.
3. Add `session_settings_hash: str` and `session_settings: Mapping[str, str] | None` fields to `PlanBundleOptions` / `DataFusionPlanBundle`.
4. Include `session_settings_hash` in the composite `plan_fingerprint` computation (not just logical plan text).
5. Store the raw settings snapshot in `PlanArtifactRow` for regression attribution.

**Files to modify:**
- `src/datafusion_engine/plan/bundle.py` - Add settings capture to `build_plan_bundle()`
- `src/datafusion_engine/schema/introspection.py` - Add `capture_df_settings()` utility
- `src/serde_artifacts.py` - Add optional `session_settings_hash` field to `PlanArtifacts`
- `src/datafusion_engine/plan/artifact_store.py` - Persist settings with artifacts

#### Gap 2: Planning-Relevant Config Knobs Not Systematically Fingerprinted

**Current state:** `DataFusionRuntimeProfile` captures many settings but the fingerprint path (`policy_hash`) is a coarse hash of the profile struct. Individual optimizer knobs that affect plan shape are not tracked at the plan level.

**Impact:** Changing `datafusion.optimizer.max_passes`, `datafusion.optimizer.skip_failed_rules`, `datafusion.execution.parquet.pushdown_filters`, or join thresholds changes plan shape without any signal in our artifacts.

**Target state:** Define a `PlanEnvironmentFingerprint` that hashes the specific subset of `df_settings` known to affect plan shape (parser settings, optimizer settings, execution settings, Parquet settings). This fingerprint is a component of the plan identity.

**Implementation:**
1. Define `PLAN_SHAPE_SETTINGS: frozenset[str]` listing all `datafusion.*` keys that affect plan shape (approximately 40 keys spanning parser, optimizer, and execution namespaces).
2. Compute `plan_environment_hash` from only those keys.
3. Use this hash as a component in `plan_identity_hash` computation.

**Files to modify:**
- `src/datafusion_engine/session/runtime.py` - Define `PLAN_SHAPE_SETTINGS` constant
- `src/datafusion_engine/plan/bundle.py` - Use `plan_environment_hash` in identity computation

---

### Workstream 2: Optimizer Observability (High Priority)

**Rationale:** The reference docs treat `EXPLAIN VERBOSE` as the authoritative optimizer proof surface. DataFusion's optimizer is a multi-pass rewrite pipeline, and understanding *which rules fired* and *what they changed* is essential for debugging plan regressions and verifying optimization effectiveness.

#### Gap 3: EXPLAIN Capture Is Disabled By Default

**Current state:** `capture_explain()` in `plan/profiler.py` is gated by `CODEANATOMY_DISABLE_DF_EXPLAIN=1` (default "1" = disabled). This means no plan explain output is ever captured in normal operation.

**Impact:** The `PlanDetailInputs` class has fields for `explain_tree`, `explain_verbose`, and `explain_analyze`, but they are always `None` in practice. The entire explain-based diagnostic pathway is dead code.

**Target state:** Enable explain capture by default for plan bundles used in scheduling. Capture at minimum `EXPLAIN FORMAT indent` (diff-friendly) and, when requested, `EXPLAIN VERBOSE` (rule provenance). Gate `EXPLAIN ANALYZE` on explicit opt-in since it triggers execution.

**Implementation:**
1. Remove the `CODEANATOMY_DISABLE_DF_EXPLAIN` environment gate or change the default to "0" (enabled).
2. Add `capture_explain_indent: bool = True` and `capture_explain_verbose: bool = False` to `PlanBundleOptions`.
3. In `build_plan_bundle()`, capture the explain text and store it in the plan details.
4. Add `explain_indent_text: str | None` to `PlanArtifacts` for persistence.

**Files to modify:**
- `src/datafusion_engine/plan/profiler.py` - Change default; add non-executing explain variants
- `src/datafusion_engine/plan/bundle.py` - Wire explain capture into bundle building

#### Gap 4: No Rule-by-Rule Optimizer Tracing

**Current state:** No code captures or parses `EXPLAIN VERBOSE` output to extract per-rule plan deltas.

**Impact:** When a plan regression occurs (e.g., pushdown stops working), there is no way to identify which optimizer rule changed behavior. Debugging requires manual SQL experimentation.

**Target state:** Provide an `OptimizerTrace` artifact that, when enabled, captures the rule-by-rule plan evolution from `EXPLAIN VERBOSE` and stores it as structured data.

**Implementation:**
1. Add `OptimizerTrace` dataclass with `stages: tuple[OptimizerStage, ...]` where each stage has `rule_name: str`, `plan_text: str`, `phase: str` (logical/physical).
2. Add `parse_explain_verbose(text: str) -> OptimizerTrace` that parses the `plan_type` rows from verbose explain output.
3. Add `capture_optimizer_trace: bool = False` to `PlanBundleOptions`.
4. Store traces alongside plan artifacts for regression analysis.

**Files to modify:**
- `src/datafusion_engine/plan/profiler.py` - Add `OptimizerTrace`, `OptimizerStage`, `parse_explain_verbose()`
- `src/datafusion_engine/plan/bundle.py` - Wire trace capture
- `src/datafusion_engine/plan/diagnostics.py` - Add trace to diagnostics payload

---

### Workstream 3: Statistics-Aware Planning (High Priority)

**Rationale:** DataFusion's optimizer uses table/column statistics for selectivity estimation, cost-based join planning, and scan pruning decisions. Without statistics, the optimizer "flies blind" and makes conservative choices.

#### Gap 5: No Verification That Statistics Exist Before Planning

**Current state:** `collect_statistics` is set to `"true"` in some session configurations, but there is no verification that statistics were actually collected, no observation of their quality, and no diagnostic when the optimizer lacks statistics.

**Impact:** Cost-based optimizations (join ordering, hash join single-partition thresholds, perfect hash join) silently degrade when statistics are absent. File-level and row-group pruning may also be suboptimal.

**Target state:** After plan bundle construction, probe the physical plan for statistics availability and emit a diagnostic when scans have `Statistics: Unknown`. Include a `stats_quality` signal in the plan bundle.

**Implementation:**
1. Add `PlanStatsQuality` enum: `EXACT`, `ESTIMATED`, `UNKNOWN`, `MIXED`.
2. After building the execution plan, enable `datafusion.explain.show_statistics=true` and capture the explain output to detect `Statistics: Unknown` markers.
3. Add `stats_quality: PlanStatsQuality` to `DataFusionPlanBundle`.
4. Log a warning when `stats_quality == UNKNOWN` for plans with joins (cost-based join planning is degraded).

**Files to modify:**
- `src/datafusion_engine/plan/bundle.py` - Add stats quality assessment
- `src/datafusion_engine/plan/profiler.py` - Add stats parsing from explain output

#### Gap 6: Parquet Pruning Configuration Not Systematically Managed

**Current state:** `parquet.pushdown_filters` and `parquet.pruning` appear in some session configurations, but `enable_page_index`, `bloom_filter_on_read`, and `metadata_size_hint` are not explicitly configured.

**Impact:** Missing page-level and bloom filter pruning means more I/O for selective queries. The metadata_size_hint optimization for remote object stores is not leveraged.

**Target state:** The `DataFusionRuntimeProfile` should expose a `ParquetPruningPolicy` that explicitly configures all pruning knobs, and the plan environment fingerprint should include them.

**Implementation:**
1. Add `ParquetPruningPolicy` to `session/runtime.py` with fields for `pruning`, `enable_page_index`, `bloom_filter_on_read`, `pushdown_filters`, `metadata_size_hint`, `skip_metadata`.
2. Apply these settings in session construction.
3. Include in plan environment fingerprint.

**Files to modify:**
- `src/datafusion_engine/session/runtime.py` - Add `ParquetPruningPolicy` and apply in session construction

---

### Workstream 4: Physical Plan Intelligence (Medium Priority)

**Rationale:** The current implementation treats the physical plan as an opaque blob captured for persistence. The reference docs show that the physical plan contains actionable intelligence about partitioning, distribution requirements, ordering enforcement, and scan work units that should drive scheduling and resource allocation.

#### Gap 7: No Physical Plan Property Extraction

**Current state:** The `execution_plan` is captured in plan bundles via `df.execution_plan()`, and its `display_indent()` text is available, but no structured extraction of physical plan properties (partitioning, ordering, distribution requirements) occurs.

**Impact:** Cannot detect unnecessary repartitions/sorts, cannot verify ordering correctness for window functions, and cannot use physical plan topology for resource budgeting.

**Target state:** Extract structured physical plan properties including `partition_count`, `partitioning_scheme` (Hash/RoundRobin/Unknown), `has_sort_exec`, `has_repartition_exec`, `scan_file_group_count` from the execution plan.

**Implementation:**
1. Add `PhysicalPlanProperties` dataclass with `partition_count: int`, `partitioning_kind: str`, `sort_exec_count: int`, `repartition_exec_count: int`, `coalesce_exec_count: int`, `scan_count: int`.
2. Implement `extract_physical_properties(plan) -> PhysicalPlanProperties` by traversing `children()` and inspecting `partition_count` and `display_indent()` text.
3. Add `physical_properties: PhysicalPlanProperties | None` to `DataFusionPlanBundle`.
4. Use `partition_count` as a scheduling input for resource allocation.

**Files to modify:**
- `src/datafusion_engine/plan/bundle.py` - Add physical properties extraction
- New: `src/datafusion_engine/plan/physical_analysis.py` - Physical plan analysis utilities

#### Gap 8: No Physical Plan Regression Detection

**Current state:** Plan determinism validation checks logical plan fingerprints but not physical plan topology.

**Impact:** A change in DataFusion version or session configuration can introduce unnecessary repartitions or remove beneficial sort avoidance without any signal.

**Target state:** Physical plan topology hash (operator type sequence + partition counts) is computed and stored alongside logical plan fingerprints. Changes in physical topology trigger diagnostic warnings.

**Implementation:**
1. Compute `physical_topology_hash` from the operator type sequence and partition counts in the physical plan tree.
2. Store alongside plan artifacts.
3. Compare across runs; divergence without logical plan change indicates a configuration or engine version change.

**Files to modify:**
- `src/datafusion_engine/plan/bundle.py` - Add physical topology hashing
- `src/datafusion_engine/plan/artifact_store.py` - Store and compare physical topology hashes

#### Gap 9: Scan Pushdown Verification Is Not Validated Against Residual Filters

**Current state:** `ScanLineage` captures `projected_columns` and `pushed_filters` from `TableScan` variant inspection, but there is no comparison against residual `Filter` nodes to verify pushdown completeness.

**Impact:** A predicate that *should* push down to the scan but remains as a residual `Filter` represents a silent performance regression. The current lineage extraction cannot distinguish "intentionally not pushed" from "pushdown failed."

**Target state:** The lineage report includes a `pushdown_analysis` section that compares scan-level pushed filters with residual filters above each scan, classifying each predicate as `pushed`, `residual`, or `partially_pushed`.

**Implementation:**
1. During lineage extraction, track `Filter` nodes and their predicates alongside `TableScan` nodes.
2. For each scan, identify Filter nodes in the ancestry path and compare their predicates with the scan's `pushed_filters`.
3. Add `PushdownAnalysis` to `LineageReport` with per-scan filter classification.

**Files to modify:**
- `src/datafusion_engine/lineage/datafusion.py` - Add pushdown analysis to lineage extraction

---

### Workstream 5: Advanced Planning Capabilities (Lower Priority, Higher Leverage)

**Rationale:** These capabilities represent strategic improvements that unlock new optimization opportunities but require more design work.

#### Gap 10: No Plan Template / Prepared Statement Support

**Current state:** All SQL is constructed ad-hoc. No use of `PREPARE/EXECUTE` or `param_values` for parameterized queries.

**Impact:** Plan stability is tied to literal values in the query text. Changing a filter threshold changes the plan fingerprint even when the plan shape is identical. No opportunity for plan template caching.

**Target state:** View builders that produce parameterized queries can use `PREPARE/EXECUTE` or `param_values` to separate plan shape from runtime values. Plan fingerprints hash the template, not the literal values.

**Implementation:**
1. Add `ParameterizedViewBuilder` protocol that returns `(sql_template: str, param_values: dict)` instead of a DataFrame directly.
2. In plan bundle construction, detect parameterized views and use `param_values` for substitution.
3. Hash the SQL template (with placeholder markers) for plan fingerprinting, not the substituted SQL.

**Files to modify:**
- `src/datafusion_engine/views/graph.py` - Add parameterized builder support
- `src/datafusion_engine/plan/bundle.py` - Handle parameterized plan construction

#### Gap 11: Cost-Based Join Planning Not Explicitly Configured

**Current state:** Join planning knobs (`prefer_hash_join`, `hash_join_single_partition_threshold`, `hash_join_single_partition_threshold_rows`, `hash_join_inlist_pushdown_max_size`) are at DataFusion defaults.

**Impact:** For the workloads CodeAnatomy runs (many small-to-medium tables with known cardinalities), default thresholds may cause suboptimal join strategy selection.

**Target state:** The `DataFusionRuntimeProfile` exposes a `JoinPlanningPolicy` with explicit thresholds, and these are part of the plan environment fingerprint.

**Implementation:**
1. Add `JoinPlanningPolicy` to session runtime with fields for all join-related optimizer config keys.
2. Apply as session settings.
3. Include in plan environment fingerprint.

**Files to modify:**
- `src/datafusion_engine/session/runtime.py` - Add `JoinPlanningPolicy`

#### Gap 12: No Catalog Namespace Pinning Strategy

**Current state:** Session creation sets some catalog defaults, but table references in SQL may use implicit default resolution without explicit qualification.

**Impact:** Unqualified table names can resolve differently across sessions if default catalog/schema settings drift. This breaks plan reproducibility.

**Target state:** Enforce a deterministic namespace policy: either always fully-qualify table references in generated SQL, or pin `default_catalog` and `default_schema` in every session and include them in the plan fingerprint.

**Implementation:**
1. Add `NamespacePolicy` with `default_catalog: str` and `default_schema: str`.
2. Ensure all sessions are constructed with `with_default_catalog_and_schema()`.
3. Include in plan environment fingerprint.
4. Optionally add a validation pass that checks for unqualified table references in generated SQL.

**Files to modify:**
- `src/datafusion_engine/session/runtime.py` - Enforce namespace pinning

#### Gap 13: Delta File-Level Pruning Not Observable in Plan Artifacts

**Current state:** Delta file pruning happens in the Rust control plane (`build_delta_provider()`) and the `FilePruningPolicy` in Python, but the pruning results (files skipped, files remaining) are not captured as plan artifacts.

**Impact:** Cannot verify that Delta's file-level pruning is actually effective for a given query. No way to detect pruning regressions without execution.

**Target state:** The `ScanUnit` includes pruning metadata: `candidate_file_count`, `pruned_file_count`, `pruning_predicates` applied. This information is already partially available via `DeltaAddActionPayload` and `EvidenceNode.scan_candidate_file_count`.

**Implementation:**
1. Enrich `ScanUnit` with `total_file_count: int | None`, `selected_file_count: int | None`, `pruning_predicates: tuple[str, ...] | None`.
2. Populate from `DeltaProviderBundle.add_actions` count and `DeltaScanOverrides` predicates.
3. Add pruning ratio to plan diagnostics (flag when ratio is < 0.5 = more than half of files scanned).

**Files to modify:**
- `src/datafusion_engine/lineage/scan.py` - Enrich `ScanUnit`
- `src/datafusion_engine/delta/control_plane.py` - Surface pruning metadata

#### Gap 14: Plan Bundle Production Bundle Not Complete Per Reference Specification

**Current state:** Plan bundle contains `(logical, optimized_logical, execution_plan, substrait, fingerprint, delta_pins, scan_units, required_udfs)`.

**Missing:** `EXPLAIN FORMAT indent` text (diff-friendly), `EXPLAIN VERBOSE` text (rule provenance), `df_settings` snapshot, physical topology hash, stats quality assessment, pushdown analysis.

**Target state:** A "production-grade plan bundle" matches the reference specification: `(P0, P1, P2) + EXPLAIN indent + EXPLAIN VERBOSE + df_settings + stats_quality + physical_topology_hash + pushdown_analysis`.

This is the meta-gap: implementing Gaps 1, 3, 4, 5, 7, 8, 9 collectively closes this gap.

---

## Implementation Priority and Dependencies

```
Phase 1: Plan Environment Determinism (Gaps 1, 2)
  No dependencies. Foundation for all regression analysis.

Phase 2: Optimizer Observability (Gaps 3, 4)
  Depends on Phase 1 (settings capture needed for context).
  Unblocks regression debugging.

Phase 3: Statistics-Aware Planning (Gaps 5, 6)
  Independent of Phase 2.
  Unblocks cost-based optimization improvements.

Phase 4: Physical Plan Intelligence (Gaps 7, 8, 9)
  Depends on Phase 2 (explain capture needed for stats parsing).
  Provides scheduling-relevant signals.

Phase 5: Advanced Capabilities (Gaps 10, 11, 12, 13)
  Independent; can be done in any order.
  Each gap is a standalone improvement.
```

### Suggested Implementation Order

| Order | Gap | Effort | Impact | Description |
|---|---|---|---|---|
| 1 | Gap 3 | Small | High | Enable explain capture (currently dead code) |
| 2 | Gap 1 | Medium | High | Capture session settings as plan artifact |
| 3 | Gap 2 | Small | Medium | Fingerprint plan-shape settings |
| 4 | Gap 5 | Medium | High | Verify statistics availability |
| 5 | Gap 6 | Small | Medium | Configure all Parquet pruning knobs |
| 6 | Gap 9 | Medium | High | Pushdown verification in lineage |
| 7 | Gap 7 | Medium | Medium | Physical plan property extraction |
| 8 | Gap 4 | Medium | Medium | Rule-by-rule optimizer tracing |
| 9 | Gap 8 | Small | Medium | Physical topology regression detection |
| 10 | Gap 13 | Medium | Medium | Delta pruning observability |
| 11 | Gap 12 | Small | Medium | Catalog namespace pinning |
| 12 | Gap 11 | Small | Low | Join planning policy |
| 13 | Gap 10 | Large | Medium | Parameterized query support |

---

## Architectural Principles

These improvements should follow the existing codebase conventions:

1. **Additive, not disruptive:** All new fields are optional with sensible defaults. Existing plan bundles continue to work.
2. **Inference-driven:** New capabilities produce signals that feed into the existing inference-driven scheduling pipeline.
3. **Fingerprint-inclusive:** Any new planning input that affects plan shape must be included in the composite fingerprint.
4. **Observable:** New capabilities must produce diagnostic artifacts that can be persisted to Delta and diffed across runs.
5. **Graceful degradation:** If explain capture fails, if statistics are unavailable, if physical plan traversal encounters unknown operators, the plan bundle is still valid with appropriate `None` / `UNKNOWN` sentinel values.
6. **msgspec contracts:** New cross-module contracts use `msgspec.Struct` per CQ model boundary policy.
7. **No custom optimizer rules (yet):** Phases 1-4 focus on *observing and configuring* DataFusion's built-in optimizer, not on writing custom rules. Custom rules (e.g., predicate canonicalization for deterministic scheduling keys) are a Phase 5+ consideration that should be addressed in the existing Rust `physical_rules.rs` / `planner_rules` infrastructure.

---

## Relationship to Existing Rust Extensions

The Rust extension (`rust/datafusion_ext/`) already contains:

- **`physical_rules.rs`**: Custom `PhysicalOptimizerRule` infrastructure with `CodeAnatomyPhysicalConfig`. Currently limited to coalesce partitions/batches control. This is the correct place to add custom physical optimizer rules if needed in Phase 5+.
- **`planner_rules` module**: Logical plan optimization infrastructure exists but scope needs investigation.
- **`function_rewrite` module**: Expression rewriting for UDFs.
- **`expr_planner` module**: Expression planning extensions.

These modules represent the correct Rust extension points for future custom optimizer work. The Python-side improvements in Phases 1-4 should consume the outputs of these extensions but not require modifications to them.

---

## Summary

The CodeAnatomy DataFusion planning implementation has a solid foundation but treats planning primarily as a lineage extraction mechanism. To achieve best-in-class planning, the implementation needs to:

1. **Treat the session environment as a first-class plan artifact** (Workstream 1)
2. **Make the optimizer pipeline observable** (Workstream 2)
3. **Verify that statistics drive cost-based decisions** (Workstream 3)
4. **Extract actionable intelligence from physical plans** (Workstream 4)
5. **Enable advanced planning patterns** (Workstream 5)

The improvements are additive, follow existing conventions, and can be implemented incrementally. Phase 1 (Gaps 1-3) provides the highest ROI and should be implemented first.
