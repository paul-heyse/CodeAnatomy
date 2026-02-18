# Design Review: rust/codeanatomy_engine/

**Date:** 2026-02-17
**Scope:** `rust/codeanatomy_engine/`
**Focus:** Boundaries (1-6), Correctness (16-18), Quality (23-24)
**Depth:** moderate
**Files reviewed:** 20 source files + Cargo.toml

---

## Executive Summary

The `codeanatomy_engine` crate is the most disciplined module in the CodeAnatomy codebase relative to DataFusion usage patterns. `SessionStateBuilder`-first session construction, `with_analyzer_rules`/`with_optimizer_rules`/`with_physical_optimizer_rules` registration, and `Optimizer::with_rules` for offline lab experiments all follow DataFusion's canonical builder API precisely. The planning-object audit finds no parallel state management and no manual plan-node assembly; everything flows through `DataFrame`, `SessionContext.table()`, and registered views.

The three actionable findings are: (1) `rules/physical.rs` references `CoalescePartitionsExec` in a way that will break under DF52 which removed `CoalesceBatchesExec` and integrated coalescing into operators — the module's `apply_post_filter_coalescing` is structurally equivalent to the removed operator and must be audited for removal; (2) `compiler/optimizer_pipeline.rs`'s `run_optimizer_pipeline` captures an analyzer pass as a no-op clone rather than invoking the actual analyzer chain, producing misleading compliance traces; (3) `scan_config.rs`'s `ProviderCapabilities` struct uses boolean flags as a coarse wrapper alongside the richer `FilterPushdownStatus` model in `pushdown_contract.rs`, creating a semantic duplication that should be consolidated.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | — | low | Internals are well-guarded; `pub(crate)` on `CpgRuleSet` fields is correct |
| 2 | Separation of concerns | 3 | — | low | Policy (rulepack) cleanly separated from session construction and execution |
| 3 | SRP | 2 | medium | low | `executor/pipeline.rs` orchestrates compile, metrics, maintenance, and warning assembly in one 330-LOC function |
| 4 | High cohesion, low coupling | 3 | — | low | Module boundaries map cleanly to DF pipeline stages |
| 5 | Dependency direction | 3 | — | low | Spec drives session; session does not know about spec internals |
| 6 | Ports & Adapters | 2 | small | low | `ProviderCapabilities` booleans duplicate the `FilterPushdownStatus` port model |
| 16 | Functional core, imperative shell | 2 | medium | medium | `compiler/optimizer_pipeline.rs` analyzer trace is a no-op clone, not a real pass; optimizer trace captures string length instead of plan diff |
| 17 | Idempotency | 3 | — | low | `register_or_replace_table` semantics and idempotent Delta writes maintained |
| 18 | Determinism / reproducibility | 2 | medium | high | DF52 removes `CoalesceBatchesExec`; `apply_post_filter_coalescing` in `rules/physical.rs` is equivalent and will drift |
| 23 | Design for testability | 2 | small | low | `validate_plan` on `SemanticPlanCompiler` uses `format!("{:?}", ...)` on plan objects — not assertable in tests |
| 24 | Observability | 3 | — | low | Structured tracing gates, BLAKE3 step digests, and warning taxonomy are well-designed |

---

## Detailed Findings

### Category: Boundaries

#### P3. Single Responsibility — Alignment: 2/3

**Current state:**

`executor/pipeline.rs:execute_pipeline` (lines 106–328) is a 220-body-line `async fn` that performs six sequential responsibilities: plan compilation (step 1), plan-bundle capture with compliance trace (step 2), pushdown contract enforcement (within step 2), Delta materialization (step 3), physical metrics aggregation (step 4), Delta maintenance (step 5), and `RunResult` assembly (step 6). All six concerns are interleaved in one function body, with a shared `warnings: Vec<RunWarning>` accumulator and a shared `builder: RunResult::builder()` that is mutated across all phases.

The module-level comment explicitly lists what the pipeline does and does not do, which is evidence of awareness that the function is doing multiple things.

**Findings:**

- `rust/codeanatomy_engine/src/executor/pipeline.rs:106-328` — `execute_pipeline` has six sequential responsibilities connected only by a shared `warnings` vector and builder, with a non-trivial internal helper `enforce_pushdown_contracts` and `build_task_graph_and_costs` already extracted (indicating SRP pressure is felt). The compliance-capture block (lines 168–244) is 76 lines of nested match arms that mixes optimization trace capture, pushdown probing, and artifact building in one arm.

**Suggested improvement:**

Extract a `PlanBundleCaptureOrchestrator` that accepts `(ctx, output_plans, spec, rulepack_fingerprint, provider_identities, planning_surface_hash)` and returns `(Vec<PlanBundleArtifact>, Vec<RunWarning>)`. The `execute_pipeline` function would then delegate step 2 entirely to this struct. This would shrink `execute_pipeline` to four sequential calls and make the compliance trace path independently testable.

**Effort:** medium
**Risk if unaddressed:** low — the current structure works, but the function will accumulate more features and become increasingly difficult to test in isolation.

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**

`providers/scan_config.rs` defines `ProviderCapabilities` (line 119) and `PushdownStatus` (line 148) as a coarse two-value enum (`Supported`/`Unsupported`). Meanwhile, `providers/pushdown_contract.rs` defines the three-value `FilterPushdownStatus` (`Unsupported`/`Inexact`/`Exact`) and a full probe/report model. Both modules model the same domain — provider filter capability — but at different fidelity levels.

The `ProviderCapabilities.predicate_pushdown: bool` at `scan_config.rs:120` loses exactness information that `FilterPushdownStatus` preserves. The `pushdown_status_from_capabilities` function at line 178 translates from boolean to `PushdownStatus::Supported`/`Unsupported`, discarding the distinction between `Inexact` and `Exact` that the contract model relies on for correctness auditing.

**Findings:**

- `rust/codeanatomy_engine/src/providers/scan_config.rs:119-155` — `ProviderCapabilities` (bool flags) and `PushdownStatus` (2-value enum) duplicate the semantic space already covered by `FilterPushdownStatus` (3-value enum) in `pushdown_contract.rs`.
- `rust/codeanatomy_engine/src/providers/scan_config.rs:178-186` — `pushdown_status_from_capabilities` loses the `Inexact` vs `Exact` distinction when converting from capabilities.

**Suggested improvement:**

Replace `ProviderCapabilities.predicate_pushdown: bool` with `predicate_pushdown: Option<FilterPushdownStatus>` (from `pushdown_contract`), making `scan_config.rs` a thin adapter that maps `DeltaScanConfig` fields directly to the canonical three-level model. Remove `PushdownStatus` from `scan_config.rs` entirely; use `FilterPushdownStatus` as the shared vocabulary across both files. `pushdown_status_from_capabilities` can then be removed — callers query `probe_pushdown` directly.

**Effort:** small
**Risk if unaddressed:** low — the coarse model only affects `infer_capabilities` / introspection paths, not the actual pushdown enforcement which already uses the richer model.

---

### Category: Correctness

#### P16. Functional Core, Imperative Shell — Alignment: 2/3

**Current state:**

`compiler/optimizer_pipeline.rs:run_optimizer_pipeline` (lines 83–147) captures an "analyzer pass trace" by comparing `unoptimized_plan` to itself (line 92: `let analyzed = unoptimized_plan.clone()`). The trace recorded at lines 94–103 is therefore always a no-op (before and after are identical), and the rule name is hardcoded as `"analyzer_noop"`. This produces compliance artifacts that misrepresent the actual analyzer pass behavior — the real `state.analyze()` (or equivalent) is never called for tracing purposes; only `state.optimize()` is called at line 106.

Additionally, `optimize_with_rules` (lines 205–242) captures `plan_diff` as `Some(format!("after_len={}", after_text.len()))` — a string length, not an actual textual diff. This is noted in the function body but is architecturally a correctness gap for compliance consumers who expect a diff for change auditing.

**Findings:**

- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs:92` — `let analyzed = unoptimized_plan.clone()` means the analyzer trace always records a no-op, never the actual analysis pass.
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs:108-117` — Trace at "logical:pass0" treats `state.optimize(&analyzed)` as a single bulk step, losing per-rule granularity for the production optimizer pass (only the lab path gets per-rule traces via `optimize_with_rules`).
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs:232-236` — `plan_diff: Some(format!("after_len={}", after_text.len()))` is not a diff; it is a length scalar. Compliance trace consumers cannot reconstruct what changed.

**Suggested improvement:**

For the analyzer trace: call `ctx.state().analyze(unoptimized_plan.clone())?` before the optimizer pass and use its output as `analyzed`. This makes the before/after digest meaningful and produces a real record of analyzer transformations. For the plan diff: replace the `after_len` stub with a Myers diff of `before_text` vs `after_text` (the `similar` or `dissimilar` crates are appropriate here), or at minimum record both `before_text` and `after_text` truncated to a bounded length. This changes only the compliance capture path and has no execution impact.

**Effort:** medium
**Risk if unaddressed:** medium — compliance artifacts produced by the lab and the production optimizer trace capture give a misleading picture of what the analyzer pass does, which is the primary consumer of `run_optimizer_compile_only`.

---

#### P18. Determinism / Reproducibility — Alignment: 2/3

**Current state:**

The crate pins `datafusion = "51.0.0"` (Cargo.toml line 17) and `datafusion-substrait = "51.0.0"` (line 50), and `datafusion-proto = "51.0.0"` (line 54). DataFusion 52 (released January 2026) contains two breaking changes that directly affect this crate:

1. **`CoalesceBatchesExec` removed (DF52 section H).** `rules/physical.rs:apply_post_filter_coalescing` (lines 133–140) conditionally wraps plans in `CoalescePartitionsExec`. While the import at line 9 (`CoalescePartitionsExec`) is correct, the intent of the function — "reduce small batch overhead after filters" — maps exactly to the behavior `CoalesceBatchesExec` provided. Under DF52, DataFusion integrates coalescing into operators, making this rule redundant or potentially harmful (double-coalescing). The function must be audited and likely removed when upgrading.

2. **Projection pushdown moved from `FileScanConfig` to `FileSource` (DF52 section D.3).** `providers/scan_config.rs` uses `DeltaScanConfigBuilder` which wraps `FileScanConfig`. The DF52 migration removes `FileSource::with_projection` and introduces `try_pushdown_projection`. The `standard_scan_config` builder path at `scan_config.rs:33-45` may break because it calls `builder.build(snapshot)?` — if `DeltaScanConfigBuilder` is not updated for DF52's `FileScanConfigBuilder::with_projection_indices` returning `Result<Self>`, this becomes a compile error.

3. **FFI provider constructors require `TaskContextProvider` (DF52 section K.1).** If any `providers/` module is exposed via PyCapsule FFI, the constructors must supply `TaskContextProvider`.

**Findings:**

- `rust/codeanatomy_engine/Cargo.toml:17,50,54` — Pinned to DF51; no migration plan exists in the module.
- `rust/codeanatomy_engine/src/rules/physical.rs:133-140` — `apply_post_filter_coalescing` is functionally equivalent to the removed `CoalesceBatchesExec`.
- `rust/codeanatomy_engine/src/providers/scan_config.rs:33-45` — `DeltaScanConfigBuilder::build(snapshot)` and surrounding scan-config construction will require review against DF52's `FileSource`-based projection API.
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs:212-213` — `Optimizer::with_rules` API signature is stable in DF52, but `OptimizerContext::new().with_max_passes(clamped_passes)` must be verified for any signature change in DF52's `M` section (PhysicalOptimizerRule API change).

**Suggested improvement:**

Create a tracking issue against DF52 upgrade with three concrete action items: (a) remove or replace `apply_post_filter_coalescing` in `rules/physical.rs` — test that operator-embedded coalescing achieves equivalent batch consolidation; (b) audit `standard_scan_config` in `scan_config.rs` against DF52's `FileSource::try_pushdown_projection` API and update `DeltaScanConfigBuilder` usage accordingly; (c) verify `datafusion-substrait` and `datafusion-proto` pinned versions have DF52-compatible releases (both were at 51.0.0 at time of authoring).

**Effort:** large
**Risk if unaddressed:** high — the DF52 `CoalesceBatchesExec` removal is a compile-time break; the `FileScanConfig` projection API change is also compile-time. Both will block the upgrade entirely.

---

### Category: Quality

#### P23. Design for Testability — Alignment: 2/3

**Current state:**

`compiler/plan_compiler.rs:validate_plan` (lines 441–459) is a `pub async fn` that extracts plan diagnostics by formatting plan objects with `format!("{:?}", ...)`:

```rust
let unoptimized_plan = format!("{:?}", df.logical_plan());
let physical_plan = format!("{:?}", physical_plan_obj);
let explain_verbose = format!("{:?}", explain_batches);
```

The `Debug` format of DataFusion plan objects produces an unstable, implementation-defined string. Tests asserting on `PlanValidation.unoptimized_plan` would be brittle across DataFusion minor versions. The correct approach for plan validation is `df.logical_plan().display_indent().to_string()` (for `LogicalPlan`) and `displayable(physical_plan.as_ref()).indent(false).to_string()` (for `ExecutionPlan`), which produce the same format as `EXPLAIN` and are stable across runs.

The `CpgRuleSet` struct exposes `pub(crate)` fields (`analyzer_rules`, `optimizer_rules`, `physical_rules`, `fingerprint`) that tests must access via the accessor methods; this is well-designed. Tests in `plan_compiler.rs` use `setup_test_context()` which creates a real `SessionContext` — appropriate and lightweight for Rust unit tests.

**Findings:**

- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:443` — `format!("{:?}", df.logical_plan())` uses debug format, which is unstable. Use `df.logical_plan().display_indent().to_string()` for reproducible output.
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:447` — `format!("{:?}", physical_plan_obj)` same issue. Use `datafusion::physical_plan::displayable(physical_plan_obj.as_ref()).indent(false).to_string()`.
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:451-452` — `explain_batches` collected from `explain(true, false)?` and formatted with `{:?}` will vary across DF versions. Use the `arrow::util::pretty::print_batches` path or extract the `plan_type`/`plan` column text directly from the Arrow batch.

**Suggested improvement:**

Replace all three `format!("{:?}", ...)` calls in `validate_plan` with their canonical display-format equivalents. The `PlanValidation` struct becomes stable across DF minor versions and the output is directly comparable to `EXPLAIN` SQL output, making golden-file tests feasible.

**Effort:** small
**Risk if unaddressed:** low in production, but tests asserting on `PlanValidation` fields will be fragile when DataFusion is upgraded and the `Debug` output format changes.

---

#### P24. Observability — Alignment: 3/3

The observability design is exemplary. BLAKE3 step digests on per-rule optimizer passes (`optimizer_pipeline.rs:RuleStep`) enable deterministic diff comparison across runs. The `WarningCode` taxonomy with `WarningStage` context threading through `execute_pipeline` provides structured, filterable warning telemetry. The tracing feature gate is correctly designed — the `#[cfg_attr(feature = "tracing", instrument(...))]` pattern ensures zero overhead when the feature is disabled. `session/envelope.rs` captures DataFusion version, crate version, target partitions, and batch size for full session provenance.

The one minor gap is that `plan_diff` in `OptimizerPassTrace` is a length stub (noted under P16), but the observation infrastructure itself is well-structured.

---

## Cross-Cutting Themes

### Theme 1: DF51 Lock-In Risk

All three `datafusion-*` dependencies are pinned at 51.0.0. The engine is architecturally ready for DF52 — the `SessionStateBuilder`-first construction, `Optimizer::with_rules` pattern, and explicit rule registration all align with DF52 expectations — but three specific code points will require migration work that is not currently tracked: `CoalesceBatchesExec` removal, `FileScanConfig` projection API change, and FFI constructor changes. The determinism contract (`are_inputs_deterministic`, plan digest comparison) relies on reproducible plan shapes that may change across DF versions; pinning is correct practice, but an upgrade path should be documented.

Affected principles: P18 (determinism), P6 (pushdown adapter), P23 (testability via plan format).

### Theme 2: Compliance Trace Fidelity Gap

The production optimizer trace path in `run_optimizer_pipeline` captures a bulk `state.optimize()` call rather than per-rule traces, while the offline lab path in `optimize_with_rules` (used by `run_optimizer_compile_only`) captures per-rule traces with BLAKE3 digests. This creates an asymmetry: compliance captures from production runs have coarser traces than offline lab runs. The analyzer no-op (theme root: `let analyzed = unoptimized_plan.clone()`) is a consequence of DataFusion's optimizer not exposing per-rule callback hooks in the standard `state.optimize()` path — the lab path works around this by constructing its own `Optimizer`. The resolution is to route the compliance path through `optimize_with_rules` rather than `state.optimize()`, which the `run_optimizer_compile_only` function already does correctly.

Affected principles: P16 (functional core), P23 (testability).

### Theme 3: `execute_pipeline` Gravity Well

`execute_pipeline` is accumulating responsibility as new features (pushdown probing, task scheduling, compliance capture, maintenance) are added. The function's `#[allow(clippy::too_many_arguments)]` is a signal that the parameter surface is already pushing against good design. Without an extraction of the plan-bundle capture phase (suggested under P3), this function will continue to grow as the surface for new pipeline features.

Affected principles: P3 (SRP), P16 (core/shell separation).

---

## Planning-Object Consolidation

This is the primary audit per the agent instructions. The assessment is strongly positive.

| Area | DF Built-in Used | Assessment |
|------|-----------------|------------|
| Session construction | `SessionStateBuilder` with `with_config`, `with_runtime_env`, `with_analyzer_rules`, `with_optimizer_rules`, `with_physical_optimizer_rules` | Canonical. No parallel state management. |
| Plan compilation | `DataFrame` API, `SessionContext.table()`, `df.into_view()`, `register_table()` | Canonical. No manual plan-node assembly. |
| Optimizer rules | `Optimizer::with_rules(rules)` + `OptimizerContext::new()` | Canonical. DF's optimizer orchestration is used directly. |
| Analyzer registration | `ruleset.analyzer_rules().to_vec()` passed to `SessionStateBuilder::with_analyzer_rules` | Canonical. |
| Physical rule registration | `ruleset.physical_rules().to_vec()` + instrumentation appended as last rule | Canonical. Ordering contract is explicitly documented. |
| Plan artifacts | `df.logical_plan()`, `df.create_physical_plan().await?`, `df.explain(true, false)?` | Canonical, but `format!("{:?}", ...)` on plan objects is the one divergence (see P23). |
| Rule orchestration (offline lab) | `Optimizer::with_rules(rules)` + `optimizer.optimize(plan, &context, callback)` | Canonical and idiomatic. |

**No bespoke code replaces DataFusion built-ins.** `SemanticPlanCompiler` builds views using the DataFrame composition API, not by manually constructing `LogicalPlan::Join` or `LogicalPlan::Projection` nodes. This is the correct approach and should serve as the reference pattern for the Python layer.

**DF52 LOC impact:** Removing `apply_post_filter_coalescing` in `rules/physical.rs` would eliminate approximately 20 lines. Updating the `scan_config.rs` projection API would be a mechanical substitution. No LOC reduction from plan-compiler changes — the architecture is already right.

---

## DF52 Migration Impact

| Module | DF52 Delta | Breaking? | Action Required |
|--------|-----------|-----------|-----------------|
| `rules/physical.rs:133-140` | `CoalesceBatchesExec` removed; coalescing now operator-embedded | Yes (behavior) | Audit `apply_post_filter_coalescing`; likely remove and verify operator-embedded coalescing achieves equivalent consolidation |
| `providers/scan_config.rs:33-45` | `FileScanConfig` projection API moved to `FileSource`; builder `with_projection_indices` returns `Result<Self>` | Likely (compile) | Update `DeltaScanConfigBuilder` usage; handle `Result` propagation |
| `compiler/optimizer_pipeline.rs:212-216` | `OptimizerContext::with_max_passes` API stable per DF52 docs; verify no signature change | No | Confirm in upgrade validation |
| `compiler/substrait.rs` | `datafusion-substrait = "51.0.0"` pinned | Compile-break on upgrade | Update pin when DF52-compatible substrait release is available |
| `compiler/plan_codec.rs` | `datafusion-proto = "51.0.0"` pinned | Compile-break on upgrade | Same as substrait |
| `session/factory.rs:127` | `config_opts.execution.coalesce_batches = true` config key still valid in DF52 | No | No action needed |
| `providers/pushdown_contract.rs` | `TableProviderFilterPushDown` enum unchanged in DF52 | No | No action needed |
| `providers/snapshot.rs` | `TableProvider` `DELETE`/`UPDATE` hooks added in DF52 | No (additive) | Optional: implement new hooks if mutation semantics needed |
| `session/factory.rs` | FFI `TaskContextProvider` requirement added (K.1) | Only if FFI-exposed | Verify no PyCapsule surface for `providers/` |

---

## Rust Migration Candidates

These Python-layer concerns should migrate to or be canonically represented in this Rust crate, which already has the better architecture:

| Python Location | Description | Rust Target | Priority |
|----------------|-------------|-------------|----------|
| `src/datafusion_engine/session/` — session construction with ad-hoc config dicts | Session construction should mirror `SessionFactory` + `SessionStateBuilder` pattern | `session/factory.rs` is the reference; Python layer should serialize a `RuntimeProfileSpec` rather than constructing session config in Python | High |
| `src/datafusion_engine/plan/` — per-step optimizer plan capture | `compiler/optimizer_pipeline.rs` already has the per-rule trace model; Python should consume the `OptimizerCompileReport` JSON rather than reimplementing trace capture | `compiler/plan_codec.rs` FFI surface | Medium |
| `src/relspec/rustworkx_graph.py` — task graph construction | `compiler/scheduling.rs` implements `TaskGraph::from_inferred_deps` in Rust; Python should call this via the FFI surface rather than maintaining a parallel rustworkx graph | `compiler/scheduling.rs` | Medium |

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P23 (Testability) | Replace `format!("{:?}", df.logical_plan())` with `display_indent()` in `validate_plan` | small | Makes `PlanValidation` output stable across DF versions; enables golden-file tests |
| 2 | P6 (Ports & Adapters) | Replace `ProviderCapabilities` bool flags with `Option<FilterPushdownStatus>` | small | Eliminates dual pushdown vocabulary; single source of truth for filter capability |
| 3 | P18 (Determinism) | Document DF52 migration plan with concrete action items for `CoalesceBatchesExec` and `FileSource` projection API | small (doc) | Prevents surprise compile-break when upgrading |
| 4 | P16 (Functional Core) | Route `run_optimizer_pipeline` analyzer trace through `state.analyze()` rather than `unoptimized_plan.clone()` | medium | Makes compliance artifacts truthful about analyzer pass transformations |
| 5 | P3 (SRP) | Extract `PlanBundleCaptureOrchestrator` from `execute_pipeline` lines 148–244 | medium | Reduces function to six sequential delegation calls; makes compliance path independently testable |

---

## Recommended Action Sequence

1. **P23 / P18 — `validate_plan` display format** (Quick Win 1, independent): Replace `{:?}` formatting in `compiler/plan_compiler.rs:443,447,451`. Zero risk, immediate testability improvement. Do this before any DF52 upgrade to establish stable baselines.

2. **P6 — pushdown vocabulary consolidation** (Quick Win 2, independent): Migrate `ProviderCapabilities` in `providers/scan_config.rs:119` to use `FilterPushdownStatus` from `pushdown_contract.rs`. Update `infer_capabilities` accordingly. Affects only the `scan_config.rs` → `pushdown_contract.rs` internal path.

3. **P18 — DF52 migration tracking** (Quick Win 3): File a migration issue documenting `rules/physical.rs:133-140` (`apply_post_filter_coalescing` removal), `providers/scan_config.rs:33-45` (`FileSource` projection API), and `compiler/substrait.rs` / `compiler/plan_codec.rs` version pin updates. This unblocks the upgrade.

4. **P16 — analyzer trace truthfulness**: In `compiler/optimizer_pipeline.rs:92`, replace `let analyzed = unoptimized_plan.clone()` with `let analyzed = ctx.state().analyze(unoptimized_plan)?`. Update `trace_logical_pass` call at lines 94–103 to use the real analyzed plan as `after`. This requires changing the function signature from `run_optimizer_pipeline` to accept a `ctx` (already received at line 88).

5. **P3 — `execute_pipeline` extraction**: Extract the compliance capture block (lines 148–244 of `executor/pipeline.rs`) into a `PlanBundleCaptureOrchestrator` that encapsulates compliance capture, optimizer trace, and pushdown report assembly. `execute_pipeline` delegates to it as a single `await` call. This is the final structural improvement and should follow the functional corrections above.
