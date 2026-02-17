# Design Review: Rust Engine Execution, Session, and Rules

**Date:** 2026-02-17
**Scope:** `rust/codeanatomy_engine/src/{executor,session,rules,providers,tuner,compliance,compat,lib.rs}`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 47

## Executive Summary

The Rust engine execution layer is architecturally well-structured with strong determinism contracts (three-layer BLAKE3 hashing), a clean session construction pattern (SessionStateBuilder), and principled rules-as-policy composition. The prior improvement plan (S1-S14, D1-D5) resolved most critical issues. Remaining gaps cluster around two themes: (1) structural triplication forced by DataFusion's three distinct rule trait hierarchies (AnalyzerRule/OptimizerRule/PhysicalOptimizerRule), and (2) knowledge duplication between the spec layer and the providers/executor layers (dual `PushdownEnforcementMode` enums, duplicated `map_pushdown_mode` functions). The strongest areas are dependency direction (5), determinism (18), and ports-and-adapters boundary design (6). The weakest area remains DRY (7), though the prior plan's macro consolidation (S9) improved it from 0.8/3 to the current state.

## Prior Review Context

All items from the 2026-02-16 improvement plan are marked Complete:
- S1-S14 (structural improvements) -- Complete
- D1-D5 (detailed improvements) -- Complete

Findings below are either **NEW** issues discovered in this review or **RESIDUAL** issues that persisted despite prior fixes. Items that were fully resolved receive score 3 and are noted briefly.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `CpgRuleSet` exposes `pub(crate)` fields; `maintenance.rs` stub internals are public |
| 2 | Separation of concerns | 3 | -- | -- | Clean separation: orchestration/tracing/metrics/rules/session |
| 3 | SRP | 2 | medium | low | `rule_instrumentation.rs` (819 LOC) handles sentinel + wrapper + wiring for 3 traits |
| 4 | High cohesion, low coupling | 2 | small | low | `pipeline.rs` imports from 7 internal modules; `PushdownEnforcementMode` alias coupling |
| 5 | Dependency direction | 3 | -- | -- | Core spec depends on nothing; executor/session/rules depend on spec |
| 6 | Ports & Adapters | 3 | -- | -- | `PlanningSurfaceSpec` as port; `SessionStateBuilder` as adapter; `DeltaProvider` as tech adapter |
| 7 | DRY | 1 | medium | medium | Dual `PushdownEnforcementMode` enums; duplicated `map_pushdown_mode`; triplicated sentinel/wrapper/sort patterns |
| 8 | Design by contract | 3 | -- | -- | Explicit preconditions on `safe_vacuum_retention`, `validate_scan_config`, `PushdownContractReport` |
| 9 | Parse, don't validate | 2 | small | low | `is_correctness_rule` and `is_scan_operator` use string matching instead of typed classification |
| 10 | Make illegal states unrepresentable | 2 | small | low | `WriteOutcome` uses `Option<i64>` fields that are always `None` together; `maintenance.rs` stub types compile without real operations |
| 11 | CQS | 3 | -- | -- | `AdaptiveTuner` observe/propose/apply pattern; `capture` vs. `compute` naming |
| 12 | DI + explicit composition | 3 | -- | -- | `SessionFactory` accepts profiles/rulesets/configs; `RulepackFactory` composes from intents |
| 13 | Composition over inheritance | 3 | -- | -- | No inheritance; Instrumented* wrappers compose via delegation |
| 14 | Law of Demeter | 2 | small | low | `compatibility_facts` reaches into `snapshot.table_properties.get(...)` |
| 15 | Tell, don't ask | 2 | small | low | `read_write_outcome` probes `table.snapshot()?.snapshot().log_data()` chain |
| 16 | Functional core, imperative shell | 2 | small | low | `collect_plan_metrics` is pure; `execute_pipeline` mixes IO (registration, write) with orchestration logic |
| 17 | Idempotency | 3 | -- | -- | Delta writes are append-only with version tracking; `safe_vacuum_retention` enforces minimum |
| 18 | Determinism | 3 | -- | -- | Three-layer BLAKE3: spec_hash + envelope_hash + rulepack_fingerprint; sorted registration order |
| 19 | KISS | 2 | medium | low | 819-LOC `rule_instrumentation.rs` could be replaced by `instrument_rules_with_info_spans!` on DataFusion 52.x upgrade |
| 20 | YAGNI | 2 | small | low | `maintenance.rs` (434 LOC) is entirely stub; `AdaptiveTuner` features exist but effectiveness is unvalidated |
| 21 | Least astonishment | 3 | -- | -- | Consistent naming, clear builder patterns, well-documented warning codes |
| 22 | Declare and version contracts | 3 | -- | -- | `DeterminismContract`, `SessionEnvelope`, `PlanningSurfaceManifest` are explicit versioned contracts |
| 23 | Design for testability | 2 | small | low | `maintenance.rs` stubs prevent testing real operations; `is_correctness_rule` is untestable in isolation |
| 24 | Observability | 3 | -- | -- | Feature-gated tracing, `WarningCode` enum, `TraceMetricsSummary`, `ComplianceCapture` |

**Overall weighted score: 2.38/3** (57/72 raw points across 24 principles)

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules expose clean public surfaces. The `CpgRuleSet` in `registry.rs` uses `pub(crate)` for its three rule vectors, which is appropriate for crate-internal consumers but does allow direct field mutation from `overlay.rs:198-213` rather than through methods.

**Findings:**
- `rust/codeanatomy_engine/src/rules/registry.rs`: `CpgRuleSet` fields `analyzer_rules`, `optimizer_rules`, `physical_rules` are `pub(crate)`, allowing `overlay.rs:apply_priority_ordering` to directly sort them. A method like `sort_by_priority(&mut self, priorities: &[RulePriority])` would keep sorting logic within the owning type.
- `rust/codeanatomy_engine/src/executor/maintenance.rs:34-100`: `MaintenanceSchedule`, `CompactPolicy`, `VacuumPolicy`, `ConstraintSpec` are fully public structs with no invariant enforcement, despite being stubs. If these become real, public construction could produce invalid states.

**Suggested improvement:**
Add a `sort_by_priority` method to `CpgRuleSet` that encapsulates the sorting logic, removing the need for `overlay.rs` to access internal fields directly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 3/3

**Current state:**
The module structure cleanly separates concerns: `executor/` handles orchestration and materialization, `session/` handles session construction and identity capture, `rules/` handles policy composition, `providers/` handles Delta provider registration and pushdown contracts, `tuner/` handles adaptive parameter adjustment, `compliance/` handles audit capture. Each sub-module has a focused responsibility.

The `pipeline.rs` orchestrator follows a clear phase structure: compile -> plan bundle -> execute -> metrics -> maintenance -> assemble.

**No findings requiring action.**

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most modules have a single reason to change. The exception is `rule_instrumentation.rs` (819 LOC), which changes for three reasons: (a) AnalyzerRule trait evolution, (b) OptimizerRule trait evolution, (c) PhysicalOptimizerRule trait evolution. Each change to a DataFusion rule trait signature requires parallel updates to both the sentinel and instrumented wrapper for that trait.

**Findings:**
- `rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs:281-432`: Three sentinel structs (`AnalyzerPhaseSentinel`, `OptimizerPhaseSentinel`, `PhysicalOptimizerPhaseSentinel`) with near-identical `PLANNING_CONTEXT.with(|cell| {...})` bodies that differ only in `PlanningPhase` variant and trait method signature (`analyze` vs `rewrite` vs `optimize`).
- `rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs:474-714`: Three instrumented wrapper structs (`InstrumentedAnalyzerRule`, `InstrumentedOptimizerRule`, `InstrumentedPhysicalOptimizerRule`) with similar span/diff/record patterns.
- **dfdl_ref finding:** DataFusion 52.x `instrument_rules_with_info_spans!` macro (from `datafusion-tracing`) could replace all 819 LOC. The project is pinned to DataFusion 51.x, making this a planned migration rather than an immediate fix.

**Suggested improvement:**
When upgrading to DataFusion 52.x, replace the entire `rule_instrumentation.rs` module with `instrument_rules_with_info_spans!` from `datafusion-tracing`. Until then, the `instrument_phase_rules!` macro (lines 716-757) already consolidates the wiring layer, limiting the triplication to the unavoidable trait-impl level.

**Effort:** medium (requires DataFusion major version upgrade)
**Risk if unaddressed:** low (stable; risk is maintenance burden, not correctness)

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Modules are highly cohesive. The coupling concern is the `PushdownEnforcementMode` alias pattern: `pipeline.rs:54` imports `providers::PushdownEnforcementMode as ContractEnforcementMode` while also importing `spec::runtime::PushdownEnforcementMode`, creating a confusing naming alias that obscures the fact that there are two structurally identical enums.

**Findings:**
- `rust/codeanatomy_engine/src/executor/pipeline.rs:54,59`: Imports both `providers::PushdownEnforcementMode as ContractEnforcementMode` and `spec::runtime::PushdownEnforcementMode`, requiring a mapping function at line 332-338.
- `rust/codeanatomy_engine/src/compiler/compile_contract.rs:28,34`: Same pattern -- imports both enums with same alias naming, same mapping function at line 274-278.

**Suggested improvement:**
Unify the two `PushdownEnforcementMode` enums into a single definition (either in `spec::runtime` or `providers::pushdown_contract`) and remove the mapping functions entirely. See P7 for the full DRY analysis.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependency direction is well-enforced. The `spec` module has zero dependencies on executor, session, or rules. The `executor`, `session`, and `rules` modules depend on `spec` for type definitions. The `providers` module depends on `spec` for `InputRelation` and on `datafusion_ext` for Delta integration. No circular dependencies exist.

**No findings requiring action.**

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The ports-and-adapters pattern is clearly implemented:
- `PlanningSurfaceSpec` (`session/planning_surface.rs`) is the port specifying what planning-time registrations are needed.
- `SessionStateBuilder` integration (`session/factory.rs:269+`) is the adapter that applies the spec to DataFusion's concrete session construction.
- `DeltaProviderFromSessionRequest` (`providers/registration.rs:129`) is the adapter for Delta table loading.
- `RuleOverlayProfile` (`rules/overlay.rs:38`) is a declarative port; `build_overlaid_session` is the adapter.

**No findings requiring action.**

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 1/3

**Current state:**
DRY is the weakest principle in this scope. While the prior plan's S9 consolidation introduced `instrument_phase_rules!`, several knowledge duplications remain.

**Findings:**
- **NEW -- Dual `PushdownEnforcementMode` enums.** `spec/runtime.rs:20-25` defines `PushdownEnforcementMode { Warn, Strict, Disabled }`. `providers/pushdown_contract.rs:166-171` defines an identical enum with the same name, variants, serde attributes, and Default. These are structurally identical but are two separate types requiring mapping.
- **NEW -- Duplicated `map_pushdown_mode` function.** `executor/pipeline.rs:332-338` and `compiler/compile_contract.rs:274-278` contain identical `map_pushdown_mode` functions converting between the two enums.
- **RESIDUAL -- Triplicated sentinel patterns.** `rule_instrumentation.rs:281-432`: Three sentinel structs with near-identical `PLANNING_CONTEXT.with(|cell| {...})` bodies. The S9 consolidation added the `instrument_phase_rules!` macro for wiring but could not macro-ify the trait impls themselves due to differing method signatures (`analyze`/`rewrite`/`optimize`).
- **RESIDUAL -- Triplicated sort closures.** `overlay.rs:198-213`: Three nearly identical closures sorting `analyzer_rules`, `optimizer_rules`, and `physical_rules` by priority. Only the trait bound on `Arc<dyn T + Send + Sync>` differs.
- **NEW -- Triplicated `WriteOutcome` error fallback.** `delta_writer.rs:256-283`: Three identical `return WriteOutcome { delta_version: None, files_added: None, bytes_written: None }` blocks.

**Suggested improvement:**
1. Unify `PushdownEnforcementMode` to a single definition in `providers::pushdown_contract` (the natural owner) and have `spec::runtime` re-export or directly use it. Remove both `map_pushdown_mode` functions.
2. Extract a `WriteOutcome::empty()` constructor to replace the three identical fallback blocks.
3. For the triplicated sort closures in `overlay.rs`, consider a generic helper `sort_rules_by_priority<R: HasName>(rules: &mut Vec<R>, priorities: &HashMap<&str, i32>)` using a trait bound.
4. The sentinel/wrapper triplication is constrained by DataFusion's three distinct trait hierarchies; plan for consolidation with DataFusion 52.x `instrument_rules_with_info_spans!`.

**Effort:** medium (enum unification: small; WriteOutcome: small; sort generic: small; sentinel: blocked on DF 52.x)
**Risk if unaddressed:** medium (every change to pushdown semantics or rule trait signatures requires parallel edits in multiple locations)

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are explicit and well-documented:
- `maintenance.rs:20`: `MIN_VACUUM_RETENTION_HOURS = 168` enforced by `safe_vacuum_retention` function.
- `providers/scan_config.rs`: `validate_scan_config` enforces invariants on `DeltaScanConfig` before use.
- `pushdown_contract.rs:173-178`: `PushdownContractReport` with explicit `assertions` and `violations` vectors.
- `session/envelope.rs:189-208`: `compute_envelope_hash` documents all 14 inputs that compose the determinism fingerprint.
- `executor/result.rs`: `DeterminismContract` with `is_replay_valid` combining three hashes.

**No findings requiring action.**

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Most boundary inputs are parsed into typed representations (e.g., `SemanticExecutionSpec`, `RuntimeProfileSpec`, `RuleIntent`). However, two locations use string matching to classify already-parsed data, deferring classification to point-of-use rather than point-of-parse.

**Findings:**
- `rust/codeanatomy_engine/src/rules/rulepack.rs:151-157`: `is_correctness_rule(rule_name: &str)` uses `rule_name.contains("integrity")`, `rule_name.contains("validation")`, etc. Rule correctness classification should be a property of the rule definition (e.g., a `RuleClassification` enum on `RuleIntent`), not inferred from naming conventions at runtime.
- `rust/codeanatomy_engine/src/executor/metrics_collector.rs:154-156`: `is_scan_operator(name)` checks `name.contains("Scan") || name.contains("Parquet") || name.contains("Delta")`. This classifies DataFusion operator types via string matching, which is fragile against upstream naming changes.

**Suggested improvement:**
1. Add a `classification: RuleClassification` field to `RuleIntent` (or `CpgRuleMetadata`) with variants like `Correctness`, `Optimization`, `Policy`. Replace `is_correctness_rule` with a field check.
2. For `is_scan_operator`, this is a pragmatic constraint (DataFusion does not expose a typed operator classification API), so document the fragility with a comment and add a test that verifies known scan operator names.

**Effort:** small
**Risk if unaddressed:** low (correctness classification is only used for rulepack filtering, not safety-critical paths)

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most types enforce valid states. Two patterns allow representable-but-meaningless states.

**Findings:**
- `rust/codeanatomy_engine/src/executor/delta_writer.rs:22-26`: `WriteOutcome` has three `Option` fields (`delta_version`, `files_added`, `bytes_written`) that are always either all `Some` or all `None`. This could be modeled as `Option<WriteOutcomeDetails>` where `WriteOutcomeDetails` has non-optional fields.
- `rust/codeanatomy_engine/src/executor/maintenance.rs:34-100`: The full `MaintenanceSchedule`/`CompactPolicy`/`VacuumPolicy`/`ConstraintSpec` type hierarchy is publicly constructable, but the `execute_maintenance` function is a stub returning success with a message. This allows callers to construct valid-looking schedules that produce no effect.

**Suggested improvement:**
1. Refactor `WriteOutcome` to `Option<WriteOutcomeDetails>` where `WriteOutcomeDetails { delta_version: i64, files_added: u64, bytes_written: u64 }`.
2. For maintenance stubs, add `#[cfg(feature = "maintenance")]` gating or mark the types as `#[doc(hidden)]` until implementation is complete.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 3/3

**Current state:**
CQS is well-respected throughout:
- `AdaptiveTuner` (`tuner/adaptive.rs`) cleanly separates `observe` (command: records metrics), `propose` (query: returns `TunerProposal`), and `apply` (command: mutates config).
- `SessionEnvelope::capture` (query) vs. `register_extraction_inputs` (command that returns registration records as a side-product -- documented and justified).
- `compliance/capture.rs`: `ComplianceCapture::capture_explain_verbose` returns explain text without side effects.

**No findings requiring action.**

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 3/3

**Current state:**
Dependency injection is the primary composition strategy:
- `SessionFactory` receives `EnvironmentProfile`, `CpgRuleSet`, `TracingConfig`, and `SessionBuildOverrides` as explicit parameters.
- `RulepackFactory::build_ruleset` composes rules from `EnvironmentProfile` + `Vec<RuleIntent>`.
- `execute_pipeline` receives all dependencies explicitly: `SessionContext`, `SemanticExecutionSpec`, `CompileOptions`.
- No hidden global state or service locators.

**No findings requiring action.**

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No struct inheritance. All behavior composition is via trait delegation:
- `InstrumentedAnalyzerRule` wraps `Arc<dyn AnalyzerRule + Send + Sync>` and delegates `analyze()`.
- `InstrumentedOptimizerRule` wraps `Arc<dyn OptimizerRule + Send + Sync>` and delegates `rewrite()`.
- `InstrumentedPhysicalOptimizerRule` wraps `Arc<dyn PhysicalOptimizerRule + Send + Sync>` and delegates `optimize()`.
- `RuleOverlayProfile` composes behavior by producing a new `SessionState`, not by subclassing.

**No findings requiring action.**

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. Two locations reach through multiple layers.

**Findings:**
- `rust/codeanatomy_engine/src/providers/registration.rs:79-84` (`compatibility_facts`): Reaches into `snapshot.table_properties.get("delta.columnMapping.mode")` -- accessing a specific key from a nested map on a collaborator's collaborator. The snapshot could expose a `column_mapping_mode()` accessor.
- `rust/codeanatomy_engine/src/executor/delta_writer.rs:289-294`: `table.snapshot()?.snapshot().log_data()` chains three method calls deep to reach file metadata. This is constrained by the DeltaLake API structure.

**Suggested improvement:**
The `compatibility_facts` function is a boundary adapter extracting data from an external library type -- the deep access is justified at this adapter boundary. For `delta_writer.rs`, the chain is similarly constrained by the DeltaLake API. Document both with comments explaining why the deep access is necessary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most interactions follow tell-don't-ask. The `read_write_outcome` function in `delta_writer.rs` is the notable exception, probing external state through a sequence of fallible queries.

**Findings:**
- `rust/codeanatomy_engine/src/executor/delta_writer.rs:253-300` (`read_write_outcome`): Queries `ensure_table_uri`, then `DeltaTable::try_from_url`, then `table.load()`, then `table.version()`, then `table.snapshot()?.snapshot().log_data()`. This is a pure query function (consistent with CQS) but uses ask-then-interpret rather than telling the Delta table to report its outcome. However, this is a necessary pattern at the technology adapter boundary.

**Suggested improvement:**
No change needed -- this is an adapter function at the technology boundary where ask-style probing is the only viable pattern. The three identical error fallbacks should be consolidated per P7.

**Effort:** small (consolidate fallbacks only)
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Pure computation is well-separated in many places: `collect_plan_metrics` is pure tree-walking, `compute_envelope_hash` is pure hashing, `compatibility_facts` is pure extraction. However, `execute_pipeline` in `pipeline.rs` mixes orchestration logic with IO (session creation, Delta registration, execution, metrics collection, maintenance) in a single function.

**Findings:**
- `rust/codeanatomy_engine/src/executor/pipeline.rs:80-330` (`execute_pipeline`): This 250-line function interleaves pure logic (task graph construction, warning assembly, result building) with IO (provider registration, plan execution, metrics collection, maintenance execution). The function is well-structured with clear phases but is not decomposed into pure-transform + IO-shell.
- `rust/codeanatomy_engine/src/session/envelope.rs:87-186` (`SessionEnvelope::capture`): Queries `information_schema` tables via SQL (IO) and computes hashes (pure) in the same function.

**Suggested improvement:**
The pipeline function is already well-structured with clear phases. A deeper decomposition into a pure `PipelinePlan` computation + impure `execute_plan` would be architecturally cleaner but is not urgently needed given the clear phase documentation. No immediate action recommended.

**Effort:** medium (would be a refactor of the central orchestration function)
**Risk if unaddressed:** low (the function is well-documented and tested)

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Idempotency is well-supported:
- Delta writes use `SaveMode::Overwrite` with version tracking.
- `safe_vacuum_retention` (`maintenance.rs`) enforces a 7-day minimum floor, preventing accidental data loss from repeated runs.
- Session construction is deterministic (sorted inputs, BLAKE3 hashes), so re-running with the same spec produces the same session identity.
- `AdaptiveTuner` bounded ranges prevent unbounded drift.

**No findings requiring action.**

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Determinism is a first-class architectural concern with a three-layer contract:
- `spec_hash` (`spec` module): Hashes the execution specification.
- `envelope_hash` (`session/envelope.rs:193-208`): Hashes 14 session identity components via BLAKE3.
- `rulepack_fingerprint` (`rules/registry.rs`): Hashes rule names in sorted order via BLAKE3.

All hash computations sort their inputs for order-independence:
- `planning_manifest.rs`: Sorts analyzer names, optimizer names, physical optimizer names, file format names, table factory names, etc.
- `registration.rs:114`: Sorts inputs by logical_name before registration.
- `envelope.rs:171-186`: `hash_provider_identities` sorts provider identities.

`DeterminismContract::is_replay_valid` (`result.rs`) compares all three hashes for replay validation.

**No findings requiring action.**

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. The primary complexity concern is `rule_instrumentation.rs` at 819 LOC, which exists because DataFusion 51.x does not expose a unified rule instrumentation API.

**Findings:**
- `rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs` (819 LOC): The entire file implements functionality that `datafusion-tracing` 52.x provides via `instrument_rules_with_info_spans!`. The current implementation is correct but carries significant accidental complexity.
- **dfdl_ref finding:** `datafusion-tracing` 52.x `instrument_rules_with_info_spans!(state_builder, options)` would replace:
  - 3 sentinel structs (150 LOC)
  - 3 instrumented wrapper structs (240 LOC)
  - 1 tree traverser (40 LOC)
  - 1 wiring macro + functions (100 LOC)
  - Supporting types, constants, and helpers (289 LOC)
- `rust/codeanatomy_engine/src/session/envelope.rs:192-208`: `compute_envelope_hash` takes 14 parameters, which is complex but justified by the need to capture all identity-affecting inputs in a single deterministic hash.

**Suggested improvement:**
Plan the DataFusion 52.x upgrade to eliminate `rule_instrumentation.rs` entirely. In the interim, no simplification is possible without losing correctness. For `compute_envelope_hash`, consider grouping parameters into a `EnvelopeHashInputs` struct.

**Effort:** medium (DataFusion upgrade is a cross-crate effort)
**Risk if unaddressed:** low (current code is stable and correct)

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves immediate needs. Two areas have speculative structure.

**Findings:**
- `rust/codeanatomy_engine/src/executor/maintenance.rs` (434 LOC): Entirely stub implementation. The types (`MaintenanceSchedule`, `CompactPolicy`, `VacuumPolicy`, `ConstraintSpec`, `MaintenanceReport`, `MaintenanceStepResult`) are fully defined with documentation but `execute_maintenance` returns stub messages. This is 434 LOC of code that produces no real behavior.
- `rust/codeanatomy_engine/src/tuner/adaptive.rs` (549 LOC): The `AdaptiveTuner` with `observe`/`propose`/`apply` and regression detection exists but there is no evidence of calibration or validation that the auto-tuning produces better outcomes than static defaults.

**Suggested improvement:**
1. For `maintenance.rs`: Keep the type definitions (they document the target API contract) but reduce the stub implementations to a single `todo!()` or a compile-time feature gate. The current approach of silently succeeding with stub messages is misleading.
2. For `AdaptiveTuner`: Add benchmark tests that validate the tuner actually improves performance over static defaults, or gate it behind a feature flag until validated.

**Effort:** small
**Risk if unaddressed:** low (stubs are inert; tuner has bounded ranges preventing harm)

---

#### P21. Least astonishment -- Alignment: 3/3

**Current state:**
APIs behave as expected:
- `SessionFactory::build_session_state` and `build_session_state_from_profile` clearly communicate their intent.
- `WarningCode` enum with 17 variants + `as_str()` provides machine-readable codes alongside human-readable messages.
- `RuleOverlayProfile` clearly documents that it produces a NEW session state, never mutating in place.
- Builder patterns (`RunResultBuilder`, `SessionStateBuilder`) follow standard conventions.
- `DeterminismContract::is_replay_valid` does exactly what the name implies.

**No findings requiring action.**

---

#### P22. Declare and version contracts -- Alignment: 3/3

**Current state:**
Public contracts are explicit and versioned:
- `SessionEnvelope` with `codeanatomy_version` field for version tracking.
- `PlanningSurfaceManifest` with deterministic fingerprinting.
- `DeterminismContract` with three versioned hash layers.
- `CompileResponse` with explicit fields for spec_hash, envelope_hash, planning_surface_hash.
- `WarningCode` enum is exhaustive and machine-readable.
- `lib.rs:27-29` explicitly re-exports the public API surface.

**No findings requiring action.**

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Most components are testable via DI. Two areas have testability gaps.

**Findings:**
- `rust/codeanatomy_engine/src/executor/maintenance.rs`: Stub implementation means there is nothing to test. When real operations are added, the function takes `SessionContext` + `MaintenanceSchedule` + output table names, which is DI-friendly.
- `rust/codeanatomy_engine/src/rules/rulepack.rs:151-157`: `is_correctness_rule` uses string matching, making it hard to test without constructing rules with specific naming patterns. A typed classification would be directly testable.
- `rust/codeanatomy_engine/src/executor/metrics_collector.rs:154-156`: `is_scan_operator` has no dedicated test verifying it recognizes the actual operator names DataFusion produces.

**Suggested improvement:**
1. Add a unit test for `is_scan_operator` that verifies it recognizes `ParquetExec`, `DeltaScan`, and other known DataFusion scan operator names.
2. When implementing maintenance, ensure operations are injectable (e.g., accept a `MaintenanceOps` trait for compact/checkpoint/vacuum rather than calling Delta APIs directly).

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Observability is comprehensive and well-designed:
- Feature-gated tracing (`#[cfg(feature = "tracing")]`) for zero-overhead default builds.
- `exec_instrumentation.rs:30+` uses `datafusion_tracing::instrument_with_info_spans!` for execution-level span instrumentation.
- `rule_instrumentation.rs` provides per-rule span creation with plan-diff capabilities.
- `TraceMetricsSummary` (`metrics_collector.rs:85-89`) captures output rows, elapsed compute, spill count, operator count, scan selectivity, and warning counts.
- `ComplianceCapture` (`compliance/capture.rs`) captures explain traces, rule impact, and pushdown probes.
- `WarningCode` enum with 17 distinct codes + `WarningStage` with 7 stages enables structured alerting.
- `RunWarning` includes `context: Option<String>` for additional detail.

**No findings requiring action.**

---

### Python Integration Boundary Assessment

The Rust engine interfaces with Python through `rust/codeanatomy_engine_py/`. The boundary design within the reviewed scope supports clean integration:

1. **Serializable contracts.** `SessionEnvelope`, `TableRegistration`, `DeltaCompatibilityFacts`, `PushdownContractReport`, `WriteOutcome`, `DeterminismContract`, `RunResult`, and all warning types derive `Serialize`/`Deserialize`. Python can consume them as JSON without custom conversion.

2. **`SemanticExecutionSpec` as boundary type.** The spec is the sole input contract. Python constructs the spec; Rust consumes it. No Rust types leak into Python construction paths.

3. **`CompileResponse` as boundary output.** Contains all compilation artifacts (plans, envelope, warnings, metrics) in a single serializable response.

4. **Feature-gated tracing.** Python-side tracing integration is opt-in via the `tracing` feature flag, preventing overhead in non-instrumented builds.

5. **Concern:** The dual `PushdownEnforcementMode` enums (P7) could create confusion at the Python boundary if Python needs to deserialize either variant. Unifying the enums eliminates this risk.

## Cross-Cutting Themes

### Theme 1: DataFusion Triple-Trait Triplication

**Root cause:** DataFusion's rule system uses three separate trait hierarchies (`AnalyzerRule`, `OptimizerRule`, `PhysicalOptimizerRule`) with different method signatures. Any operation that applies uniformly to all rule types (instrumentation, sorting, fingerprinting) must be tripled.

**Affected principles:** P3 (SRP), P7 (DRY), P19 (KISS)

**Affected locations:**
- `rule_instrumentation.rs:281-714` (3 sentinels + 3 wrappers)
- `overlay.rs:198-213` (3 sort closures)
- `registry.rs:compute_ruleset_fingerprint` (3 name extractions)
- `intent_compiler.rs` (3 compile functions)

**Approach:** The `instrument_phase_rules!` macro (S9 fix) already addresses the wiring layer. For sorting and fingerprinting, a `HasName` trait with blanket impls for the three rule traits could reduce some duplication. The definitive fix is upgrading to DataFusion 52.x, which provides unified rule instrumentation macros.

### Theme 2: Spec-vs-Provider Knowledge Duplication

**Root cause:** The `spec::runtime` module defines types for the execution specification (what the caller requests), while the `providers` module defines types for the provider layer (what the engine operates on). When concepts like `PushdownEnforcementMode` exist in both layers with identical definitions, it creates unnecessary coupling and mapping code.

**Affected principles:** P4 (coupling), P7 (DRY)

**Affected locations:**
- `spec/runtime.rs:20-25` vs `providers/pushdown_contract.rs:166-171`
- `pipeline.rs:332-338` vs `compile_contract.rs:274-278`

**Approach:** Choose one canonical location (recommendation: `providers::pushdown_contract` since it owns the domain concept) and have `spec::runtime` re-export it, or use a shared crate-level types module.

### Theme 3: Stub Modules Occupying Real LOC

**Root cause:** `maintenance.rs` (434 LOC) is a fully-documented type hierarchy with stub implementations. This creates an illusion of functionality and adds maintenance burden for code that produces no behavior.

**Affected principles:** P20 (YAGNI), P23 (testability)

**Approach:** Either implement the maintenance operations or reduce the stubs to type definitions + `todo!()` markers. The current "silently succeed with stub messages" pattern is the worst of both worlds.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Unify dual `PushdownEnforcementMode` enums and remove 2 duplicated `map_pushdown_mode` functions | small | Eliminates 4 files of mapping code, reduces confusion at Python boundary |
| 2 | P7 (DRY) | Extract `WriteOutcome::empty()` constructor | small | Removes 3 identical struct literal blocks in `delta_writer.rs` |
| 3 | P7 (DRY) | Generic `sort_rules_by_priority<R: HasName>` to replace 3 closures in `overlay.rs` | small | Reduces overlay.rs by ~15 LOC and prevents copy-paste on rule type additions |
| 4 | P23 (testability) | Add unit test for `is_scan_operator` with known DataFusion operator names | small | Catches upstream naming changes early |
| 5 | P9 (parse) | Add `RuleClassification` enum to `RuleIntent` replacing `is_correctness_rule` string matching | small | Makes rule classification explicit, testable, and extensible |

## Recommended Action Sequence

1. **Unify `PushdownEnforcementMode`** (P4, P7). Move the canonical definition to `providers::pushdown_contract`, re-export from `spec::runtime`, and delete both `map_pushdown_mode` functions in `pipeline.rs` and `compile_contract.rs`. This is the highest-signal, lowest-effort improvement.

2. **Extract `WriteOutcome::empty()`** (P7, P10). Add a `const fn empty() -> Self` to `WriteOutcome` and replace the 3 identical fallback blocks in `delta_writer.rs:256-283`.

3. **Add `HasName` trait + generic sort** (P7). Define `trait HasName { fn name(&self) -> &str; }` with blanket impls for `Arc<dyn AnalyzerRule + Send + Sync>`, etc. Use it to genericize `apply_priority_ordering` in `overlay.rs`.

4. **Add `RuleClassification` to `RuleIntent`** (P9). Replace `is_correctness_rule` string matching with a typed enum field, closing the parse-don't-validate gap.

5. **Add `is_scan_operator` test** (P23). Create a test with known DataFusion operator names (`ParquetExec`, `DeltaScan`, `CsvExec`, etc.) to catch upstream naming changes.

6. **Group `compute_envelope_hash` parameters** (P19). Introduce `EnvelopeHashInputs` struct to replace the 14-parameter function signature.

7. **Resolve `maintenance.rs` stubs** (P20). Either implement real Delta maintenance operations via `datafusion_ext::delta_maintenance::*` or reduce to type definitions + `unimplemented!()` with `#[cfg(feature = "maintenance")]` gating.

8. **Plan DataFusion 52.x upgrade** (P3, P7, P19). Track the upgrade as a roadmap item. When executed, replace all 819 LOC of `rule_instrumentation.rs` with `instrument_rules_with_info_spans!` from `datafusion-tracing`.
