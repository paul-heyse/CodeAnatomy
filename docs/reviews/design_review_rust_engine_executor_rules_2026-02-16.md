# Design Review: rust/codeanatomy_engine/src/{executor,rules,providers,tuner,compliance}

**Date:** 2026-02-16
**Scope:** `rust/codeanatomy_engine/src/executor/`, `rust/codeanatomy_engine/src/rules/`, `rust/codeanatomy_engine/src/providers/`, `rust/codeanatomy_engine/src/tuner/`, `rust/codeanatomy_engine/src/compliance/`
**Focus:** All principles (1-24), with weighted emphasis on Composition (12-15), Quality (23-24), Correctness (16-18), Boundaries (1-6)
**Depth:** deep
**Files reviewed:** 34

## Executive Summary

The Rust engine modules demonstrate strong design discipline in several areas: the determinism contract (two-layer spec_hash + envelope_hash identity), feature-gated zero-overhead tracing and compliance, bounded adaptive tuning with regression rollback, and immutable session state construction via the builder pattern. The most significant structural concern is substantial code duplication in `runner.rs` (two near-identical materialization functions) and `rule_instrumentation.rs` (three structurally identical sentinel/wrapper/wiring function triplets that reimplement what the `datafusion-tracing` crate already provides). The pipeline orchestrator (`pipeline.rs:105`) handles six distinct responsibilities in a single 260-line function, creating SRP tension. These are addressable with moderate effort and would meaningfully improve maintainability.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `CpgRuleSet` exposes all fields as `pub` |
| 2 | Separation of concerns | 2 | medium | medium | `pipeline.rs:execute_pipeline` mixes compilation, pushdown probing, materialization, metrics, maintenance |
| 3 | SRP | 1 | medium | medium | `rule_instrumentation.rs` (815 LOC) owns sentinel management, wrapper impls, AND session wiring |
| 4 | High cohesion, low coupling | 2 | small | low | Well-organized module structure; minor cross-module coupling via string-based detection |
| 5 | Dependency direction | 3 | - | - | Core types have no outward deps; adapters wrap DataFusion/Delta |
| 6 | Ports & Adapters | 2 | medium | low | `FilterPushdownStatus` mirror type is clean; scan detection uses string matching instead of trait |
| 7 | DRY | 1 | medium | high | `runner.rs` duplicates ~100 lines; `rule_instrumentation.rs` triplicates sentinel/wrapper/wiring |
| 8 | Design by contract | 3 | - | - | Strong contracts: `MIN_VACUUM_RETENTION_HOURS`, bounded tuner ranges, pushdown probe validation |
| 9 | Parse, don't validate | 3 | - | - | `FilterPushdownStatus::from()` converts at boundary; `RulepackProfile` enum parsed once |
| 10 | Make illegal states unrepresentable | 2 | small | low | `TunerMode` enum is good; `MaintenanceReport.success` bool + message is weaker than Result |
| 11 | CQS | 2 | small | low | `AdaptiveTuner::observe()` both mutates state and returns config proposal |
| 12 | Dependency inversion | 2 | medium | low | Rule traits provide good inversion; `pipeline.rs` creates deps inline |
| 13 | Prefer composition over inheritance | 3 | - | - | Decorator pattern for instrumented rules; no inheritance hierarchies |
| 14 | Law of Demeter | 2 | small | low | `pipeline.rs:211-213` chains `ctx.state().config().options()` |
| 15 | Tell, don't ask | 2 | small | low | `CpgRuleSet` fields accessed directly; `PushdownProbe` has query methods |
| 16 | Functional core, imperative shell | 2 | medium | low | `safe_vacuum_retention` is pure; but `execute_pipeline` interleaves pure transforms with IO |
| 17 | Idempotency | 3 | - | - | Delta write modes (Append/Overwrite) are idempotent-safe; BLAKE3 fingerprints are stable |
| 18 | Determinism / reproducibility | 3 | - | - | Two-layer determinism contract; BLAKE3 fingerprints; deterministic rule name ordering |
| 19 | KISS | 2 | small | low | Sentinel pattern in `rule_instrumentation.rs` is complex vs `instrument_rules_with_spans!` macro |
| 20 | YAGNI | 2 | small | low | `maintenance.rs` is entirely stub code; `memory_usage: 0` placeholder in metrics |
| 21 | Least astonishment | 2 | small | low | `observe()` name suggests pure query but mutates; dual return semantics |
| 22 | Declare and version public contracts | 3 | - | - | `RunResult`, `MaterializationResult`, `CollectedMetrics` all derive Serialize/Deserialize |
| 23 | Design for testability | 2 | medium | medium | `execute_pipeline` hard to unit test in isolation; pure functions like `safe_vacuum_retention` excellent |
| 24 | Observability | 2 | small | low | Feature-gated tracing comprehensive; `metrics_collector.rs` uses string-based scan detection |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`CpgRuleSet` at `registry.rs:20-29` exposes all four fields as `pub`: `analyzer_rules`, `optimizer_rules`, `physical_rules`, and `fingerprint`. The `RulepackFactory::build_snapshot()` at `rulepack.rs:119-134` and `overlay.rs` both directly access these fields, coupling them to the internal representation.

**Findings:**
- `registry.rs:20-29`: All fields of `CpgRuleSet` are `pub`, allowing any consumer to bypass the typed accessor methods (`analyzer_count()`, `optimizer_count()`, etc.)
- `rulepack.rs:119-134`: `build_snapshot` iterates directly over `ruleset.analyzer_rules`, `ruleset.optimizer_rules`, `ruleset.physical_rules` fields rather than using iterator accessors
- `overlay.rs` accesses `ruleset.analyzer_rules` directly for overlay composition

**Suggested improvement:**
Make `CpgRuleSet` fields `pub(crate)` and expose iteration via typed accessor methods (e.g., `fn analyzer_rules(&self) -> &[Arc<dyn AnalyzerRule + Send + Sync>]`). This preserves internal access for the `rules` module while preventing external code from depending on the Vec representation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The pipeline orchestrator at `pipeline.rs:105-366` handles six distinct concerns in a single async function: (1) plan compilation, (2) cost model / task graph construction, (3) pushdown probing and enforcement, (4) plan bundle capture, (5) materialization, (6) maintenance and result assembly.

**Findings:**
- `pipeline.rs:124-157`: Task graph construction and cost model derivation is inlined within the pipeline orchestrator
- `pipeline.rs:159-179`: Pushdown probing logic (iterating predicates, calling `probe_provider_pushdown`, building probe map) lives inside the orchestrator rather than a dedicated pushdown module
- `pipeline.rs:186-285`: Plan bundle capture logic (100 lines) including optimizer report generation is embedded inline
- `pipeline.rs:326-349`: Maintenance scheduling and warning propagation mixed into the same function

**Suggested improvement:**
Extract the following into helper functions or dedicated submodules: (a) `build_task_graph_and_costs()` for lines 130-157, (b) `probe_pushdown_contracts()` for lines 159-179, (c) `capture_plan_bundles()` for lines 186-285. The `execute_pipeline` function would become a thin orchestration sequence calling these extractable units.

**Effort:** medium
**Risk if unaddressed:** medium -- As more pipeline phases are added, this function will grow and become harder to understand, test, and modify independently.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`rule_instrumentation.rs` (815 LOC) has three distinct reasons to change: (1) sentinel lifecycle management (how phase boundaries are detected), (2) wrapper instrumentation logic (how individual rules are traced), and (3) session state wiring (how instrumented rules are composed into a SessionState).

**Findings:**
- `rule_instrumentation.rs:186-337`: Three structurally identical sentinel structs (`AnalyzerPhaseSentinel`, `OptimizerPhaseSentinel`, `PhysicalOptimizerPhaseSentinel`) each implementing their respective rule trait with the same phase-boundary logic
- `rule_instrumentation.rs:379-619`: Three structurally identical instrumented wrapper structs (`InstrumentedAnalyzerRule`, `InstrumentedOptimizerRule`, `InstrumentedPhysicalOptimizerRule`) with the same span/diff/modification detection pattern
- `rule_instrumentation.rs:621-722`: Three structurally identical wiring functions (`instrument_analyzer_rules`, `instrument_optimizer_rules`, `instrument_physical_optimizer_rules`)
- `runner.rs:38-137` vs `runner.rs:148-250`: Two near-identical functions `execute_and_materialize` and `execute_and_materialize_with_plans` differing only in physical plan collection

**Suggested improvement:**
For `runner.rs`: Make `execute_and_materialize` call `execute_and_materialize_with_plans` and discard the plans, or add a boolean/enum parameter controlling plan capture. For `rule_instrumentation.rs`: Consider adopting the `datafusion-tracing` crate's `instrument_rules_with_spans!` macro (already used successfully in `exec_instrumentation.rs:38-55`) which provides equivalent sentinel + wrapper functionality. If the macro does not cover all requirements (plan_diff, PlanningContext), factor the shared logic into generic helper functions parameterized by the rule trait type.

**Effort:** medium
**Risk if unaddressed:** medium -- The triplicated code in `rule_instrumentation.rs` means every behavioral change (e.g., adding a new span attribute) must be applied three times, risking drift.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Module boundaries are generally well-drawn. The `rules/` module is cohesive around rule composition, `providers/` around data source capabilities, and `tuner/` around adaptive configuration. Minor coupling exists through string-based operator detection in metrics collection.

**Findings:**
- `metrics_collector.rs:151`: Scan detection uses `name.contains("Scan") || name.contains("Parquet") || name.contains("Delta")` -- string-based coupling to operator naming conventions
- Good: `providers/pushdown_contract.rs` is fully self-contained with its own types and no cross-module leakage
- Good: `compliance/capture.rs` depends only on `PushdownProbe` and `PushdownContractReport` from `providers`, which is a narrow, justified coupling

**Suggested improvement:**
Define a constant set or helper function `is_scan_operator(name: &str) -> bool` in a shared location, reducing the chance of drift if DataFusion renames operators or new scan types are added.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction (inward dependencies) -- Alignment: 3/3

**Current state:**
The dependency direction is clean. Core domain types (`RunResult`, `DeterminismContract`, `TunerConfig`, `MaintenanceSchedule`) depend on nothing external. Adapter code (`delta_writer.rs`, `registration.rs`) wraps DataFusion and DeltaLake specifics. The `spec::` module provides the inward-facing contracts that drive the engine.

**Findings:**
No violations found. The architecture properly layers: `spec` -> `rules/providers/tuner` -> `executor` -> `compliance`, with DataFusion/Delta dependencies confined to adapter boundaries.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`FilterPushdownStatus` at `pushdown_contract.rs:28-43` is a clean port/adapter pattern: a serializable mirror type with bidirectional `From` conversions to DataFusion's native enum. The tracing module uses feature gates as an adapter boundary (`mod.rs` provides no-op stubs when tracing is disabled). Scan detection, however, relies on string matching rather than a typed port.

**Findings:**
- `pushdown_contract.rs:57-75`: Bidirectional `From<TableProviderFilterPushDown>` conversions -- clean adapter pattern
- `executor/tracing/mod.rs`: Feature-gated facade provides seven functions with dual implementations -- good adapter boundary
- `metrics_collector.rs:151`: String-based scan detection (`name.contains("Scan")`) bypasses any structured port for operator classification
- `delta_writer.rs`: Arrow-to-Delta type mapping is a well-structured adapter function

**Suggested improvement:**
Introduce a small trait or enum for operator classification (`OperatorKind::Scan | Join | Aggregate | ...`) that can be derived from the `ExecutionPlan` name or trait, replacing ad-hoc string matching.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the most significant violation across the reviewed modules. Two major instances of semantic duplication exist.

**Findings:**
- `runner.rs:38-137` vs `runner.rs:148-250`: The materialization loop logic (schema validation, table registration, native vs DataFusion writer selection, commit options, result assembly) is duplicated verbatim between `execute_and_materialize` and `execute_and_materialize_with_plans`. The only difference is the `_with_plans` variant collects `physical_plans.push(Arc::clone(&plan))` at line 162.
- `rule_instrumentation.rs:186-228` vs `230-285` vs `287-337`: Three sentinel structs share identical phase-boundary logic (check PlanningContext, open/close phase span) but must each implement a different DataFusion rule trait.
- `rule_instrumentation.rs:379-446` vs `448-544` vs `546-619`: Three instrumented wrapper structs share identical span creation, plan cloning, diff detection, and modification recording logic.
- `rule_instrumentation.rs:621-653` vs `655-687` vs `689-722`: Three wiring functions share identical structure (early return, capacity allocation, sentinel insertion, rule wrapping, trailing sentinel).
- `rule_instrumentation.rs:746-764` vs `766-789`: Two span creation closures in `instrument_session_state` duplicate the same field setup (trace.spec_hash, trace.rulepack, trace.profile, trace.custom_fields_json).

**Suggested improvement:**
(1) For `runner.rs`: Make `execute_and_materialize` a thin wrapper that calls `execute_and_materialize_with_plans` and discards the returned plans vector. (2) For `rule_instrumentation.rs`: The `datafusion-tracing` crate's `instrument_rules_with_spans!` macro already provides sentinel + wrapper functionality and is successfully used in `exec_instrumentation.rs:38-55`. Adopt it for rule-phase instrumentation to eliminate approximately 500 lines of triplicated code. (3) For span creation closures: Extract a single generic closure builder parameterized on span name.

**Effort:** medium
**Risk if unaddressed:** high -- Behavioral drift between duplicated implementations is the primary risk. A bug fix or feature addition in one copy that misses the others creates subtle inconsistencies.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Strong contract enforcement throughout. Safety invariants are explicitly documented and enforced.

**Findings:**
- `maintenance.rs:20`: `MIN_VACUUM_RETENTION_HOURS = 168` enforced via `safe_vacuum_retention()` at line 243 -- single enforcement point with clear documentation
- `adaptive.rs:88-91`: Tuner bounds computed from initial config (`partitions_floor`, `partitions_ceiling`, `batch_size_floor`, `batch_size_ceiling`) -- bounded adjustment contract
- `adaptive.rs:213-220`: Regression detection contract: `elapsed_ms > 2 * last.elapsed_ms` triggers rollback
- `pushdown_contract.rs:203-209`: Probe validates response count matches filter count
- `result.rs:77-79`: `DeterminismContract::is_replay_valid()` requires both layers to match

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
Boundary conversions parse external representations into internal types cleanly.

**Findings:**
- `pushdown_contract.rs:57-65`: `From<TableProviderFilterPushDown>` converts DataFusion's enum into the serializable `FilterPushdownStatus` at the boundary
- `rulepack.rs:80-100`: `RulepackProfile` enum pattern-matched once; downstream code operates on the parsed profile
- `maintenance.rs:267-284`: `MaintenanceSchedule` deserialized from JSON with serde defaults, validated once at parse time

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Enums are used effectively for profiles and modes. Some data structures allow technically invalid combinations.

**Findings:**
- `adaptive.rs:14-19`: `TunerMode` enum cleanly distinguishes `ObserveOnly` vs `BoundedAdapt`
- `pushdown_contract.rs:157-162`: `PushdownContractResult` enum with four variants captures all possible contract outcomes
- `maintenance.rs:116-125`: `MaintenanceReport` uses `success: bool` + `message: Option<String>` rather than a `Result`-like enum. A successful report with `success: false` and `message: None` is representable but meaningless.
- `result.rs:19-45`: `RunResult` allows `collected_metrics: None` with `trace_metrics_summary: Some(...)`, which is logically inconsistent (summary without source data)

**Suggested improvement:**
Consider replacing `MaintenanceReport`'s `success: bool` + `message: Option<String>` with a `MaintenanceOutcome` enum: `Success { message: String } | Failed { error: String }`. For `RunResult`, consider a `MetricsBundle` struct that groups `collected_metrics` and `trace_metrics_summary` together so they are always present or absent as a unit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The `AdaptiveTuner::observe()` method is the notable exception.

**Findings:**
- `adaptive.rs:120-208`: `observe()` both mutates internal state (increments `observation_count`, updates `current_config`, updates `stable_config`, stores `last_metrics`) AND returns `Option<TunerConfig>`. This combines command and query semantics.
- Good: `pushdown_contract.rs:106-135`: Query methods (`all_exact()`, `has_unsupported()`, `has_inexact()`, `status_counts()`) are pure queries with no side effects
- Good: `metrics_collector.rs:102-125`: `collect_plan_metrics()` is a pure query that reads metrics from an already-executed plan

**Suggested improvement:**
Split `observe()` into two methods: `record_metrics(&mut self, metrics: &ExecutionMetrics)` (command) and `propose_adjustment(&self) -> Option<TunerConfig>` (query). The regression rollback would be handled in `record_metrics`, and `propose_adjustment` would be a pure function of current state.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
DataFusion's rule traits (`AnalyzerRule`, `OptimizerRule`, `PhysicalOptimizerRule`) provide excellent dependency inversion -- the engine depends on these trait abstractions, not on specific rule implementations. The pipeline orchestrator, however, directly constructs several internal dependencies.

**Findings:**
- `rules/intent_compiler.rs`: Compiles `RuleIntent` to concrete rule implementations via pattern matching on `RuleClass` -- clean factory pattern
- `rules/overlay.rs`: Builds new `SessionState` via `SessionStateBuilder` -- immutable construction pattern avoids mutation
- `pipeline.rs:125-126`: `SemanticPlanCompiler::new(ctx, spec)` is directly constructed inline rather than injected
- `pipeline.rs:146`: `CostModelConfig::default()` is directly constructed rather than passed in

**Suggested improvement:**
Accept `CostModelConfig` as a parameter to `execute_pipeline` rather than defaulting it inline. This would allow callers (native vs Python path) to configure cost model behavior differently.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase uses composition exclusively. The decorator pattern for rule instrumentation wraps rules via composition rather than inheritance. No class hierarchies exist.

**Findings:**
- `rule_instrumentation.rs:379-406`: `InstrumentedAnalyzerRule` wraps `Arc<dyn AnalyzerRule>` -- classic decorator via composition
- `overlay.rs`: `RuleOverlayProfile` composes additional rules onto an existing `CpgRuleSet` by building a new set rather than subclassing
- `result.rs:102-219`: `RunResultBuilder` uses the builder pattern -- composition of result parts

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. A few instances of deep chaining exist in the pipeline orchestrator.

**Findings:**
- `pipeline.rs:211-213`: `ctx.state().config().options()` chains three levels deep to access optimizer config options
- `runner.rs:48`: `df.clone().create_physical_plan().await?` followed by `plan.output_partitioning().partition_count()` -- two-level chain

**Suggested improvement:**
Extract `ctx.state().config().options()` into a local binding at the top of the relevant block. The `partition_count` chain is borderline acceptable given DataFusion's API design.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`CpgRuleSet` exposes raw data for external logic to operate on. `PushdownProbe` provides good encapsulated query methods.

**Findings:**
- `rulepack.rs:119-134`: `build_snapshot` asks for `ruleset.analyzer_rules`, iterates, and maps -- logic that could live on `CpgRuleSet` itself
- Good: `pushdown_contract.rs:104-136`: `PushdownProbe` encapsulates its query methods (`all_exact()`, `has_unsupported()`, `status_counts()`) rather than exposing raw statuses for external iteration

**Suggested improvement:**
Add a `fn snapshot(&self, profile: &RulepackProfile) -> RulepackSnapshot` method to `CpgRuleSet` itself, encapsulating the name-extraction logic.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Several pure functions exist and are properly separated. The pipeline orchestrator interleaves pure computation with IO-heavy operations.

**Findings:**
- Good: `maintenance.rs:243-244`: `safe_vacuum_retention()` is a pure function with no IO
- Good: `metrics_collector.rs:102-125`: `collect_plan_metrics()` is a pure walk of an in-memory plan tree
- Good: `adaptive.rs:213-220`: `is_regression()` is a pure predicate
- `pipeline.rs:105-366`: `execute_pipeline` interleaves pure computation (task graph construction, pushdown probe map building) with async IO (materialization, maintenance execution)
- Good: `pipeline.rs:416-445`: `aggregate_physical_metrics()` is properly extracted as a pure function

**Suggested improvement:**
The pure computation phases of `execute_pipeline` (task graph/cost derivation at lines 130-157, pushdown probe map construction) could be extracted as synchronous pure functions that return their results, with the async orchestrator consuming those results. This would make the pure logic independently testable without needing a `SessionContext`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
The design supports idempotent execution. Delta write modes are explicit, BLAKE3 fingerprints are deterministic, and the determinism contract enables safe replay.

**Findings:**
- `result.rs:59-80`: `DeterminismContract` with `is_replay_valid()` enables idempotent replay detection
- `registry.rs:95-118`: `compute_ruleset_fingerprint()` produces deterministic BLAKE3 hashes from rule names in fixed order
- `runner.rs:52-55`: `MaterializationMode::Append` vs `Overwrite` mapped to `InsertOp` -- explicit mode control

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class design goal throughout. The two-layer identity contract (spec_hash + envelope_hash) is well-implemented.

**Findings:**
- `result.rs:17-45`: `RunResult` carries `spec_hash`, `envelope_hash`, and `rulepack_fingerprint` -- three-layer identity
- `registry.rs:95-118`: Fingerprint computed by hashing rule names in deterministic order (analyzer, then optimizer, then physical)
- `rulepack.rs:102-107`: Deduplication via `HashSet::insert` preserves first-seen order for deterministic output
- `adaptive.rs:1-9`: Design principle #2 explicitly states "NEVER mutate correctness-affecting options" -- determinism safeguard

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The sentinel pattern in `rule_instrumentation.rs` is significantly more complex than the `datafusion-tracing` crate's macro-based approach used in `exec_instrumentation.rs`.

**Findings:**
- `exec_instrumentation.rs:38-55`: Uses `instrument_with_info_spans!` macro -- 18 lines, clean delegation
- `rule_instrumentation.rs:1-815`: Hand-rolls the same conceptual pattern as triplicated sentinels + wrappers + wiring functions -- 815 lines of complexity
- The module header at line 3 acknowledges: "DataFusion 51 does not expose native rule-phase instrumentation macros" -- but `datafusion-tracing`'s `instrument_rules_with_spans!` macro does provide this capability

**Suggested improvement:**
Evaluate whether `instrument_rules_with_spans!` from the `datafusion-tracing` crate covers the required semantics (phase spans, rule spans, plan diffs, effective rule tracking). If it does, replace the hand-rolled implementation. If it lacks specific features (e.g., PlanningContext thread-local), extend the macro or wrap it rather than reimplementing the full stack.

**Effort:** medium
**Risk if unaddressed:** low -- The current implementation works correctly; the complexity tax is in maintenance, not correctness.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Some placeholder/stub code exists that was built speculatively ahead of integration.

**Findings:**
- `maintenance.rs:151-236`: `execute_maintenance` is entirely a stub. Every operation produces a hardcoded `success: true` report with `(stub)` suffix messages. The function signature and data structures are correct, but no actual Delta operations execute.
- `metrics_collector.rs:173`: `memory_usage: 0` with comment "Populated from gauge if available in future" -- placeholder for unimplemented functionality

**Suggested improvement:**
The maintenance stubs are documented as integration placeholders and serve as a specification for the actual implementation, which is a reasonable YAGNI tradeoff. No immediate action required, but track these stubs to ensure they are either implemented or removed before production use.

**Effort:** small
**Risk if unaddressed:** low -- Risk is that stubs create false confidence in maintenance reports showing `success: true`.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. The `observe()` method name is mildly surprising given its mutation semantics.

**Findings:**
- `adaptive.rs:120`: `observe()` suggests passive observation but actively mutates `current_config`, `stable_config`, and `observation_count` while also returning a proposal -- dual semantics
- Good: `rulepack.rs:39`: `build_ruleset()` clearly communicates construction
- Good: `result.rs:84`: `RunResult::builder()` follows standard builder pattern convention
- Good: `maintenance.rs:243`: `safe_vacuum_retention()` name clearly communicates its safety-enforcing purpose

**Suggested improvement:**
Rename `observe()` to `record_and_propose()` or split into `record_metrics()` + `propose_adjustment()` to make the mutation explicit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
All cross-boundary types derive `Serialize` and `Deserialize`, making them explicitly versioned and stable.

**Findings:**
- `result.rs:18-45`: `RunResult` with full serde derives -- explicit contract
- `metrics_collector.rs:18-31`: `TraceMetricsSummary` with serde derives -- stable observability contract
- `maintenance.rs:33-65`: `MaintenanceSchedule` with serde derives and `#[serde(default)]` annotations -- backward-compatible evolution
- `compliance/capture.rs:13-37`: `ComplianceCapture` with `skip_serializing_if` for optional fields -- clean versioning

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure functions like `safe_vacuum_retention`, `is_correctness_rule`, `is_regression`, and `aggregate_physical_metrics` are easily testable and well-tested. The `execute_pipeline` function requires a fully configured `SessionContext` to test, making unit testing difficult.

**Findings:**
- Good: `maintenance.rs:247-434`: 8 tests covering safety invariant, serde defaults, operation ordering, target filtering, and retention enforcement -- all without heavy setup
- Good: `adaptive.rs:266-517`: 8 tests covering creation, observe-only mode, stability window, regression detection, spill pressure, selective scan, small result, and reset -- purely in-memory
- Good: `pushdown_contract.rs:218-372`: 10 tests covering conversions, predicates, serde, probing, and edge cases
- `pipeline.rs:105`: `execute_pipeline` cannot be tested without a real `SessionContext` with registered tables, compiled specs, and provider registrations
- `runner.rs:38-250`: `execute_and_materialize` requires a real SessionContext with registered Delta tables -- no seam for test doubles

**Suggested improvement:**
Extract the pure computational phases from `execute_pipeline` (task graph construction, pushdown probe aggregation, metrics aggregation) as standalone functions that can be tested independently. The existing `aggregate_physical_metrics` at line 416 is already a good example of this pattern.

**Effort:** medium
**Risk if unaddressed:** medium -- As the pipeline grows, the inability to test individual phases in isolation increases the risk of regressions.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The tracing infrastructure is comprehensive and well-designed with feature-gated zero overhead. However, scan operator detection in metrics collection relies on brittle string matching.

**Findings:**
- Good: `executor/tracing/mod.rs`: Feature-gated facade with seven functions providing clean no-op stubs when tracing is disabled
- Good: `executor/tracing/bootstrap.rs`: OnceLock-based OTEL initialization with configurable sampler, endpoint, and protocol
- Good: `executor/tracing/exec_instrumentation.rs`: Proper delegation to `datafusion-tracing` crate's `instrument_with_info_spans!` macro
- Good: `warnings.rs`: `WarningCode` enum (17 variants) with `WarningStage` provides structured warning classification
- `metrics_collector.rs:150-151`: `name.contains("Scan") || name.contains("Parquet") || name.contains("Delta")` -- string-based scan detection is brittle if operator names change across DataFusion versions
- `rule_instrumentation.rs:746-764`: Span fields include `trace.spec_hash`, `trace.rulepack`, `trace.profile` -- good correlation context

**Suggested improvement:**
Extract the scan operator classification logic into a named function (e.g., `is_scan_operator(name: &str) -> bool`) and add DataFusion version-specific tests that verify the operator names match expectations. This provides a single point of failure detection when upgrading DataFusion.

**Effort:** small
**Risk if unaddressed:** low -- Risk is silently missing scan metrics after a DataFusion upgrade changes operator naming.

---

## Cross-Cutting Themes

### Theme 1: Structural triplication in rule instrumentation

The sentinel + wrapper + wiring pattern in `rule_instrumentation.rs` is applied identically three times for the three DataFusion rule trait types. This triplication is the single largest source of maintenance burden in the reviewed modules. The `datafusion-tracing` crate (already successfully used in `exec_instrumentation.rs`) provides equivalent functionality via macros. The hand-rolled implementation may predate the crate's rule-phase support or may require features the macro does not yet expose (e.g., `PlanningContext` thread-local, effective rule tracking).

**Root cause:** DataFusion's three separate rule traits (`AnalyzerRule`, `OptimizerRule`, `PhysicalOptimizerRule`) require separate implementations even when the instrumentation logic is identical. Rust's trait system does not support the kind of generic-over-traits abstraction that would eliminate this duplication without macros.

**Affected principles:** P3 (SRP), P7 (DRY), P19 (KISS)

**Suggested approach:** Evaluate the `datafusion-tracing` crate's `RuleInstrumentationOptions` and `instrument_rules_with_spans!` capabilities. If they cover the required semantics, adopt them. If specific features are missing, contribute them upstream or create a thin wrapper. The goal is to reduce 815 LOC to approximately 100 LOC of configuration + delegation.

### Theme 2: Materialization duplication in runner.rs

The two materialization functions in `runner.rs` share approximately 100 lines of identical logic. This is a straightforward DRY violation with a simple fix: make the simpler function delegate to the more capable one.

**Root cause:** The `_with_plans` variant was likely added as an extension of the original function without refactoring for reuse.

**Affected principles:** P7 (DRY), P3 (SRP)

**Suggested approach:** Have `execute_and_materialize` call `execute_and_materialize_with_plans` and discard the `physical_plans` vector from the returned tuple. This is a 10-minute change with zero behavioral impact.

### Theme 3: Pipeline orchestrator doing too much

`execute_pipeline` at 260 lines handles six phases of the execution lifecycle. While each phase is well-documented and the function reads top-to-bottom, it conflates responsibilities that change for different reasons.

**Root cause:** Centralized orchestration is natural for pipeline functions, but the current implementation embeds the logic of each phase rather than delegating to phase-specific functions.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P16 (functional core), P23 (testability)

**Suggested approach:** Extract each phase into a named helper function. The orchestrator becomes a thin sequential caller: `compile() -> probe_pushdown() -> capture_bundles() -> materialize() -> collect_metrics() -> maintain() -> assemble_result()`.

### Theme 4: Python integration boundary concerns

Three files in scope have direct Python integration counterparts noted in the review request: `delta_writer.rs` <-> `write_core.py`/`write_delta.py`, `maintenance.rs` <-> `control_plane_maintenance.py`, `metrics_collector.rs` <-> `engine_metrics_bridge.py`. The Rust-side contracts (`MaterializationResult`, `CollectedMetrics`, `MaintenanceReport`) are properly serializable, which supports clean cross-language boundaries. The maintenance stubs represent an incomplete integration that should be tracked.

**Root cause:** The engine is designed for dual native/Python execution paths with shared pipeline orchestration (`pipeline.rs` boundary contract documented at lines 8-24).

**Affected principles:** P22 (declare and version contracts), P6 (ports & adapters)

**Suggested approach:** Ensure the Python bridge files validate incoming Rust types against the same serde schemas. The maintenance stubs should be annotated with a tracking issue or completion marker.

### Theme 5: Determinism contract chain (cross-agent)

The two-layer determinism contract (`spec_hash` + `envelope_hash`) at `result.rs:59-64` is well-implemented in Rust. This pattern should be verified end-to-end with the Python side to ensure the same hash computation produces identical results across the FFI boundary. The `rulepack_fingerprint` adds a third layer that is less formally documented.

**Affected principles:** P18 (determinism), P8 (design by contract)

**Suggested approach:** Document the three-layer identity contract (spec_hash, envelope_hash, rulepack_fingerprint) in a shared specification that both the Rust and Python implementations can reference. Add cross-language golden tests that verify hash identity across the FFI boundary.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Make `execute_and_materialize` delegate to `execute_and_materialize_with_plans` in `runner.rs` | small | Eliminates 100 lines of exact duplication |
| 2 | P1 (Information hiding) | Make `CpgRuleSet` fields `pub(crate)` and add accessor methods in `registry.rs` | small | Prevents external code from depending on internal Vec representation |
| 3 | P11 (CQS) | Split `AdaptiveTuner::observe()` into `record_metrics()` and `propose_adjustment()` | small | Clearer API semantics; enables pure testing of proposal logic |
| 4 | P4 (Cohesion) | Extract `is_scan_operator()` helper from string-matching logic in `metrics_collector.rs:151` | small | Single point of maintenance for operator classification |
| 5 | P15 (Tell, don't ask) | Move `build_snapshot` logic onto `CpgRuleSet` as a method in `registry.rs` | small | Encapsulates name-extraction logic near the data it operates on |

## Recommended Action Sequence

1. **Eliminate `runner.rs` materialization duplication (P7).** Have `execute_and_materialize` delegate to `execute_and_materialize_with_plans`. This is the highest-value, lowest-risk change -- 10 minutes, zero behavioral change.

2. **Tighten `CpgRuleSet` visibility (P1, P15).** Change fields to `pub(crate)`, add typed accessor methods, and move `build_snapshot` onto the struct. Update `rulepack.rs:119-134` and `overlay.rs` accordingly.

3. **Split `AdaptiveTuner::observe()` (P11, P21).** Separate mutation from query to improve API clarity and testability.

4. **Extract scan operator classification (P4, P24).** Create `is_scan_operator()` helper to replace string matching at `metrics_collector.rs:151`.

5. **Extract `execute_pipeline` phase helpers (P2, P3, P23).** Factor out `build_task_graph_and_costs()`, `probe_pushdown_contracts()`, `capture_plan_bundles()`, and `aggregate_physical_metrics()` (already done) as standalone functions. This enables independent unit testing of each phase.

6. **Evaluate `datafusion-tracing` crate for rule instrumentation (P3, P7, P19).** Compare `instrument_rules_with_spans!` capabilities against the hand-rolled implementation in `rule_instrumentation.rs`. If the macro covers requirements, adopt it; if not, file an upstream feature request and create a thin extension. Target: reduce 815 LOC to ~100 LOC.

7. **Document cross-language determinism contract (P18, P8).** Create a shared specification for the three-layer identity (spec_hash, envelope_hash, rulepack_fingerprint) and add golden tests that verify hash consistency across the Rust/Python boundary.
