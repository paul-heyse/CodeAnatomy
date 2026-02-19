# Design Review: rust/codeanatomy_engine/

**Date:** 2026-02-18
**Scope:** rust/codeanatomy_engine/
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 130

---

## Executive Summary

The Rust engine crate is the strongest part of the CodeAnatomy codebase by design-principle alignment. The core execution model — a single `SemanticExecutionSpec` boundary with BLAKE3-fingerprinted determinism layers — is architecturally sound, and the `spec/`, `rules/`, and `session/` modules exhibit exemplary contract discipline. Two medium-priority issues dominate: (1) the topological sort algorithm for view ordering is implemented independently in both `compiler/scheduling.rs` (`TaskGraph::topological_sort`) and `compiler/plan_compiler.rs` (`SemanticPlanCompiler::topological_sort`), encoding the same knowledge in two places with subtle behavioral differences; and (2) the `enforce_pushdown_contracts` private function is duplicated verbatim between `compiler/compile_phases.rs:131` and `executor/pipeline.rs:389`. A structural concern exists in `providers/interval_align_provider.rs`, which creates a bare `SessionContext::new()` inside its scan path, bypassing the configured `SessionFactory` and coupling IO to a stateless internal session. The observability layer is the weakest axis: the `tracing` feature flag fully gates all OpenTelemetry integration, leaving non-feature-flagged builds with no instrumentation path and no structured spans at pipeline boundaries.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `pub` fields on `TaskGraph`, `TaskNode`, `CachePolicyDecision` expose internals |
| 2 | Separation of concerns | 2 | medium | medium | `execute_and_materialize` in `runner.rs` mixes Delta table creation, schema validation, and write dispatch |
| 3 | SRP | 2 | medium | low | `pipeline.rs::execute_pipeline` owns compile, pushdown probe, bundle capture, materialization, metrics, and maintenance |
| 4 | High cohesion, low coupling | 3 | — | low | Module boundaries are sharp; spec/, compiler/, executor/ are cleanly separated |
| 5 | Dependency direction | 3 | — | low | `spec/` has no outward deps; `executor/` → `compiler/` → `spec/` is correct |
| 6 | Ports & Adapters | 2 | medium | medium | `IntervalAlignProvider::build_dataframe` creates bare `SessionContext::new()`, bypassing the adapter |
| 7 | DRY | 1 | small | medium | Topological sort duplicated; `enforce_pushdown_contracts` duplicated verbatim |
| 8 | Design by contract | 3 | — | low | `serde(deny_unknown_fields)`, `spec_hash` computed at construction, `DeterminismContract` enforced |
| 9 | Parse, don't validate | 3 | — | low | `SemanticExecutionSpec::new` canonicalizes at boundary; `ViewTransform` enum prevents invalid states |
| 10 | Make illegal states unrepresentable | 3 | — | low | `ViewTransform`, `WriteOutcome`, `WarningCode`, `TaskType` enums prevent impossible combinations |
| 11 | CQS | 2 | small | low | `SemanticPlanCompiler::ensure_source_registered` is side-effecting but named like a query |
| 12 | Dependency inversion + explicit composition | 2 | medium | medium | `SessionFactory::build_session_state_internal` accepts 9 parameters and owns too much composition |
| 13 | Prefer composition over inheritance | 3 | — | low | All extension via traits (`TableProvider`, `OptimizerRule`) and struct composition |
| 14 | Law of Demeter | 2 | small | low | `compile_phases.rs:189` chains `ctx.state().config().options().optimizer.max_passes` |
| 15 | Tell, don't ask | 2 | small | low | `runner.rs:79-88` asks `target.delta_location.is_some()` and `ctx.table()` to decide write path |
| 16 | Functional core, imperative shell | 2 | medium | medium | Scheduling and cost model are pure; `plan_compiler.rs` mixes pure graph traversal with session side effects |
| 17 | Idempotency | 3 | — | low | `register_or_replace_table`, `ensure_output_table`, deterministic version pinning all support rerun safety |
| 18 | Determinism / reproducibility | 3 | — | low | Three-layer hash contract (spec_hash + envelope_hash + rulepack_fingerprint) with BLAKE3 and BTreeMap ordering |
| 19 | KISS | 2 | small | low | `build_interval_align_sql` and `build_output_schema` duplicate column resolution logic inline |
| 20 | YAGNI | 2 | small | low | `DeterminismContract` type exists in both `spec/hashing.rs` and `executor/result.rs` with different fields |
| 21 | Least astonishment | 2 | small | low | `ensure_source_registered` is a command (side effect) with "ensure" naming that reads as a predicate |
| 22 | Declare and version public contracts | 2 | medium | medium | `SPEC_SCHEMA_VERSION` constant present but spec fields are all `pub` with no stability annotations |
| 23 | Design for testability | 2 | medium | medium | Many unit tests use bare `SessionContext::new()` rather than `SessionFactory`; tests do not inject `CpgRuleSet` |
| 24 | Observability | 1 | medium | high | All OTEL integration gated behind `#[cfg(feature = "tracing")]`; no structured spans in default build |

---

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding — Alignment: 2/3

**Current state:**
The module hierarchy cleanly encapsulates major subsystems. `spec/`, `compiler/`, `executor/`, and `session/` expose narrow public interfaces through `mod.rs`. Most internal types are `pub(crate)` or private.

**Findings:**
- `compiler/scheduling.rs:23-28`: `TaskGraph` fields `dependencies`, `nodes`, `topological_order`, and `is_reduced` are all `pub`. Callers in `compiler/compile_phases.rs:64-68` reach directly into `task_graph.dependencies` to extract the map, bypassing any encapsulation.
- `compiler/scheduling.rs:9-13` and `15-18`: `TaskType` and `TaskNode` are `pub` with all fields public, making the internal representation a de facto contract.
- `compiler/scheduling.rs:49-55`: `CachePolicyDecision.view_name`, `.policy`, `.confidence`, `.rationale` are all `pub`. The `derive_cache_policies` function returns `Vec<CachePolicyDecision>`, and callers must inspect `.policy` as a raw `String` to act on it, rather than having a method that tells them what to do.

**Suggested improvement:**
Add a `policy_kind() -> CachePolicyKind` method to `CachePolicyDecision` returning a typed enum (`None`, `DeltaStaging`, `DeltaOutput`). Make `TaskGraph.dependencies` and `TaskGraph.nodes` `pub(crate)`, and expose intent-revealing accessors (`task_type_of(&str)`, `deps_of(&str)`) for the narrow cross-module use in `compile_phases.rs`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**
The broad separation between compilation (`compiler/`) and execution (`executor/`) is well maintained. The `spec/` layer contains no IO. However, two functions blend multiple distinct responsibilities.

**Findings:**
- `executor/runner.rs:56-155` (`execute_and_materialize_with_plans`): A single function performs Delta table creation (`ensure_output_table`), schema validation (`validate_output_schema`), write-path selection (native Delta writer vs. DataFusion `write_table`), row counting, and outcome capture. These are five separate concerns that cannot be individually tested or swapped.
- `executor/runner.rs:79-88`: The decision logic for `use_native_delta_writer` (checking `target.delta_location.is_some() || pre_registered_target.is_none()`) embeds policy about when to use which write path inside the execution function, not in the policy layer.
- `executor/pipeline.rs:106-328` (`execute_pipeline`): The pipeline function owns compilation (step 1), pushdown probing (inline), plan bundle capture (step 2), materialization (step 3), metrics aggregation (step 4), maintenance (step 5), and result assembly (step 6). The doc comment acknowledges this but names it a "boundary contract." The compile and compliance phases are sufficiently different change vectors that they belong in separate functions.

**Suggested improvement:**
Extract the write-path selection and Delta table management from `execute_and_materialize_with_plans` into a `WriteStrategy::resolve(target, ctx)` function that returns an enum (`NativeDelta`, `DataFusionTable`). The actual write dispatch can then be a short match with no policy logic inline.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P3. SRP — Alignment: 2/3

**Current state:**
Most modules have a single clear purpose. `spec/`, `rules/registry.rs`, `tuner/adaptive.rs`, and `providers/pushdown_contract.rs` each have one reason to change.

**Findings:**
- `executor/pipeline.rs` (`execute_pipeline`): The function changes for at least five independent reasons: compilation strategy changes, compliance capture policy changes, materialization strategy changes, maintenance schedule changes, and metrics schema changes. The doc comment acknowledges the boundary contract but the function is 220 lines long.
- `executor/delta_writer.rs`: This file combines Arrow-to-Delta type mapping (`map_arrow_type`), table creation (`ensure_output_table`), schema validation (`validate_output_schema`), write execution (`execute_delta_write`), write outcome capture (`read_write_outcome`), and commit metadata assembly (`build_commit_properties`). Type mapping and commit metadata construction are independent change vectors.

**Suggested improvement:**
Split `delta_writer.rs` into `delta_type_mapping.rs` (Arrow→Delta conversion, changes only when Delta kernel changes), `delta_table_manager.rs` (ensure/validate table, changes with Delta API), and `delta_commit.rs` (commit metadata, changes with lineage policy). This is a pure reorganization with no semantic change.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P4. High cohesion, low coupling — Alignment: 3/3

The module boundaries are correctly drawn. `spec/` contains only data definitions; `compiler/` contains only plan compilation; `rules/` contains only rule management; `session/` contains only session lifecycle. Cross-module coupling flows through narrow, stable types (`SemanticExecutionSpec`, `CpgRuleSet`, `SessionFactory`).

N/A: No action needed.

---

#### P5. Dependency direction — Alignment: 3/3

The dependency hierarchy is correct and consistently maintained:
- `spec/` (leaf): no internal crate dependencies.
- `rules/`: depends only on DataFusion traits and `spec/`.
- `compiler/`: depends on `spec/`, `rules/`, `providers/`, `contracts/`.
- `executor/`: depends on `compiler/`, `session/`, `spec/`, `providers/`.
- `session/`: depends on `spec/`, `rules/`.
- `python/`: depends on `providers/` and DataFusion (adapter layer).

The core (`spec/`) depends on nothing internal; the periphery depends on the core. This is the correct direction.

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**
The `SessionFactory` plays the role of the adapter that creates DataFusion sessions with the right rules. The `CpgRuleSet` plays the role of the port. The `providers/` module acts as the adapter for Delta Lake storage. These patterns are well-executed at the level of the full pipeline.

**Findings:**
- `providers/interval_align_provider.rs:108-116` (`IntervalAlignProvider::build_dataframe`): Creates a bare `SessionContext::new()` internally. This hardcoded session has none of the engine-configured rules, no memory pool bounds, no determinism knobs, and no registered UDFs. It operates outside the `SessionFactory` adapter contract entirely. When the `scan()` method on the `TableProvider` trait is called by the configured session, the inner join is evaluated by a second, unconfigured session.
- `providers/interval_align_provider.rs:564-569` (`execute_interval_align`): Same pattern: a second bare `SessionContext::new()` is created to execute the outer `SELECT *` query.

**Suggested improvement:**
Pass the active `Session` reference (available in `TableProvider::scan`) down to `build_dataframe`, or pre-materialize the join result during `IntervalAlignProvider::try_new` using the caller's session state. The `state: &dyn Session` parameter in `scan` already provides access to a correctly configured planning context via `state.create_physical_plan()`.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Knowledge (7-11)

#### P7. DRY — Alignment: 1/3

**Current state:**
Two instances of semantic knowledge duplication exist at the algorithm level.

**Findings:**
- **Topological sort duplication**: `compiler/scheduling.rs:225-271` (`TaskGraph::topological_sort`) and `compiler/plan_compiler.rs:171-248` (`SemanticPlanCompiler::topological_sort`) both implement Kahn's algorithm for topological ordering. They differ subtly: the `TaskGraph` version uses a `VecDeque` without re-sorting, while the `SemanticPlanCompiler` version re-sorts the queue at every insertion to enforce lexicographic tie-breaking. This means the two implementations can produce different orderings on identical graphs, which matters for determinism.
- **`enforce_pushdown_contracts` duplication**: `compiler/compile_phases.rs:131-164` and `executor/pipeline.rs:389-422` contain byte-for-byte identical private functions. If the enforcement logic changes (e.g., adding a new `PushdownEnforcementMode` variant), both copies must be updated simultaneously.
- **Task graph construction duplication**: `compiler/compile_phases.rs:38-54` (`build_task_schedule_phase`) and `executor/pipeline.rs:330-358` (`build_task_graph_and_costs`) both extract `view_deps`, `scan_deps`, and `output_deps` from the spec and construct a `TaskGraph`. The data extraction logic is identical.
- **`DeterminismContract` type duplication**: `spec/hashing.rs:68-78` and `executor/result.rs:82-86` each define a `DeterminismContract` struct with slightly different fields (`spec_hash + envelope_hash` vs. `spec_hash + envelope_hash + rulepack_fingerprint`).

**Suggested improvement:**
1. Remove `SemanticPlanCompiler::topological_sort` and delegate to `TaskGraph::topological_sort`. The `TaskGraph` version needs the lexicographic tie-breaking added to match the deterministic behavior.
2. Move `enforce_pushdown_contracts` to `providers/pushdown_contract.rs` as a `pub(crate)` free function and import it from both call sites.
3. Extract the task-graph construction into a single `build_task_graph(spec: &SemanticExecutionSpec) -> Result<TaskGraph>` in `compiler/scheduling.rs`.
4. Remove `spec/hashing.rs::DeterminismContract` (it is unused at the contract level) or merge the two `DeterminismContract` definitions into one canonical type in `executor/result.rs`.

**Effort:** small
**Risk if unaddressed:** medium

---

#### P8. Design by contract — Alignment: 3/3

Contract discipline throughout the codebase is strong:
- `SemanticExecutionSpec` uses `serde(deny_unknown_fields)` on all nested types (`relations.rs:8`, `19`, `127`, `145`, `155`), preventing accidental schema drift.
- `spec_hash` is computed at construction time (`execution_spec.rs:91`) and stored as `#[serde(skip)]`, ensuring it cannot be deserialized incorrectly.
- `CpgRuleSet` computes its fingerprint at construction (`rules/registry.rs:50-52`).
- `validate_output_schema` enforces column contract before writes (`delta_writer.rs:203-232`).
- `validate_required_columns` in `interval_align_provider.rs:268-298` enforces preconditions before SQL generation.

---

#### P9. Parse, don't validate — Alignment: 3/3

The `ViewTransform` enum (`relations.rs:32-112`) is the canonical example: an incoming JSON document is parsed at the boundary into a variant that structurally encodes the transform type. Once parsed, no downstream code needs to check string-valued "kind" fields. The `serde(tag = "kind")` attribute means the discriminant is structural, not a runtime string comparison. The `IntervalAlignProviderConfig` `Default` implementation and `#[serde(default)]` usage mean missing fields are filled with valid values, not propagated as `None` chains.

---

#### P10. Make illegal states unrepresentable — Alignment: 3/3

The Rust type system is used effectively:
- `ViewTransform` enum (`relations.rs`): impossible to have a `Relate` transform without `join_type` and `join_keys`.
- `WriteOutcome` enum with `Captured` and `Unavailable` variants (`result.rs:60-69`) eliminates the need for nullable outcome fields.
- `WriteOutcomeUnavailableReason` enum (`result.rs:72-78`) prevents free-form error strings for unavailable outcomes.
- `TaskType` enum (`scheduling.rs:9-13`) prevents nodes from having an untyped task category.
- `WarningCode` enum (`warnings.rs:9-27`) prevents free-form warning codes that drift over time.
- `TunerMode` enum (`tuner/adaptive.rs:14-19`) makes "observe only" a type-level property, not a flag to check.

---

#### P11. CQS — Alignment: 2/3

**Current state:**
Most functions are clearly queries (returning values without state change) or commands (mutating state without returning domain values). One violation exists.

**Findings:**
- `compiler/plan_compiler.rs:373-383` (`SemanticPlanCompiler::ensure_source_registered`): This function mutates the `SessionContext` by registering a table (command), but returns `Result<String>` containing the source name (query). A caller reading the signature sees a function that returns a string and must know to infer the registration side effect. The method is called with `let source = self.ensure_source_registered(...)` at lines `267`, `285`, `295`, `310`, `314`, `323` — all callers use the returned string, but the registration is the actual purpose.
- `tuner/adaptive.rs:121-134` (`AdaptiveTuner::record_metrics`): This is a pure command that updates internal state. Correctly does not return domain data. However, it also performs rollback logic when a regression is detected — rollback is a separate concern from recording.

**Suggested improvement:**
Rename `ensure_source_registered` to `register_inline_source_if_needed` (or make it void and have callers use the `source` parameter they already hold). The returned `String` is always just the input `source: &str` coerced — extracting that as a no-op return conflates two actions.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
The `SessionFactory` pattern is the right abstraction: high-level code depends on the factory interface, not on DataFusion session construction details. However, one function has too many injected concrete dependencies.

**Findings:**
- `session/factory.rs:247-305` (`SessionFactory::build_session_state_internal`): This method takes 9 parameters (`profile_name`, `memory_pool_bytes`, `config`, `runtime`, `ruleset`, `spec_hash`, `tracing_config`, `overrides`, `build_warnings`) and is annotated `#[allow(clippy::too_many_arguments)]`. The parameter list is a sign that this method assembles too many concerns that could be captured in a single configuration type.
- `compiler/compile_phases.rs:171-180` (`compile_artifacts_phase`): Also annotated `#[allow(clippy::too_many_arguments)]` with 8 parameters. The parameters `stats_quality` and `provider_identities` and `planning_surface_hash` are passed individually but could be wrapped in an `ArtifactBuildContext` struct.
- The creation of `CpgRuleSet` is explicit via the `new` constructor, and its injection into both `SessionFactory` and `compile_request` is correct.

**Suggested improvement:**
Introduce `SessionBuildParams` (grouping `profile_name`, `spec_hash`, `tracing_config`, `build_warnings`) to consolidate the `build_session_state_internal` parameters from 9 to 5. Similarly, introduce `ArtifactPhaseContext` to wrap `stats_quality`, `provider_identities`, and `planning_surface_hash` in `compile_artifacts_phase`.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P13. Prefer composition over inheritance — Alignment: 3/3

The codebase avoids inheritance entirely. All extension points use Rust traits (`TableProvider`, `AnalyzerRule`, `OptimizerRule`, `PhysicalOptimizerRule`) with composition via `Vec<Arc<dyn Trait>>`. The `AdaptiveTuner` is a standalone struct with no inheritance. The `CpgRuleSet` composes three independent rule collections. No `impl X for Y where Y: Z` inheritance chains exist in the crate.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
Most cross-module interactions are single-level dereferences. Two violations exist where internal state is traversed across multiple levels.

**Findings:**
- `compiler/compile_phases.rs:189-196`: The chain `ctx.state().config().options().optimizer.max_passes` traverses four levels (`ctx` → `state()` → `config()` → `options()` → `.optimizer.max_passes`). The `SessionContext` API exposes this chain but does not provide a direct accessor for optimizer options.
- `executor/runner.rs:80`: `pre_registered_target.is_some() && target.delta_location.is_some()` then immediately `let _ = ctx.deregister_table(...)` — the logic peeks into two separate structures to derive a decision about a third (the write path), spreading the write policy logic across multiple levels.
- `executor/delta_writer.rs:360-372`: The snapshot access chain `table.snapshot()?.snapshot().log_data().iter()` traverses three wrapper types to reach file metadata.

**Suggested improvement:**
For the optimizer config pattern, extract a `read_optimizer_config(ctx: &SessionContext) -> OptimizerSnapshot` helper to `session/capture.rs` (which already performs `planning_config_snapshot`). For the delta snapshot chain, introduce a `snapshot_file_stats(table: &DeltaTable) -> Option<(u64, u64)>` helper adjacent to `read_write_outcome`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
Most domain logic is correctly encapsulated. One place exposes raw data and reconstructs policy externally.

**Findings:**
- `executor/runner.rs:78-102`: The code asks `target.delta_location.is_some()`, asks `ctx.table()`, then decides `use_native_delta_writer` based on their combination, then executes two different write paths with duplicate `build_delta_commit_options` calls. The `OutputTarget` struct holds the data but does not encode the write strategy.
- `compiler/plan_compiler.rs:100-107`: `output_views` and `ref_counts` are computed from `self.spec` and passed to `compute_inline_policy`, rather than having `SemanticExecutionSpec` provide a method like `inline_policy_inputs() -> (Vec<String>, HashMap<&str, usize>)`.

**Suggested improvement:**
Add `OutputTarget::write_strategy() -> WriteStrategy` where `WriteStrategy` is an enum `NativeDelta { location: String }` | `DataFusionTable { name: String }`. The runner then dispatches on this without inspecting the internal fields.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell — Alignment: 2/3

**Current state:**
The `spec/` layer, `compiler/scheduling.rs`, `compiler/cost_model.rs`, `tuner/adaptive.rs`, and `compliance/capture.rs` are pure functions operating on values. These form a strong functional core. The imperative shell is concentrated in `executor/runner.rs` and `executor/pipeline.rs`.

**Findings:**
- `compiler/plan_compiler.rs` (`SemanticPlanCompiler`): The struct holds `&SessionContext` and `&SemanticExecutionSpec` as references, making every method on it an implicit IO operation (DataFusion table registration and plan creation are IO). The topological sort (`plan_compiler.rs:171-248`) is a pure algorithm but lives inside an IO-capable struct, preventing it from being tested without a session context.
- `compiler/plan_compiler.rs:113-121`: The registration loop (`register_or_replace_table`, `inline_cache.insert`) interleaves compilation (pure) with session mutation (IO) in a single loop. Pure view-to-DataFrame compilation and session registration are the same concern in the current design.
- The `AdaptiveTuner` is a clean functional core: `propose_adjustment` is side-effect free, and `apply_adjustment` is the explicit state mutation.

**Suggested improvement:**
Extract `SemanticPlanCompiler::topological_sort` as a pure free function `topological_sort_views(views: &[ViewDefinition]) -> Result<Vec<&ViewDefinition>>` that takes only data and returns only data. This function can then be unit-tested without any DataFusion setup.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency — Alignment: 3/3

Idempotency is explicitly designed for:
- `table_registration.rs`: `register_or_replace_table` (via its name, deregistering before re-registering).
- `delta_writer.rs:144-200` (`ensure_output_table`): Creates only if the table does not exist; validates schema if it does.
- `providers/registration.rs:113-115`: Inputs sorted by name before registration, ensuring deterministic order across retries.
- `spec/execution_spec.rs:101-106` (`are_inputs_deterministic`): Version pins explicitly gate whether a re-run is guaranteed to see the same data.
- `tuner/adaptive.rs:263-278` (`reset`): Explicit reset path for retry scenarios.

---

#### P18. Determinism / reproducibility — Alignment: 3/3

This is the standout strength of the crate. The determinism architecture is comprehensive and explicitly documented:
- Three-layer hash contract: `spec_hash` + `envelope_hash` + `rulepack_fingerprint`, each BLAKE3-computed (`executor/result.rs:19-45`).
- `spec/hashing.rs` canonicalizes JSON key ordering via `BTreeMap` before hashing, preventing map-insertion-order nondeterminism.
- `providers/registration.rs:113-115`: Input registration sorted lexicographically for deterministic session state.
- `rules/registry.rs:111-134`: Ruleset fingerprint computed by hashing rule names in deterministic order (analyzer → optimizer → physical).
- `compiler/scheduling.rs:186-196`: `derive_cache_policies` sorts keys before iterating to ensure deterministic output.
- `compiler/plan_compiler.rs:186-196`: Topological sort tie-breaks via lexicographic sort of the queue.
- `session/envelope.rs` captures the full DataFusion version, session config snapshot, registered UDFs, and provider identities into the envelope hash.

---

### Category: Simplicity (19-22)

#### P19. KISS — Alignment: 2/3

**Current state:**
The codebase is largely lean. No speculative generality or over-engineered abstractions. Two functions are more complex than necessary due to inline logic duplication.

**Findings:**
- `providers/interval_align_provider.rs:301-365` (`build_output_schema`) and `providers/interval_align_provider.rs:368-529` (`build_interval_align_sql`): Both functions begin with the same four operations: `schema_columns(left_schema)`, `schema_columns(right_schema)`, emptiness check, `validate_required_columns(config, ...)`. The `select_columns` and `resolve_right_aliases` calls are then duplicated verbatim in both functions. This doubles the size of the provider module for no design benefit.
- `compiler/plan_compiler.rs:80-163` (`compile_with_warnings`): The function performs six sequential steps in one body. The six steps are well-commented but could each be a named method, allowing individual step testing and reducing visual complexity.

**Suggested improvement:**
Introduce `IntervalAlignContext::prepare(config, left_schema, right_schema) -> Result<Self>` to compute `left_keep`, `right_keep`, `right_aliases`, and `join_mode` once, shared between `build_output_schema` and `build_interval_align_sql`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
The feature set is well-justified by current use cases. One speculative duplication exists.

**Findings:**
- `spec/hashing.rs:68-78` defines `DeterminismContract { spec_hash: [u8; 32], envelope_hash: [u8; 32] }` and `is_replay_valid`. `executor/result.rs:82-86` defines `DeterminismContract { spec_hash: [u8; 32], envelope_hash: [u8; 32], rulepack_fingerprint: [u8; 32] }` and `is_replay_valid`. Both structs exist in the codebase but are used independently. The `spec/hashing.rs` version is a simpler subset that appears to have been superseded but not removed.
- `executor/result.rs:89-96` (`TuningHint` struct): Defined and included in `RunResult`, but no code in this crate or the test suite reads or populates `tuner_hints` in `RunResult`. The adaptive tuner (`tuner/adaptive.rs`) proposes `TunerConfig`, not `TuningHint`. The mapping from `TunerConfig` to `TuningHint` is not present.

**Suggested improvement:**
Remove `spec/hashing.rs::DeterminismContract` (unused at the contract level; the full three-hash version in `result.rs` is the canonical one). If `TuningHint` in `RunResult.tuner_hints` is not yet populated, either remove it from `RunResult` or add a `// TODO: populated by Python path` comment to document the intent.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
Public APIs generally match developer expectations. One naming violation creates unnecessary cognitive load.

**Findings:**
- `compiler/plan_compiler.rs:373-383` (`ensure_source_registered`): The name "ensure" combined with a return type of `Result<String>` reads as a predicate or idempotent state assertion. In reality, this function calls `ctx.register_table()` as a side effect (command). A developer seeing `let source = self.ensure_source_registered(...)` expects a pure check, not registration.
- `compiler/scheduling.rs:57-63` (`CachePolicyRequest`): The field `cache_overrides: BTreeMap<String, String>` maps view names to policy strings (`"none"`, `"delta_staging"`, `"delta_output"`). These raw strings are also what the `CachePolicyDecision.policy` field returns. A caller cannot tell from the type that only three values are valid without reading the `normalized_override` function at line 89.
- `executor/delta_writer.rs:294` (`let _ = (table_properties, enable_features, commit_metadata_required);`): The `DeltaWritePayload` struct has three fields that are immediately discarded in `execute_delta_write`. This is surprising — a caller populating these fields will silently have them ignored.

**Suggested improvement:**
Rename `ensure_source_registered` to `register_inline_source` to communicate the command nature. For `DeltaWritePayload`, either remove the three discarded fields (`table_properties`, `enable_features`, `commit_metadata_required`) and document them as future expansion, or pass them through to the underlying `DeltaWriteBatchesRequest`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
`SPEC_SCHEMA_VERSION: u32 = 4` at `spec/execution_spec.rs:16` provides a versioning point. `lib.rs` explicitly re-exports the four public contract types (`compile_request`, `compile_response_to_json`, `CompilePlanArtifact`, `CompileRequest`, `CompileResponse`). This is the correct narrowing of the public surface.

**Findings:**
- `spec/execution_spec.rs:24-59`: `SemanticExecutionSpec` has all fields `pub`. Since this type crosses the Python-Rust boundary via JSON, its field names are stable contracts. However, nothing in the code distinguishes "stable contract fields" (e.g., `version`, `input_relations`, `view_definitions`) from "internal implementation fields that happen to be public" (e.g., `cache_policy`, `maintenance`). The comment "Wave 3 expansion fields" is a marker but not a Rust stability annotation.
- `executor/warnings.rs:9-27` (`WarningCode` enum): 17 variants are all `pub`. Adding a new variant is a non-breaking change in Rust, but removing or renaming one would break any Python-side pattern-matching on the serialized `as_str()` form. No versioning documentation exists for which codes are considered stable.
- The `lib.rs` public re-export is appropriately narrow: only 5 symbols are exported at the crate root, meaning most internal types are inaccessible to downstream consumers.

**Suggested improvement:**
Add `/// # Stability` doc comments to `SemanticExecutionSpec` distinguishing "core contract" fields (stable, never removed) from "expansion fields" (may evolve). Add a comment to `WarningCode` documenting which codes are stable machine-readable contracts vs. internal diagnostics. Consider `#[non_exhaustive]` on `WarningCode` to signal that new variants may be added.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Quality (23-24)

#### P23. Design for testability — Alignment: 2/3

**Current state:**
The `spec/`, `compiler/scheduling.rs`, `compiler/cost_model.rs`, and `tuner/adaptive.rs` modules are highly testable: they are pure functions over plain data. The test suite is substantial with 37 test files and inline `#[cfg(test)]` modules across production files.

**Findings:**
- `compiler/plan_compiler.rs` in-module tests (`#[tokio::test]` at lines 568-782): All tests call `SessionContext::new()` rather than a `SessionFactory`-built context. This means tests run with none of the engine's configured rules, UDFs, or optimizer settings. Plan compilation tests do not exercise the actual pipeline rule configuration.
- `providers/interval_align_provider.rs:108` and `:564`: Two `SessionContext::new()` instances created in production code bypass the engine's configured session, making behavior in production different from test behavior (tests in `tests/interval_align_provider.rs` also use bare `SessionContext::new()`).
- `compiler/scheduling.rs` and `compiler/cost_model.rs`: Fully pure and testable. Tests at `scheduling.rs:351-403` and `cost_model.rs:227-249` work without any IO setup.
- `session/factory.rs:417-511`: The factory's own tests do use `SessionFactory` correctly but create `CpgRuleSet { analyzer_rules: vec![], optimizer_rules: vec![], ... }` directly, bypassing the fingerprint computation in `CpgRuleSet::new`. This means tests don't exercise the `compute_ruleset_fingerprint` path.

**Suggested improvement:**
Introduce a `test_session()` helper in `tests/common/mod.rs` that calls `SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small)).build_session_state(...)` with a standard empty `CpgRuleSet::new(vec![], vec![], vec![])`. This ensures plan compiler tests use the same session construction path as production, catching configuration-dependent issues.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability — Alignment: 1/3

**Current state:**
The tracing architecture is present and well-structured: `executor/tracing/` contains span bootstrap (`bootstrap.rs`), execution instrumentation (`exec_instrumentation.rs`), rule instrumentation (`rule_instrumentation.rs`), object store tracing (`object_store.rs`), and context (`context.rs`). The `tracing/mod.rs` provides clean no-op shims for the non-feature build.

**Findings:**
- `executor/tracing/mod.rs:18-99`: Every public function has a `#[cfg(feature = "tracing")]` implementation and a `#[cfg(not(feature = "tracing"))]` no-op fallback. This means that in the default build (without the `tracing` feature), no spans are opened, no pipeline stages are instrumented, and no OTEL exports occur. There is no `log` crate fallback or structured stderr output.
- `executor/runner.rs:182-198`: The execution span and OTEL lifecycle are entirely inside `#[cfg(feature = "tracing")]` blocks. A production run without the feature flag produces no observable telemetry.
- `executor/warnings.rs`: The `RunWarning` type with `WarningCode` and `WarningStage` is the only always-on observability mechanism. It is returned in `RunResult.warnings` but not logged to any sink during execution.
- `compiler/compile_phases.rs`, `executor/pipeline.rs`: No `log::debug!` or `log::info!` calls exist. Pipeline stage boundaries produce no observable output in the default build.
- `executor/tracing/exec_instrumentation.rs` and `rule_instrumentation.rs`: These provide rich rule-level tracing but only when the `tracing` feature is enabled. CI environments without this feature flag see no evidence of rule application.

**Suggested improvement:**
Add `log` crate (`log::info!`, `log::debug!`) calls at key pipeline boundaries in `execute_pipeline` — at compilation start/end, at materialization start per output, and at maintenance execution. These do not require the `tracing` feature and provide always-available structured evidence. Specifically: log the `spec_hash` hex at pipeline start, log each `target.table_name` with row count at materialization end, and log each `RunWarning` with its `stage` and `code` at emission. This costs no performance in production (log levels controlled by env) and provides actionable diagnostics without requiring the full OTEL stack.

**Effort:** medium
**Risk if unaddressed:** high

---

## Cross-Cutting Themes

### Theme 1: Duplicated Topological Sort Encodes Divergent Behavior

The `TaskGraph::topological_sort` (in `compiler/scheduling.rs:225-271`) and `SemanticPlanCompiler::topological_sort` (in `compiler/plan_compiler.rs:171-248`) implement the same Kahn's algorithm but differ in tie-breaking. The `SemanticPlanCompiler` version re-sorts the queue at every insertion, guaranteeing lexicographic order among ready nodes. The `TaskGraph` version uses FIFO insertion order. This means that for the same spec, the task schedule and the view compilation order may differ — the task schedule sees one topological order and the actual compilation sees another. Since both are used by `compile_with_warnings` (via the compiler) and `build_task_schedule_phase` (via the scheduling module), the emitted `TaskSchedule.execution_order` may not match the actual execution order observed at runtime.

**Root cause:** The plan compiler needed a topological sort but could not reuse `TaskGraph` (which works on string names, not `ViewDefinition` objects) without a refactor.
**Affected principles:** P7, P16, P18.
**Suggested approach:** Introduce `topological_sort_view_names(views: &[ViewDefinition]) -> Result<Vec<&str>>` as a pure free function in `scheduling.rs` that wraps `TaskGraph::topological_sort`. Let `SemanticPlanCompiler::topological_sort` delegate to it and look up `ViewDefinition` by name.

---

### Theme 2: `IntervalAlignProvider` Bypasses the Session Adapter

`IntervalAlignProvider::build_dataframe` (`interval_align_provider.rs:102-116`) creates an unconfigured `SessionContext::new()` to execute its internal join SQL. This is the only production code path in the crate that creates a session outside the `SessionFactory`. Consequences:
- No configured UDFs are available during the interval-align join.
- No optimizer rules are applied.
- No memory pool bounds are enforced.
- The planning-surface hash does not cover this session.

**Root cause:** The `TableProvider::scan` signature provides `state: &dyn Session` but the provider's internal computation uses a temporary materialization approach rather than composing with the caller's state.
**Affected principles:** P6, P16, P23.
**Suggested approach:** Change `IntervalAlignProvider::build_dataframe` to accept `state: &dyn Session` and use `state.create_physical_plan(&plan.build()?)` directly instead of routing through an internal `SessionContext`.

---

### Theme 3: Always-On Observability Gap

The warning system (`RunWarning`, `WarningCode`) is excellent for surfacing non-fatal issues in the returned `RunResult`, but it is a post-hoc mechanism — warnings are collected and returned, not emitted during execution. Combined with all OTEL spans gated behind `#[cfg(feature = "tracing")]`, there is no way to observe pipeline progress in real time in the default build. This affects both development debugging and production monitoring.

**Root cause:** Architectural decision to make the `tracing` feature optional for minimal builds, without a fallback logging layer.
**Affected principles:** P24, P2.
**Suggested approach:** Add the `log` crate as an unconditional dependency and add `log::info!` calls at the six pipeline stages in `execute_pipeline`. This does not break the feature-gated OTEL path and gives always-available observability at zero runtime cost when the log level is `Error`.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 DRY | Move `enforce_pushdown_contracts` to `providers/pushdown_contract.rs` and remove duplicate at `compiler/compile_phases.rs:131` | small | Eliminates divergence risk on any future `PushdownEnforcementMode` changes |
| 2 | P7 DRY | Extract `build_task_graph(spec)` free function; remove duplicated task-graph construction in `compile_phases.rs:38-54` and `pipeline.rs:330-358` | small | Single authoritative task-graph construction |
| 3 | P21 Least astonishment | Rename `ensure_source_registered` to `register_inline_source` to communicate command semantics | small | Removes "ensure" misnaming that reads as a predicate |
| 4 | P24 Observability | Add `log::info!` calls at pipeline stage boundaries in `execute_pipeline` (compilation start, materialization per output, maintenance) | small | Always-on structured output without requiring `tracing` feature |
| 5 | P20 YAGNI | Remove `spec/hashing.rs::DeterminismContract` (superseded by `result.rs::DeterminismContract`); add doc comment to `RunResult::tuner_hints` explaining it is unpopulated | small | Reduces dead code and signals incomplete feature |

---

## Recommended Action Sequence

1. **(P7) Extract and deduplicate `enforce_pushdown_contracts`.** Move the function to `providers/pushdown_contract.rs` as `pub(crate) fn enforce_pushdown_mode(...)`. Import from both `compile_phases.rs` and `pipeline.rs`. No behavior change.

2. **(P7) Deduplicate task-graph construction.** Add `pub fn build_task_graph_from_spec(spec: &SemanticExecutionSpec) -> Result<TaskGraph>` to `compiler/scheduling.rs`. Replace the two identical extraction blocks in `compile_phases.rs:38-54` and `pipeline.rs:330-358`.

3. **(P7 + P16) Unify topological sort.** Add lexicographic tie-breaking to `TaskGraph::topological_sort` to match the deterministic behavior of `SemanticPlanCompiler::topological_sort`. Remove `SemanticPlanCompiler::topological_sort` and delegate to `TaskGraph::from_inferred_deps` → `topological_order`. This requires mapping from `String` names back to `ViewDefinition` references, which is a lookup-table operation.

4. **(P24) Add `log` crate calls at pipeline boundaries.** In `executor/pipeline.rs::execute_pipeline`, add `log::info!` at steps 1, 3, and 5 with structured fields (spec_hash, target name, rows_written). In `executor/runner.rs::execute_and_materialize_with_plans`, log the write path chosen per output target.

5. **(P6) Fix `IntervalAlignProvider` session isolation.** Refactor `IntervalAlignProvider::scan` to pass `state` into the join computation, replacing the `SessionContext::new()` in `build_dataframe`. This is the highest-risk change (alters query execution path) and should be accompanied by regression tests in `tests/interval_align_provider.rs`.

6. **(P21) Rename `ensure_source_registered`.** Rename to `register_inline_source` (or make it a `void` side-effecting method). Update all six call sites in `compile_view`.

7. **(P22) Add stability annotations to `SemanticExecutionSpec`.** Add `/// # Stability` doc sections distinguishing core-contract fields from expansion fields. Add `#[non_exhaustive]` to `WarningCode` to signal new-variant additions are permitted.

8. **(P12) Introduce parameter wrapper types.** Introduce `SessionBuildParams` grouping `profile_name`, `spec_hash`, `tracing_config`, `build_warnings` for `build_session_state_internal`. Remove the `#[allow(clippy::too_many_arguments)]` suppression as a quality gate.
