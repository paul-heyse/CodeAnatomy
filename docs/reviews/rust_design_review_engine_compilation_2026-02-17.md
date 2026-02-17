# Design Review: Engine Compilation Pipeline

**Date:** 2026-02-17
**Scope:** `rust/codeanatomy_engine/src/compiler/`, `rust/codeanatomy_engine/src/spec/`, `rust/codeanatomy_engine/src/schema/`, `rust/codeanatomy_engine/src/stability/`
**Focus:** All principles (1-24)
**Depth:** deep (all files in scope)
**Files reviewed:** 37 source files across 4 modules

## Executive Summary

The engine compilation pipeline is well-architected with strong separation between the immutable spec contract (`spec/`), compilation orchestration (`compiler/`), schema utilities (`schema/`), and stability infrastructure (`stability/`). The prior review's systemic DRY weakness (avg 0.8/3) has been substantially addressed by S3's schema-hash unification and S5's cross-crate deduplication, but **three within-scope DRY violations remain as NEW findings**: duplicated `hash_bytes`/`blake3_hash_bytes`, duplicated `view_index` helpers, and parallel `compute_fanout`/`compute_ref_counts` implementations. The spec module is the strongest area (excellent information hiding, parse-don't-validate, illegal-state prevention via tagged enums). The compilation pipeline has good functional core separation but `compile_contract.rs:compile_request` is a 200-line orchestrator that could benefit from decomposition.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | - | - | Spec internals well-hidden behind `SemanticExecutionSpec::new()` |
| 2 | Separation of concerns | 2 | medium | medium | `compile_contract.rs` mixes scheduling, pushdown probing, and artifact assembly |
| 3 | SRP (one reason to change) | 2 | medium | medium | `plan_bundle.rs` (1268 LOC) handles runtime capture, artifact assembly, diffing, explain, lineage extraction, and migration |
| 4 | High cohesion, low coupling | 2 | small | low | `compile_contract.rs` directly imports from 10+ modules |
| 5 | Dependency direction | 3 | - | - | Core spec depends on nothing; compiler depends on spec |
| 6 | Ports & Adapters | 2 | medium | low | `policy_ops.rs` uses raw `serde_json::Value` instead of typed domain objects |
| 7 | DRY (knowledge) | 1 | small | medium | NEW: 3 duplicated helpers across compiler modules |
| 8 | Design by contract | 3 | - | - | `SemanticExecutionSpec` has explicit version, hash, schema contracts |
| 9 | Parse, don't validate | 3 | - | - | `ViewTransform` tagged enum, `ParameterValue::to_scalar_value()` parse at boundary |
| 10 | Make illegal states unrepresentable | 3 | - | - | `ViewTransform` enum variants encode exactly the valid fields per transform |
| 11 | CQS | 2 | small | low | `ensure_source_registered` in `plan_compiler.rs:370` is documented side-effecting query |
| 12 | Dependency inversion | 2 | medium | low | `cache_policy.rs` takes `&SessionContext` directly rather than a trait |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance; all behavior composed via function delegation |
| 14 | Law of Demeter | 2 | small | low | `compile_contract.rs:160-162`: `prepared.ctx.state().config().options()` chain |
| 15 | Tell, don't ask | 2 | small | low | `diff_artifacts` rebuilds summary by querying 16 boolean flags |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure transforms well-separated except `compile_request` which mixes IO + logic |
| 17 | Idempotency | 3 | - | - | Compilation is deterministic given same spec + context |
| 18 | Determinism / reproducibility | 3 | - | - | blake3 digests, canonical spec hashing, deterministic sort tie-breaking |
| 19 | KISS | 2 | small | low | `PlanDiff` has 16+ boolean fields; could use a change set enum |
| 20 | YAGNI | 2 | small | low | `_metrics_store: Option<&MetricsStore>` accepted but unused in cache_policy |
| 21 | Least astonishment | 3 | - | - | API naming is clear and predictable |
| 22 | Declare and version public contracts | 3 | - | - | `SPEC_SCHEMA_VERSION = 4`, `artifact_version: 2`, `migrate_artifact` |
| 23 | Design for testability | 2 | small | low | Tests use heavyweight `SessionContext`; pure logic not always separated |
| 24 | Observability | 2 | medium | medium | `compile_request` has `#[instrument]` but inner phases lack span boundaries |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 3/3

**Current state:**
The `spec/` module is exemplary. `SemanticExecutionSpec` computes and stores its own canonical hash on construction (`execution_spec.rs:91`), hiding the hashing strategy. The `spec_hash` field is `#[serde(skip)]` (`execution_spec.rs:58`), preventing external code from serializing/deserializing stale hashes. `RuntimeConfig` uses `#[serde(deny_unknown_fields)]` (`runtime.rs:200`) preventing accidental coupling to internal field names.

No action needed.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most modules have clear concerns: `graph_validator.rs` validates structure, `semantic_validator.rs` validates semantics, `plan_compiler.rs` orchestrates compilation. However, `compile_contract.rs` (`compile_contract.rs:73-266`) is a 200-line orchestrator that mixes four distinct concerns: (1) session preparation, (2) compilation + warning collection, (3) task graph construction + scheduling + cost derivation, and (4) pushdown probe extraction + artifact assembly.

**Findings:**
- `compile_contract.rs:90-130`: Task graph construction, cost derivation, and scheduling are interleaved with warning collection -- this is scheduling concern code mixed with compilation orchestration.
- `compile_contract.rs:132-153`: Pushdown probe extraction and provider probing is a third distinct concern within the same function.
- `compile_contract.rs:155-248`: Per-output artifact loop mixes optimizer trace capture, pushdown verification, and plan bundle building.

**Suggested improvement:**
Extract three focused helper functions: `build_task_schedule(spec) -> (TaskSchedule, costs, warnings)`, `probe_pushdown_contracts(ctx, spec) -> (probes, warnings)`, and `capture_output_artifacts(ctx, outputs, probes, ...) -> (artifacts, warnings)`. The orchestrator then becomes a linear composition of these phases.

**Effort:** medium
**Risk if unaddressed:** medium -- any change to scheduling, pushdown, or artifact logic requires reading the entire 200-line function

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
`plan_bundle.rs` (1268 LOC) has at least 5 distinct change reasons: (1) runtime plan capture, (2) artifact assembly, (3) plan diffing, (4) explain capture, (5) lineage/UDF extraction, and (6) artifact migration. The file is well-organized with section comments but all code shares the same module scope.

**Findings:**
- `plan_bundle.rs:304-317`: `capture_plan_bundle_runtime` -- plan capture concern
- `plan_bundle.rs:342-480`: `build_plan_bundle_artifact_with_warnings` -- artifact assembly concern
- `plan_bundle.rs:489-586`: `diff_artifacts` -- diffing concern
- `plan_bundle.rs:643-703`: `extract_provider_lineage`, `extract_referenced_tables`, `extract_required_udfs` -- lineage extraction concern
- `plan_bundle.rs:707-727`: `migrate_artifact` -- schema migration concern

**Suggested improvement:**
Split `plan_bundle.rs` into focused submodules: `plan_bundle/capture.rs` (runtime capture), `plan_bundle/artifact.rs` (assembly + migration), `plan_bundle/diff.rs` (diffing), `plan_bundle/lineage.rs` (lineage extraction). The parent `plan_bundle/mod.rs` re-exports the public surface.

**Effort:** medium
**Risk if unaddressed:** low -- the file is well-organized internally, but changes to any one concern require navigating 1268 LOC

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Module boundaries are generally well-drawn. The `spec/` module is highly cohesive with low coupling. However, `compile_contract.rs` imports from 10+ sibling modules, which reflects its orchestrator role but also its breadth of concerns.

**Findings:**
- `compile_contract.rs:1-35`: 13 import groups from across the crate -- this is the highest fan-in file in scope.
- `cache_boundaries.rs` and `cache_policy.rs` are closely related but split into two files with `cache_boundaries` importing from `cache_policy` -- this split is appropriate but the naming could be clearer (`cache_boundaries` = insertion logic, `cache_policy` = decision logic).

**Suggested improvement:**
No structural change needed beyond the P2 decomposition of `compile_contract.rs`, which would naturally reduce its import fan-in.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
The dependency direction is correct: `spec/` has zero dependencies on `compiler/`, `schema/`, or `stability/`. The `compiler/` module depends on `spec/` for its input contract. `stability/` depends on `compiler/` for its optimizer lab but not vice versa. `schema/` is a leaf utility with no internal dependencies.

No action needed.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `spec/` module serves as a clean port between Python and Rust. However, `schema/policy_ops.rs` operates entirely on `serde_json::Value` objects (`policy_ops.rs:5-65`), which is an untyped boundary.

**Findings:**
- `policy_ops.rs:27-53`: `dataset_name`, `dataset_schema`, `dataset_policy`, `dataset_contract` all accept `&Value` and return `Value` or `Option<String>` -- there is no typed domain model for dataset policies.
- `policy_ops.rs:55-65`: `apply_scan_policy` and `apply_delta_scan_policy` perform recursive JSON merging without any schema validation.

**Suggested improvement:**
Introduce typed structs for `DatasetPolicy`, `ScanPolicy`, and `DeltaScanPolicy` that are deserialized at the boundary, replacing the raw `Value` threading. The merge operation would then be a typed field-by-field overlay.

**Effort:** medium
**Risk if unaddressed:** low -- these functions are currently small and well-contained

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The prior review round (S3) successfully unified schema hashing between `plan_bundle.rs` and `introspection.rs` -- `plan_bundle.rs:632-634` now delegates to `hash_arrow_schema`. However, **three NEW within-scope DRY violations** exist:

**Findings:**

1. **Duplicated blake3 byte hashing:**
   - `plan_bundle.rs:623-627`: `fn blake3_hash_bytes(bytes: &[u8]) -> [u8; 32]` using `blake3::Hasher`
   - `optimizer_pipeline.rs:276-278`: `fn hash_bytes(bytes: &[u8]) -> [u8; 32]` using `blake3::hash`
   - These implement identical semantics (BLAKE3 over bytes) with different function names and slightly different implementations. Both are private, preventing cross-module reuse.

2. **Duplicated view index construction:**
   - `graph_validator.rs:99-104`: `fn view_index(spec) -> BTreeMap<&str, &ViewDefinition>`
   - `pushdown_probe_extract.rs:34-39`: `fn view_index(spec) -> HashMap<&str, &ViewDefinition>`
   - These differ only in map type (`BTreeMap` vs `HashMap`) but encode the same knowledge: "index views by name for O(1) lookup."

3. **Parallel fanout/ref-count computation:**
   - `cache_boundaries.rs:20-41`: `fn compute_fanout(spec) -> HashMap<String, usize>` (owned keys)
   - `plan_compiler.rs:387-408`: `fn compute_ref_counts(&self) -> HashMap<&'a str, usize>` (borrowed keys)
   - These implement identical logic (count downstream references per view) with different key ownership strategies.

4. **Duplicated `normalize_physical`:**
   - `plan_bundle.rs:618-620`: `fn normalize_physical(plan: &dyn ExecutionPlan) -> String`
   - `optimizer_pipeline.rs:272-274`: `fn normalize_physical(plan: &dyn ExecutionPlan) -> String`
   - Identical implementations using `displayable(plan).indent(true)`.

**Suggested improvement:**
Create a shared `compiler/hashing.rs` utility with `pub(crate) fn blake3_bytes(bytes: &[u8]) -> [u8; 32]` and `pub(crate) fn normalize_physical(plan: &dyn ExecutionPlan) -> String`. Create a `compiler/spec_index.rs` utility with `pub(crate) fn view_index(spec) -> HashMap<&str, &ViewDefinition>` and `pub(crate) fn compute_view_fanout(spec) -> HashMap<String, usize>`.

**Effort:** small (< 1 hour for all four)
**Risk if unaddressed:** medium -- divergent implementations can produce inconsistent hashes or fanout counts if modified independently

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are explicit and well-enforced:
- `SemanticExecutionSpec` has a versioned schema (`SPEC_SCHEMA_VERSION = 4`, `execution_spec.rs:16`)
- `PlanBundleArtifact` has `artifact_version: u32` with `migrate_artifact` for forward compatibility (`plan_bundle.rs:707-727`)
- `#[serde(deny_unknown_fields)]` is applied consistently to boundary types (`relations.rs:8,18,31`, `outputs.rs:20`, `runtime.rs:200`)
- `validate_graph` enforces preconditions before compilation (`graph_validator.rs:36-97`)

No action needed.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The `spec/` module exemplifies parse-don't-validate:
- `ViewTransform` is a tagged enum (`relations.rs:32-112`) that can only represent valid transform configurations
- `ParameterValue` variants parse to `ScalarValue` at the boundary (`parameters.rs:77-93`)
- `CachePlacementPolicy` uses typed enums (`CacheAction`) rather than string configuration
- `SemanticExecutionSpec::new()` computes the canonical hash eagerly, ensuring the hash is always current

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 3/3

**Current state:**
The type system prevents many invalid states:
- `ViewTransform` variants encode exactly the required fields per transform type -- a `Relate` cannot exist without `left`, `right`, `join_type`, and `join_keys` (`relations.rs:39-44`)
- `CpgOutputKind` is a closed enum (`relations.rs:116-123`) preventing invalid output families
- `ParameterTarget` uses tagged enum variants (`PlaceholderPos` vs `FilterEq`) making it impossible to have a parameter with both a position and a filter column (`parameters.rs:33-42`)
- `ReplayCompatibilityFlags` uses booleans rather than stringly-typed indicators (`plan_bundle.rs:160-174`)

No action needed.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS cleanly. The one notable exception is explicitly documented.

**Findings:**
- `plan_compiler.rs:370-380`: `ensure_source_registered` is documented as "intentionally side-effecting" -- it registers a table in the `SessionContext` (command) AND returns the source name (query). The documentation is honest but the method still violates CQS.

**Suggested improvement:**
Split into `register_inline_source(ctx, source, cache)` (command, no return) and keep the `resolve_source` method (`plan_compiler.rs:415-425`) as the query. The caller would call `register_inline_source` first, then `resolve_source`.

**Effort:** small
**Risk if unaddressed:** low -- the violation is documented and contained

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
Dependencies are passed in rather than created internally. `SemanticPlanCompiler::new(ctx, spec)` takes both collaborators by reference. However, the `cache_policy.rs:compute_cache_boundaries` function takes `&SessionContext` directly (`cache_policy.rs:104-110`) rather than a trait for row estimation, making it untestable without a full DataFusion session.

**Findings:**
- `cache_policy.rs:167-180`: `estimate_view_rows` requires a full `SessionContext` to create a physical plan for row estimation. The test module works around this with a "dummy SessionContext" wrapper (`cache_policy.rs:399-410`).

**Suggested improvement:**
Extract a `trait RowEstimator { async fn estimate_rows(&self, view_name: &str) -> u64 }` and have `compute_cache_boundaries` accept `&dyn RowEstimator`. The production implementation wraps `SessionContext`; tests provide a stub returning fixed values.

**Effort:** medium
**Risk if unaddressed:** low -- tests already work via the SessionContext fallback

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
There is no inheritance in the scope. All behavior is composed through function delegation and struct composition. `StabilityFixture` is a trait used for test infrastructure, not an inheritance hierarchy.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. One notable chain exists in `compile_contract.rs`.

**Findings:**
- `compile_contract.rs:160-162`: `prepared.ctx.state().config().options().optimizer.max_passes` -- a 4-level chain reaching into DataFusion's configuration internals. This is fragile if DataFusion's configuration API changes.
- `compile_contract.rs:165-169`: Same pattern for `options.optimizer.skip_failed_rules`.

**Suggested improvement:**
Extract a helper `fn optimizer_config_from_ctx(ctx: &SessionContext) -> OptimizerPipelineConfig` that encapsulates the chain. This localizes the DataFusion API coupling to one place.

**Effort:** small
**Risk if unaddressed:** low -- but a DataFusion version upgrade would require changes in multiple places if the pattern spreads

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most code follows tell-don't-ask well. The `diff_artifacts` function is the main exception.

**Findings:**
- `plan_bundle.rs:489-586`: `diff_artifacts` queries 16 boolean fields from two artifacts to build a summary. The function is 97 lines of `if changed { summary.push(...) }` patterns. The artifact does not encapsulate its own change description.

**Suggested improvement:**
Add a method `PlanBundleArtifact::changed_aspects(&self, other: &Self) -> Vec<ChangedAspect>` where `ChangedAspect` is an enum variant per diffable dimension. The `diff_artifacts` function then becomes a thin wrapper that maps `ChangedAspect` to `PlanDiff` fields.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The spec module is purely functional. The compiler module has a clear separation: pure functions (`graph_validator::validate_graph`, `inline_policy::compute_inline_policy`, `scheduling::TaskGraph::from_inferred_deps`) vs async orchestration (`plan_compiler::compile`, `compile_contract::compile_request`). However, `compile_request` mixes IO (session preparation, table probing) with pure logic (cost derivation, schedule computation).

**Findings:**
- `compile_contract.rs:73-266`: `compile_request` is the imperative shell but also contains pure logic (task graph construction, cost model derivation) that could be extracted and tested independently.
- `semantic_validator.rs:69-322`: `validate_semantics` is mostly pure analysis but requires `&SessionContext` for schema resolution of input relations (`semantic_validator.rs:78-83`). This forces async even though most validation is synchronous.

**Suggested improvement:**
Extract the pure scheduling/cost logic from `compile_request` into a synchronous `fn build_schedule_and_costs(spec) -> ScheduleOutcome` function. For `validate_semantics`, consider pre-resolving input schemas into a `HashMap<String, BTreeMap<String,String>>` before calling the validator, making the core validation pure.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Compilation is inherently idempotent: given the same `SemanticExecutionSpec` and `SessionContext`, the same outputs are produced. The `spec_hash` ensures identity. `capture_plan_bundle_runtime` produces deterministic plans. Tests explicitly verify digest stability across runs (`plan_bundle.rs:865-913`).

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class design concern:
- Topological sort uses sorted tie-breaking for deterministic ordering (`plan_compiler.rs:188-191`)
- Plan digests use BLAKE3 over normalized text (`plan_bundle.rs:444-446`)
- Spec hashing uses canonical JSON with sorted keys (`hashing.rs:28-33`)
- `DeterminismContract` explicitly tracks spec + envelope hashes for replay validation (`hashing.rs:68-79`)
- `schedule_tasks_with_quality` falls back to deterministic topological order when statistics quality is unknown (`cost_model.rs:172-198`)

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most code is straightforward. The `PlanDiff` struct is the main complexity outlier.

**Findings:**
- `plan_bundle.rs:241-285`: `PlanDiff` has 20 fields (16 booleans + 4 optional codec digests). This is a flat bag of flags rather than a structured representation of what changed.
- `plan_compiler.rs:188-215`: The topological sort re-sorts the queue on every insertion for determinism. While correct, using a `BTreeSet` as the queue would be simpler and equally deterministic.

**Suggested improvement:**
For `PlanDiff`, consider a `changes: Vec<PlanChangeKind>` where `PlanChangeKind` is an enum (`P0DigestChanged`, `SchemaChanged(SchemaFingerprints, SchemaFingerprints)`, etc.). The boolean flags can be derived as `changes.iter().any(|c| matches!(c, PlanChangeKind::P0DigestChanged))`. For the topological sort, replace the `VecDeque` + re-sort pattern with a `BTreeSet<&str>`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code is lean. Two instances of accepted-but-unused parameters exist.

**Findings:**
- `cache_policy.rs:109`: `_metrics_store: Option<&MetricsStore>` is accepted but unused (reserved for `use_historical_metrics`). The parameter is documented as "currently accepted but unused."
- `cache_policy.rs:38-39`: `use_historical_metrics: bool` field on `CachePlacementPolicy` is serialized and checked but the historical metrics path is never implemented.
- `cost_model.rs:68-70`: `rows_factor` from `CollectedMetrics` is used but `output_rows` is the only signal consumed -- the `CollectedMetrics` type likely has richer data that is not used.

**Suggested improvement:**
Mark `_metrics_store` and `use_historical_metrics` with a `// TODO: implement historical metrics learning` comment and add `#[allow(unused)]` if the compiler warns. The current approach of accepting the parameter to maintain API stability is acceptable given the documented intent.

**Effort:** small
**Risk if unaddressed:** low -- the parameter placeholder is honest and forward-compatible

---

#### P21. Least astonishment -- Alignment: 3/3

**Current state:**
API naming is clear and predictable:
- `capture_plan_bundle_runtime` / `build_plan_bundle_artifact` clearly distinguish runtime handles from persisted artifacts
- `validate_graph` / `validate_semantics` clearly distinguish structural from semantic validation
- `compile` / `compile_with_warnings` follows DataFusion's own pattern
- `ForceCache` / `NeverCache` are unambiguous

No action needed.

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
Contract versioning is explicit and well-managed:
- `SPEC_SCHEMA_VERSION = 4` (`execution_spec.rs:16`)
- `artifact_version: 2` in `PlanBundleArtifact` (`plan_bundle.rs:79`)
- `migrate_artifact` handles version 1 -> 2 migration (`plan_bundle.rs:707-727`)
- `#[serde(deny_unknown_fields)]` on all boundary types prevents schema drift
- `#[serde(default, skip_serializing_if = "...")]` on optional fields enables backward-compatible evolution (`plan_bundle.rs:125-147`)

No action needed.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Tests are comprehensive -- every module has inline `#[cfg(test)]` tests. However, several tests require a full `SessionContext` even when testing pure logic.

**Findings:**
- `cache_policy.rs:399-410`: `compute_cache_boundaries_sync` creates a dummy `SessionContext` to test policy logic that is conceptually pure.
- `plan_compiler.rs:556-570`: `setup_test_context` builds a full context with MemTable for topological sort tests that don't actually execute queries.
- `plan_bundle.rs:797-814`: `test_ctx()` builds a full context for digest determinism tests.

**Suggested improvement:**
The P12 suggestion (extracting a `RowEstimator` trait for cache policy) would eliminate the need for SessionContext in cache policy tests. For plan_compiler tests, the topological sort is already pure -- the test context is only needed for the integration-level `test_compile_simple_spec` test, which is appropriate.

**Effort:** small
**Risk if unaddressed:** low -- tests work correctly, they are just heavier than necessary

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`compile_contract.rs:72` has `#[cfg_attr(feature = "tracing", instrument(skip(request)))]` for the top-level compile function. The warning system (`RunWarning` with `WarningCode` and `WarningStage`) provides structured observability for non-fatal issues.

**Findings:**
- `compile_contract.rs:73-266`: Only the top-level `compile_request` has a tracing span. The inner phases (scheduling, pushdown probing, artifact capture) lack their own spans, making it difficult to attribute latency to specific compilation phases.
- `plan_compiler.rs:75-159`: `compile_with_warnings` has no tracing instrumentation despite being the core compilation orchestrator.
- `optimizer_pipeline.rs:80-144`: `run_optimizer_pipeline` captures duration internally but does not emit tracing spans.

**Suggested improvement:**
Add `#[instrument]` attributes to `compile_with_warnings`, `run_optimizer_pipeline`, `build_plan_bundle_artifact_with_warnings`, and `extract_input_filter_predicates`. These are the natural phase boundaries within the compilation pipeline. This aligns with the `datafusion-tracing` patterns already used in the execution layer.

**Effort:** medium
**Risk if unaddressed:** medium -- latency attribution in production compilation is blind beyond the top-level span

---

## Cross-Cutting Themes

### Theme 1: Within-scope helper duplication (NEW)

The prior review (S3, S5) successfully eliminated cross-crate DRY violations. However, **within the `compiler/` module**, four pairs of duplicated helpers have accumulated. This is a natural consequence of modules being developed independently -- each file created its own private utilities for hashing, plan normalization, view indexing, and fanout computation. The risk is drift: if one copy is updated (e.g., to change the hashing algorithm), the other may not be.

**Affected principles:** P7
**Root cause:** Private (`fn`, not `pub(crate) fn`) helper scope prevented cross-module reuse.
**Suggested approach:** Create `compiler/common.rs` with `pub(crate)` exports for the four duplicated helpers.

### Theme 2: compile_request is the monolithic orchestrator

`compile_contract.rs:compile_request` is the single function that ties together session preparation, compilation, scheduling, pushdown probing, and artifact assembly. While orchestrators inherently have broad scope, this one conflates pure logic (task graph, cost model) with IO (session creation, table probing). This is the main testability and maintainability concern in the scope.

**Affected principles:** P2, P4, P16, P24
**Root cause:** The compile contract evolved by accretion as new capabilities (scheduling, pushdown, artifacts) were added to the compilation pipeline.
**Suggested approach:** Decompose into phase-specific helper functions that are independently testable, then compose them in the orchestrator.

### Theme 3: plan_bundle.rs scope creep

At 1268 LOC, `plan_bundle.rs` is the largest file in scope and handles 5+ distinct concerns. While the file is well-organized with section comments, it would benefit from decomposition into focused submodules.

**Affected principles:** P3, P15
**Root cause:** Plan bundle is the central artifact type, so all artifact-related logic naturally gravitates to it.
**Suggested approach:** Split into `plan_bundle/` directory with focused submodules.

## Python Integration Boundary Assessment

### Compile Contract Surface

`compile_contract.rs` defines `CompileRequest` and `CompileResponse` as the boundary types. `CompileResponse` derives `Serialize, Deserialize` (`compile_contract.rs:52-53`) and is JSON-serializable via `compile_response_to_json` (`compile_contract.rs:269-272`). This is the JSON boundary consumed by `codeanatomy_engine_py/src/materializer.rs`.

**Assessment:** The boundary is well-defined:
- `CompileResponse` includes all scheduling metadata (`task_schedule`, `task_costs`, `bottom_level_costs`, `slack_by_task`, `dependency_map`) making the Python side's scheduling decisions data-driven.
- `provider_identities` and `plan_artifacts` carry the full determinism contract.
- `warnings` are structured with `WarningCode` and `WarningStage` for programmatic handling on the Python side.
- `CompileRequest` uses references (`&'a`) so it cannot be directly serialized from Python -- it is an internal Rust-side request object that the Python bridge must construct.

**One concern:** `CompileRequest` takes `&CpgRuleSet` which contains `Arc<dyn OptimizerRule>` objects -- these are not serializable. The Python bridge must construct the ruleset on the Rust side, which is the correct pattern.

### SemanticExecutionSpec Contract

`SemanticExecutionSpec` (`execution_spec.rs:22-59`) is the "immutable contract between Python and Rust" as documented. The `#[serde(deny_unknown_fields)]` attribute ensures Python cannot pass unexpected fields. The `SPEC_SCHEMA_VERSION` constant enables version-aware deserialization.

**Assessment:** Strong boundary design. The `spec_hash` field being `#[serde(skip)]` means Python must accept that the hash is recomputed on the Rust side after deserialization, which is the correct security posture (prevents hash spoofing).

### DataFusion/DeltaLake API Usage Assessment (dfdl_ref-informed)

1. **LogicalPlan tree walking:** `plan_bundle.rs:648-670` and `pushdown_probe_extract.rs:356-382` use manual `stack.push(plan.inputs())` iteration. DataFusion provides `plan.apply(|node| ...)` via `TreeNode` trait (`lineage.rs:37` already uses this). The manual iteration in `plan_bundle.rs` and `pushdown_probe_extract.rs` is functionally correct but inconsistent with the `TreeNode` API used in `lineage.rs`.

   **Recommendation:** Migrate `extract_provider_lineage` and `collect_filter_observations` to use `plan.apply()` for consistency. This is a small change with no behavioral difference.

2. **OptimizerRule trait conformance:** `optimizer_pipeline.rs:201-238` uses `Optimizer::with_rules(rules)` and the `optimize()` method with an observer closure. This correctly uses the DataFusion 51.x `Optimizer` API. The `OptimizerContext::new().with_max_passes().with_skip_failing_rules()` pattern (`optimizer_pipeline.rs:210-212`) correctly uses the builder API.

3. **SessionStateBuilder pattern:** The scope does not construct `SessionState` directly -- it relies on `SessionContext::new()` and `ctx.state()`. This is appropriate for this layer, which operates above session construction.

4. **Physical plan statistics:** `cache_policy.rs:172` uses `plan.partition_statistics(None)` which is the correct DataFusion 51+ API (replacing the deprecated `plan.statistics()`). The comment at line 171 correctly notes this.

5. **Plan normalization:** `plan_bundle.rs:610-620` uses `plan.display_indent()` for logical plans and `displayable(plan).indent(true)` for physical plans. These are the correct DataFusion APIs for stable plan text representations.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Extract shared `blake3_bytes`/`normalize_physical` into `compiler/common.rs` | small | Eliminates 4 duplicated implementations, prevents hash algorithm drift |
| 2 | P7 | Extract shared `view_index`/`compute_view_fanout` into `compiler/common.rs` | small | Eliminates 2 duplicated view-indexing implementations |
| 3 | P14 | Extract `optimizer_config_from_ctx` helper in `compile_contract.rs` | small | Localizes DataFusion configuration API coupling |
| 4 | P11 | Split `ensure_source_registered` into command + query | small | Clean CQS alignment in plan_compiler |
| 5 | P24 | Add `#[instrument]` to `compile_with_warnings`, `run_optimizer_pipeline`, `build_plan_bundle_artifact_with_warnings` | small | Enables per-phase latency attribution |

## Recommended Action Sequence

1. **Create `compiler/common.rs`** with `pub(crate)` exports for `blake3_bytes`, `normalize_physical`, `normalize_logical`, `view_index`, and `compute_view_fanout`. Update `plan_bundle.rs`, `optimizer_pipeline.rs`, `graph_validator.rs`, `pushdown_probe_extract.rs`, `cache_boundaries.rs`, and `plan_compiler.rs` to import from the shared module. (P7, quick wins #1 and #2)

2. **Add tracing spans** to `compile_with_warnings`, `run_optimizer_pipeline`, `build_plan_bundle_artifact_with_warnings`, and `extract_input_filter_predicates`. (P24, quick win #5)

3. **Extract `compile_contract.rs` helper functions** -- `build_task_schedule`, `probe_pushdown_contracts`, `capture_output_artifacts` -- to decompose the monolithic orchestrator. (P2, P4, P16)

4. **Split `plan_bundle.rs`** into `plan_bundle/` directory with `capture.rs`, `artifact.rs`, `diff.rs`, `lineage.rs`, and `mod.rs`. (P3)

5. **Migrate manual plan tree walking** in `plan_bundle.rs:extract_provider_lineage` and `pushdown_probe_extract.rs:collect_filter_observations` to use `plan.apply()` for consistency with `lineage.rs`. (DataFusion API alignment)

6. **Extract `RowEstimator` trait** from `cache_policy.rs` to decouple cache boundary computation from `SessionContext`. (P12, P23)

7. **Introduce `PlanChangeKind` enum** as an alternative to `PlanDiff`'s 16 boolean fields. (P15, P19)
