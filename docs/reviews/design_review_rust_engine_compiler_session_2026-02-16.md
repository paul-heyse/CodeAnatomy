# Design Review: Rust Engine Compiler, Session, and Spec Modules

**Date:** 2026-02-16
**Scope:** `rust/codeanatomy_engine/src/{compiler,session,spec,schema,stability,compat}/`
**Focus:** Boundaries (1-6) weighted heavily, Correctness (16-18) weighted heavily, Knowledge (7-11) weighted heavily; all 24 principles scored
**Depth:** deep (all files in scope)
**Files reviewed:** 50 (compiler: 23, session: 10, spec: 9, schema: 3, stability: 3, compat: 2)

## Executive Summary

The Rust engine modules demonstrate strong architectural alignment overall, with particularly excellent work on determinism contracts (P18), information hiding between the `PlanBundleRuntime`/`PlanBundleArtifact` split (P1), and the immutable `SemanticExecutionSpec` boundary contract (P22). The primary areas for improvement center on three themes: (1) duplicated knowledge across session capture paths -- config key lists, information_schema SQL queries, and governance enums are defined in multiple locations (P7); (2) a CQS tension in `resolve_source_name` which silently registers tables as a side effect of name resolution (P11); and (3) a `SchemaDiff` type alias that should be a named struct to make illegal states more self-documenting (P10). The codebase is well-tested and the functional-core pattern is applied consistently in the compiler module.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | - | - | Runtime/Artifact split cleanly separates live handles from persisted payload |
| 2 | Separation of concerns | 2 | medium | medium | `compile_contract.rs:70-266` mixes orchestration, scheduling, pushdown probing, and artifact construction |
| 3 | SRP (one reason to change) | 2 | medium | low | `plan_bundle.rs` serves both artifact construction and diff/migration concerns |
| 4 | High cohesion, low coupling | 2 | small | low | Duplicated config-snapshot SQL in `envelope.rs` and `planning_manifest.rs` creates implicit coupling |
| 5 | Dependency direction | 3 | - | - | Core spec types depend on nothing; compiler/session depend inward on spec |
| 6 | Ports & Adapters | 2 | medium | low | `policy_ops.rs` uses `serde_json::Value` loosely; no typed port for Python policy boundary |
| 7 | DRY (knowledge) | 1 | medium | high | Schema hashing, config key lists, governance enums, and SQL queries duplicated across modules |
| 8 | Design by contract | 3 | - | - | `#[serde(deny_unknown_fields)]` on all spec types; `validate_graph` as explicit precondition |
| 9 | Parse, don't validate | 3 | - | - | `ViewTransform` tagged enum with typed variants; `SemanticExecutionSpec` parsed once at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `SchemaDiff` is a bare tuple alias; `PlanDiff` uses 16 boolean fields |
| 11 | CQS | 2 | small | medium | `resolve_source_name` registers tables as side effect while appearing as a query |
| 12 | DI + explicit composition | 3 | - | - | `SessionFactory` and `PlanningSurfaceSpec` use explicit builder composition |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; behavior composed via spec/builder/adapter patterns |
| 14 | Law of Demeter | 2 | small | low | `compile_contract.rs:157-169` chains `ctx.state().config().options().optimizer.*` |
| 15 | Tell, don't ask | 2 | small | low | `compile_contract.rs:213-216` asks spec for `input_relations` to compute `deterministic_inputs` externally |
| 16 | Functional core, imperative shell | 3 | - | - | `inline_policy.rs`, `hashing.rs`, `graph_validator.rs` are pure; IO at edges in `plan_compiler.rs` |
| 17 | Idempotency | 3 | - | - | All hashing, validation, and diff operations are idempotent by construction |
| 18 | Determinism / reproducibility | 3 | - | - | BLAKE3 digests, canonical JSON, sorted name vectors, `DeterminismContract` |
| 19 | KISS | 2 | small | low | `PlanDiff` has 22 fields; `diff_artifacts` is 100 lines of mechanical comparisons |
| 20 | YAGNI | 3 | - | - | No speculative abstractions; seams exist for extension (e.g., `artifact_version`) |
| 21 | Least astonishment | 2 | small | medium | `resolve_source_name` name suggests pure lookup but performs registration |
| 22 | Declare and version contracts | 3 | - | - | `SPEC_SCHEMA_VERSION`, `artifact_version`, `#[serde(deny_unknown_fields)]` |
| 23 | Design for testability | 3 | - | - | Pure functions dominate; tests use in-memory tables; no heavyweight setup |
| 24 | Observability | 2 | medium | low | Tracing infrastructure exists but warning codes are not structured as enums with diagnostic context |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 3/3

**Current state:**
The `PlanBundleRuntime` / `PlanBundleArtifact` split at `plan_bundle.rs:36-147` is a textbook example of information hiding. Runtime plan handles (live `LogicalPlan`, `ExecutionPlan` objects) are kept in a non-serializable struct, while the persisted payload contains only canonical digests and optional text. Callers never need to know how DataFusion plan objects work internally -- they interact with stable BLAKE3 digests and versioned artifacts.

**Findings:**
- `plan_bundle.rs:36-43`: `PlanBundleRuntime` correctly uses `#[derive(Debug)]` without `Serialize/Deserialize`, enforcing the boundary
- `plan_bundle.rs:76-147`: `PlanBundleArtifact` exposes only serializable fields with versioning
- `planning_surface.rs:31-56`: `PlanningSurfaceSpec` encapsulates all planning-time registration decisions

No action needed.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most modules have clean concern separation. The `compile_contract.rs` function `compile_request` is the primary exception -- it orchestrates compilation, scheduling, cost modeling, pushdown probing, optimizer tracing, and artifact construction all within a single 196-line function.

**Findings:**
- `compile_contract.rs:70-266`: `compile_request()` handles 7 distinct concerns: (1) context preparation, (2) compilation, (3) dependency graph construction, (4) cost modeling and scheduling, (5) pushdown probing, (6) optimizer tracing, and (7) artifact bundle construction. Each of these is a separable pipeline stage.
- `plan_bundle.rs:342-479`: `build_plan_bundle_artifact_with_warnings()` cleanly separates plan capture from artifact construction -- this is well done.

**Suggested improvement:**
Extract the 7 concerns in `compile_request` into named pipeline stages (e.g., `compile_views`, `build_task_graph`, `probe_pushdown`, `capture_artifacts`) that each return their partial result. The top-level `compile_request` would become a short orchestrator composing these stages. This matches the existing pattern in `plan_compiler.rs` where `compile()` delegates to `compile_view()`.

**Effort:** medium
**Risk if unaddressed:** medium -- the function will grow as new compliance captures are added, making it harder to test and modify individual stages independently.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most files have clear single responsibilities. `plan_bundle.rs` at 1,271 LOC serves three distinct purposes: (1) artifact construction, (2) artifact diffing, and (3) artifact migration. These change for different reasons.

**Findings:**
- `plan_bundle.rs:488-585`: `diff_artifacts` is a 100-line function that changes when new fields are added to `PlanBundleArtifact` -- it should live with the diff concern, not the construction concern
- `plan_bundle.rs:710-730`: `migrate_artifact` is a version migration concern that changes when the artifact schema evolves
- `plan_bundle.rs:646-706`: Provider lineage extraction and UDF extraction are tree-walking utilities that change when DataFusion's plan tree API changes

**Suggested improvement:**
Split `plan_bundle.rs` into three focused modules: `plan_bundle.rs` (construction only), `plan_diff.rs` (diffing), and `plan_migration.rs` (version migration). The tree-walking extraction functions could move to a `plan_extraction.rs` utility.

**Effort:** medium
**Risk if unaddressed:** low -- the current single-file structure works but will become harder to navigate as artifact schema evolves.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Module cohesion is generally strong. The coupling concern is that `envelope.rs` and `planning_manifest.rs` both independently query `information_schema.df_settings` via SQL, creating an implicit coupling to the same config-capture logic.

**Findings:**
- `envelope.rs:81-112`: Queries `information_schema.df_settings` and parses `StringArray` columns
- `planning_manifest.rs:351-387`: Same SQL query, same `StringArray` parsing, same batch iteration logic
- Both modules use identical error handling for `downcast_ref::<arrow::array::StringArray>()`

**Suggested improvement:**
Extract a shared `capture_df_settings(ctx: &SessionContext) -> Result<BTreeMap<String, String>>` function into a session utility module. Both `envelope.rs` and `planning_manifest.rs` would call this single source of truth.

**Effort:** small
**Risk if unaddressed:** low -- but the duplicated parsing code will diverge silently if one is updated and the other is not.

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependency direction is excellent. The `spec/` module depends on nothing except `serde` and standard library. The `compiler/` and `session/` modules depend inward on `spec/` types. The `schema/` module depends only on `arrow` and standard library. This follows the "core logic depends on nothing" principle precisely.

**Findings:**
- `spec/mod.rs:1-13`: Pure data types with no framework dependencies
- `compiler/mod.rs:1-28`: Depends on `spec/`, `session/`, `providers/`, and DataFusion -- correctly at the "detail" layer
- `schema/introspection.rs:1-11`: Depends only on `arrow::datatypes` -- pure utility

No action needed.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `SemanticExecutionSpec` serves as an excellent port between Python and Rust. The `PlanningSurfaceSpec` is a clean port for session construction. The weakness is in `schema/policy_ops.rs` which uses untyped `serde_json::Value` for the Python policy boundary.

**Findings:**
- `policy_ops.rs:1-66`: All 6 public functions accept and return `serde_json::Value` -- callers must know the internal JSON structure to use them correctly
- `planning_surface.rs:78-104`: `apply_to_builder` is a clean adapter from typed spec to DataFusion builder API

**Suggested improvement:**
Define typed structs for `DatasetSpec`, `ScanPolicy`, and `DeltaScanPolicy` that mirror the JSON shapes, then parse `serde_json::Value` into these types at the boundary. The `policy_ops` functions would then operate on typed inputs with compile-time safety.

**Effort:** medium
**Risk if unaddressed:** low -- `policy_ops.rs` is small and stable, but the untyped interface makes it easy to pass incorrect JSON shapes without detection.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the most significant principle violation in the reviewed scope. Four distinct knowledge items are duplicated across modules.

**Findings:**

1. **Schema hashing strategy divergence:**
   - `plan_bundle.rs:632-637`: `hash_schema()` uses `serde_json::to_string(schema)` for JSON-based hashing
   - `schema/introspection.rs:34-79`: `hash_arrow_schema()` uses field-by-field BLAKE3 hashing (field count, names, types, nullability, metadata)
   - These two functions compute *different hashes for the same schema*. `hash_schema` includes Arrow's serde representation (which may include metadata ordering), while `hash_arrow_schema` uses a manually-constructed canonical form. Either could be authoritative, but having two is a semantic DRY violation.

2. **Planning-affecting config key lists:**
   - `planning_manifest.rs:332-349`: `PLANNING_AFFECTING_CONFIG_KEYS` as a `&[&str]` constant with 16 keys
   - `factory.rs:360-399`: `planning_config_snapshot()` hardcodes 9 keys inline via `snapshot.insert()` calls
   - The factory captures a *subset* of the keys that the manifest considers "planning-affecting". If a new planning-affecting key is added to the manifest constant, the factory will silently miss it.

3. **Extension governance enum duplication:**
   - `planning_surface.rs:64-70`: `ExtensionGovernancePolicy` enum with 3 variants
   - `spec/runtime.rs:26-33`: `ExtensionGovernanceMode` enum with identical 3 variants and different serde attributes
   - `compile_contract.rs:274-280`: `map_pushdown_mode()` manually maps between `PushdownEnforcementMode` (spec) and `ContractEnforcementMode` (providers) -- a third representation of the same concept

4. **Information schema SQL query pattern:**
   - `envelope.rs:81-112`: SQL query + StringArray parsing
   - `planning_manifest.rs:351-387`: Same SQL query + StringArray parsing
   - Identical error messages, batch iteration, null handling

**Suggested improvement:**
1. Consolidate schema hashing into a single `hash_arrow_schema` in `schema/introspection.rs` and have `plan_bundle.rs` call it instead of its local `hash_schema`. Choose one canonical strategy.
2. Extract `PLANNING_AFFECTING_CONFIG_KEYS` to a shared constant in a `session/config_keys.rs` module. Have both `factory.rs` and `planning_manifest.rs` reference it.
3. Define a single `GovernancePolicy` enum and use it everywhere, with serde attributes on the single type. Provide `From` impls for any framework-specific equivalents.
4. Extract `capture_df_settings` as described in P4.

**Effort:** medium
**Risk if unaddressed:** high -- the schema hashing divergence means `plan_bundle` artifacts and `schema/introspection` drift detection compute different identities for the same schema. The config key list divergence means planning surface identity may not capture all planning-affecting settings.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are explicit and enforced throughout. The `#[serde(deny_unknown_fields)]` annotation on all spec types prevents malformed input. `validate_graph` enforces preconditions before compilation. `SPEC_SCHEMA_VERSION` and `artifact_version` enable version validation.

**Findings:**
- `execution_spec.rs:23-24`: `#[serde(deny_unknown_fields)]` on root spec
- `relations.rs:8-9, 19-20, 31, 127, 146, 155`: `deny_unknown_fields` on all relation types
- `runtime.rs:76, 106-107, 208`: `deny_unknown_fields` on `TraceExportPolicy`, `TracingConfig`, `RuntimeConfig`
- `graph_validator.rs:36-97`: Explicit precondition validation with diagnostic error messages

No action needed.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The `ViewTransform` tagged enum at `relations.rs:32-112` is a strong example of parse-don't-validate. Each transform variant carries exactly the data it needs -- a `Normalize` cannot be confused with a `Relate` because they have different field sets. The `#[serde(tag = "kind")]` attribute ensures that deserialization parses into the correct variant at the boundary.

**Findings:**
- `relations.rs:32-112`: 10 `ViewTransform` variants, each with typed fields
- `execution_spec.rs:61-94`: `SemanticExecutionSpec::new()` computes and stores the spec hash at construction time, ensuring all downstream consumers see a valid hash
- `runtime.rs:7-72`: Mode enums (`RuntimeTunerMode`, `PushdownEnforcementMode`, etc.) are all parsed at deserialization into strongly-typed values

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most types effectively prevent illegal states through enum variants and typed fields. Two areas could be improved.

**Findings:**
- `schema/introspection.rs:13`: `pub type SchemaDiff = (Vec<String>, Vec<String>, Vec<(String, DataType, DataType)>)` -- this bare tuple type alias makes it easy to confuse `added` with `removed` or to destructure in the wrong order. A named struct would make the semantics self-documenting and prevent transposition errors.
- `plan_bundle.rs:241-284`: `PlanDiff` has 16 boolean fields that are all independent. While boolean fields are not inherently wrong, the combination of `delta_codec_logical_changed` with `delta_codec_logical_before/after` creates a state where the boolean could disagree with the presence of digest summaries. A richer type (e.g., `Option<(CodecDigestSummary, CodecDigestSummary)>` that is `Some` when changed and `None` when unchanged) would prevent this.

**Suggested improvement:**
Replace `SchemaDiff` type alias with a named struct:
```rust
pub struct SchemaDiff {
    pub added_fields: Vec<String>,
    pub removed_fields: Vec<String>,
    pub changed_fields: Vec<FieldTypeChange>,
}
pub struct FieldTypeChange {
    pub field_name: String,
    pub old_type: DataType,
    pub new_type: DataType,
}
```

**Effort:** small
**Risk if unaddressed:** low -- the current code works correctly, but the unnamed tuple makes callsite code harder to read and review.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions cleanly separate queries from commands. One notable exception exists.

**Findings:**
- `plan_compiler.rs:362-372`: `resolve_source_name()` returns a `Result<String>` (query semantics -- "what is the name?") but as a side effect registers the DataFrame into the `SessionContext` via `self.ctx.register_table()` (command semantics). The function name `resolve_source_name` suggests a pure lookup, but it mutates session state. This is the most significant CQS violation in the reviewed scope.
- `plan_compiler.rs:407-417`: `resolve_source()` returns a `DataFrame` and correctly does NOT have side effects -- it checks the inline cache, then falls back to `ctx.table()`. This is the correct pattern.

**Suggested improvement:**
Rename `resolve_source_name` to `ensure_source_registered` or split into two functions: one that checks if the source needs registration (query) and one that performs registration (command). The `compile_view` method would call both explicitly, making the side effect visible at the callsite.

**Effort:** small
**Risk if unaddressed:** medium -- the hidden side effect makes it difficult to reason about when table registrations happen during compilation, which matters for determinism.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 3/3

**Current state:**
Dependencies are explicitly passed and composed. `SessionFactory` takes an `EnvironmentProfile` at construction. `SemanticPlanCompiler` takes `&SessionContext` and `&SemanticExecutionSpec` as constructor parameters. `PlanningSurfaceSpec` is composed through explicit field setting. No hidden singletons or global state.

**Findings:**
- `factory.rs:42-44`: `SessionFactory` takes `EnvironmentProfile` via constructor injection
- `plan_compiler.rs:47-56`: `SemanticPlanCompiler` takes both dependencies as constructor params
- `compile_contract.rs:35-40`: `CompileRequest` bundles all dependencies as a parameter object

No action needed.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No class inheritance hierarchies exist anywhere in the reviewed scope. Behavior is composed through struct composition, trait implementations, and function delegation. `ViewTransform` variants are composed at the spec level; the compiler dispatches via pattern matching.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code follows the Law of Demeter. The primary violation is in `compile_contract.rs` where long access chains reach into nested DataFusion configuration.

**Findings:**
- `compile_contract.rs:157-169`: `prepared.ctx.state().config().options().optimizer.max_passes` and similar chains (4 levels deep). This exposes the internal structure of DataFusion's `SessionState` -> `SessionConfig` -> `ConfigOptions` -> `OptimizerOptions`.
- `factory.rs:126-139`: Similar chains through `config.options_mut()` are acceptable because `factory.rs` *owns* the config construction -- it is configuring, not querying.

**Suggested improvement:**
In `compile_contract.rs`, extract the needed config values into a local struct at the beginning of the function (e.g., `let optimizer_opts = OptimizerPipelineConfig::from_context(&prepared.ctx)`), then reference the local struct. This encapsulates the DataFusion config structure access.

**Effort:** small
**Risk if unaddressed:** low -- the chain is stable within a DataFusion major version, but would break across major upgrades.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most modules encapsulate their logic well. One instance of "asking" stands out.

**Findings:**
- `compile_contract.rs:213-216`: `deterministic_inputs: spec.input_relations.iter().all(|relation| relation.version_pin.is_some())` -- this reaches into the spec's `input_relations` to compute a property that the spec itself should know. The `SemanticExecutionSpec` should provide an `are_inputs_deterministic()` method.

**Suggested improvement:**
Add `pub fn are_inputs_deterministic(&self) -> bool` to `SemanticExecutionSpec` that encapsulates this logic.

**Effort:** small
**Risk if unaddressed:** low -- the logic is simple and currently used in one place.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 3/3

**Current state:**
This principle is excellently applied. The core logic modules are pure functions with no IO.

**Findings:**
- `inline_policy.rs:41-67`: `compute_inline_policy()` is a pure function -- takes data in, returns data out, no side effects
- `hashing.rs:15-65`: `hash_spec()`, `build_canonical_form()`, `canonicalize_value()` are all pure
- `graph_validator.rs:36-97`: `validate_graph()` is a pure function over immutable spec data
- `schema/introspection.rs:34-314`: All functions are pure -- `hash_arrow_schema`, `schema_diff`, `is_additive_evolution`, `cast_alignment_exprs`, `is_safe_cast`
- `plan_compiler.rs:69-159`: The imperative shell -- `compile()` is async, performs IO (table registration, plan optimization), and orchestrates the pure core

No action needed.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All operations are idempotent by construction. Hashing the same spec produces the same hash. Validating the same graph produces the same result. Diffing the same artifacts produces the same diff. The `register_or_replace_table` function at `plan_compiler.rs:114` explicitly uses "replace" semantics so re-registration is safe.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class design goal throughout. This is the strongest principle alignment in the reviewed scope.

**Findings:**
- `hashing.rs:22-33`: Canonical JSON via BTreeMap ensures key ordering is deterministic
- `planning_manifest.rs:74-101`: `hash()` clones and sorts all name vectors before serialization -- explicitly removes registration-order nondeterminism
- `envelope.rs:203-217`: `hash_provider_identities` sorts by name before hashing
- `plan_compiler.rs:166-243`: Topological sort uses sorted queue for deterministic tie-breaking
- `plan_bundle.rs:622-626`: `blake3_hash_bytes` is deterministic by construction
- `spec/runtime.rs:267-292`: `effective_tracing()` applies a deterministic merge order (preset -> explicit config -> legacy booleans)
- `hashing.rs:67-79`: `DeterminismContract` provides replay validation

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. `PlanDiff` at 22 fields and `diff_artifacts` at 100 lines represent unnecessary mechanical complexity that could be reduced.

**Findings:**
- `plan_bundle.rs:241-284`: `PlanDiff` has 22 fields. 16 are booleans, 4 are `Option<CodecDigestSummary>`, 1 is `Vec<String>`, and 1 is `bool`. The boolean fields follow a mechanical pattern that could be generated or represented more compactly.
- `plan_bundle.rs:488-585`: `diff_artifacts` is 100 lines of mechanical field-by-field comparison and summary building. This is correct but could be simplified with a derive macro or a comparison trait.

**Suggested improvement:**
Consider a `DiffField` enum or a `Vec<DiffEntry>` where each entry records what changed, rather than 16 named boolean fields. This would make the diff extensible without adding new fields each time `PlanBundleArtifact` grows. Alternatively, implement a custom `PartialEq`-adjacent comparison that returns structured diff results.

**Effort:** small (for the type restructuring; medium for a macro approach)
**Risk if unaddressed:** low -- the current approach works, but each new artifact field requires adding a boolean to `PlanDiff`, a comparison in `diff_artifacts`, and a summary string.

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative abstractions were found. Extension points exist (e.g., `artifact_version` for migration, `PortableArtifactPolicy` for future portability formats) but they serve near-term or active use cases.

No action needed.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. The `resolve_source_name` CQS violation (P11) also creates a least-astonishment issue.

**Findings:**
- `plan_compiler.rs:362-372`: `resolve_source_name` returns a `String` but has the side effect of registering a table. A competent reader would expect a function returning `String` to be a pure lookup.
- `plan_bundle.rs:632-637`: `hash_schema` uses `serde_json::to_string` while `schema/introspection.rs:34` `hash_arrow_schema` uses field-by-field hashing. A reader encountering both would expect them to be equivalent -- they are not.

**Suggested improvement:**
See P11 for `resolve_source_name`. For schema hashing, add a doc comment to `hash_schema` noting that it differs from `hash_arrow_schema` and when each should be used, or consolidate to one implementation.

**Effort:** small
**Risk if unaddressed:** medium -- the naming confusion can lead to incorrect assumptions about function behavior.

---

#### P22. Declare and version contracts -- Alignment: 3/3

**Current state:**
Public contracts are explicitly declared and versioned.

**Findings:**
- `execution_spec.rs:16`: `pub const SPEC_SCHEMA_VERSION: u32 = 4;`
- `plan_bundle.rs:78,443`: `artifact_version: u32` with explicit version bump in `build_plan_bundle_artifact_with_warnings`
- `plan_bundle.rs:710-730`: `migrate_artifact` handles version evolution
- All spec types use `#[serde(deny_unknown_fields)]` for strict contract enforcement

No action needed.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 3/3

**Current state:**
The codebase is highly testable. Pure functions dominate the core logic. Tests use in-memory tables and default sessions -- no heavyweight setup required.

**Findings:**
- `inline_policy.rs:69-208`: Tests use plain data structures -- no async, no IO
- `graph_validator.rs:237-617`: Tests create specs from helper functions and validate synchronously
- `plan_bundle.rs:792-1271`: Tests use `test_ctx()` with `MemTable` -- fast and deterministic
- `hashing.rs:81-177`: Tests are completely pure -- no DataFusion dependency
- `spec/runtime.rs:330-460`: Tests cover serde roundtrip, defaults, and config merging

No action needed.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Tracing infrastructure exists via `engine_tracing` integration in `factory.rs`. Warning codes are structured via `WarningCode` and `WarningStage` enums. However, the compile_contract orchestration does not emit structured trace spans for its internal phases.

**Findings:**
- `compile_contract.rs:70-266`: No tracing spans within the `compile_request` function. An operator cannot tell from telemetry which phase (compilation, scheduling, pushdown probing, artifact construction) is slow.
- `factory.rs:256-266`: Properly initializes OTel tracing and instruments session state
- `plan_bundle.rs:374-477`: Capture warnings are structured with `WarningCode` and `WarningStage`

**Suggested improvement:**
Add `#[instrument]` or manual tracing spans to the major phases within `compile_request` (compilation, scheduling, pushdown probing, artifact capture) so that observability covers the full compile pipeline.

**Effort:** medium
**Risk if unaddressed:** low -- operational visibility is limited for compile-mode diagnostics, but the warning system covers error cases.

---

## Cross-Cutting Themes

### Theme 1: Knowledge duplication across session capture paths

**Root cause:** The session, envelope, and planning manifest modules evolved independently to capture overlapping aspects of session state. Each needed config snapshot and identity hashing but implemented it locally.

**Affected principles:** P7 (DRY), P4 (coupling), P21 (least astonishment)

**Suggested approach:** Create a `session/capture.rs` utility module that provides:
- `capture_df_settings(ctx) -> BTreeMap<String, String>` (used by envelope and manifest)
- `PLANNING_AFFECTING_CONFIG_KEYS` (used by factory and manifest)
- A single `GovernancePolicy` enum (used by planning_surface, spec/runtime, and compile_contract)

This consolidation eliminates 4 instances of duplicated knowledge with a single extraction.

### Theme 2: Determinism contract chain integrity

**Root cause:** The system has an excellent determinism design, but the schema hashing divergence threatens it.

**Affected principles:** P7 (DRY), P18 (determinism), P22 (contracts)

**Suggested approach:** The `plan_bundle.rs::hash_schema` and `schema/introspection.rs::hash_arrow_schema` functions MUST produce the same hash for the same schema, or one must be designated authoritative with the other deprecated. The field-by-field approach in `introspection.rs` is more robust (handles metadata, is order-aware), while the JSON approach in `plan_bundle.rs` depends on serde stability. Consolidate to the field-by-field approach.

### Theme 3: Python-Rust boundary type safety

**Root cause:** `policy_ops.rs` uses `serde_json::Value` as the Python boundary type, while the rest of the codebase uses strongly-typed structs with `deny_unknown_fields`.

**Affected principles:** P6 (ports & adapters), P9 (parse don't validate), P8 (design by contract)

**Suggested approach:** Define typed structs for the policy shapes and parse at the boundary. This aligns `policy_ops.rs` with the pattern used by `SemanticExecutionSpec` and `RuntimeConfig`.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate schema hashing: replace `plan_bundle.rs:632-637` local `hash_schema` with call to `schema::introspection::hash_arrow_schema` | small | Eliminates divergent identity hashes for the same schema |
| 2 | P11 (CQS) | Rename `plan_compiler.rs:362` `resolve_source_name` to `ensure_source_registered` and document the side effect | small | Makes table registration visible and expected |
| 3 | P10 (illegal states) | Replace `SchemaDiff` tuple alias at `introspection.rs:13` with a named struct | small | Prevents field transposition errors at callsites |
| 4 | P7 (DRY) | Extract shared `capture_df_settings` function from `envelope.rs:81-112` and `planning_manifest.rs:351-387` | small | Single source of truth for config snapshot parsing |
| 5 | P7 (DRY) | Extract `PLANNING_AFFECTING_CONFIG_KEYS` to a shared constant referenced by both `factory.rs:360-399` and `planning_manifest.rs:332-349` | small | Ensures factory captures all planning-affecting keys |

## Recommended Action Sequence

1. **Consolidate schema hashing (P7, P18).** Replace `plan_bundle.rs:632-637` `hash_schema()` with a call to `schema::introspection::hash_arrow_schema()`. This is the highest-priority change because it affects determinism contract integrity. Verify that all tests pass with the consolidated hash.

2. **Extract shared config capture utilities (P7, P4).** Create `session/capture.rs` with `capture_df_settings()` and `PLANNING_AFFECTING_CONFIG_KEYS`. Update `envelope.rs`, `planning_manifest.rs`, and `factory.rs` to use the shared implementations.

3. **Consolidate governance policy enum (P7).** Define a single `GovernancePolicy` enum (either in `spec/runtime.rs` or a shared location) and add `From` impls for the framework-specific types. Remove `ExtensionGovernancePolicy` from `planning_surface.rs`.

4. **Fix CQS violation in resolve_source_name (P11, P21).** Rename to `ensure_source_registered` or split into query + command. Update callsites in `compile_view`.

5. **Replace SchemaDiff type alias (P10).** Convert `(Vec<String>, Vec<String>, Vec<(String, DataType, DataType)>)` to a named struct. Update all callsites.

6. **Decompose compile_request (P2, P24).** Extract 7 concerns into named functions. Add tracing spans to each. This is the largest change and benefits from the earlier extractions being in place.
