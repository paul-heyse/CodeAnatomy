# Design Review: rust/datafusion_ext/

**Date:** 2026-02-17
**Scope:** `rust/datafusion_ext/src/` and `rust/datafusion_ext/tests/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 59 source files, 10 test files
**Prior review baseline:** 2026-02-16 plan (S1-S14, D1-D5 all marked Complete)

## Executive Summary

The `datafusion_ext` crate is in strong shape following the S1-S14 improvement sweep. The registry decomposition (S12), `TableVersion` enum (S3), `ConfigExtension`-based configuration (S14), and shared helper extraction (S4-S6) have materially improved separation of concerns, information hiding, and DRY compliance. The overall alignment average is approximately 2.2/3. The primary remaining gaps are: (1) `span_struct_type()` duplication across `udf/common.rs` and `registry/snapshot.rs`, (2) two `snapshot_payload()` functions with the same name but different return types creating a naming hazard, (3) `FunctionMetadata` sentinel empty-string `name` field, and (4) `RegistrySnapshot` exposing all fields as `pub` without accessors. All are small-to-medium effort fixes with low-to-medium risk.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `RegistrySnapshot` fields all `pub`; no accessor methods |
| 2 | Separation of concerns | 3 | - | - | Clean module boundaries: UDF, delta, registry, rules |
| 3 | SRP (one reason to change) | 3 | - | - | Each module has a single responsibility |
| 4 | High cohesion, low coupling | 2 | small | low | `udf/common.rs` is high-cohesion but 685 LOC; snapshot depends on UDF internals |
| 5 | Dependency direction | 3 | - | - | Core types in dedicated modules; details depend on core |
| 6 | Ports & Adapters | 2 | medium | low | `compat.rs` shim for DataFusion API versions is good; delta subsystem tightly coupled to deltalake crate |
| 7 | DRY (knowledge) | 1 | small | medium | `span_struct_type()` defined in two modules; `snapshot_payload()` name collision |
| 8 | Design by contract | 2 | small | low | Good `coerce_types`/`return_field_from_args` contracts; `FunctionMetadata.name=""` violates postcondition |
| 9 | Parse, don't validate | 3 | - | - | `TableVersion::from_options()` + `config_macros` parse-once patterns |
| 10 | Make illegal states unrepresentable | 3 | - | - | `TableVersion` enum replaces `Option` pairs; `FunctionKind` enum |
| 11 | CQS | 3 | - | - | Functions are queries or commands, not mixed |
| 12 | Dependency inversion | 2 | medium | low | UDF trait impls use concrete `ConfigOptions` directly rather than abstraction |
| 13 | Prefer composition over inheritance | 3 | - | - | `RenamedAggregateUdf`/`RenamedWindowUdf` delegate via composition |
| 14 | Law of Demeter | 2 | small | low | Several `.as_any().downcast_ref()` chains in UDF invoke methods |
| 15 | Tell, don't ask | 2 | small | low | `register_all_with_policy` inspects boolean flags instead of policy object |
| 16 | Functional core, imperative shell | 3 | - | - | Pure UDF `invoke` / `simplify`; IO only in delta and registration |
| 17 | Idempotency | 3 | - | - | UDF registration is idempotent; `register_all` can be called repeatedly |
| 18 | Determinism / reproducibility | 3 | - | - | All UDFs marked `Immutable`; hash functions use Blake2b deterministic output |
| 19 | KISS | 2 | small | low | `udf_conformance.rs` test file is 1368 lines; `normalize_type_name` duplicates logic |
| 20 | YAGNI | 3 | - | - | No speculative abstractions detected |
| 21 | Least astonishment | 2 | small | medium | `snapshot_payload` name collision between two modules with different return types |
| 22 | Declare and version public contracts | 2 | small | low | `RegistrySnapshot::CURRENT_VERSION = 1` is versioned; `registry_snapshot.rs` facade exists but `legacy.rs` has `#[allow(unused_imports)]` |
| 23 | Design for testability | 3 | - | - | Comprehensive test suite; `udf_conformance.rs` covers SQL round-trip, simplify, and snapshot |
| 24 | Observability | 3 | - | - | All delta functions `#[instrument]`; `delta_observability.rs` provides structured payloads |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The registry decomposition (S12) moved types into proper submodules. However, `RegistrySnapshot` at `rust/datafusion_ext/src/registry/snapshot_types.rs:11-28` exposes all 16 fields as `pub` with no accessor methods. Callers can freely mutate snapshot internals.

**Findings:**
- `rust/datafusion_ext/src/registry/snapshot_types.rs:11-28`: All fields on `RegistrySnapshot` are `pub` -- `scalar`, `aggregate`, `window`, `table`, `aliases`, `parameter_names`, `volatility`, `rewrite_tags`, `simplify`, `coerce_types`, `short_circuits`, `signature_inputs`, `return_types`, `config_defaults`, `custom_udfs`. This allows any consumer to modify the snapshot after construction. [NEW]
- `rust/datafusion_ext/src/registry/snapshot_types.rs:64-75`: `FunctionHookCapabilities` similarly has all fields `pub`. [NEW]

**Suggested improvement:**
Make `RegistrySnapshot` fields `pub(crate)` and provide read-only accessor methods (e.g., `pub fn scalar(&self) -> &[String]`). The `registry_snapshot()` builder function already owns construction. External consumers (Python bindings) should access via Serde serialization, not field mutation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 3/3

**Current state:**
Module boundaries are clean. UDF logic (`udf/`), aggregation (`udaf_builtin.rs`, `udaf_arg_best.rs`), Delta operations (`delta_control_plane.rs`, `delta_maintenance.rs`, `delta_mutations.rs`), registry (`registry/`), rules (`physical_rules.rs`, `planner_rules.rs`), and configuration (`udf_config.rs`, `async_udf_config.rs`) each have distinct responsibilities. The S1-S14 sweep successfully decomposed the prior monolith.

**Findings:**
No violations detected.

---

#### P3. SRP (one reason to change) -- Alignment: 3/3

**Current state:**
Each module changes for exactly one reason. `udaf_builtin.rs` changes only for aggregate UDF behavior. `delta_control_plane.rs` changes only for provider construction. `registry/snapshot.rs` changes only for snapshot assembly logic.

**Findings:**
No violations detected.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Modules are cohesive. `udf/common.rs` at 685 lines is the largest utility module but remains cohesive around shared UDF helpers.

**Findings:**
- `rust/datafusion_ext/src/registry/snapshot.rs:372`: `span_struct_type()` is a private helper that duplicates the same function at `rust/datafusion_ext/src/udf/common.rs:483`. This creates implicit coupling: if the span schema changes, both must be updated. The snapshot module reaches into UDF-domain knowledge for schema construction. [NEW]
- `rust/datafusion_ext/src/udf/common.rs:1-685`: While cohesive, this is a large module that houses ~40 helper functions. Some (like `semantic_type_from_prefix`, `normalize_semantic_type`) are metadata-domain rather than UDF-plumbing. [Observation, not a violation]

**Suggested improvement:**
Export `span_struct_type()` from `udf/common.rs` as `pub(crate)` (it already is) and import it in `registry/snapshot.rs` instead of reimplementing. This is a one-line fix.

**Effort:** small
**Risk if unaddressed:** low -- but the duplication could cause a silent schema drift if one definition is updated without the other.

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Core types (`function_types.rs`, `errors.rs`, `config_macros.rs`) have no dependencies on higher-level modules. UDF implementations depend on `udf/common.rs`. Registration depends on UDF specs. This follows the "details depend on core" direction.

**Findings:**
No violations detected.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`compat.rs` (lines 1-9) serves as an explicit adapter for DataFusion API version changes, re-exporting the types UDFs need. This is a clean port. The Delta subsystem depends directly on `deltalake` crate types, which is appropriate given that Delta is the concrete storage adapter.

**Findings:**
- `rust/datafusion_ext/src/compat.rs:1-9`: Well-designed compatibility shim. When DataFusion upgrades change trait signatures, only this file needs updating. [Positive observation]
- The `generated/delta_types.rs` auto-generated types serve as a boundary contract between Python and Rust. [Positive observation]
- `rust/datafusion_ext/src/delta_control_plane.rs`, `delta_maintenance.rs`, `delta_mutations.rs` directly use `deltalake::DeltaTable`, `deltalake::DeltaOps`, etc. without an abstraction layer. This is pragmatic (YAGNI) but means testing requires real Delta tables. [Observation]

**Suggested improvement:**
No action needed now. If Delta testing becomes a bottleneck, consider a trait-based table provider abstraction. Currently YAGNI applies.

**Effort:** medium (if needed)
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The S4-S6 improvements successfully consolidated many duplicated helpers. However, two significant knowledge duplications remain.

**Findings:**
- `rust/datafusion_ext/src/udf/common.rs:483` and `rust/datafusion_ext/src/registry/snapshot.rs:372`: Both define `span_struct_type()` returning the same `DataType::Struct` with fields `[bstart: Int64, bend: Int64, line_base: Int32, col_unit: Utf8, end_exclusive: Boolean]`. The registry's version is used only for `custom_signatures()`. This is duplicated schema knowledge -- the canonical span struct shape is defined in two places. [NEW]
- `rust/datafusion_ext/src/delta_protocol.rs:223` and `rust/datafusion_ext/src/delta_observability.rs:52`: Both define `pub fn snapshot_payload()` accepting `&DeltaSnapshotInfo`. The protocol version returns `HashMap<String, String>` while the observability version returns `HashMap<String, serde_json::Value>`. Same name, same input, different output types. This is a naming collision that violates DRY at the interface level -- callers must know which module to import. [NEW]
- `rust/datafusion_ext/src/udaf_builtin.rs:1699`, `rust/datafusion_ext/src/udf/mod.rs:112`, `rust/datafusion_ext/src/udwf_builtin.rs:80`: `FunctionMetadata { name: "" }` used as a default when metadata is not applicable. The empty string is a sentinel value that represents "no metadata name" but is indistinguishable from a bug. [NEW]

**Suggested improvement:**
1. Delete `span_struct_type()` from `registry/snapshot.rs` and import from `udf::common`.
2. Rename `delta_protocol::snapshot_payload()` to `snapshot_info_strings()` or similar to disambiguate from the observability version.
3. Change the `FunctionMetadata.name` default from `""` to using `Option<String>` or a dedicated "anonymous" sentinel constant.

**Effort:** small (items 1 and 2), medium (item 3 requires `FunctionMetadata` struct change)
**Risk if unaddressed:** medium -- span schema drift is a silent correctness risk; naming collision causes confusion during maintenance.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
UDF implementations consistently enforce preconditions via `coerce_types()` and postconditions via `return_field_from_args()`. The `config_macros.rs` `impl_extension_options!` macro enforces typed parsing contracts.

**Findings:**
- `rust/datafusion_ext/src/udaf_builtin.rs:1699`: `function_metadata()` returns `FunctionMetadata { name: "", ... }` which violates the implicit postcondition that metadata should be meaningful. [NEW]
- `rust/datafusion_ext/src/udf/hash.rs:143-148`: `StableHash128Udf::coerce_types` error message says "stable_hash_any expects between one and three arguments" but the function is `stable_hash128`. Copy-paste error in the error message. [NEW]

**Suggested improvement:**
Fix the error message in `StableHash128Udf::coerce_types` to reference `stable_hash128`. Make `FunctionMetadata.name` an `Option<String>` or define a constant for the fallback case.

**Effort:** small
**Risk if unaddressed:** low -- the error message mismatch may confuse users debugging type coercion failures.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
`TableVersion::from_options()` at `delta_protocol.rs` parses raw options into a structured enum once. `CodeAnatomyUdfConfig::from_config()` parses `ConfigOptions` into a structured config once. `config_macros.rs` provides typed parsing for all config values.

**Findings:**
No violations detected.

---

#### P10. Make illegal states unrepresentable -- Alignment: 3/3

**Current state:**
`TableVersion` enum (S3) successfully replaced `(Option<i64>, Option<String>)` mutual-exclusion pairs. `FunctionKind` enum (S2) provides a single authoritative function type discriminator. `ColUnit` enum in `position.rs` restricts encoding to valid values.

**Findings:**
No violations detected. The prior plan's S3 improvement is well-implemented.

---

#### P11. CQS -- Alignment: 3/3

**Current state:**
Functions consistently separate queries from commands. `registry_snapshot()` is a pure query. `register_all()` is a command. UDF `invoke_with_args()` and `simplify()` are pure transforms. Delta mutation functions are commands that return result values.

**Findings:**
No violations detected.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
UDF registration uses `ScalarUdfSpec` structs with function pointers, which is a form of explicit composition. However, `with_updated_config` on UDFs takes a concrete `&ConfigOptions` rather than an abstraction.

**Findings:**
- `rust/datafusion_ext/src/udf/span.rs:248-258`, `rust/datafusion_ext/src/udf/string.rs:289-299`, `rust/datafusion_ext/src/udf/collection.rs:352-362`: `with_updated_config(&self, config: &ConfigOptions)` depends on the concrete `ConfigOptions` type from DataFusion. This is dictated by the DataFusion trait definition, not a design choice in this crate. [Observation -- trait-imposed, not actionable]
- `rust/datafusion_ext/src/udf_registry.rs:86-142`: `register_all_with_policy` takes four boolean/option parameters instead of a registration policy struct. [NEW]

**Suggested improvement:**
Extract a `RegistrationPolicy { enable_async: bool, async_udf_timeout_ms: Option<u64>, async_udf_batch_size: Option<usize> }` struct and pass it to `register_all_with_policy` instead of loose parameters. This follows the existing `CodeAnatomyUdfConfig`/`CodeAnatomyPhysicalConfig` pattern.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
`RenamedAggregateUdf` and `RenamedWindowUdf` in `sql_macro_factory.rs` use delegation (composition) rather than inheritance. `SqlMacroUdf` wraps template expressions via composition. `CollectSetUdaf` delegates to `ListUniqueAccumulator`.

**Findings:**
No violations detected.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
UDF `invoke_with_args` methods frequently use `.as_any().downcast_ref::<T>()` chains to access Arrow array data. This is inherent to the Arrow API design -- there is no way to avoid it.

**Findings:**
- Throughout `udf/hash.rs`, `udf/span.rs`, `udf/collection.rs`, etc.: Patterns like `array.as_any().downcast_ref::<StructArray>()` are pervasive. [Inherent to Arrow, not actionable]
- `rust/datafusion_ext/src/udf_registry.rs:125-131`: `ctx.state_ref().write().config_mut().set_extension(...)` is a 4-level chain. [NEW]

**Suggested improvement:**
The Arrow downcast chains are not fixable within the current API. The `ctx.state_ref().write().config_mut().set_extension()` chain could be wrapped in a helper function `install_config_extension(ctx, extension)` to reduce the chain depth.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most UDF implementations properly encapsulate behavior. However, `register_all_with_policy` at `udf_registry.rs:86` asks for raw boolean `enable_async` rather than accepting a policy object that knows how to configure itself.

**Findings:**
- `rust/datafusion_ext/src/udf_registry.rs:86-91`: `register_all_with_policy(ctx, enable_async, async_udf_timeout_ms, async_udf_batch_size)` -- the caller must know which flags to set. A policy object would be more tell-oriented. [NEW]
- `rust/datafusion_ext/src/udf_expr.rs:77-162`: `expr_from_name` uses a long `match` on string names. While not a tell/ask violation per se, a registry-based dispatch would be more extensible. [Observation]

**Suggested improvement:**
Same as P12 suggestion: introduce a `RegistrationPolicy` struct.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 3/3

**Current state:**
UDF implementations (`invoke_with_args`, `simplify`, `coerce_types`, `return_field_from_args`) are pure functions. IO is confined to Delta operations and session registration. The `async_runtime.rs` shared runtime is correctly isolated at the shell boundary.

**Findings:**
No violations detected.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
`register_all()` can be called multiple times safely -- DataFusion's `register_udf` replaces existing registrations. `SHARED_RUNTIME` in `async_runtime.rs` uses `OnceLock` for exactly-once initialization.

**Findings:**
No violations detected.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
All custom UDFs are marked `Volatility::Immutable`. Hash functions use Blake2b with deterministic output. `BTreeMap` and `BTreeSet` are used in snapshot assembly and collection UDFs to ensure deterministic ordering.

**Findings:**
No violations detected. This is a notable strength of the crate.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The crate is well-structured overall. However, the `udf_conformance.rs` test file at 1368 lines and `normalize_type_name()` within it (139 lines of repetitive type matching) could be simplified.

**Findings:**
- `rust/datafusion_ext/tests/udf_conformance.rs:65-138`: `normalize_type_name()` contains ~70 lines of repetitive `if text.contains(...)` checks. This could be a simple lookup table. [NEW, test code]
- `rust/datafusion_ext/src/registry/snapshot.rs:300-370`: `custom_signatures()` contains ~70 lines of hardcoded signature definitions for custom UDFs. These could be co-located with the UDF definitions themselves. [Observation]
- `rust/datafusion_ext/src/registry/snapshot.rs:395-440`: `custom_table_signatures()` similarly hardcodes table function schemas. [Observation]

**Suggested improvement:**
For `custom_signatures()` and `custom_table_signatures()`, consider having each UDF provide its own signature metadata via the `FunctionMetadata` trait, rather than centralizing all signatures in the snapshot module. This would also address the cohesion concern in P4.

**Effort:** medium
**Risk if unaddressed:** low -- the current approach works but requires updating the snapshot module whenever a new UDF is added.

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative abstractions detected. The `compat.rs` shim is minimal (9 lines). The `generated/` module is auto-generated from a script with no unused fields. Feature-gated async UDF support avoids paying for unused capability.

**Findings:**
No violations detected.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. However, the `snapshot_payload` naming collision is surprising.

**Findings:**
- `rust/datafusion_ext/src/delta_protocol.rs:223` vs `rust/datafusion_ext/src/delta_observability.rs:52`: Both are `pub fn snapshot_payload(snapshot: &DeltaSnapshotInfo)` but return `HashMap<String, String>` and `HashMap<String, serde_json::Value>` respectively. A developer importing both would get a compilation error, but the identical name is confusing during code navigation. [NEW]
- `rust/datafusion_ext/src/udf/hash.rs:143-148`: Error message in `StableHash128Udf::coerce_types` incorrectly references "stable_hash_any" instead of "stable_hash128". [NEW]

**Suggested improvement:**
Rename `delta_protocol::snapshot_payload()` to `snapshot_info_as_strings()` and `delta_observability::snapshot_payload()` to `snapshot_info_as_values()` to disambiguate.

**Effort:** small
**Risk if unaddressed:** medium -- the naming collision will confuse future maintainers.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
`RegistrySnapshot::CURRENT_VERSION = 1` at `snapshot_types.rs:31` explicitly versions the snapshot contract. The `registry_snapshot.rs` and `registry/legacy.rs` facades provide backward-compatible re-exports.

**Findings:**
- `rust/datafusion_ext/src/registry/legacy.rs:3-6`: `#[allow(unused_imports)]` suppresses warnings for re-exports that may no longer be consumed. If these imports are truly unused, the legacy module should be removed. [NEW]
- `rust/datafusion_ext/src/lib.rs:1-122`: All 37 modules are declared `pub`. Some internal-only modules (like `error_conversion`, `config_macros`, `operator_utils`) could be `pub(crate)` to reduce the public surface. [Observation]

**Suggested improvement:**
Audit which modules need to be `pub` vs `pub(crate)`. Internal utility modules (`error_conversion`, `config_macros`, `operator_utils`, `async_runtime`) should be `pub(crate)`. Remove `legacy.rs` if its re-exports are not consumed externally.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

(Covered above -- all scored 3/3.)

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 3/3

**Current state:**
The test suite is comprehensive. `udf_conformance.rs` (1368 lines, 30+ tests) covers SQL round-trip execution, constant folding via `simplify`, named arguments, type coercion, snapshot metadata completeness, information_schema conformance, window function sliding behavior, null treatment, async UDF configuration, and expr planner rewrites. Dedicated test files exist for delta common helpers, control plane builders, function types, operator utils, async config, registry metadata, registry capabilities, registry snapshots, and arg-best aggregates.

**Findings:**
- `rust/datafusion_ext/tests/registry_snapshot_tests.rs:1-15`: Tests that decomposed snapshot matches legacy facade -- validates the S12 migration. [Positive]
- All tests use `SessionContext` and SQL execution, providing integration-level confidence. [Positive]
- No mocking is needed because UDFs are pure functions testable via DataFusion's in-memory engine. [Positive]

**Suggested improvement:**
No action needed. Test coverage is strong.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
All Delta operations are annotated with `#[instrument]` (verified in `delta_control_plane.rs`, `delta_maintenance.rs`, `delta_mutations.rs`). `delta_observability.rs` provides structured payload builders (`snapshot_payload`, `scan_config_payload`, `add_action_payloads`, `mutation_report_payload`, `maintenance_report_payload`) that convert internal state to structured key-value maps suitable for tracing spans.

**Findings:**
No violations detected.

---

## Cross-Cutting Themes

### Theme 1: Residual Knowledge Duplication After S1-S14 Sweep

The prior improvement plan successfully addressed the major DRY violations (average improved from 0.8/3 to 2.2/3 estimated). However, two duplications were introduced or missed: `span_struct_type()` duplication and `snapshot_payload()` name collision. Both are low-effort fixes but represent the kind of "new duplication introduced during refactoring" that should be caught in review.

**Root cause:** The S12 registry decomposition moved snapshot logic into its own module but did not remove the local `span_struct_type()` definition when the import from `udf::common` would have sufficed. The `snapshot_payload` name existed in `delta_protocol.rs` before `delta_observability.rs` was created, and the new module reused the name with a different return type.

**Affected principles:** P7 (DRY), P21 (Least Astonishment), P4 (Coupling)

**Suggested approach:** Two small PRs -- one to deduplicate `span_struct_type`, one to rename the ambiguous `snapshot_payload` functions.

### Theme 2: Empty-String Sentinel in FunctionMetadata

Three modules return `FunctionMetadata { name: "" }` as a default. This pattern uses an empty string as a sentinel value rather than `Option<String>`, which can mask bugs (is the name empty because metadata was not populated, or because the function truly has no metadata name?).

**Root cause:** `FunctionMetadata` was introduced (S8) with a required `name: String` field. Functions that do not need metadata were given `""` as a placeholder rather than making the field optional.

**Affected principles:** P8 (Design by contract), P10 (Make illegal states unrepresentable)

**Suggested approach:** Change `FunctionMetadata.name` to `Option<String>` and update the three call sites. Small effort, improves type safety.

### Theme 3: Excellent DataFusion API Alignment

The crate makes strong use of DataFusion's extension points:
- `FunctionFactory` for SQL macro support
- `ConfigExtension` for session-scoped UDF configuration (replacing global state)
- `ExprPlanner` and `FunctionRewrite` for operator customization
- `PhysicalOptimizerRule` and `AnalyzerRule` for plan customization
- `#[user_doc]` macro for auto-generated documentation
- `with_updated_config` for config-aware UDF specialization
- `GroupsAccumulator` for high-performance aggregate paths

No cases were found where custom code replaces available DataFusion built-in functionality. The `udwf_builtin.rs` window UDFs properly delegate to DataFusion's built-in `row_number`, `lag`, and `lead` rather than reimplementing them.

### Theme 4: Strong Correctness Foundation

Determinism and reproducibility are well-maintained:
- All UDFs use `Volatility::Immutable`
- Hash functions use Blake2b (deterministic)
- Collection operations use `BTreeMap`/`BTreeSet` for ordered output
- `ConfigExtension` replaces the prior `OnceLock` global state
- `TableVersion` enum prevents mutual-exclusion bugs

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Delete duplicate `span_struct_type()` from `registry/snapshot.rs:372` and import from `udf::common` | small | Eliminates silent schema drift risk |
| 2 | P21 (Astonishment) | Rename `delta_protocol::snapshot_payload()` to `snapshot_info_strings()` | small | Eliminates naming confusion |
| 3 | P8 (Contract) | Fix error message in `udf/hash.rs:143-148` referencing wrong function name | small | Prevents user confusion |
| 4 | P1 (Info hiding) | Make `RegistrySnapshot` fields `pub(crate)` and add accessors | small | Prevents external mutation |
| 5 | P22 (Contracts) | Audit `lib.rs` module visibility; mark internal modules `pub(crate)` | small | Reduces public surface area |

## Recommended Action Sequence

1. **[P7, Quick Win 1]** Delete `span_struct_type()` from `registry/snapshot.rs` and import from `udf::common::span_struct_type`. Single-line fix, eliminates schema drift risk. Dependency: none.

2. **[P21, Quick Win 2]** Rename `delta_protocol::snapshot_payload()` to `snapshot_info_as_strings()` and update its one call site in `delta_protocol.rs`. Dependency: none.

3. **[P8, Quick Win 3]** Fix the error message in `StableHash128Udf::coerce_types` at `udf/hash.rs:143-148` to reference "stable_hash128" instead of "stable_hash_any". Dependency: none.

4. **[P1, Quick Win 4]** Make `RegistrySnapshot` and `FunctionHookCapabilities` fields `pub(crate)` in `snapshot_types.rs`. Add read-only accessor methods. Update external consumers (Python bindings) if they access fields directly. Dependency: check Python binding usage.

5. **[P22, Quick Win 5]** Audit `lib.rs` module declarations. Change `error_conversion`, `config_macros`, `operator_utils`, and `async_runtime` from `pub` to `pub(crate)`. Remove `registry/legacy.rs` if its re-exports are confirmed unused. Dependency: check cross-crate imports.

6. **[P8/P10, Theme 2]** Change `FunctionMetadata.name` from `String` to `Option<String>`. Update the three call sites in `udaf_builtin.rs:1699`, `udf/mod.rs:112`, and `udwf_builtin.rs:80` to use `None`. Dependency: items 4-5 (public contract change).

7. **[P12/P15]** Extract `RegistrationPolicy` struct to replace loose parameters in `register_all_with_policy`. Dependency: none, but lower priority than items 1-6.

## Python Integration Boundary Assessment

The Python-Rust integration boundary is well-defined:

1. **Type bridge:** `generated/delta_types.rs` provides `DeltaAppTransaction`, `DeltaCommitOptions`, and `DeltaFeatureGate` with `#[cfg_attr(feature = "python", pyclass)]` annotations. These are auto-generated from `scripts/codegen_delta_types.py`, ensuring Python and Rust stay in sync.

2. **Registry bridge:** `RegistrySnapshot` is serialized to JSON via `#[derive(Serialize)]` and consumed by `rust/datafusion_python/` for Python-side registry introspection. The `CURRENT_VERSION = 1` field enables forward-compatible schema evolution.

3. **Config bridge:** `CodeAnatomyUdfConfig`, `CodeAnatomyPhysicalConfig`, and `CodeAnatomyPolicyConfig` all implement `ConfigExtension`, allowing Python to configure Rust UDF behavior via `SET` statements rather than direct struct access.

4. **UDF bridge:** `udf_expr.rs` provides `expr_from_name()` and `expr_from_registry_or_specs()` which allow the Python side to construct UDF expressions by name without importing Rust UDF types directly. This is the primary programmatic API for Python callers.

5. **Delta bridge:** Delta request structs (`DeltaControlPlaneRequest`, `DeltaMaintenanceRequest`, `DeltaMutationRequest`) use `TableVersion` enum for version specification, which is cleanly mapped to Python via the generated types.

The boundary is healthy. The one risk is `RegistrySnapshot`'s `pub` fields (P1 finding) -- if Python bindings access fields directly, the accessor migration (Quick Win 4) needs coordination.

## DataFusion/DeltaLake Library Capability Assessment

Based on examination of the crate's DataFusion integration patterns:

1. **No reinvented wheels detected.** Window UDFs (`udwf_builtin.rs`) properly delegate to built-in `row_number`, `lag`, `lead`. Expression construction (`udf_expr.rs`) uses `datafusion_functions::core::expr_fn`, `datafusion_functions_aggregate::expr_fn`, `datafusion_functions_nested::expr_fn`, and `datafusion_functions_window::expr_fn` rather than reimplementing.

2. **Custom UDFs are justified.** The hashing UDFs (`stable_hash64`, `stable_hash128`, etc.) provide Blake2b-based deterministic hashing not available in DataFusion's built-in functions. The span UDFs provide domain-specific struct operations. The CDF UDFs provide Delta-specific change type classification. None of these overlap with DataFusion built-ins.

3. **Optimizer hook adoption is comprehensive.** UDFs implement `simplify` for constant folding, `coerce_types` for type coercion, `return_field_from_args` for field-level return type inference, `short_circuits` where applicable, and `with_updated_config` for session-scoped configuration. Aggregates implement `GroupsAccumulator` for vectorized execution and `retract_batch` for sliding window support.

4. **DeltaLake API usage is current.** `DeltaScanConfigBuilder` is used for scan configuration (S4). `DeltaOps` is used for maintenance and mutation operations. `TableProviderBuilder` is used for table provider construction. No deprecated DeltaLake APIs were identified.
