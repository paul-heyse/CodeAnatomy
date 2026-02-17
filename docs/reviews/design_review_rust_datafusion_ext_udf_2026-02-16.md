# Design Review: rust/datafusion_ext UDF Implementations

**Date:** 2026-02-16
**Scope:** `rust/datafusion_ext/src/udf/` (11 modules), `udaf_builtin.rs`, `udtf_builtin.rs`, `udtf_sources.rs`, `udwf_builtin.rs`, `tests/udf_conformance.rs`
**Focus:** All principles (1-24); weighted emphasis on Knowledge (7-11), Simplicity (19-22), Quality (23-24)
**Depth:** deep (all files)
**Files reviewed:** 17

## Executive Summary

The UDF crate demonstrates strong alignment on dependency direction, determinism, and testability. All UDFs are properly marked `Volatility::Immutable`, conformance tests validate row-count invariants and information-schema metadata, and the `simplify` optimizer hook is widely used for constant folding. The primary design concerns are (1) significant semantic duplication in `udaf_builtin.rs` where `CollectSetUdaf` and `ListUniqueUdaf` share identical accumulator implementations, (2) three utility functions (`string_array_any`, `scalar_to_string`, `scalar_to_i64`) are duplicated between `udf/common.rs` and `udaf_builtin.rs`, (3) copy-paste error messages in `coerce_types` for `CpgScoreUdf` and `Utf8NullIfBlankUdf`, and (4) dead parameters in `variadic_any_signature`. The highest-impact improvements are consolidating duplicated knowledge and fixing incorrect error messages.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `pub(crate)` fields on UDF structs expose `signature` and `policy` |
| 2 | Separation of concerns | 3 | - | - | Clean separation between UDF logic and registration/factory |
| 3 | SRP | 2 | medium | low | `udaf_builtin.rs` (2225 LOC) mixes 11 aggregates + 3 utility functions |
| 4 | High cohesion, low coupling | 2 | medium | low | `udaf_builtin.rs` does not use `udf/common.rs`, re-implements utilities |
| 5 | Dependency direction | 3 | - | - | Core UDF logic depends on nothing outside Arrow/DataFusion |
| 6 | Ports & Adapters | 3 | - | - | N/A; UDFs are the adapter layer by nature |
| 7 | DRY (knowledge) | 1 | medium | medium | Three functions duplicated; `CollectSet`/`ListUnique` identical behavior |
| 8 | Design by contract | 2 | small | medium | Wrong error messages in `CpgScoreUdf.coerce_types` and `Utf8NullIfBlankUdf.coerce_types` |
| 9 | Parse, don't validate | 2 | small | low | `cdf_rank` uses magic numbers (0-3) without named constants |
| 10 | Make illegal states unrepresentable | 2 | medium | low | `variadic_any_signature` ignores min/max args; CDF rank is bare `i32` |
| 11 | CQS | 3 | - | - | All UDFs are pure query functions |
| 12 | DI + explicit composition | 3 | - | - | Factory pattern with `build_udf` match dispatch is explicit |
| 13 | Composition over inheritance | 3 | - | - | Rust trait impl composition; no inheritance hierarchy |
| 14 | Law of Demeter | 2 | small | low | `ScalarValue::try_from_array` per-element in loops bypasses typed downcasts |
| 15 | Tell, don't ask | 3 | - | - | UDFs encapsulate their logic behind trait methods |
| 16 | Functional core, imperative shell | 3 | - | - | All scalar/aggregate UDFs are pure transforms; IO only in `udtf_sources.rs` |
| 17 | Idempotency | 3 | - | - | All UDFs `Volatility::Immutable`; same inputs produce same outputs |
| 18 | Determinism | 3 | - | - | Blake2b hashing with bit-masking ensures reproducible hashes |
| 19 | KISS | 2 | medium | low | 4-way scalar/array match arms in 2-arg hash UDFs; could use `to_array` |
| 20 | YAGNI | 2 | small | low | `touch_policy_fields` is dead-code suppression; `variadic_any_signature` dead params |
| 21 | Least astonishment | 1 | small | high | `CpgScoreUdf.coerce_types` says "stable_id_parts expects..."; `Utf8NullIfBlankUdf.coerce_types` says "qname_normalize expects..." |
| 22 | Public contracts | 2 | small | low | `udf/mod.rs` glob re-exports all pub items; no explicit public surface declaration |
| 23 | Design for testability | 3 | - | - | Excellent conformance test suite; simplify/constant-fold tests; information_schema validation |
| 24 | Observability | 2 | medium | low | No structured logging/tracing in UDF execution paths |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
UDF struct fields are declared `pub(crate)`, which is appropriate for crate-internal use. However, the `signature` and `policy` fields are directly accessible by other modules within the crate.

**Findings:**
- `rust/datafusion_ext/src/udf/hash.rs:33` -- `StableHash64Udf.signature` is `pub(crate)`, allowing `function_factory.rs` to construct UDFs by directly setting the field rather than through a constructor.
- `rust/datafusion_ext/src/udf/span.rs` -- `SpanMakeUdf.policy` is `pub(crate)`, exposing the config object.
- `rust/datafusion_ext/src/udf/mod.rs:29-43` -- `config_defaults_for` accesses `udf.policy` directly via `downcast_ref`, coupling to concrete struct layout.

**Suggested improvement:**
Add `new(signature)` constructors to each UDF struct and use those in `build_udf` instead of direct field construction. This would let internal field visibility be reduced to private.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 3/3

**Current state:**
Clean separation exists between UDF domain logic (hash computation, span arithmetic, string normalization), registration (`udf_registry`), and factory dispatch (`function_factory.rs`). Each UDF module focuses on one domain area.

No action needed.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most UDF modules are well-scoped. The exception is `udaf_builtin.rs`.

**Findings:**
- `rust/datafusion_ext/src/udaf_builtin.rs` (2225 lines) -- Contains 11 aggregate UDF implementations, 3 accumulator types, 3 groups-accumulator types, 3 sliding-accumulator types, and 3 local utility functions (`string_array_any`, `scalar_to_string`, `scalar_to_i64`). This file changes for many different reasons: new aggregate UDFs, changes to accumulator logic, changes to scalar-to-type conversion.

**Suggested improvement:**
Extract `ArgBestAccumulator` and its three consumers (`AnyValueDetUdaf`, `ArgMaxUdaf`, `ArgMinUdaf`, `AsofSelectUdaf`) into a separate `udaf_arg_best.rs` module. Move the duplicated utility functions to `udf/common.rs`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
`udf/common.rs` provides shared utilities, but `udaf_builtin.rs` does not use them, instead re-implementing the same functions locally.

**Findings:**
- `rust/datafusion_ext/src/udaf_builtin.rs:2213-2224` -- `string_array_any` is a verbatim copy of `udf/common.rs:233-247`.
- `rust/datafusion_ext/src/udaf_builtin.rs:708-716` -- `scalar_to_string` is a reduced copy of `udf/common.rs:249-268`.
- `rust/datafusion_ext/src/udaf_builtin.rs:718-757` -- `scalar_to_i64` is a near-copy of `udf/common.rs:277-299` with minor differences (timestamp handling, error message context).
- `rust/datafusion_ext/src/udaf_builtin.rs:266-273` -- `string_signature` is a local duplicate of signature construction patterns in `common.rs`.
- `rust/datafusion_ext/src/udaf_builtin.rs:275-281` -- `signature_with_names` is a local duplicate of `common.rs:31-37`.

**Suggested improvement:**
Make `udaf_builtin.rs` import `string_array_any`, `scalar_to_string`, and `scalar_to_i64` from `udf::common`. The `udaf_builtin.rs` local `scalar_to_i64` has additional timestamp handling; unify by adding those arms to the canonical version in `common.rs`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
All UDF modules depend inward on Arrow primitives and DataFusion trait definitions. No UDF module imports from `src/` Python-side code. The `udf_config::CodeAnatomyUdfConfig` is a clean inward dependency carried via the `with_updated_config` hook.

No action needed.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The UDF layer is itself the adapter layer -- it adapts domain logic (hashing, span arithmetic) into DataFusion's UDF trait system. The `udtf_sources.rs` file cleanly adapts Delta Lake operations into table functions via `delta_control_plane`.

No action needed.

---

### Category: Knowledge (7-11) -- Focus Area

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the most significant design concern in the reviewed scope. Multiple forms of semantic duplication exist.

**Findings:**
- **Identical accumulator behavior:** `rust/datafusion_ext/src/udaf_builtin.rs:106-165` (`ListUniqueUdaf`) and `rust/datafusion_ext/src/udaf_builtin.rs:185-244` (`CollectSetUdaf`) share identical `accumulator`, `create_groups_accumulator`, and `create_sliding_accumulator` implementations. Both delegate to `ListUniqueAccumulator`, `ListUniqueGroupsAccumulator`, and `ListUniqueSlidingAccumulator`. The only differences are the UDF name and state field name.
- **Duplicated utilities across modules:** `string_array_any` at `udaf_builtin.rs:2213` duplicates `udf/common.rs:233`. `scalar_to_string` at `udaf_builtin.rs:708` duplicates `udf/common.rs:249`. `scalar_to_i64` at `udaf_builtin.rs:718` duplicates `udf/common.rs:277`.
- **Near-identical trait impls:** `ArgMaxUdaf` (`udaf_builtin.rs:951`), `ArgMinUdaf` (`udaf_builtin.rs:1028`), and `AsofSelectUdaf` (`udaf_builtin.rs:1094`) each implement `AggregateUDFImpl` with near-identical bodies differing only in name, `ArgBestMode::Max`/`Min`, and state field naming.
- **Duplicated CDF signature construction:** `cdf.rs:206-243` -- Three CDF UDF constructor functions (`cdf_change_rank_udf`, `cdf_is_upsert_udf`, `cdf_is_delete_udf`) build identical signatures.
- **Duplicated hash64/hash128 in test:** `tests/udf_conformance.rs:34-49` reimplements `hash64_value` and `hash128_value` rather than importing from the crate.

**Suggested improvement:**
1. Extract a generic `ListCollectorUdaf<const NAME: &str>` or parameterize via a config struct to eliminate the `CollectSetUdaf`/`ListUniqueUdaf` duplication.
2. Move `scalar_to_string`, `scalar_to_i64`, and `string_array_any` to `udf/common.rs` and import them in `udaf_builtin.rs`.
3. Consider a macro or helper to generate the `AggregateUDFImpl` boilerplate for `ArgMax`/`ArgMin`/`AsofSelect` since they share 90%+ of their trait method implementations.
4. Extract a `cdf_string_signature()` helper for the three CDF constructors.

**Effort:** medium
**Risk if unaddressed:** medium -- Duplicated knowledge drifts over time (the `scalar_to_i64` copies already differ in handling of timestamp types and error message format).

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Most UDFs enforce preconditions via `coerce_types` and argument-count checks in `invoke_with_args`. However, two functions have incorrect error messages that violate the contract's self-documentation promise.

**Findings:**
- `rust/datafusion_ext/src/udf/metadata.rs:51-54` -- `CpgScoreUdf.coerce_types` reports: `"stable_id_parts expects between two and sixty-five arguments"`. This is the error message from `StableIdPartsUdf`, not `CpgScoreUdf`. A caller receiving this error would be misled about which function failed.
- `rust/datafusion_ext/src/udf/string.rs:331-335` -- `Utf8NullIfBlankUdf.coerce_types` reports: `"qname_normalize expects between one and three arguments"`. This is the error message from `QNameNormalizeUdf`. A user calling `utf8_null_if_blank` with wrong arguments sees a confusing error about a different function.
- `rust/datafusion_ext/src/udf/metadata.rs:50-57` -- `CpgScoreUdf.coerce_types` accepts 2-65 arguments, but `invoke_with_args` at line 100 expects exactly 1 argument. These contracts contradict each other.

**Suggested improvement:**
Fix the error messages to reference the correct function names. Also reconcile the `CpgScoreUdf.coerce_types` argument range (2-65) with `invoke_with_args` which only handles 1 argument.

**Effort:** small
**Risk if unaddressed:** medium -- Incorrect error messages actively mislead users debugging SQL query failures.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Most UDFs parse inputs at the boundary (via `coerce_types` and `string_array_any`). The main gap is CDF rank values.

**Findings:**
- `rust/datafusion_ext/src/udf/cdf.rs:17-26` -- `cdf_rank` returns raw `i32` magic numbers (0, 1, 2, 3, -1) without named constants. Callers at lines 141 and 201 compare against these magic numbers directly (`rank == 3 || rank == 2`, `rank == 1`).

**Suggested improvement:**
Introduce named constants (`const CDF_RANK_UPDATE_POSTIMAGE: i32 = 3`, etc.) or a `CdfChangeType` enum, and expose predicate methods (`is_upsert()`, `is_delete()`) on it rather than having callers interpret raw integers.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The type system generally prevents illegal states. The main gap is dead parameters that create misleading APIs.

**Findings:**
- `rust/datafusion_ext/src/udf/common.rs:64-70` -- `variadic_any_signature` accepts `_min_args: usize` and `_max_args: usize` parameters but ignores them entirely, delegating to `user_defined_signature(volatility)`. This function signature promises arity validation that never happens, making the API deceptive.
- `rust/datafusion_ext/src/udf/cdf.rs:17-26` -- CDF rank returns `i32` including `-1` for unknown types, but callers never check for `-1`. An unknown change type silently produces a rank of -1, which is never `is_upsert` or `is_delete`, but this implicit "neither" state is not documented.

**Suggested improvement:**
Remove the dead `_min_args` and `_max_args` parameters from `variadic_any_signature`, or implement actual arity validation. For CDF, document the `-1` sentinel or use `Option<i32>`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 3/3

**Current state:**
All scalar UDFs are pure query functions. `invoke_with_args` returns `ColumnarValue` without side effects. Aggregate accumulators mutate state through `update_batch` and return results through `evaluate` -- this is the expected CQS pattern for stateful aggregation.

No action needed.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 3/3

**Current state:**
`build_udf` in `function_factory.rs:303-428` is an explicit composition root that maps UDF names to concrete implementations. `register_primitives` iterates the policy and calls `build_udf` for each. The `udf_registry` module is the top-level composition point.

No action needed.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
Rust's trait system naturally enforces composition. `ArgBestAccumulator` is shared across `ArgMaxUdaf`, `ArgMinUdaf`, and `AsofSelectUdaf` via composition (stored as accumulator), not via inheritance.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most UDFs interact with their direct collaborators (Arrow arrays, ScalarValue). One pattern violates this principle.

**Findings:**
- `rust/datafusion_ext/src/udaf_builtin.rs:829-834` -- `ArgBestAccumulator.update_from_arrays` calls `ScalarValue::try_from_array(values.as_ref(), row)` per element, materializing a `ScalarValue` from each array row, then immediately decomposing it via `scalar_to_string`/`scalar_to_i64`. This reaches through ScalarValue as an intermediary when direct typed array access (`downcast_ref::<StringArray>()`) would be more direct and more performant.
- `rust/datafusion_ext/src/udf/collection.rs` -- Similar per-element `ScalarValue::try_from_array` pattern in collection operations.

**Suggested improvement:**
In `update_from_arrays`, downcast `values` to `StringArray`/`LargeStringArray`/`StringViewArray` and `keys` to `Int64Array` directly, iterating with typed accessors. This eliminates the ScalarValue intermediary and improves both clarity and performance.

**Effort:** small
**Risk if unaddressed:** low (correctness is fine; performance and readability impact only)

---

#### P15. Tell, don't ask -- Alignment: 3/3

**Current state:**
UDFs encapsulate their transformation logic behind `invoke_with_args`. Callers (DataFusion engine) tell UDFs to compute results rather than extracting internal state.

No action needed.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 3/3

**Current state:**
All scalar and aggregate UDFs are pure functional transforms. The only IO-performing code is in `udtf_sources.rs` which properly isolates async Delta Lake operations behind `async_runtime::block_on`.

No action needed.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All UDFs are `Volatility::Immutable`. Re-invoking with the same inputs produces identical outputs. Hash functions use deterministic Blake2b. The `_v1` naming convention on Python-side outputs (noted in CLAUDE.md) aligns with this.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Blake2b hashing with explicit bit-masking (`hash64_value` masks to 63 bits for signed i64 compatibility) ensures reproducible outputs. `BTreeSet`/`BTreeMap` usage in accumulators produces deterministic ordering. `any_value_det` explicitly selects by minimum order key for deterministic tie-breaking.

No action needed.

---

### Category: Simplicity (19-22) -- Focus Area

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most UDFs are straightforward. The main complexity concern is the 4-way match pattern in two-argument UDFs.

**Findings:**
- `rust/datafusion_ext/src/udf/hash.rs` -- Two-argument UDFs like `PrefixedHash64Udf` and `StableIdUdf` handle four match arms: (Scalar, Scalar), (Array, Array), (Array, Scalar), (Scalar, Array). This pattern repeats across multiple UDFs. DataFusion's `ColumnarValue::to_array()` could reduce all cases to a single array-based code path.
- `rust/datafusion_ext/src/udf/span.rs:487-544` -- `SpanOverlapsUdf.invoke_with_args` and `span_contains` (starting at line 599) have nearly identical struct-extraction boilerplate (~45 lines each) before the single line that differs (overlap vs. containment predicate).

**Suggested improvement:**
For two-argument hash UDFs, use `ColumnarValue::to_array(num_rows)` to normalize all inputs to arrays, then process uniformly. For span comparison UDFs, extract a shared `span_pair_comparison` helper that takes a predicate closure.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves a clear purpose. Two items are dead or vestigial.

**Findings:**
- `rust/datafusion_ext/src/udf/function_factory.rs:274-281` -- `touch_policy_fields` exists solely to suppress unused-field warnings by reading `policy.allow_async`, `policy.domain_operator_hooks`, `primitive.return_type`, and `primitive.description`. This is a workaround for incomplete feature usage rather than a design choice.
- `rust/datafusion_ext/src/udf/common.rs:64-70` -- `variadic_any_signature` ignores its `_min_args` and `_max_args` parameters. These were presumably intended for future use but create a misleading API.

**Suggested improvement:**
For `touch_policy_fields`, either use the fields in `register_primitives` (e.g., set description on the UDF, validate return_type) or add `#[allow(dead_code)]` with a comment explaining the roadmap intent. For `variadic_any_signature`, either implement the arity check or remove the dead parameters.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Two copy-paste error messages actively violate user expectations. This is the highest-risk finding.

**Findings:**
- `rust/datafusion_ext/src/udf/metadata.rs:51-54` -- `CpgScoreUdf.coerce_types` returns the error message: `"stable_id_parts expects between two and sixty-five arguments"`. A user calling `cpg_score(x, y)` who triggers this error would see a message about a completely different function (`stable_id_parts`), creating confusion about which function failed.
- `rust/datafusion_ext/src/udf/string.rs:331-335` -- `Utf8NullIfBlankUdf.coerce_types` returns the error message: `"qname_normalize expects between one and three arguments"`. A user calling `utf8_null_if_blank(x, y, z, w)` would see an error about `qname_normalize`.
- `rust/datafusion_ext/src/udf/metadata.rs:50-57` vs `rust/datafusion_ext/src/udf/metadata.rs:100-103` -- `CpgScoreUdf.coerce_types` accepts 2-65 args but `invoke_with_args` expects exactly 1 arg. If coercion succeeds with 2 args, `invoke_with_args` returns a different error. The contract is self-contradictory.

**Suggested improvement:**
1. Fix `CpgScoreUdf.coerce_types` error message to `"cpg_score expects one argument"` and change the range check to `arg_types.len() != 1`.
2. Fix `Utf8NullIfBlankUdf.coerce_types` error message to `"utf8_null_if_blank expects one argument"` and change the range check to `arg_types.len() != 1`.

**Effort:** small
**Risk if unaddressed:** high -- Incorrect error messages are user-facing and cause debugging confusion. The contradictory contract in `CpgScoreUdf` could also cause silent failures if DataFusion optimizer passes 2+ arguments through coercion.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
`udf/mod.rs:12-20` uses glob re-exports (`pub use cdf::*`, etc.) which implicitly makes all `pub` items from submodules part of the crate's public surface.

**Findings:**
- `rust/datafusion_ext/src/udf/mod.rs:12-20` -- Glob re-exports from 9 submodules. Adding a `pub` function to any submodule automatically extends the public contract. There is no explicit listing of which items are intentionally public.
- The UDF constructor functions (e.g., `stable_hash64_udf()`, `span_make_udf()`) are the intended public surface, but the UDF structs themselves are `pub(crate)` and don't leak. This is partially good.

**Suggested improvement:**
Replace glob re-exports with explicit re-exports of the intended public surface (constructor functions and any necessary types). This makes the public contract explicit and prevents accidental API expansion.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24) -- Focus Area

#### P23. Design for testability -- Alignment: 3/3

**Current state:**
The conformance test suite in `tests/udf_conformance.rs` (1368 lines, 30+ tests) is comprehensive and well-designed.

**Findings (positive):**
- `tests/udf_conformance.rs:141-153` -- `stable_hash64_matches_expected` validates output against an independent Blake2b computation.
- `tests/udf_conformance.rs:203-216` -- `scalar_udfs_preserve_row_count` validates the row-count invariant across null inputs.
- `tests/udf_conformance.rs:219-234` -- `scalar_udf_snapshot_metadata_complete` validates that all registered UDFs have complete metadata.
- `tests/udf_conformance.rs:237-341` -- `information_schema_routines_match_snapshot` cross-validates the registry snapshot against DataFusion's information_schema.
- `tests/udf_conformance.rs:366-400` -- `simplify_span_len_constant_folds` tests the optimizer hook directly.
- `tests/udf_conformance.rs:1029-1073` -- `list_unique_null_treatment_respects_clause` tests IGNORE NULLS / RESPECT NULLS semantics.
- `tests/udf_conformance.rs:1076-1114` -- `list_unique_sliding_window_retracts` tests sliding window retraction behavior.

Pure functions (`hash64_value`, `cdf_rank`, `normalize_text`) are easily testable without mocks. The `eval_udf_on_literals` helper in `common.rs:187-231` enables testing simplify hooks without a full session.

No action needed.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
There is no structured logging or tracing within UDF execution paths. While UDFs are typically too hot for per-invocation logging, factory registration and error paths would benefit from structured diagnostics.

**Findings:**
- `rust/datafusion_ext/src/udf/function_factory.rs:58-63` -- `install_function_factory_native` registers primitives without logging which UDFs were registered, how many, or the policy version.
- `rust/datafusion_ext/src/udtf_sources.rs:29-38` -- `register_external_udtfs` registers table functions without structured logging.
- UDF error paths use `DataFusionError::Plan(format!(...))` which produces unstructured error strings. No error codes or structured fields.

**Suggested improvement:**
Add `tracing::info!` spans around factory registration (count of UDFs registered, policy version). Consider structured error types or error codes for UDF validation failures to enable programmatic error handling.

**Effort:** medium
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: udaf_builtin.rs as a monolith with internal duplication

`udaf_builtin.rs` at 2225 lines is the largest file in scope and contains multiple forms of duplication: identical accumulator behavior between `CollectSetUdaf` and `ListUniqueUdaf`, near-identical trait implementations across `ArgMax`/`ArgMin`/`AsofSelect`, and re-implemented utility functions that exist in `udf/common.rs`. This single file drives findings for P3 (SRP), P4 (cohesion/coupling), P7 (DRY), and P19 (KISS).

**Root cause:** The aggregate UDFs were likely developed incrementally by copying and modifying existing implementations, without a consolidation pass.

**Affected principles:** P3, P4, P7, P14, P19

**Suggested approach:** Consolidate in three steps: (1) move duplicated utilities to `udf/common.rs`, (2) extract `ArgBestAccumulator` and its consumers to a new `udaf_arg_best.rs`, (3) parameterize `CollectSetUdaf` to delegate to `ListUniqueUdaf` or extract shared behavior.

### Theme 2: Copy-paste error message drift

Two `coerce_types` implementations contain error messages from different UDFs, indicating copy-paste without update. This is a variant of semantic duplication -- the "knowledge" of which function is being invoked was duplicated in error strings and drifted.

**Root cause:** Each UDF `coerce_types` hardcodes its function name in error strings. When implementations are copied, the strings are not updated.

**Affected principles:** P8, P21

**Suggested approach:** Fix the immediate bugs. Longer-term, consider a `coerce_exact(name, arg_types, expected_count)` helper in `common.rs` that generates correct error messages from the UDF name, similar to `expect_arg_len_exact`.

### Theme 3: Cross-agent theme -- Two function_factory.rs files

There is `rust/datafusion_ext/src/udf/function_factory.rs` (696 lines) handling IPC policy deserialization and UDF construction, and a top-level `rust/datafusion_ext/src/function_factory.rs` (referenced in module structure) handling SQL macro factory installation. The naming collision could confuse developers navigating the codebase.

**Suggested approach:** Rename `udf/function_factory.rs` to `udf/policy_factory.rs` or `udf/ipc_factory.rs` to distinguish it from the top-level SQL macro factory.

### Theme 4: Python integration surface

The UDF crate exposes its functions to Python through PyO3 bindings and the `udf_registry` module. Key integration points:
- `udf_config::CodeAnatomyUdfConfig` carries domain configuration across the Python-Rust boundary via `with_updated_config`.
- `function_factory.rs` accepts IPC-serialized Arrow batches as the policy payload, enabling Python to drive UDF registration.
- `registry_snapshot` provides metadata for Python-side introspection.
- All UDFs marked `Volatility::Immutable` aligns with the Python-side determinism contract.

No immediate design concerns for the integration surface.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21, P8 | Fix copy-paste error messages in `CpgScoreUdf.coerce_types` (metadata.rs:51-54) and `Utf8NullIfBlankUdf.coerce_types` (string.rs:331-335) | small | Eliminates user-facing confusion; fixes contradictory contract in CpgScoreUdf |
| 2 | P7 | Move `string_array_any`, `scalar_to_string`, `scalar_to_i64` from `udaf_builtin.rs` to `udf/common.rs` | small | Eliminates triple knowledge duplication |
| 3 | P10, P20 | Remove dead params from `variadic_any_signature` (common.rs:64-70) | small | Removes misleading API |
| 4 | P9 | Add named constants for CDF rank values in `cdf.rs:17-26` | small | Self-documenting code; prevents magic number drift |
| 5 | P7 | Consolidate `CollectSetUdaf` to delegate to `ListUniqueUdaf` internals (udaf_builtin.rs:167-244) | medium | Eliminates ~80 lines of identical accumulator code |

## Recommended Action Sequence

1. **Fix error messages** (P8, P21): Correct `CpgScoreUdf.coerce_types` at `metadata.rs:50-57` to say `"cpg_score expects one argument"` and accept exactly 1 argument. Correct `Utf8NullIfBlankUdf.coerce_types` at `string.rs:330-335` to say `"utf8_null_if_blank expects one argument"`.

2. **Consolidate duplicated utilities** (P7, P4): Move `string_array_any`, `scalar_to_string`, `scalar_to_i64` from `udaf_builtin.rs` into `udf/common.rs`. Unify the `scalar_to_i64` variants (add timestamp handling from `udaf_builtin.rs:731-740` to the canonical version). Import in `udaf_builtin.rs`.

3. **Clean up dead parameters** (P10, P20): Remove `_min_args` and `_max_args` from `variadic_any_signature` in `common.rs:64-70`. Update all callers (search for `variadic_any_signature` across the crate).

4. **Add CDF rank constants** (P9): Replace magic numbers in `cdf.rs:17-26` with named constants. Update comparisons at `cdf.rs:141` and `cdf.rs:201`.

5. **Consolidate CollectSet/ListUnique** (P7): Make `CollectSetUdaf` delegate to the same accumulator types as `ListUniqueUdaf`, or extract a shared `UniqueListCollector` that both use, parameterized only by name and state field prefix.

6. **Extract span comparison helper** (P19): Create a shared `span_pair_invoke` function in `span.rs` that handles struct extraction and iteration, parameterized by a comparison closure. Use it in both `SpanOverlapsUdf` and `SpanContainsUdf`.

7. **Rename udf/function_factory.rs** (cross-agent theme): Rename to `udf/policy_factory.rs` to disambiguate from the top-level `function_factory.rs`.

8. **Replace glob re-exports** (P22): In `udf/mod.rs`, replace `pub use cdf::*` etc. with explicit re-exports of constructor functions.

9. **Add registration logging** (P24): Add `tracing::info!` to `install_function_factory_native` and `register_external_udtfs` recording count of registered UDFs and policy metadata.

10. **Extract ArgBest module** (P3, P7): Move `ArgBestAccumulator`, `ArgBestMode`, `AnyValueDetUdaf`, `ArgMaxUdaf`, `ArgMinUdaf`, `AsofSelectUdaf` to a dedicated `udaf_arg_best.rs` module to reduce `udaf_builtin.rs` by ~500 lines.
