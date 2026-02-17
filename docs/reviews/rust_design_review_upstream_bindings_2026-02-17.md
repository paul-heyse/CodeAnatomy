# Design Review: Upstream Expression Bindings & Type Machinery

**Date:** 2026-02-17
**Scope:** `rust/datafusion_python/src/expr/` (58 files), `expr.rs`, `udaf.rs`, `udf.rs`, `udwf.rs`, `udtf.rs`, `substrait.rs`, `sql/` (3 files), `unparser/` (2 files), `physical_plan.rs`, `dataset.rs`, `dataset_exec.rs`, `record_batch.rs`, `table.rs`, `pyarrow_filter_expression.rs`, `pyarrow_util.rs`, `build.rs`, `common/data_type.rs`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 79 (58 expr/ + 21 supporting files), 9,819 LOC total

## Executive Summary

This scope is a PyO3 binding layer wrapping DataFusion's expression, logical plan, and UDF types for Python consumption. The code is architecturally sound as a thin adapter layer with consistent `From`/`TryFrom` patterns, but suffers from severe DRY violations -- 10 structurally identical unary boolean expression wrappers in `bool_expr.rs` and 58 files following nearly identical boilerplate patterns that could be reduced via declarative macros. There are two concrete correctness bugs (duplicate `PyLiteral` registration in `init_module`, copy-paste error message "ScalarValue::LargeList" for the `Union` case in `data_type.rs`), a `todo!()` panic in `indexed_field.rs`, duplicated error helper functions across `errors.rs` and `sql/exceptions.rs`, and duplicated builder methods between `PyExpr` and `PyExprFuncBuilder`. The overall alignment is moderate (mean 2.0/3) with DRY as the systemic weakness.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `PyExpr.expr` is `pub` field, exposing DF `Expr` directly |
| 2 | Separation of concerns | 3 | - | - | Clean adapter layer; conversion logic is well-separated from DF internals |
| 3 | SRP | 2 | medium | low | `expr.rs` (851 LOC) mixes dispatch, operator methods, type inference, and module registration |
| 4 | High cohesion, low coupling | 2 | small | low | Expression wrappers cohesive individually but coupling to DF types is inherent |
| 5 | Dependency direction | 3 | - | - | Correct: adapter depends on core DF types, not vice versa |
| 6 | Ports & Adapters | 3 | - | - | This IS the adapter layer; well-positioned architecturally |
| 7 | DRY | 0 | large | medium | 10 identical boolean wrappers; 6 identical builder methods duplicated; duplicate error helpers; duplicate `PyLiteral` registration |
| 8 | Design by contract | 2 | small | medium | `todo!()` in `indexed_field.rs:64`; some `unwrap()` in `dataset.rs:79-81` |
| 9 | Parse, don't validate | 2 | small | low | String-based window frame units parsed inline; parquet type string parsing duplicates logic |
| 10 | Make illegal states unrepresentable | 2 | medium | low | `DatasetExec` accepts `Option<Vec<String>>` columns + `Option<Py<PyAny>>` filter as separate optionals |
| 11 | CQS | 3 | - | - | Methods are clearly queries (getters) or conversions; no mixed command/query |
| 12 | Dependency inversion | 2 | small | low | UDF factories use `Py<PyAny>` opaque handle; no protocol abstraction |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; uses composition via wrapped inner types |
| 14 | Law of Demeter | 2 | small | low | `dataset.getattr("schema")?.call_method1("field", ...)?.getattr("name")?` chains in `dataset_exec.rs:87-91` |
| 15 | Tell, don't ask | 2 | small | low | `PyWindowExpr` methods take `expr: PyExpr` and interrogate it internally |
| 16 | Functional core, imperative shell | 3 | - | - | All conversions are pure; IO boundary is Python GIL acquisition |
| 17 | Idempotency | 3 | - | - | All operations are stateless conversions or constructors |
| 18 | Determinism | 3 | - | - | Deterministic conversions throughout |
| 19 | KISS | 2 | medium | low | Boilerplate volume obscures the simple underlying pattern |
| 20 | YAGNI | 2 | small | low | `PySignature` (signature.rs) is dead code (`#[allow(dead_code)]`, empty `#[pymethods]`) |
| 21 | Least astonishment | 2 | small | medium | Copy-paste error: `ScalarValue::Union` error says "ScalarValue::LargeList" |
| 22 | Public contracts | 2 | small | low | No versioning or explicit stable surface; `to_variant` has 4 unsupported variants returning runtime errors |
| 23 | Design for testability | 1 | large | medium | 0 tests in scope for ~9.8K LOC; all existing tests are for `codeanatomy_ext` decomposition |
| 24 | Observability | 1 | medium | low | No tracing/logging; errors lose Python traceback context via generic `format!("{e:?}")` |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `PyExpr` wrapper at `rust/datafusion_python/src/expr.rs:119-121` exposes `pub expr: Expr`, allowing any consumer to bypass the wrapper and directly manipulate the inner DataFusion `Expr`. Similarly, `PyAggregateUDF.function`, `PyScalarUDF.function`, `PyWindowUDF.function` use `pub(crate)` which is appropriate for crate-internal access.

**Findings:**
- `rust/datafusion_python/src/expr.rs:120` -- `pub expr: Expr` allows Python-side callers and internal code to bypass all wrapper methods. While PyO3 does not expose Rust field visibility to Python, Rust-side consumers in this crate access `.expr` directly rather than through accessor methods.
- `rust/datafusion_python/src/substrait.rs:33` -- `PyPlan` has `pub plan: Plan`, same concern.

**Suggested improvement:**
Change `PyExpr.expr` to `pub(crate) expr: Expr`. This is safe because all internal access is within the crate. The `From<PyExpr> for Expr` impl already provides the canonical way to extract the inner type.

**Effort:** small
**Risk if unaddressed:** low

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
`expr.rs` (851 LOC) is the largest file in the expression subsystem and serves 5 distinct roles: (1) `PyExpr` struct definition and `From` impls, (2) `to_variant` dispatch, (3) Python operator overloads, (4) `_types` type inference, and (5) `init_module` registration of 80+ classes. These change for different reasons.

**Findings:**
- `rust/datafusion_python/src/expr.rs:143-202` -- `to_variant` is a 60-line match that must be updated whenever a new `Expr` variant is added to DataFusion.
- `rust/datafusion_python/src/expr.rs:703-758` -- `_types` is a type-inference function embedded in the `PyExpr` impl that has its own set of match arms over `Operator` and `Expr` variants.
- `rust/datafusion_python/src/expr.rs:762-851` -- `init_module` is 90 lines of sequential `add_class` calls, duplicating `PyLiteral` registration at lines 765 and 767.

**Suggested improvement:**
Extract `init_module` into a dedicated `expr/registration.rs`. Extract `_types` into a standalone function in `common/data_type.rs` where the type mapping knowledge already lives. This reduces `expr.rs` to ~600 LOC focused on `PyExpr` methods.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

This is the most severely violated principle, consistent with the prior round's finding (avg 0.8/3).

**Current state:**
Multiple structural patterns are duplicated with high volume and high risk of drift.

**Findings:**

1. **Boolean expression wrappers (10x identical pattern):** `rust/datafusion_python/src/expr/bool_expr.rs:25-323` defines 10 structs (`PyNot`, `PyIsNotNull`, `PyIsNull`, `PyIsTrue`, `PyIsFalse`, `PyIsUnknown`, `PyIsNotTrue`, `PyIsNotFalse`, `PyIsNotUnknown`, `PyNegative`) that are structurally identical: each wraps a single `Expr`, implements `new()`, `Display`, and a `fn expr() -> PyResult<PyExpr>` method. This is 300 lines for what a macro could express in ~20 lines. **NEW issue** not covered in prior review.

2. **Builder method duplication (6x2 methods):** `rust/datafusion_python/src/expr.rs:569-598` defines `order_by`, `filter`, `distinct`, `null_treatment`, `partition_by`, and `window_frame` on `PyExpr`, then `rust/datafusion_python/src/expr.rs:653-686` duplicates all six methods identically on `PyExprFuncBuilder`. The only difference is the receiver type (`self.expr.clone()` vs `self.builder.clone()`). **NEW issue.**

3. **Error helper duplication:** `rust/datafusion_python/src/errors.rs:77-83` defines `py_type_err(e: impl Debug)` and `py_runtime_err(e: impl Debug)`. `rust/datafusion_python/src/sql/exceptions.rs:22-28` defines `py_type_err(e: impl Debug + Display)` and `py_runtime_err(e: impl Debug + Display)` with a slightly different trait bound. Both are used across the codebase. This is semantic duplication -- two authorities for the same error-construction policy. **NEW issue.**

4. **Duplicate `PyLiteral` class registration:** `rust/datafusion_python/src/expr.rs:765` and `rust/datafusion_python/src/expr.rs:767` both call `m.add_class::<PyLiteral>()?;`. The second call is redundant. **NEW issue** (correctness concern -- no runtime error but wasteful and confusing).

5. **LogicalNode boilerplate across 20+ files:** Every logical plan wrapper (e.g., `filter.rs`, `join.rs`, `drop_table.rs`, `create_memory_table.rs`, etc.) implements the identical pattern: struct wrapping DF type, `From<X> for PyX`, `From<PyX> for X`, `Display`, `LogicalNode` trait impl, and `#[pymethods]`. At least 25 files follow this exact template with no macro abstraction.

6. **Copy-paste error message:** `rust/datafusion_python/src/common/data_type.rs:339-341` -- `ScalarValue::Union(_, _, _)` returns `PyNotImplementedError::new_err("ScalarValue::LargeList".to_string())`. The error message says "LargeList" but the variant is `Union`. This is a copy-paste bug from the `LargeList` arm two lines above. **NEW issue** (correctness).

7. **DataType mapping triplication:** `map_from_arrow_type` (data_type.rs:82-250), `map_from_scalar_to_arrow` (data_type.rs:258-348), and `friendly_arrow_type_name` (data_type.rs:536-580) all exhaustively match on `DataType` or `ScalarValue` variants. Any new Arrow type requires updating all three independently.

**Suggested improvements:**
- Create a `unary_bool_expr!` macro that generates the struct, `new`, `Display`, and `#[pymethods]` for a given name string. Reduces `bool_expr.rs` from 300 to ~30 LOC.
- Create a `logical_plan_wrapper!` macro for the `From`/`Display`/`LogicalNode` boilerplate across the 25+ plan wrapper files.
- Consolidate `errors.rs` and `sql/exceptions.rs` error helpers into a single module.
- Fix the copy-paste error message for `ScalarValue::Union`.
- Remove the duplicate `PyLiteral` registration.

**Effort:** large (macro work); small for the bug fixes
**Risk if unaddressed:** medium -- new DataFusion versions add Expr/Plan variants requiring updates in many files; copy-paste bugs propagate

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Most functions have clear Rust type contracts via `PyResult<T>` return types, but several contain runtime panics or incomplete handling.

**Findings:**
- `rust/datafusion_python/src/expr/indexed_field.rs:64` -- `_ => todo!()` will panic at runtime for non-`NamedStructField` field access patterns (e.g., list index, map key). **NEW issue.**
- `rust/datafusion_python/src/dataset.rs:79-81` -- Two `unwrap()` calls on Python attribute access (`getattr("schema").unwrap().extract().unwrap()`). The comment says "this can panic but...should never" -- but a malformed PyArrow dataset could trigger it. **NEW issue.**
- `rust/datafusion_python/src/udwf.rs:134` -- `unwrap_or(false)` silently swallows errors from Python method calls (`supports_retract_batch`, `is_causal`, `include_rank`, etc.). This is intentional fail-open behavior but is not documented.

**Suggested improvement:**
Replace `todo!()` in `indexed_field.rs:64` with a proper `PyNotImplementedError`. Replace `unwrap()` in `dataset.rs:79-81` with `?` propagation via a fallible `schema()` method.

**Effort:** small
**Risk if unaddressed:** medium -- `todo!()` panics crash the entire Python process

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Window frame unit parsing in `rust/datafusion_python/src/expr/window.rs:186-194` converts a raw `&str` to `WindowFrameUnits` via inline string matching. This is a boundary parse that would be cleaner as a `TryFrom<&str> for WindowFrameUnits` implementation.

**Findings:**
- `rust/datafusion_python/src/expr/window.rs:186-194` -- String-based unit parsing with inline match, repeated for start/end bounds.
- `rust/datafusion_python/src/common/data_type.rs:367-384` -- Parquet type string parsing with inline match.
- `rust/datafusion_python/src/common/data_type.rs:603-628` -- Arrow type string parsing with inline match.

These are all boundary parsing operations that convert stringly-typed Python inputs into Rust types, which is correct placement. The concern is that the string-to-type mapping logic is scattered rather than centralized.

**Suggested improvement:**
Use DataFusion's built-in `WindowFrameUnits::from_str` if available in DF 51, or extract a `parse_window_frame_units` helper.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most types use Rust's type system effectively (enum wrappers, `From`/`TryFrom`). However, `DatasetExec` has an opportunity for improvement.

**Findings:**
- `rust/datafusion_python/src/dataset_exec.rs:67-75` -- `DatasetExec` stores `columns: Option<Vec<String>>` and `filter_expr: Option<Py<PyAny>>` as independent optionals. While this correctly represents "no projection" and "no filter" as distinct from "empty projection/filter", the `Py<PyAny>` for `filter_expr` is completely untyped.
- `rust/datafusion_python/src/udtf.rs:42-45` -- `PyTableFunctionInner` enum correctly distinguishes `PythonFunction` from `FFIFunction`, which is good state modeling.

**Suggested improvement:**
No critical improvement needed. The `Option` usage is appropriate for this adapter pattern.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion -- Alignment: 2/3

**Current state:**
UDF bridge types (`RustAccumulator`, `RustPartitionEvaluator`) depend on `Py<PyAny>` -- a completely opaque Python handle. There is no Rust-side protocol/trait that defines what methods the Python object must implement.

**Findings:**
- `rust/datafusion_python/src/udaf.rs:38-39` -- `RustAccumulator` wraps `Py<PyAny>` and calls `call_method0("state")`, `call_method0("evaluate")`, etc. by string name. If the Python object is missing a method, the error is discovered at runtime with a generic "attribute not found" message.
- `rust/datafusion_python/src/udwf.rs:44-45` -- Same pattern for `RustPartitionEvaluator`.

**Suggested improvement:**
This is an inherent limitation of cross-language binding via PyO3. A protocol-like validation at construction time (checking `hasattr` for required methods) would provide earlier error detection. However, this is a YAGNI consideration -- the current approach matches upstream DataFusion-Python.

**Effort:** small
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Several methods traverse deep into Python object graphs.

**Findings:**
- `rust/datafusion_python/src/dataset_exec.rs:87-91` -- `dataset.getattr("schema")?.call_method1("field", (*index,))?.getattr("name")?.extract()?` -- 4-level chain through Python objects. This is fragile to PyArrow API changes.
- `rust/datafusion_python/src/dataset_exec.rs:195-197` -- Similar chain for `dataset.getattr("schema")` in the `execute` method.

**Suggested improvement:**
Extract a helper function `fn dataset_field_name(dataset: &Bound<PyAny>, index: usize) -> PyResult<String>` to encapsulate the traversal.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`PyWindowExpr` methods take an `expr: PyExpr` argument, unwrap it to check if it is a `WindowFunction` variant, and then extract fields. This "ask, then act" pattern is repeated 5 times.

**Findings:**
- `rust/datafusion_python/src/expr/window.rs:123-166` -- Five methods (`get_sort_exprs`, `get_partition_exprs`, `get_args`, `window_func_name`, `get_frame`) all take `expr: PyExpr`, match on `Expr::WindowFunction`, and return an error otherwise. The window function expr should be stored as a typed field rather than re-validated on every access.

**Suggested improvement:**
Store window function expressions in `PyWindowExpr` as a pre-validated field during construction, eliminating repeated match-and-validate in each accessor.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 3/3

All conversion logic is pure. The only side-effectful operations are Python GIL acquisition (via `Python::attach`), which is correctly confined to the outermost boundary in PyO3 method implementations and UDF bridges.

---

#### P17. Idempotency -- Alignment: 3/3

All operations in scope are stateless constructors or conversions. No mutation of shared state.

---

#### P18. Determinism -- Alignment: 3/3

All conversions are deterministic. No randomness, no non-deterministic ordering.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The underlying pattern is extremely simple (wrap DF type, provide From/TryFrom, expose accessor methods via PyO3). However, the sheer volume of boilerplate (58 files of nearly identical structure) makes it difficult to see this simplicity.

**Findings:**
- The expr/ directory contains 58 files for what is fundamentally a single architectural pattern applied to ~50 DataFusion types. The per-file simplicity is high, but the aggregate complexity of maintaining 58 parallel files is significant.
- `rust/datafusion_python/src/expr.rs:396-510` -- `rex_call_operands` is a 115-line method that handles every `Expr` variant individually. While thorough, this is complex enough to warrant extraction.

**Suggested improvement:**
Introduce declarative macros for the three dominant patterns: (1) unary boolean expression wrappers, (2) logical plan wrappers, (3) simple expression wrappers. This could reduce the total file count from 58 to ~30 while preserving the same public API.

**Effort:** medium
**Risk if unaddressed:** low -- but increases maintenance burden for DF version upgrades

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
One file is clearly dead code.

**Findings:**
- `rust/datafusion_python/src/expr/signature.rs` -- `PySignature` is marked `#[allow(dead_code)]` and has an empty `#[pymethods]` block. It wraps `TypeSignature` and `Volatility` but exposes no methods to Python. This appears to be scaffolding that was never completed. **NEW issue.**
- `rust/datafusion_python/src/common/data_type.rs:41-48` -- `RexType` enum has a `Other` variant that is never constructed.

**Suggested improvement:**
Remove `signature.rs` or complete it by adding Python-visible accessor methods. Remove the `Other` variant from `RexType` if it is never used.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs follow predictable patterns. Two specific surprises exist.

**Findings:**
- `rust/datafusion_python/src/common/data_type.rs:339-341` -- `ScalarValue::Union` returns error message "ScalarValue::LargeList". A Python user debugging a Union type conversion would be misled. **NEW correctness bug.**
- `rust/datafusion_python/src/expr.rs:172-177` -- `to_variant()` for `ScalarFunction` and `WindowFunction` returns runtime errors. These are two of the most common expression types. A user calling `expr.to_variant()` on a simple function call like `abs(col("x"))` would get an unexpected error. **Pre-existing issue.**
- `rust/datafusion_python/src/physical_plan.rs:83-86` -- `from_proto` error message says "Unable to decode logical node" for what is a physical plan node. Copy-paste from `logical.rs`.

**Suggested improvement:**
Fix the Union error message. Implement `to_variant()` for `ScalarFunction` and `WindowFunction`. Fix the physical plan error message.

**Effort:** small (error messages); medium (ScalarFunction/WindowFunction `to_variant`)
**Risk if unaddressed:** medium -- incorrect error messages waste developer debugging time

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
The Python module surface (`datafusion.expr`, `datafusion.substrait`, `datafusion.unparser`) is implicitly defined by `init_module` and `#[pyclass]` annotations. There is no explicit contract versioning beyond the crate version.

**Findings:**
- `rust/datafusion_python/src/expr.rs:762-851` -- `init_module` is the sole authority for which types are exposed to Python. It registers 80+ classes but has no mechanism for deprecation warnings when types change between DataFusion versions.
- 4 `Expr` variants return `py_unsupported_variant_err` from `to_variant()` (`ScalarFunction`, `WindowFunction`, `Wildcard`, `OuterReferenceColumn`). These are implicit contract gaps -- callers cannot reliably use `to_variant()` without checking the variant first.

**Suggested improvement:**
Add a `supported_variants()` class method or documentation that makes the coverage gap explicit.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
There are zero Rust-side tests for the entire 9.8K LOC scope. All existing tests in `rust/datafusion_python/tests/` are for the `codeanatomy_ext` decomposition (9 test files, all from the prior review's S8 improvements). The upstream expression bindings, UDF bridges, type conversions, and plan wrappers have no Rust-side test coverage.

**Findings:**
- The `From`/`TryFrom` conversions are easily testable without Python -- they are pure Rust functions that can be tested with `#[test]` functions.
- The `_types` function in `expr.rs:703-758` has complex matching logic over `Operator` and `Expr` variants that could be table-driven tested.
- `PyArrowFilterExpression::try_from` in `pyarrow_filter_expression.rs:103-179` requires Python runtime, making it harder to test, but the expression translation logic could be extracted into a pure function.
- `data_type.rs` mapping functions (750+ LOC) have zero test coverage despite being pure functions.

**Suggested improvement:**
Add `#[cfg(test)]` modules for: (1) `DataTypeMap::map_from_arrow_type` round-trip tests for all supported types, (2) `DataTypeMap::map_from_scalar_to_arrow` coverage, (3) `PyExpr::_types` for each operator category, (4) `py_expr_list` conversion.

**Effort:** large
**Risk if unaddressed:** medium -- type mapping bugs (like the Union/LargeList copy-paste) go undetected

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
No tracing spans, no structured logging, no metrics in the entire scope. Error messages are constructed with `format!("{e:?}")` which loses Python traceback structure.

**Findings:**
- `rust/datafusion_python/src/errors.rs:93-94` -- `pyerr_to_dferr` converts Python errors to DataFusion errors by formatting the error as a string, losing the original Python traceback. This makes debugging Python UDF failures difficult.
- `rust/datafusion_python/src/udaf.rs:57` / `udwf.rs:57` -- Errors from Python accumulator/evaluator methods are logged with a brief context string but no tracing span.
- The `dataset_exec.rs` `execute` method (the most performance-critical code path) has no tracing instrumentation.

**Suggested improvement:**
Add `tracing::instrument` attributes to `DatasetExec::execute`, `RustAccumulator` methods, and `RustPartitionEvaluator` methods. Preserve Python traceback in error conversion by including `traceback.format_exception()` output.

**Effort:** medium
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Boilerplate Volume is the Systemic Issue

**Root cause:** The scope wraps 50+ DataFusion types for Python, and each wrapper follows an identical pattern (struct, From, TryFrom, Display, #[pymethods]). Without Rust macros to codegen this pattern, every type requires its own file with 30-100 lines of boilerplate.

**Affected principles:** P7 (DRY), P3 (SRP), P19 (KISS), P23 (testability)

**Suggested approach:** Introduce three declarative macros:
1. `unary_expr_wrapper!` for `bool_expr.rs` patterns (eliminates ~270 LOC)
2. `logical_plan_wrapper!` for plan types like `DropTable`, `Filter`, etc. (eliminates ~600 LOC across 20+ files)
3. `simple_expr_wrapper!` for types like `PyAlias`, `PyColumn`, `PyBetween`, etc.

These macros should generate `From`/`TryFrom` impls, `Display`, `LogicalNode` trait impl, and basic `#[pymethods]` accessors from a declarative specification.

### Theme 2: Error Message Correctness

**Root cause:** Copy-paste during type mapping exhaustive matches produces wrong error messages.

**Affected principles:** P7 (DRY), P8 (design by contract), P21 (least astonishment)

**Specific instances:**
- `data_type.rs:340` -- "ScalarValue::LargeList" for Union variant
- `physical_plan.rs:84` -- "logical node" for physical plan
- `expr.rs:765,767` -- duplicate PyLiteral registration

### Theme 3: Upstream Completeness Gaps

**Root cause:** This is a fork of `apache/datafusion-python` at v51. Some DF 51 `Expr` variants (`ScalarFunction`, `WindowFunction`, `OuterReferenceColumn`, `Wildcard`) are not supported in `to_variant()`, and `PySignature` is dead code.

**Affected principles:** P22 (public contracts), P21 (least astonishment), P20 (YAGNI)

---

## Upstream Divergence from apache/datafusion-python

Based on the code structure and patterns observed:

1. **`codeanatomy_ext/` module additions:** The `rust/datafusion_python/src/codeanatomy_ext/` directory (out of this review's scope but visible in project structure) is a CodeAnatomy-specific extension. The upstream `expr/` layer itself appears to closely track `apache/datafusion-python`.

2. **Error handling divergence:** `rust/datafusion_python/src/errors.rs` imports from `datafusion_ext::errors` (`impl_error_from!`, `ExtError`), which is a CodeAnatomy-specific error extension. The upstream uses a simpler error type. The `PyDataFusionError::ExtError` variant is CodeAnatomy-specific.

3. **`common/data_type.rs` type mapping:** The `DataTypeMap`, `RexType`, `PythonType`, and `SqlType` types are upstream DataFusion-Python constructs. No CodeAnatomy-specific modifications detected.

4. **No custom expression types added:** All expression wrappers correspond to standard DataFusion `Expr` and `LogicalPlan` variants. No CodeAnatomy-specific expression types were found.

5. **`indexed_field.rs` `todo!()` is upstream:** This appears to be an incomplete upstream implementation that was inherited.

---

## Python Integration Boundary Assessment

**Test coverage gap:** The 9 test files in `rust/datafusion_python/tests/` test only the `codeanatomy_ext` decomposition. The upstream expression bindings layer (~9.8K LOC) has zero Rust-side tests. Python-side tests for this layer exist in the Python test suite but were not counted in this scope.

**GIL acquisition patterns:** All Python interactions correctly use `Python::attach(|py| ...)` (the current PyO3 v0.23+ pattern, replacing the older `Python::with_gil`). This is consistent and well-applied.

**Memory model:** All PyO3 classes use `#[pyclass(frozen)]`, which is correct for immutable DataFusion types. This prevents Python-side mutation of Rust-owned data.

**Boundary safety:** The `Py<PyAny>` handles in UDF bridges (`udaf.rs`, `udwf.rs`, `udtf.rs`) correctly acquire the GIL before accessing Python objects. No GIL safety violations detected.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7, P21 | Fix copy-paste error: `data_type.rs:340` says "LargeList" for Union | small | High -- correctness bug |
| 2 | P7 | Remove duplicate `PyLiteral` registration at `expr.rs:767` | small | Medium -- eliminates confusion |
| 3 | P8 | Replace `todo!()` in `indexed_field.rs:64` with `PyNotImplementedError` | small | High -- prevents process crash |
| 4 | P21 | Fix `physical_plan.rs:84` error message ("logical node" -> "physical plan node") | small | Medium -- error clarity |
| 5 | P7 | Consolidate `errors.rs` and `sql/exceptions.rs` error helpers into single module | small | Medium -- eliminates parallel authorities |

## Recommended Action Sequence

1. **[P7/P21/P8] Fix correctness bugs first** (items 1-4 from Quick Wins). These are small, zero-risk changes: fix `data_type.rs:340` Union error message, remove duplicate `PyLiteral` registration, replace `todo!()` with proper error, fix `physical_plan.rs` error message. (~30 minutes)

2. **[P7] Consolidate error helpers.** Merge `sql/exceptions.rs` functions into `errors.rs` (or vice versa). Update 6 importing files. (~1 hour)

3. **[P20] Remove dead code.** Remove `signature.rs` (dead `PySignature` class) or add methods to justify its existence. Remove `RexType::Other` if unused. (~30 minutes)

4. **[P7] Introduce `unary_bool_expr!` macro.** Collapse 10 identical structs in `bool_expr.rs` from 300 LOC to ~30 LOC. This is the highest-LOC-savings single change. (~2 hours)

5. **[P7] Introduce `logical_plan_wrapper!` macro.** Collapse the `From`/`Display`/`LogicalNode` boilerplate across 20+ plan wrapper files. (~4 hours)

6. **[P8] Replace `unwrap()` in `dataset.rs:79-81`.** Convert to `?` propagation to prevent panics on malformed PyArrow datasets. (~30 minutes)

7. **[P23] Add pure-Rust unit tests for `data_type.rs` mapping functions.** These are table-driven tests over all DataType variants -- high value for correctness and easy to write without Python runtime. (~3 hours)

8. **[P3] Extract `init_module` and `_types` from `expr.rs`.** Reduce SRP violations in the main expression file. (~2 hours)

9. **[P22/P21] Implement `to_variant()` for `ScalarFunction` and `WindowFunction`.** These are the most commonly encountered Expr variants; having them return errors is a significant API gap. (~4 hours)

10. **[P24] Add tracing instrumentation to `DatasetExec::execute` and UDF bridges.** Integrate with existing `datafusion-tracing` infrastructure. (~3 hours)
