# DataFusion Rust UDF Best-in-Class Implementation Plan (v4)

Goal: Apply every high-leverage capability in `docs/python_library_reference/datafusion_rust_UDFs.md` to make our Rust UDF stack and its integration across the codebase fully best-in-class.

Constraints
- Rust-only UDFs (no Python/PyArrow/Pandas UDF execution lanes).
- No raw SQL strings in production paths where builder/Expr APIs are available.
- Design phase: aggressive migration allowed; remove legacy behavior when replaced.

Scope ordering: The items below are written to be executed in sequence; later steps assume earlier ones are complete.

Status update (January 26, 2026)
- Completed: Scopes 1-6, 10
- Partial: Scopes 8-9
- Deferred/N/A: Scope 7 (no custom UDWFs yet)
- Pending: Scope 11

---

## Scope 1 - Scalar UDF fast paths (avoid scalar expansion)

Intent: Eliminate `ColumnarValue::values_to_arrays` hot paths by implementing explicit scalar/array handling.

Status: Completed.

Representative pattern
```rust
// rust/datafusion_ext/src/udf_custom.rs
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let n = args.number_rows;
    let (lhs, rhs) = (&args.args[0], &args.args[1]);
    match (lhs, rhs) {
        (ColumnarValue::Scalar(a), ColumnarValue::Scalar(b)) => {
            Ok(ColumnarValue::Scalar(self.eval_scalar(a, b)?))
        }
        (ColumnarValue::Array(a), ColumnarValue::Scalar(b)) => {
            Ok(ColumnarValue::Array(self.eval_array_scalar(a, b, n)?))
        }
        (ColumnarValue::Scalar(a), ColumnarValue::Array(b)) => {
            Ok(ColumnarValue::Array(self.eval_scalar_array(a, b, n)?))
        }
        (ColumnarValue::Array(a), ColumnarValue::Array(b)) => {
            Ok(ColumnarValue::Array(self.eval_array_array(a, b)?))
        }
    }
}
```

Target files
- `rust/datafusion_ext/src/udf_custom.rs`

Deletions
- Remove scalar-expanding `ColumnarValue::values_to_arrays` paths where explicit fast paths are added.

Implementation checklist
- [x] Add scalar/array fast paths for all UDFs currently using `values_to_arrays`.
- [x] Use `to_array_of_size` or `into_array_of_size` when arrayifying is unavoidable.
- [x] Keep row-count invariants (output length must match `args.number_rows`).

Notes
- Implemented explicit scalar/array handling for `cpg_score`, `stable_hash64`, `stable_hash128`,
  `prefixed_hash64`, `stable_id`, `position_encoding_norm`, and `col_to_byte` in
  `rust/datafusion_ext/src/udf_custom.rs`.

---

## Scope 2 - Dynamic CREATE FUNCTION docs + named-arg metadata

Intent: Make SQL-defined macros fully discoverable through `SHOW FUNCTIONS` and `information_schema` by synthesizing `Documentation` plus parameter names from the `CreateFunction` AST.

Status: Completed.

Representative pattern
```rust
// rust/datafusion_ext/src/function_factory.rs
struct SqlMacroUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
    template: Expr,
    parameter_names: Vec<String>,
    documentation: Documentation,
}

impl ScalarUDFImpl for SqlMacroUdf {
    fn documentation(&self) -> Option<&Documentation> {
        Some(&self.documentation)
    }
}
```

Target files
- `rust/datafusion_ext/src/function_factory.rs`

Deletions
- None.

Implementation checklist
- [x] Build `Documentation` from `CreateFunction` (args, types, defaults, syntax example).
- [x] Store docs on `SqlMacroUdf` and return via `documentation()`.
- [x] Ensure parameter names are always applied (and stable) via `.with_parameter_names`.
- [x] Keep `return_type` implemented for `information_schema` compatibility.

Notes
- Added SQL macro docs in `rust/datafusion_ext/src/function_factory.rs` with a dedicated
  `DocSection` ("SQL Macros") and parameter descriptions.

---

## Scope 3 - UDTF argument coercion + rich options

Intent: Make UDTF argument handling resilient and engine-native: constant folding, explicit coercion, optional parameters for format-specific behavior.

Status: Completed.

Representative pattern
```rust
// rust/datafusion_ext/src/udtf_external.rs
fn extract_optional_limit(args: &[Expr]) -> Result<Option<usize>> {
    let expr = args.get(1).ok_or_else(|| plan_err!("missing limit"))?;
    let simplified = simplify_expr(expr.clone())?;
    let value = extract_literal_usize(&simplified)
        .ok_or_else(|| plan_err!("limit must be an integer literal"))?;
    Ok(Some(value))
}
```

Target files
- `rust/datafusion_ext/src/udtf_external.rs`
- `rust/datafusion_ext/src/udtf_builtin.rs` (if we extend built-ins similarly)

Deletions
- None.

Implementation checklist
- [x] Support coercion of constant expressions into literals (for example `CAST(1 AS INT64)`).
- [x] Add optional args for `read_csv` and `read_parquet` (schema hints, header, delimiter, compression).
- [x] Validate argument types explicitly (UDTF args are not coerced by DataFusion).

Notes
- `read_parquet` now accepts `schema_ipc` and `limit`.
- `read_csv` now accepts `schema_ipc`, `has_header`, `delimiter`, `compression`, and `limit`.
- Documentation updated in `rust/datafusion_ext/src/udf_docs.rs`.

---

## Scope 4 - Schema-derived table UDF typing (runtime overrides)

Intent: Replace unknown return type placeholders with real schema-derived struct types wherever possible, while retaining a safe fallback.

Status: Completed.

Representative pattern
```python
# src/datafusion_engine/udf_catalog.py
specs = datafusion_udf_specs(
    registry_snapshot=snapshot,
    table_schema_overrides={
        "read_csv": pyarrow.schema(...),
        "read_parquet": pyarrow.schema(...),
    },
)
```

Target files
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`

Deletions
- None.

Implementation checklist
- [x] Build `table_schema_overrides` from schema registry or dataset specs.
- [x] Thread overrides into `datafusion_udf_specs(...)` call sites.
- [x] Keep a `struct<>` fallback when schema is unknown.

Notes
- `src/datafusion_engine/udf_catalog.py` now derives overrides from `schema_registry` and
  `nested_schema_for`, and passes them into `datafusion_udf_specs`.

---

## Scope 5 - Info-schema correctness for UDFs

Intent: Ensure every function visible through `information_schema` has a reliable `return_type`, even if it also implements `return_field_from_args`.

Status: Completed.

Representative pattern
```rust
impl ScalarUDFImpl for MyUdf {
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> { ... }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
}
```

Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/function_factory.rs`

Deletions
- None.

Implementation checklist
- [x] Audit all ScalarUDFImpls with `return_field_from_args` and ensure `return_type` matches.
- [x] Add a helper for consistent return type derivation when needed.

Notes
- `async_echo` now returns the argument type (`rust/datafusion_ext/src/udf_async.rs`).

---

## Scope 6 - Async UDF lane (feature-gated, opt-in)

Intent: Provide a production-ready async UDF pathway for network or IO-bound work without blocking execution threads.

Status: Completed.

Representative pattern
```rust
// rust/datafusion_ext/src/udf_async.rs
#[async_trait]
impl AsyncScalarUDFImpl for AsyncEchoUdf {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // async I/O; honor args.number_rows
        todo!()
    }
}
```

Target files
- `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/lib.rs`

Deletions
- Remove any accidental registration of async UDFs when the feature flag is off.

Implementation checklist
- [x] Gate async UDF registration behind a feature flag.
- [x] Register async UDFs via `AsyncScalarUDF::new(...).into_scalar_udf()`.
- [x] Add strict timeouts and bounded concurrency for any real async IO usage.

Notes
- Added `async-udf` feature flag in `rust/datafusion_ext/Cargo.toml` and gated registration in
  `rust/datafusion_ext/src/udf_custom.rs`, `rust/datafusion_ext/src/udf_registry.rs`, and
  module inclusion in `rust/datafusion_ext/src/lib.rs`.

---

## Scope 7 - UDWF performance hooks (future-proofing)

Intent: If we add any custom window UDFs, use the full performance-oriented API (evaluate_all, bounded execution, memoize or prune, limit_effect).

Status: Deferred (no custom UDWFs today).

Representative pattern
```rust
impl PartitionEvaluator for MyWindowEval {
    fn uses_window_frame(&self) -> bool { false }
    fn supports_bounded_execution(&self) -> bool { true }
    fn get_range(&self, idx: usize, _n: usize) -> Result<Range<usize>> {
        Ok(idx.saturating_sub(1)..idx + 1)
    }
}
```

Target files
- `rust/datafusion_ext/src/udwf_builtin.rs` (when adding new UDWFs)
- `rust/datafusion_ext/src/function_factory.rs` (for aliasing behavior)

Deletions
- None.

Implementation checklist
- [ ] Ensure any new UDWFs implement the correct evaluator mode.
- [ ] Set `limit_effect` and `is_causal` when semantics allow.
- [ ] Add plan snapshot tests for window plan shape (EXPLAIN).

---

## Scope 8 - Expression planning + rewrite discipline

Intent: Ensure all custom expression creation follows DataFusion coercion and rewrite pipelines to avoid correctness regressions.

Status: Partially complete.

Representative pattern
```rust
// Use SessionContext::create_physical_expr instead of low-level create_physical_expr
let expr = ctx.create_physical_expr(logical_expr, &schema)?;
```

Target files
- `rust/datafusion_ext/src/expr_planner.rs`
- `rust/datafusion_ext/src/function_rewrite.rs`
- Any custom planning path invoking physical expression creation

Deletions
- None.

Implementation checklist
- [x] Audit custom physical expr creation to use SessionContext or SessionState entrypoints.
- [ ] Add plan tests that assert rewrites occurred (EXPLAIN shape).

Notes
- No additional physical expression creation sites were found beyond existing rewrite/planner paths.

---

## Scope 9 - Documentation automation for static UDFs

Intent: Keep documentation metadata consistent and complete across all compiled functions; prefer declarative docs when possible.

Status: Partially complete.

Representative pattern
```rust
#[user_doc(
  doc_section("Builtin"),
  description("Stable 64-bit hash"),
  syntax_example("stable_hash64(value)")
)]
struct StableHash64Udf { ... }
```

Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udwf_builtin.rs`
- `rust/datafusion_ext/src/udf_docs.rs`

Deletions
- Remove duplicated or stale documentation entries after `#[user_doc]` migration.

Implementation checklist
- [x] Ensure every registered UDF, UDAF, UDWF has Documentation metadata.
- [ ] Move doc definitions to `#[user_doc]` where feasible, or keep `udf_docs.rs` as a single source of truth.
- [x] Keep `udf_docs_snapshot` fully aligned with registered functions.

Notes
- Added docs for cache table UDTFs and expanded CSV/Parquet docs in `rust/datafusion_ext/src/udf_docs.rs`.

---

## Scope 10 - Optional plugin distribution alignment (FFI)

Intent: Align with `datafusion-ffi` best practices for runtime-loaded UDF and TableProvider bundles (if we intend to ship extensions outside this repo).

Status: Completed.

Representative pattern
```rust
// Use FFI_* wrappers and export via a stable ABI root module.
FFI_TableProvider::new_with_ffi_codec(provider, true, runtime, codec)
```

Target files
- `rust/df_plugin_api/*`
- `rust/df_plugin_host/*`

Deletions
- Remove any direct cross-crate trait object sharing that bypasses FFI boundaries.

Implementation checklist
- [x] Add ABI handshake gates (`datafusion_ffi::version()` plus plugin ABI version).
- [x] Export UDF bundles using FFI types and import as Foreign wrappers.

Notes
- Added ABI minor-version compatibility guard in `rust/df_plugin_host/src/loader.rs`.

---

## Scope 11 - Deferred decommissioning (post-scope only)

Intent: Remove code that cannot be deleted with confidence until all above scopes are complete and validated.

Status: Pending.

Candidates (do not delete until the above scopes are green)
- Legacy inlined doc definitions in `rust/datafusion_ext/src/udf_docs.rs` if fully replaced by `#[user_doc]`.
- Any "fallback" UDF signature maps left for Ibis if Rust snapshot coverage is complete.
- Any "legacy" UDTF parsing paths if new coercion and options are fully deployed.

Implementation checklist
- [ ] Confirm all UDFs are discoverable via `information_schema` and `udf_docs_snapshot`.
- [ ] Confirm Rust and Python tests pass under strict info_schema enablement.
- [ ] Remove legacy code only after verified parity.

---

## Execution order (recommended)
1) Scope 1 - Scalar UDF fast paths
2) Scope 2 - FunctionFactory docs plus named args
3) Scope 3 - UDTF coercion plus options
4) Scope 4 - Schema-derived table UDF typing
5) Scope 5 - info_schema return type alignment
6) Scope 6 - async UDF lane (feature-gated)
7) Scope 7 - UDWF performance hooks (if any new UDWFs)
8) Scope 8 - Expr planner and rewrite discipline
9) Scope 9 - Documentation automation
10) Scope 10 - FFI plugin alignment
11) Scope 11 - Deferred decommissioning sweep
