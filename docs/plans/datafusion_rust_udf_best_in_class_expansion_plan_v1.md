# DataFusion Rust UDF Best-in-Class Expansion Plan (v1)

> **Goal**: Push the Rust UDF/UDAF/UDWF/UDTF stack to a truly best-in-class end state by adopting all high‑leverage features in `datafusion_rust_UDFs.md` and aligning usage across the broader codebase (Ibis + DataFusion runtime + registry/discovery + docs).

> **Key constraints**:
> - Rust-only UDFs (no Python UDFs).
> - No raw SQL strings in production paths (use builders/expr APIs).
> - Deterministic behavior; configuration‑driven variants must be explicit and versioned.

---

## Scope 1 — Scalar UDF: advanced typing, metadata, and optimizer hooks

**Intent**: Bring all scalar UDFs to “full‑power” `ScalarUDFImpl` semantics with metadata-aware return fields, consistent nullability, and targeted optimizer hooks.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for StableHash64Udf {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Preserve metadata + nullability from input
        let nullable = args.arg_fields.first().map_or(true, |field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        // Optional literal folding for performance / plan clarity
        if let [Expr::Literal(lit, _)] = args.as_slice() {
            if let ScalarValue::Utf8(Some(value)) = lit {
                return Ok(ExprSimplifyResult::Simplified(lit_hash64_expr(value)));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`

### Deletions
- None.

### Implementation checklist
- [x] Implement `return_field_from_args` for all scalar UDFs that currently only implement `return_type`.
- [x] Ensure nullability and metadata are preserved where possible.
- [x] Add targeted `simplify` hooks for literal‑foldable UDFs (hash/id/metadata extractors).
- [x] Add/verify parameter names on all scalar UDF signatures for named‑arg support.

---

## Scope 2 — Async Scalar UDF scaffold (optional but best‑in‑class ready)

**Intent**: Provide a clean async UDF lane for any future network/IO‑bound functions without blocking DataFusion execution threads.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_async.rs
#[derive(Debug, PartialEq, Eq, Hash)]
struct RemoteLookupUdf {
    signature: Signature,
}

impl ScalarUDFImpl for RemoteLookupUdf {
    fn name(&self) -> &str { "remote_lookup" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("remote_lookup only supported async")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for RemoteLookupUdf {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // async I/O + batch shaping
        todo!()
    }
}
```

### Target files
- **New** `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/udf_docs.rs`

### Deletions
- None.

### Implementation checklist
- [x] Add async UDF module (no production usage required yet).
- [x] Provide a simple reference async UDF (feature‑gated or test‑only) to validate runtime behavior.
- [x] Document async UDF usage and caveats in docs snapshot.

---

## Scope 3 — Aggregate UDF semantics: null handling, defaults, sliding windows

**Intent**: Align UDAFs with full DataFusion semantics, including SQL `IGNORE NULLS`, correct defaults, and bounded window support.

### Representative patterns

```rust
impl AggregateUDFImpl for CountDistinctUdaf {
    fn supports_null_handling_clause(&self) -> bool { true }

    fn default_value(&self, _data_type: &DataType) -> ScalarValue {
        ScalarValue::Int64(Some(0))
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountDistinctSlidingAccumulator::new(args.ignore_nulls)))
    }
}
```

### Target files
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deletions
- None.

### Implementation checklist
- [x] Implement `supports_null_handling_clause` and honor `ignore_nulls` in accumulator logic.
- [x] Provide `default_value` for `count_distinct_agg` (and any other count-like aggregates).
- [x] Add `create_sliding_accumulator` with `retract_batch` for window use, or document unsupported window usage.
- [x] Implement `GroupsAccumulator::convert_to_state` for perf wins on large cardinality.

---

## Scope 4 — UDTFs: constant folding, pushdown, and external reads

**Intent**: Make UDTFs best‑in‑class table producers with literal‑folded args and pushdown‑aware providers.

### Representative patterns

```rust
fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
    let simplified = simplify_expr(args[0].clone())?; // ExprSimplifier
    let path = extract_literal_string(&simplified)?;
    let provider = ListingTable::try_new(config_from_path(path)?)?;
    Ok(Arc::new(provider))
}
```

### Target files
- `rust/datafusion_ext/src/udtf_builtin.rs`
- **New** `rust/datafusion_ext/src/udtf_external.rs`
- `rust/datafusion_ext/src/lib.rs`

### Deletions
- None.

### Implementation checklist
- [x] Add constant‑folding to UDTF arg parsing (ExprSimplifier).
- [x] Add a ListingTable‑backed `read_parquet`/`read_csv` UDTF with pushdown support.
- [x] Ensure UDTFs honor projection/filters/limit in provider scan.
- [x] Document UDTF usage in docs snapshot and SQL discovery surfaces.

---

## Scope 5 — Registry + discovery + Ibis alignment

**Intent**: Make Rust registry the single source of truth for function metadata and ensure Ibis and DataFusion agree on names/signatures.

### Representative patterns

```python
# src/ibis_engine/builtin_udfs.py (generated or validated)
BUILTIN_UDFS = build_from_rust_snapshot(snapshot)
```

### Target files
- `src/ibis_engine/builtin_udfs.py`
- `src/engine/unified_registry.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/schema_registry.py`

### Deletions
- Remove hard‑coded builtin lists after generator is in place.

### Implementation checklist
- [x] Generate Ibis builtin UDF metadata from Rust registry snapshot (or assert/validate parity).
- [x] Ensure parameter names match across Rust + Ibis to support named arguments.
- [x] Keep `information_schema.parameters` consistent with snapshot data (add explicit QA check).

---

## Scope 6 — FunctionFactory expansion (SQL‑defined UDFs beyond scalar)

**Intent**: Expand SQL `CREATE FUNCTION` to return `Aggregate`, `Window`, and `Table` functions when needed.

### Representative patterns

```rust
match build_result {
    RegisterFunction::Scalar(udf) => ctx.register_udf(udf),
    RegisterFunction::Aggregate(udaf) => ctx.register_udaf(udaf),
    RegisterFunction::Window(udwf) => ctx.register_udwf(udwf),
    RegisterFunction::Table(name, udtf) => ctx.register_udtf(&name, udtf),
}
```

### Target files
- `rust/datafusion_ext/src/function_factory.rs`
- `src/datafusion_engine/function_factory.py`

### Deletions
- None.

### Implementation checklist
- [x] Extend factory to support aggregate/window/table function creation.
- [x] Validate parameter names and enforce volatility constraints.
- [x] Add test coverage for CREATE/DROP across function classes.

---

## Scope 7 — Documentation + UX completion

**Intent**: Ensure every custom function (scalar/aggregate/window/table) has rich docs and is discoverable through SQL + programmatic surfaces.

### Representative patterns

```rust
fn documentation(&self) -> Option<&Documentation> {
    Some(udf_docs::stable_hash64_doc())
}
```

### Target files
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udwf_builtin.rs`
- `rust/datafusion_ext/src/udtf_builtin.rs`

### Deletions
- None.

### Implementation checklist
- [x] Add docs for any function still missing documentation.
- [x] Ensure `udf_docs()` UDTF and `udf_docs_snapshot` expose all classes.
- [x] Add `information_schema.routines/parameters` QA checks.

---

## Scope 8 — Performance & correctness test harness

**Intent**: Lock correctness and plan shape with regression tests.

### Representative patterns

```rust
#[test]
fn udf_named_args_resolve() {
    // EXPLAIN plan checks + execution correctness
}
```

### Target files
- `rust/datafusion_ext/tests/udf_conformance.rs`
- `rust/datafusion_ext/src/function_rewrite.rs` (unit tests)
- `tests/unit/` (if applicable)

### Deletions
- None.

### Implementation checklist
- [x] Add CREATE/DROP FUNCTION tests (macro expansion and explain output).
- [x] Add UDAF null‑handling + sliding window tests.
- [x] Add UDTF constant‑folding and pushdown behavior tests.
- [x] Add parity checks between Rust registry snapshot and Ibis metadata.

---

## Scope 9 — Deferred decommissioning (only after all scopes complete)

**Intent**: Remove or replace legacy code once the new unified architecture is verified.

### Deletions (defer until all above scopes are complete)
- Static builtin UDF lists and duplicated registries (replace with Rust snapshot‑driven generation).
- Legacy function wrappers that don’t carry metadata or named‑arg support.
- Any remaining SQL‑string usage for UDFs once builder APIs exist.

### Implementation checklist
- [x] Validate parity tests across Rust snapshot, Ibis metadata, and information_schema.
- [ ] Remove legacy registry lists and redundant wrappers.
- [ ] Ensure documentation + discovery surfaces remain complete after deletion.

---

## Execution order (recommended)
1) Scope 1 (scalar UDF metadata/optimizer hooks)
2) Scope 3 (UDAF semantics)
3) Scope 4 (UDTF pushdown + external reads)
4) Scope 5 (registry + Ibis alignment)
5) Scope 6 (FunctionFactory expansion)
6) Scope 7 (docs/UX completion)
7) Scope 8 (tests)
8) Scope 2 (async UDF scaffold)
9) Scope 9 (deferred deletions)
