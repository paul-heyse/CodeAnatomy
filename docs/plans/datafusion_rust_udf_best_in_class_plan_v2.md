# DataFusion Rust UDF Best-in-Class Alignment Plan (v2)

> **Goal**: Implement a fully native, Rust-first DataFusion UDF/UDAF/UDWF/UDTF architecture with unified registry/discovery, SQL-defined function support, and high-performance execution semantics (no Python UDFs).

**Status update (2026-01-25)**: Scopes 1–6 are implemented (with tests still pending in several areas). Scopes 7–9 remain.

---

## Scope 1 — Unified function registry + discovery (built-ins + custom)

**Intent**: Provide a single authoritative registry snapshot that merges DataFusion built-ins and custom Rust UDF/UDAF/UDWF/UDTFs, and expose docs + signatures to Python and SQL discovery surfaces.

**Status**: ✅ Completed (registry snapshot + Python bridge wired; docs integration deferred to Scope 7).

### Representative patterns

```rust
// rust/datafusion_ext/src/registry_snapshot.rs
pub struct RegistrySnapshot {
    pub scalar: Vec<FunctionEntry>,
    pub aggregate: Vec<FunctionEntry>,
    pub window: Vec<FunctionEntry>,
    pub table: Vec<TableFunctionEntry>,
}

pub fn registry_snapshot(state: &SessionState) -> RegistrySnapshot {
    let scalar = state.scalar_functions()
        .iter()
        .map(|(name, udf)| FunctionEntry::from_udf(name, udf))
        .collect();
    let aggregate = state.aggregate_functions()
        .iter()
        .map(|(name, udaf)| FunctionEntry::from_udaf(name, udaf))
        .collect();
    let window = state.window_functions()
        .iter()
        .map(|(name, udwf)| FunctionEntry::from_udwf(name, udwf))
        .collect();
    let table = state.table_functions()
        .iter()
        .map(|(name, udtf)| TableFunctionEntry::from_udtf(name, udtf))
        .collect();
    RegistrySnapshot { scalar, aggregate, window, table }
}
```

```python
# src/datafusion_engine/udf_runtime.py
snapshot = datafusion_ext.registry_snapshot(ctx)
RUST_UDF_SNAPSHOT = normalize_registry(snapshot)
RUST_UDF_DOCS = normalize_docs(snapshot)
```

### Target files
- `rust/datafusion_ext/src/lib.rs`
- **New** `rust/datafusion_ext/src/registry_snapshot.rs`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/runtime.py`

### Deletions
- Remove ad-hoc `udf_registry_snapshot` (custom-only) once merged snapshot is in place.
- Remove legacy per-module UDF discovery helpers that bypass the new snapshot.

### Checklist
- [x] Build Rust snapshot from `SessionState` maps (scalar/aggregate/window/table).
- [x] Attach alias info and parameter names (from `Signature` where available).
- [x] Export snapshot to Python and normalize in `udf_runtime` for diagnostics.
- [ ] Use snapshot to populate SQL discovery artifacts when information_schema is enabled.

---

## Scope 2 — Native DataFusion `FunctionFactory` for CREATE FUNCTION

**Intent**: Replace custom policy-only installers with a real DataFusion `FunctionFactory` that supports SQL macro functions and optional domain-specific function compilers.

**Status**: ✅ Completed (factory installed; tests pending).

### Representative patterns

```rust
// rust/datafusion_ext/src/function_factory.rs
#[derive(Debug, Default)]
struct MacroFactory;

#[async_trait]
impl FunctionFactory for MacroFactory {
    async fn create(&self, _state: &SessionState, stmt: CreateFunction)
        -> Result<RegisterFunction> {
        let body = stmt.params.function_body
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing body".into()))?;
        let return_type = stmt.return_type
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing return type".into()))?;
        let signature = Signature::exact(
            stmt.args.unwrap_or_default().into_iter().map(|a| a.data_type).collect(),
            stmt.params.behavior.unwrap_or(Volatility::Volatile),
        );
        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(SqlMacroUdf::new(
            stmt.name, signature, return_type, body,
        )))))
    }
}
```

### Target files
- **New** `rust/datafusion_ext/src/function_factory.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/function_factory.py` (bridge to the new native factory)

### Deletions
- Remove custom FunctionFactory IPC registration hooks once native factory is installed via `SessionState`.

### Checklist
- [x] Implement `FunctionFactory` that supports SQL macro functions (`RETURN $1 + ...`).
- [x] Install factory via `SessionContext.with_function_factory(...)`.
- [ ] Add tests for CREATE/DROP FUNCTION, macro expansion, and explain output.

---

## Scope 3 — ExprPlanner + FunctionRewrite integration

**Intent**: Implement real DataFusion ExprPlanners and FunctionRewrite rules rather than no-op installers, enabling operator-to-function rewrites and domain-specific syntax support.

**Status**: ✅ Completed (planner + rewrite registered; tests pending).

### Representative patterns

```rust
// rust/datafusion_ext/src/expr_planner.rs
#[derive(Debug)]
struct DomainPlanner { /* policy */ }

impl ExprPlanner for DomainPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        if matches!(expr.op, BinaryOperator::Arrow) {
            let planned = self.json_get.call(vec![expr.left, expr.right]);
            return Ok(PlannerResult::Planned(planned));
        }
        Ok(PlannerResult::Original(expr))
    }
}
```

```rust
// rust/datafusion_ext/src/function_rewrite.rs
#[derive(Debug)]
struct OperatorToFunctionRewrite { /* ... */ }

impl FunctionRewrite for OperatorToFunctionRewrite {
    fn rewrite(&self, expr: Expr, schema: &DFSchema, config: &ConfigOptions)
        -> Result<Transformed<Expr>> {
        match expr {
            Expr::BinaryExpr(bin) if bin.op == Operator::ArrowAt => {
                let call = self.array_concat.call(vec![*bin.left, *bin.right]);
                Ok(Transformed::yes(call))
            }
            other => Ok(Transformed::no(other)),
        }
    }
}
```

### Target files
- **New** `rust/datafusion_ext/src/expr_planner.rs`
- **New** `rust/datafusion_ext/src/function_rewrite.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/runtime.py`

### Deletions
- Remove `install_expr_planners` default-only behavior and replace with real planner registration.

### Checklist
- [x] Implement `ExprPlanner` and `FunctionRewrite` registries in Rust.
- [x] Install via `SessionStateBuilder` without clobbering existing registries.
- [ ] Add plan snapshot tests for custom operators.

---

## Scope 4 — String typing & scalar fast-path normalization

**Intent**: Ensure all string UDFs accept Utf8/Utf8View/LargeUtf8 and handle scalar fast paths without forced casts.

**Status**: ✅ Completed (UDF signatures + runtime coercion updated; tests pending).

### Representative patterns

```rust
fn scalar_str(value: &ScalarValue) -> Result<Option<&str>> {
    value.try_as_str().map(Some).or_else(|_| Ok(None))
}

fn string_array_any(array: &ArrayRef) -> Result<Cow<'_, StringArray>> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Cow::Borrowed(arr));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Cow::Owned(arr.iter().collect::<StringArray>()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Cow::Owned(arr.iter().collect::<StringArray>()));
    }
    Err(DataFusionError::Plan("expected string input".into()))
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`

### Deletions
- Remove any unnecessary casts that force Utf8 conversion in hot paths.

### Checklist
- [x] Accept `TypeSignature::String(n)` for string UDFs.
- [x] Use `try_as_str` for scalar literals.
- [ ] Add tests for Utf8View/LargeUtf8 inputs.

---

## Scope 5 — UDAF + UDWF + UDTF expansion

**Intent**: Fill missing function classes with native Rust implementations for dataset/window semantics and table-valued utilities.

**Status**: ✅ Completed (core UDAF/UDWF/UDTFs added; GroupsAccumulator still pending if needed).

### Representative patterns

```rust
// UDTF registration
for tf in datafusion_functions_table::all_default_table_functions() {
    ctx.register_udtf(tf.name(), tf.inner().clone());
}
```

```rust
// UDAF skeleton
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SumSqUdaf { sig: Signature }

impl AggregateUDFImpl for SumSqUdaf {
    fn name(&self) -> &str { "sum_sq" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }
    fn accumulator(&self, _: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumSq::new()))
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- **New** `rust/datafusion_ext/src/udaf_builtin.rs`
- **New** `rust/datafusion_ext/src/udwf_builtin.rs`
- `src/datafusion_engine/udf_runtime.py`

### Deletions
- Remove any remaining scalar wrappers that mimic window/aggregate semantics.

### Checklist
- [x] Register default table functions (`range`, `generate_series`).
- [x] Add at least one metadata/introspection UDTF.
- [x] Implement UDAFs/UDWFs for dataset-level semantics currently expressed as scalar wrappers.
- [ ] Add GroupsAccumulator where high-cardinality group-by workloads exist.

---

## Scope 6 — Rust-only UDF enforcement

**Intent**: Ensure no Python UDF lane is used in DataFusion execution.

**Status**: ✅ Completed (non-Rust lanes rejected at registry + catalog policy).

### Representative patterns

```python
# src/datafusion_engine/udf_catalog.py
if udf_tier == "python":
    raise RuntimeError("Python UDFs are disabled; use Rust UDFs")
```

### Target files
- `src/datafusion_engine/udf_catalog.py`
- `src/engine/udf_registry.py`
- `src/ibis_engine/builtin_udfs.py`

### Deletions
- Remove `python` lane registrations or hard-disable them in DataFusion backend.

### Checklist
- [x] Disallow python/pyarrow/pandas lanes for DataFusion runtime.
- [x] Keep Ibis builtin declarations as metadata only if needed.

---

## Scope 7 — Documentation + UX surfaces

**Intent**: Surface all Rust UDF/UDAF/UDWF/UDTF docs through discovery and runtime snapshots.

**Status**: ⏳ Not started.

### Representative patterns

```rust
pub fn docs_snapshot(state: &SessionState) -> HashMap<String, Documentation> {
    // merge custom docs with built-in docs when available
}
```

### Target files
- `rust/datafusion_ext/src/udf_docs.rs`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/runtime.py`

### Deletions
- Remove any duplicate or unused doc registries once unified.

### Checklist
- [ ] Merge built-in + custom docs into a single snapshot.
- [ ] Expose docs in SQL discovery surfaces when information_schema is enabled.

---

## Scope 8 — Testing & plan-shape validation

**Intent**: Add comprehensive SQL-level coverage and plan snapshots for UDF/UDAF/UDWF/UDTF changes.

**Status**: ⏳ Not started.

### Representative patterns

```sql
# sqllogictest
query T rowsort
EXPLAIN FORMAT tree SELECT stable_hash64('a')
----
<expected plan>
```

### Target files
- `rust/datafusion_ext/tests/*`
- `tests/integration/*`
- `tests/unit/*`
- `tests/sqllogictest/*` (if present in repo)

### Deletions
- Remove tests tied to deprecated Python UDF lanes.

### Checklist
- [ ] Add sqllogictest coverage for SQL functions + UDTFs.
- [ ] Add EXPLAIN plan snapshots for ExprPlanner/FunctionRewrite behavior.
- [ ] Add UDAF/UDWF bounded-window correctness tests where applicable.

---

## Scope 9 — Plugin ABI + runtime-loaded DataFusion extensions

**Intent**: Implement the DataFusion plugin ABI spec (FFI + abi_stable) for runtime-loaded Rust extensions that export TableProviders and UDF bundles, with strict handshake validation and lifecycle management.

**Status**: ⏳ Not started.

### Representative patterns

```rust
// rust/df_plugin_api/src/manifest.rs
#[repr(C)]
#[derive(Debug, StableAbi, Clone)]
pub struct DfPluginManifestV1 {
    pub struct_size: u32,
    pub plugin_abi_major: u16,
    pub plugin_abi_minor: u16,
    pub df_ffi_major: u64,
    pub datafusion_major: u16,
    pub arrow_major: u16,
    pub plugin_name: RString,
    pub plugin_version: RString,
    pub build_id: RString,
    pub capabilities: u64,
    pub features: RVec<RString>,
}
```

```rust
// rust/df_plugin_api/src/lib.rs
#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginMod {
    pub manifest: extern "C" fn() -> DfPluginManifestV1,
    pub exports: extern "C" fn() -> DfPluginExportsV1,
    pub create_table_provider: extern "C" fn(
        name: RStr<'_>,
        options_json: ROption<RString>,
        ffi_codec: FFI_LogicalExtensionCodec,
    ) -> DfResult<FFI_TableProvider>,
}
```

```rust
// rust/df_plugin_host/src/loader.rs
pub fn load_plugin(path: &Path) -> Result<DfPluginMod_Ref> {
    let module = DfPluginMod_Ref::load_from_file(path)?;
    let manifest = (module.manifest)();
    validate_manifest(&manifest)?;
    Ok(module)
}
```

### Target files
- **New** `rust/df_plugin_api/src/lib.rs`
- **New** `rust/df_plugin_api/src/manifest.rs`
- **New** `rust/df_plugin_host/src/loader.rs`
- **New** `rust/df_plugin_host/src/registry_bridge.rs`
- **New** `src/datafusion_engine/plugin_manager.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/catalog_provider.py`

### Deletions
- None (new capability plane).

### Checklist
- [ ] Create the ABI interface crate (`df_plugin_api`) with manifest + export structs.
- [ ] Implement `abi_stable` root module export (`DfPluginMod`).
- [ ] Implement host-side loader with strict ABI/FFI version gating.
- [ ] Provide a `TaskContextProvider` codec builder for `FFI_TableProvider`.
- [ ] Register plugin UDF bundles into SessionState (scalar/aggregate/window).
- [ ] Register plugin TableProviders by name with correct lifetime handling.
- [ ] Add plugin lifecycle manager (ref-counted, no unload with live handles).
- [ ] Add integration tests for manifest validation, UDF bundle, and table provider.

---

## Files to delete (summary)

- Legacy function discovery helpers that bypass unified registry snapshot.
- Any remaining Python UDF lanes or registration helpers for DataFusion runtime.
- Obsolete UDF wrappers that hide dataset/window behavior.

---

## Global implementation checklist

- [x] Unified registry snapshot for all function classes (built-in + custom).
- [ ] Native DataFusion FunctionFactory (CREATE FUNCTION) + tests.
- [x] Real ExprPlanner + FunctionRewrite registration.
- [x] String typing normalization across UDFs.
- [x] UDAF/UDWF/UDTF expansion + dataset-level semantics migration.
- [x] Rust-only UDF enforcement.
- [ ] Unified docs snapshot exposed to discovery surfaces.
- [ ] SQL + plan-shape test harness coverage.
- [ ] Plugin ABI + runtime-loaded extensions support.

---

## Proposed execution order

1) Unified registry snapshot + docs integration
2) Native FunctionFactory + SQL macro support
3) ExprPlanner + FunctionRewrite wiring
4) String typing + scalar fast-path normalization
5) UDAF/UDWF/UDTF expansion
6) Rust-only enforcement + cleanup
7) Tests + plan snapshots
8) Plugin ABI + runtime-loaded extensions
