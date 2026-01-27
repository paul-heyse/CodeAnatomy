# DataFusion Rust UDF + Delta Best-in-Class Implementation Plan (v1)

> Goal: Deliver a best-in-class, DataFusion-native architecture that fully leverages the Rust UDF/UDAF/UDWF/UDTF surface and the Delta Lake control-plane integration described in `docs/python_library_reference/datafusion_rust_UDFs.md` and `docs/python_library_reference/deltalake_datafusion_integration.md`.
>
> End-state principles:
> - Rust-only execution surfaces (no Python UDFs in production paths).
> - Full planner-time typing, coercion, and metadata propagation for UDFs.
> - Delta control plane is the single source of truth for snapshots, providers, mutations, and maintenance.
> - Plugin-ready architecture with stable ABI and explicit version handshakes.
> - Deterministic, observable, and testable at both SQL and plan levels.

---

## Scope 1 — Volatility hardening + typed signatures (Scalar UDFs)

**Intent**: Correct volatility levels and remove overly permissive `TypeSignature::Any` usage by introducing explicit signatures and coercion hooks. This shifts errors to planning time and improves constant folding and plan reproducibility.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapNormalizeUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for MapNormalizeUdf {
    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Example: force map keys/values to Utf8 to align with runtime expectations.
        let mut out = Vec::with_capacity(arg_types.len());
        for dtype in arg_types {
            let coerced = match dtype {
                DataType::Map(_, _) => normalized_map_type(),
                other => other.clone(),
            };
            out.push(coerced);
        }
        Ok(out)
    }
}

fn map_normalize_udf() -> ScalarUDF {
    let signature = Signature::one_of(
        vec![TypeSignature::UserDefined],
        Volatility::Immutable,
    );
    ScalarUDF::new_from_shared_impl(Arc::new(MapNormalizeUdf {
        signature: SignatureEqHash::new(signature),
    }))
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deletions
- None.

### Implementation checklist
- [ ] Audit all custom scalar UDFs and update to `Volatility::Immutable` where deterministic.
- [ ] Replace `TypeSignature::Any` with explicit signatures or `TypeSignature::UserDefined` + `coerce_types`.
- [ ] Ensure `registry_snapshot` captures accurate `signature_inputs` and `return_types` after changes.
- [ ] Add parity tests for the new signatures in `udf_conformance.rs`.

---

## Scope 2 — Metadata and extension-type propagation

**Intent**: Preserve extension metadata (especially span and semantic types) across UDF boundaries using `return_field_from_args` and runtime `return_field` usage.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for SpanMakeUdf {
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        // Preserve metadata when provided by input fields.
        let mut field = Field::new(self.name(), span_struct_type(), nullable);
        if let Some(first) = args.arg_fields.first() {
            if !first.metadata().is_empty() {
                field = field.with_metadata(first.metadata().clone());
            }
        }
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Use planner-provided return_field to avoid metadata drift.
        let return_field = args.return_field.as_ref();
        let dtype = return_field.data_type();
        // Build arrays matching dtype (extension or struct).
        build_span_array(dtype, args)
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `src/arrow_utils/schema/semantic_types.py`
- `src/datafusion_engine/schema_registry.py`

### Deletions
- None.

### Implementation checklist
- [ ] Ensure span UDFs preserve metadata (or extension types) from input fields.
- [ ] Use `ScalarFunctionArgs.return_field` for runtime array construction.
- [ ] Update schema registry validation to reflect extension metadata preservation.

---

## Scope 3 — Return-field correctness for macro UDFs and dynamic UDFs

**Intent**: Align SQL macros and dynamic UDFs with the modern `return_field_from_args` interface so the planner and runtime are consistent (especially in FFI and plugin contexts).

### Representative patterns

```rust
// rust/datafusion_ext/src/function_factory.rs
impl ScalarUDFImpl for SqlMacroUdf {
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type.clone(),
            true,
        )))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        // Macro expansion + cast to return type.
        // ...
    }
}
```

### Target files
- `rust/datafusion_ext/src/function_factory.rs`
- `rust/datafusion_ext/src/udf_custom.rs`

### Deletions
- None.

### Implementation checklist
- [ ] Add `return_field_from_args` to `SqlMacroUdf`.
- [ ] Ensure dynamic UDFs (e.g., `struct_pick`) trust planner-provided return fields.
- [ ] Update tests to cover macro UDF return-field correctness.

---

## Scope 4 — Async UDF lane with separate runtime

**Intent**: Provide a production-safe async UDF execution lane that never blocks DataFusion execution threads, using a dedicated runtime or thread pool.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_async.rs
static ASYNC_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn async_runtime() -> &'static Runtime {
    ASYNC_RUNTIME.get_or_init(|| Runtime::new().expect("async runtime"))
}

#[async_trait]
impl AsyncScalarUDFImpl for RemoteLookupUdf {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let handle = async_runtime().handle().clone();
        handle.spawn(async move { do_io(args) }).await??
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/runtime.py`

### Deletions
- None.

### Implementation checklist
- [ ] Add a dedicated async runtime (or thread pool) for async UDFs.
- [ ] Gate async UDF availability via policy checks (already wired in Python).
- [ ] Add a conformance test for async UDF policy behavior.

---

## Scope 5 — UDAF/UDWF correctness matrix

**Intent**: Prove correctness for aggregate and window functions, including sliding/retract semantics, null handling, and deterministic ordering.

### Representative patterns

```rust
// rust/datafusion_ext/tests/udf_conformance.rs
#[test]
fn list_unique_retract_window() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, ""
        SELECT list_unique(value) OVER (
          ORDER BY ts
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS v
        FROM test_values
    """)?;
    assert_batches_eq!(expected, &batches);
    Ok(())
}
```

### Target files
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udwf_builtin.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deletions
- None.

### Implementation checklist
- [ ] Add window-frame tests for retract correctness (bounded windows).
- [ ] Add null-handling tests (IGNORE/RESPECT NULLS).
- [ ] Add plan snapshot tests for expected logical/physical shapes.

---

## Scope 6 — ABI-stable plugin export for UDF bundles

**Intent**: Package CodeAnatomy UDFs and table providers as a portable plugin with a stable ABI, using `df_plugin_api` + `datafusion-ffi`.

### Representative patterns

```rust
// rust/df_plugin_codeanatomy/src/lib.rs
#[export_root_module]
pub fn get_library() -> DfPluginMod_Ref {
    DfPluginMod { ... }
}

extern "C" fn exports() -> DfPluginExportsV1 {
    DfPluginExportsV1 {
        udf_bundle: DfUdfBundleV1 { scalar, aggregate, window },
        table_functions,
        table_provider_names,
    }
}
```

### Target files
- **New** `rust/df_plugin_codeanatomy/Cargo.toml`
- **New** `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/df_plugin_api/src/lib.rs`
- `rust/df_plugin_host/src/registry_bridge.rs`
- `rust/datafusion_ext/src/lib.rs`

### Deletions
- None.

### Implementation checklist
- [ ] Create plugin crate exporting `DfPluginMod` with UDF bundles and providers.
- [ ] Add manifest/handshake metadata (DataFusion/Arrow/FFI majors).
- [ ] Wire plugin loading in Python via `datafusion_ext.load_df_plugin`.

---

## Scope 7 — Delta scan config and file metadata alignment

**Intent**: Ensure all Delta scans use the same session-derived `DeltaScanConfig` with full payload fidelity (file column, partition wrapping, view types, schema override).

### Representative patterns

```rust
// rust/datafusion_ext/src/delta_control_plane.rs
let scan_config = DeltaScanConfig::new_from_session(session)
    .with_file_column_name("__delta_rs_path")
    .with_wrap_partition_values(true)
    .with_schema_force_view_types(true);
```

### Target files
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `rust/datafusion_ext/src/delta_observability.rs`
- `src/storage/deltalake/scan_profile.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/datafusion_engine/registry_bridge.py`

### Deletions
- None.

### Implementation checklist
- [ ] Include `wrap_partition_values` and schema IPC in all scan config payloads.
- [ ] Expose file column name in dataset artifacts and lineage views.
- [ ] Align Python defaults with Rust session-derived scan config.

---

## Scope 8 — Object store and storage-option unification

**Intent**: Guarantee the same object-store registry and credential resolution path for both DataFusion and delta-rs, eliminating divergent configuration.

### Representative patterns

```rust
// rust/datafusion_ext/src/delta_control_plane.rs
pub fn apply_object_store_registry(
    registry: Arc<ObjectStoreRegistry>,
    storage_options: HashMap<String, String>,
) {
    // Register object stores here before building DeltaTable.
}
```

### Target files
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/delta.py`

### Deletions
- None.

### Implementation checklist
- [ ] Centralize object-store registration in the Rust control plane.
- [ ] Update Python registration to pass object-store settings consistently.
- [ ] Add diagnostics snapshot for resolved storage options.

---

## Scope 9 — Delta CDF pushdown and incremental alignment

**Intent**: Push CDF projections/filters into the provider and ensure incremental pipelines use provider-native predicates where possible.

### Representative patterns

```python
# src/incremental/cdf_runtime.py
cdf_df = ctx.table(cdf_name)
if predicate is not None:
    cdf_df = cdf_df.filter(predicate)  # plan-level pushdown
```

### Target files
- `src/incremental/cdf_runtime.py`
- `src/storage/deltalake/delta.py`
- `rust/datafusion_ext/src/delta_control_plane.rs`

### Deletions
- None.

### Implementation checklist
- [ ] Apply CDF predicates before collection to enable pushdown.
- [ ] Record CDF snapshot + options in Delta observability artifacts.
- [ ] Add tests for CDF provider projection/filter pushdown.

---

## Scope 10 — Delta DML boundaries (INSERT vs control-plane mutations)

**Intent**: Allow DataFusion `INSERT INTO` for append/overwrite only, while routing MERGE/UPDATE/DELETE through the Rust control plane.

### Representative patterns

```python
# src/datafusion_engine/write_pipeline.py
insert_op = InsertOp.APPEND if mode == WriteMode.APPEND else InsertOp.OVERWRITE
ctx.table(table_name).write_table(
    table_name,
    write_options=DataFrameWriteOptions(insert_operation=insert_op),
)
```

### Target files
- `src/datafusion_engine/write_pipeline.py`
- `rust/datafusion_ext/src/delta_mutations.rs`
- `src/storage/deltalake/delta.py`

### Deletions
- None.

### Implementation checklist
- [ ] Gate INSERT writes to Delta append/overwrite only.
- [ ] Keep MERGE/UPDATE/DELETE routed through `delta_mutations` control plane.
- [ ] Record mutation artifacts for all operations.

---

## Scope 11 — Delta plan codecs and plugin compatibility

**Intent**: Ensure Delta logical/physical codecs are installed wherever plans are serialized or exported, including plugin-loaded contexts.

### Representative patterns

```rust
// rust/datafusion_ext/src/lib.rs
fn install_delta_plan_codecs(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let state_ref = ctx.ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    config.set_extension(Arc::new(DeltaLogicalCodec {}));
    config.set_extension(Arc::new(DeltaPhysicalCodec {}));
    Ok(())
}
```

### Target files
- `rust/datafusion_ext/src/lib.rs`
- `rust/df_plugin_host/src/registry_bridge.rs`
- `src/datafusion_engine/runtime.py`

### Deletions
- None.

### Implementation checklist
- [ ] Ensure codecs are installed in all runtime contexts that export plans.
- [ ] Extend plugin registration paths to install codecs when needed.

---

## Scope 12 — Deferred deletions and cleanup (after all scopes land)

**Intent**: Safely remove legacy paths once new integrations are proven stable.

### Deferred deletion candidates
- `src/test_support/datafusion_ext_stub.py` (only if all tests can require native extension).
- Any legacy fallback Delta registration paths (if introduced later for compatibility).
- Any legacy UDF wrappers that duplicate new plugin exports.

### Target files
- TBD (dependent on final integration outcomes).

### Deletions
- Deferred until scopes 1–11 are complete and validated.

### Implementation checklist
- [ ] Re-run full test suite with native extension required.
- [ ] Confirm no downstream import paths rely on stubs or fallbacks.
- [ ] Remove candidates only after adoption is complete.

