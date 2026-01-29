# DataFusion UDF + Delta Best-in-Class Implementation Plan (v1)

## Intent
Move the Rust UDF and DeltaLake integration to a best-in-class, DataFusion-native design that is fully FFI-based, optimizer-aware, and metadata-precise. This plan assumes a design-phase migration and allows breaking changes to reach the target state.

## Guiding Principles
- Prefer DataFusion-native integration surfaces (FFI `TableProvider`, `ScalarUDFImpl`) over fallbacks.
- Preserve metadata at plan time (schema, nullability, extension metadata).
- Push selection and pruning into Delta providers (snapshot pinning, file-level selection, CDF providers).
- Keep Python thin; move all UDF and provider logic into Rust where possible.
- Make configuration and diagnostics explicit, versioned, and introspectable.

---

## Scope 1: UDF Optimizer Hooks + Metadata Fidelity Baseline

**Goal**: Ensure all UDFs carry optimizer-relevant hooks and return-field metadata using a consistent template.
**Status**: Completed (metadata helpers added; snapshot schema extended; validation tightened).
**Remaining**: None.

### Representative code snippet
```rust
// rust/datafusion_ext/src/udf_custom.rs
fn build_signature(name: &str, volatility: Volatility) -> Signature {
    // centralize parameter naming + signature shape
    Signature::variadic_any(1, volatility)
        .with_parameter_names(vec!["value".to_string()])
        .unwrap_or_else(|_| Signature::variadic_any(1, volatility))
}

fn field_from_first_arg(args: ReturnFieldArgs, name: &str) -> Result<FieldRef> {
    let field = args
        .arg_fields
        .first()
        .ok_or_else(|| DataFusionError::Plan("missing args".into()))?;
    let mut output = Field::new(name, field.data_type().clone(), field.is_nullable());
    if !field.metadata().is_empty() {
        output = output.with_metadata(field.metadata().clone());
    }
    Ok(Arc::new(output))
}

impl ScalarUDFImpl for ExampleUdf {
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        field_from_first_arg(args, self.name())
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }
}
```

### Target files to modify
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `src/datafusion_engine/udf_runtime.py` (snapshot metadata expectations)
- `src/datafusion_engine/udf_platform.py` (platform diagnostics)

### Modules/files to delete
- None.

### Implementation checklist
- Introduce a shared helper for `return_field_from_args` that preserves metadata + nullability.
- Standardize signature creation + parameter names for UDFs that accept named args.
- Add `simplify`/`evaluate_bounds`/`preserves_lex_ordering` hooks for identity or monotonic UDFs.
- Ensure every UDF published in registry snapshot has parameter names and return type metadata.
- Update snapshot validation to enforce the richer metadata contract.

---

## Scope 2: Scalar Fast Paths + Kernel Use

**Goal**: Remove unnecessary scalar expansion in high-traffic UDFs to preserve performance.
**Status**: Completed (scalar fast paths added across map/list/struct/span/interval UDFs; columnar_result handles scalar paths).
**Remaining**: None.

### Representative code snippet
```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let [value] = args.args.as_slice() else {
        return Err(DataFusionError::Plan("expected one argument".into()));
    };
    match value {
        ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(scalar.clone())),
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::clone(array))),
    }
}
```

### Target files to modify
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_async.rs`

### Modules/files to delete
- None.

### Implementation checklist
- Identify UDFs currently calling `values_to_arrays` without need.
- Add scalar fast paths where function is row-wise and element-wise.
- Prefer Arrow kernels for vectorized logic.
- Enforce output length via `into_array_of_size` or `to_array_of_size`.

---

## Scope 3: Config-Aware UDF Instances

**Goal**: Make UDF behavior sensitive to DataFusion `ConfigOptions` where applicable.
**Status**: Completed (config-aware policy defaults wired for span/mapping/normalization UDFs; config defaults exposed in registry snapshot).
**Remaining**: None.

### Representative code snippet
```rust
fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
    let mut policy = self.policy;
    if policy.ideal_batch_size.is_none() {
        let size = config.execution.batch_size;
        if size > 0 {
            policy.ideal_batch_size = Some(size);
        }
    }
    if policy == self.policy {
        return None;
    }
    let inner = Arc::new(Self::new_with_policy(policy)) as Arc<dyn AsyncScalarUDFImpl>;
    Some(AsyncScalarUDF::new(inner).into_scalar_udf())
}
```

### Target files to modify
- `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_custom.rs`
- `src/datafusion_engine/udf_platform.py`

### Modules/files to delete
- None.

### Implementation checklist
- Establish a policy struct for any UDFs with performance tuning knobs.
- Wire UDF policy to DataFusion config via `with_updated_config`.
- Persist policy metadata in UDF registry snapshot.

---

## Scope 4: Delta TableProvider First (No Dataset Fallback)

**Goal**: Enforce FFI `TableProvider` usage for Delta and CDF across all registration paths.
**Status**: Completed (Delta DDL path removed; provider-only registration enforced).
**Remaining**: None.

### Representative code snippet
```python
# src/datafusion_engine/registry_bridge.py
provider = _delta_table_provider_from_session(request).provider
adapter.register_delta_table_provider(name, TableProviderCapsule(provider))
```

### Target files to modify
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/scan_overrides.py`
- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/io_adapter.py`

### Modules/files to delete
- Any Delta registration branches that call `register_dataset` for Delta tables.
- Any non-FFI Delta dataset fallback paths (Arrow dataset fallback for Delta).

### Implementation checklist
- Make non-FFI Delta registration a hard error (clear diagnostic).
- Remove Arrow dataset fallback for Delta registrations.
- Ensure all Delta table registrations route through the FFI provider.
- Preserve table metadata on registration (DDL, schema, constraints, CDF flags).

---

## Scope 5: File-Level Pruning Everywhere (Delta + CDF)

**Goal**: Push file selection into provider creation for all scan-unit and CDF paths.
**Status**: Completed (file-level pruning flows into plugin providers; CDF paths avoid override re-registration).
**Remaining**: None.

### Representative code snippet
```python
# src/datafusion_engine/scan_overrides.py
provider = _delta_table_provider_with_files(
    ctx,
    location=spec.location,
    scan_files=spec.scan_files,
    runtime_profile=spec.runtime_profile,
)
adapter.register_delta_table_provider(spec.name, TableProviderCapsule(provider), overwrite=True)
```

### Target files to modify
- `src/datafusion_engine/scan_overrides.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/delta.py`

### Modules/files to delete
- Any intermediate path that re-registers datasets after CDF window selection.

### Implementation checklist
- Thread file selection through scan-unit building into provider creation.
- For CDF, use `DeltaCdfTableProvider` directly instead of materialized datasets.
- Ensure predicate pushdown and pruning happen before DataFusion scan planning.

---

## Scope 6: CDF as a First-Class Dataset Kind

**Goal**: Make CDF a first-class dataset and provider type across metadata, diagnostics, and queries.
**Status**: Completed (CDF provider registration records metadata and diagnostics; dataset kind already supported).
**Remaining**: None.

### Representative code snippet
```python
# src/datafusion_engine/io_adapter.py
adapter.register_delta_cdf_provider(name, provider)
record_table_provider_metadata(
    id(ctx),
    metadata=TableProviderMetadata(
        table_name=name,
        file_format="delta",
        supports_cdf=True,
    ),
)
```

### Target files to modify
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/datafusion_engine/table_provider_metadata.py`
- `src/datafusion_engine/plan_bundle.py`

### Modules/files to delete
- None.

### Implementation checklist
- Ensure `supports_cdf` is set whenever CDF provider is registered.
- Persist CDF options in diagnostics artifacts.
- Add explicit dataset kind for `delta_cdf` wherever dataset kinds are enumerated.

---

## Scope 7: Unified Plugin Path for UDFs + Providers

**Goal**: Make the plugin path (`df_plugin_codeanatomy`) the default for all UDFs and table providers.
**Status**: Completed (plugin UDF registration is authoritative; async UDF policy is passed via plugin options; legacy registration removed).
**Remaining**: None.

### Representative code snippet
```python
# src/datafusion_engine/runtime.py
manager = DataFusionPluginManager(self.plugin_specs)
manager.load_all()
manager.register_all(ctx)
```

### Target files to modify
- `src/datafusion_engine/plugin_manager.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/storage/deltalake/delta.py`
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_ext/src/lib.rs`

### Modules/files to delete
- Legacy direct UDF registration paths once registry snapshot + docs are plugin-derived.

### Implementation checklist
- Ensure plugin manifest declares all supported table providers and UDFs.
- Route runtime plugin installation before any plan-bundle generation.
- Replace direct `datafusion_ext.register_udfs` usage with plugin-derived UDF registration + snapshot validation.
- Carry async UDF policy into plugin configuration (or explicit session config extension) so plugin registration is fully policy-aware.

---

## Scope 8: UDF Registry Snapshot Contracts + Introspection

**Goal**: Make the registry snapshot fully authoritative for planning and introspection.
**Status**: Completed (snapshot extended with simplify/coerce/short-circuit flags; validation tightened; registry UDTF updated).
**Remaining**: None.

### Representative code snippet
```python
# src/datafusion_engine/udf_runtime.py
validate_rust_udf_snapshot(snapshot)
validate_required_udfs(snapshot, required=required_udfs)
```

### Target files to modify
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`
- `rust/datafusion_ext/src/udf_registry.rs`

### Modules/files to delete
- Any ad-hoc snapshot parsing in other modules.

### Implementation checklist
- Expand snapshot to include parameter names, volatility, simplify flags.
- Validate snapshot entries for all UDFs and aliases.
- Use snapshot to power documentation views and plan metadata.

---

## Scope 9: Delta Observability and Provenance Alignment

**Goal**: All Delta provider registrations emit consistent diagnostics, artifacts, and schema fingerprints.
**Status**: Completed (provider metadata + Delta snapshot observability now carry schema + DDL fingerprints).
**Remaining**: None.

### Representative code snippet
```python
# src/datafusion_engine/registry_bridge.py
record_delta_snapshot(
    runtime_profile,
            artifact=DeltaSnapshotArtifact(
                table_uri=str(location.path),
                snapshot=response.snapshot,
                dataset_name=context.name,
                storage_options_hash=_storage_options_hash(merged_storage_options),
                schema_fingerprint=schema_fingerprint_value,
                ddl_fingerprint=ddl_fingerprint,
            ),
        )
```

### Target files to modify
- `src/datafusion_engine/delta_observability.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/table_provider_metadata.py`

### Modules/files to delete
- None.

### Implementation checklist
- Ensure all provider registrations (Delta + CDF) emit snapshot artifacts.
- Attach schema fingerprints and DDL fingerprints to provider metadata.
- Align schema evolution adapters with metadata flags.

---

## Scope 10: Deletions Deferred Until All Scopes Complete

**Goal**: Track deletions that cannot be safely removed until all changes above land.
**Status**: Completed.

### Candidates for deferred deletion
- None.

### Remaining deletions
- None.

### Implementation checklist
- Keep a running list as each scope lands.
- Remove only after end-to-end tests confirm all flows use FFI providers and plugin UDFs.

---

## Completion Gates
- All Delta registrations use FFI `TableProvider` or fail fast with clear errors.
- All UDFs implement metadata-aware `return_field_from_args` and pass scalar fast paths.
- Registry snapshot fully reflects UDF schema and optimizer hooks.
- CDF providers are first-class and provide metadata/diagnostics.
- Plugin-based UDF/provider registration is the default runtime path.
