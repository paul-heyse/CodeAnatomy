# DataFusion Rust ↔ Python Interface Excellence Plan v1

Status: Implemented (code complete; rebuild artifacts pending)
Owner: Codex (proposed)

## Goals
- Eliminate panic/unwrap aborts across Rust↔Python boundaries.
- Standardize error handling and diagnostics for all FFI/PyO3 entry points.
- Harden plugin ABI behavior (no panics, versioned payloads).
- Improve UDTF option parsing interoperability (base64 schema IPC).
- Provide a minimal capability snapshot surface for compatibility checks.
- Keep Python architecture unchanged (only thin surface additions where needed).

## Non‑Goals
- No redesign of Python runtime architecture (pooling, policy, etc.).
- No changes to upstream DataFusion or delta‑rs APIs.
- No new user‑facing functionality beyond interface robustness.

---

## Scope 1 — Eliminate panic/unwrap across PyO3/FFI boundaries

### Problem
`panic!`, `unwrap()`, and `expect()` in Rust FFI/pyfunction code can abort the Python process. These must be converted to structured errors for a best‑in‑class interface.

### Representative Snippet
```rust
// Before
let array = ArrayData::from_pyarrow_bound(&v).unwrap();

// After
let array = ArrayData::from_pyarrow_bound(&v)
    .map_err(|err| DataFusionError::Execution(format!("PyArrow conversion failed: {err}")))?;
```

### Target Files
- `rust/datafusion_python/src/udwf.rs`
- `rust/datafusion_python/src/udaf.rs`
- `rust/datafusion_python/src/store.rs`
- `rust/datafusion_python/src/substrait.rs`
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/df_plugin_host/src/loader.rs`

### Deprecate/Delete After Completion
- None (in‑place error‑handling hardening)

### Implementation Checklist
- [x] Replace `unwrap()`/`expect()` in PyO3 + FFI code paths with structured errors.
- [x] Replace `panic!` in plugin/loader code with `DfResult` or error logs.
- [x] Remove noisy `println!` in hot execution paths.

---

## Scope 2 — Centralize PyArrow conversion helpers

### Problem
UDAF/UDWF implementations duplicate error‑prone PyArrow conversion logic.

### Architecture and Code Pattern
Introduce a single helper in `rust/datafusion_python/src/utils.rs` to convert Arrow arrays to PyArrow safely.

### Representative Snippet
```rust
// utils.rs
pub fn to_pyarrow_array(py: Python<'_>, array: &ArrayRef) -> Result<Py<PyAny>, DataFusionError> {
    array
        .into_data()
        .to_pyarrow(py)
        .map_err(|err| DataFusionError::Execution(format!("PyArrow export failed: {err}")))
}
```

### Target Files
- `rust/datafusion_python/src/utils.rs` (new helper)
- `rust/datafusion_python/src/udaf.rs`
- `rust/datafusion_python/src/udwf.rs`

### Deprecate/Delete After Completion
- Inline conversion blocks in `udaf.rs` / `udwf.rs`.

### Implementation Checklist
- [x] Add conversion helper(s) in `utils.rs`.
- [x] Replace in‑line conversions in UDAF/UDWF implementations.
- [x] Ensure error context is preserved in messages.

---

## Scope 3 — Harden plugin exports and avoid panics

### Problem
`df_plugin_codeanatomy` uses `panic!` in bundle construction, which can crash plugin load in Python.

### Architecture and Code Pattern
Make bundle creation infallible; return empty bundles or `DfResult` errors (no panics) and log errors.

### Representative Snippet
```rust
fn build_udf_bundle() -> DfUdfBundleV1 {
    match build_udf_bundle_with_options(PluginUdfOptions::default()) {
        Ok(bundle) => bundle,
        Err(err) => {
            tracing::error!("UDF bundle build failed: {err}");
            DfUdfBundleV1 { scalar: RVec::new(), aggregate: RVec::new(), window: RVec::new() }
        }
    }
}
```

### Target Files
- `rust/df_plugin_codeanatomy/src/lib.rs`

### Deprecate/Delete After Completion
- Panic‑based bundle construction paths.

### Implementation Checklist
- [x] Replace `panic!` in bundle and table UDF construction with safe fallbacks.
- [x] Ensure plugin exports always return a valid `DfPluginExportsV1`.

---

## Scope 4 — Versioned Delta `scan_config` payload

### Problem
`scan_config` payload shape is not versioned, making future changes risky.

### Architecture and Code Pattern
Add `scan_config_version` and validate in plugin.

### Representative Snippet
```rust
#[derive(Debug, Deserialize)]
struct DeltaScanConfigPayload {
    scan_config_version: u32,
    // existing fields...
}

if payload.scan_config_version != 1 {
    return Err(DeltaTableError::Generic("Unsupported scan_config_version".into()));
}
```

### Target Files
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_python/src/codeanatomy_ext.rs`

### Deprecate/Delete After Completion
- Unversioned payload handling.

### Implementation Checklist
- [x] Add `scan_config_version` to payload.
- [x] Validate version in plugin; error on unsupported version.
- [x] Update payload builder in PyO3 extension.

---

## Scope 5 — Accept base64 schema IPC in UDTF options

### Problem
`read_parquet` / `read_csv` options accept hex or byte arrays only; base64 improves portability with minimal Python changes.

### Architecture and Code Pattern
Extend `schema_ipc` parsing to accept base64 strings.

### Representative Snippet
```rust
match value {
    JsonValue::String(text) if looks_like_base64(&text) => {
        let bytes = base64::decode(text)
            .map_err(|err| DataFusionError::Plan(format!("schema_ipc base64 decode: {err}")))?;
        Ok(Some(schema_from_ipc(bytes)?))
    }
    // existing hex/array cases...
}
```

### Target Files
- `rust/datafusion_ext/src/udtf_sources.rs`

### Deprecate/Delete After Completion
- None (additive format support).

### Implementation Checklist
- [x] Add base64 decode path in `schema_ipc` option parsing.
- [x] Add a small UDTF test for base64 schema IPC.

---

## Scope 6 — Rust capability snapshot surface

### Problem
Python lacks a minimal “capabilities + version info” API for fast compatibility checks.

### Architecture and Code Pattern
Add a Rust `pyfunction` returning version + registry metadata.

### Representative Snippet
```rust
#[pyfunction]
fn capabilities_snapshot() -> PyResult<Py<PyAny>> {
    let payload = json!({
        "datafusion_version": datafusion::DATAFUSION_VERSION,
        "arrow_version": arrow::ARROW_VERSION,
        "plugin_abi": {"major": DF_PLUGIN_ABI_MAJOR, "minor": DF_PLUGIN_ABI_MINOR},
        "udf_count": udf_registry::scalar_udf_specs().len(),
    });
    Ok(json_to_py(py, &payload)? )
}
```

### Target Files
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `src/datafusion_ext.pyi`

### Deprecate/Delete After Completion
- None (new surface).

### Implementation Checklist
- [x] Add `capabilities_snapshot` pyfunction in Rust.
- [x] Add stub/type entry in `datafusion_ext.pyi`.

---

## Cross‑Cutting Checklist
- [x] Replace all panics/unwraps on Rust↔Python boundaries with structured errors.
- [x] Ensure no stdout logging in hot paths.
- [x] Add minimal tests for new decoding/validation paths.
- [ ] Rebuild Rust artifacts (`scripts/rebuild_rust_artifacts.sh`).

---

## Deliverables
- Robust, panic‑free Rust/PyO3 boundary behavior.
- Consistent PyArrow conversion helpers.
- Versioned `scan_config` payload in plugin interface.
- Expanded `schema_ipc` parsing to base64.
- Capabilities snapshot API for compatibility checks.

---

## Scope 7 — Registry‑first `udf_expr` resolution in Rust

### Problem
`udf_expr` resolution should prefer the session registry to keep Rust and Python behavior aligned with the canonical function registry.

### Architecture and Code Pattern
Introduce a registry‑aware helper in `udf_expr.rs` that first checks `FunctionRegistry` and falls back to registry specs.

### Representative Snippet
```rust
pub fn expr_from_registry_or_specs(
    registry: &dyn FunctionRegistry,
    name: &str,
    args: Vec<Expr>,
    config: Option<&ConfigOptions>,
) -> Result<Expr> {
    if let Ok(udf) = registry.udf(name) {
        return Ok(udf.call(args));
    }
    expr_from_name(name, args, config)
}
```

### Target Files
- `rust/datafusion_ext/src/udf_expr.rs`
- `rust/datafusion_python/src/codeanatomy_ext.rs`

### Deprecate/Delete After Completion
- Registry‑bypass paths in `udf_expr` callsites.

### Implementation Checklist
- [x] Add `expr_from_registry_or_specs` in `udf_expr.rs`.
- [x] Use the registry‑first helper from `codeanatomy_ext::udf_expr`.
- [x] Add a unit test verifying registry‑first behavior.

---

## Scope 8 — Standardize error wrapping across FFI paths

### Problem
Multiple FFI/PyO3 paths hand‑roll `DataFusionError::Execution(format!("{e}"))`, creating inconsistent error messages and context loss.

### Architecture and Code Pattern
Centralize conversion of `PyErr` into `DataFusionError` with context labels.

### Representative Snippet
```rust
// errors.rs
pub fn pyerr_to_dferr(context: &str, err: PyErr) -> DataFusionError {
    DataFusionError::Execution(format!("{context}: {err}"))
}
```

### Target Files
- `rust/datafusion_python/src/errors.rs`
- `rust/datafusion_python/src/udaf.rs`
- `rust/datafusion_python/src/udwf.rs`
- `rust/datafusion_python/src/udtf.rs`

### Deprecate/Delete After Completion
- In‑line `DataFusionError::Execution(format!("{e}"))` patterns.

### Implementation Checklist
- [x] Add `pyerr_to_dferr` helper in `errors.rs`.
- [x] Replace hand‑rolled error conversions in UDAF/UDWF/UDTF code.
- [x] Ensure error messages include caller context.

---

## Scope 9 — Optional Arrow C Stream ingestion helper

### Problem
The best‑in‑class “streaming‑first ABI” is not enforceable from Rust without a direct helper for `__arrow_c_stream__` objects.

### Architecture and Code Pattern
Add a Rust `pyfunction` that consumes any `__arrow_c_stream__` producer using `arrow_pyarrow` and returns a streaming reader.

### Representative Snippet
```rust
#[pyfunction]
fn arrow_stream_to_batches(py: Python<'_>, obj: PyObject) -> PyResult<Py<PyAny>> {
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use arrow_pyarrow::PyArrowType;

    let PyArrowType(mut reader): PyArrowType<ArrowArrayStreamReader> = obj.extract(py)?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch.map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        batches.push(batch);
    }
    // return as pyarrow.RecordBatchReader or list of batches
    crate::utils::record_batches_to_pyarrow(py, batches)
}
```

### Target Files
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `rust/datafusion_python/src/utils.rs` (if adding RecordBatchReader conversion helpers)
- `src/datafusion_ext.pyi`

### Deprecate/Delete After Completion
- None (optional additive API).

### Implementation Checklist
- [x] Add `arrow_stream_to_batches` pyfunction in Rust.
- [x] Add conversion helper returning `pyarrow.RecordBatchReader`.
- [x] Add stub/type entry in `datafusion_ext.pyi`.

---
