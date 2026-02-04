# DataFusion Rust UDF Best-In-Class Deployment Plan v1

Status: Implemented (minor follow-ups noted in checklists)
Owner: Codex (proposed)

## Goals
- Best-in-class deployment of Rust-backed UDFs and Delta providers with strict correctness, ABI safety, and config parity across native, plugin, and fallback paths.
- Single-source-of-truth for UDF metadata (signatures, volatility, docs) with enforced parity against information_schema.
- Deterministic, debuggable behavior across all environments (local, CI, prod).

## Non-Goals
- No changes to DataFusion or delta-rs upstream APIs.
- No new UDF feature work beyond robustness/compatibility.

---

## Scope 1: Fallback Snapshot Truthfulness

### Problem
Fallback mode exposes UDF names and signature metadata that are not actually registered, allowing plan validation to pass while execution fails.

### Architecture and Code Pattern
Constrain the fallback snapshot to only the UDFs that are actually registered (or expand registration to match the snapshot). Choose one consistent path.

### Representative Snippet
```python
# src/datafusion_engine/udf/fallback.py

def _registered_fallback_names() -> tuple[str, ...]:
    return tuple(sorted(_REGISTERABLE_UDFS))


def fallback_udf_snapshot() -> dict[str, object]:
    names = _registered_fallback_names()
    # Build param_names/volatility/signatures only for names that are truly registered.
    param_names: dict[str, tuple[str, ...]] = {}
    volatility: dict[str, str] = {}
    signature_inputs: dict[str, tuple[tuple[str, ...], ...]] = {}
    return_types: dict[str, tuple[str, ...]] = {}
    for name in names:
        spec = _FALLBACK_UDF_SPECS[name]
        param_names[name] = spec.arg_names
        volatility[name] = spec.volatility
        signature_inputs[name] = (tuple(str(dtype) for dtype in spec.input_types),)
        return_types[name] = (str(spec.return_type),)
    return {
        "scalar": list(names),
        "aggregate": [],
        "window": [],
        "table": [],
        "pycapsule_udfs": [],
        "aliases": {},
        "parameter_names": param_names,
        "volatility": volatility,
        "rewrite_tags": {},
        "signature_inputs": signature_inputs,
        "return_types": return_types,
        "simplify": {},
        "coerce_types": {},
        "short_circuits": {},
        "config_defaults": {},
        "custom_udfs": [],
    }
```

### Target Files
- `src/datafusion_engine/udf/fallback.py`
- `src/datafusion_engine/udf/runtime.py` (validation behavior only if needed)

### Deprecate/Delete After Completion
- None (behavioral fix only).

### Implementation Checklist
- [x] Align fallback snapshot to registered fallback UDFs (or register full set).
- [x] Add a test that compares snapshot names to actually registered fallback UDFs.
- [x] Ensure `validate_required_udfs` fails fast when fallback cannot satisfy required UDFs.

---

## Scope 2: Plugin UDF Config Parity With Native

### Problem
Plugin UDF bundles currently skip `with_updated_config`, so per-session ConfigOptions do not flow into UDF behavior.

### Architecture and Code Pattern
Apply `with_updated_config` in the plugin UDF bundle path, mirroring native registration logic.

### Representative Snippet
```rust
// rust/df_plugin_codeanatomy/src/lib.rs
fn build_udf_bundle_from_specs(
    specs: Vec<udf_registry::ScalarUdfSpec>,
    config: &ConfigOptions,
) -> DfUdfBundleV1 {
    let mut scalar = Vec::new();
    for spec in specs {
        let mut udf = (spec.builder)();
        if let Some(updated) = udf.inner().with_updated_config(config) {
            udf = updated;
        }
        if !spec.aliases.is_empty() {
            udf = udf.with_aliases(spec.aliases.iter().copied());
        }
        scalar.push(FFI_ScalarUDF::from(Arc::new(udf)));
    }
    // aggregate + window unchanged
    ...
}
```

### Target Files
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `rust/datafusion_ext/src/udf_registry.rs` (reference pattern)

### Deprecate/Delete After Completion
- None.

### Implementation Checklist
- [x] Thread ConfigOptions into plugin UDF bundle creation.
- [x] Add a parity test: native vs plugin snapshot of config defaults/behavior.
- [x] Ensure UDF docs reflect config-driven behavior (if documentation uses defaults).
- [x] Preserve parity after UDF modularization and compat layer refactors.

---

## Scope 3: Delta Provider Scan Config Parity + Schema IPC

### Problem
Plugin Delta providers build `DeltaScanConfig::new()` directly, skipping builder/session-derived logic that attaches file columns and schema overrides.

### Architecture and Code Pattern
Use the same builder and overrides logic as the Rust control plane, and accept schema IPC in plugin options.

### Representative Snippet
```rust
// rust/df_plugin_codeanatomy/src/lib.rs
#[derive(Debug, Deserialize)]
struct DeltaProviderOptions {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    // ...
    schema_ipc: Option<Vec<u8>>, // new
}

fn delta_scan_config_from_options(
    options: &DeltaProviderOptions,
    snapshot: &EagerSnapshot,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let mut overrides = DeltaScanOverrides::default();
    overrides.file_column_name = options.file_column_name.clone();
    overrides.enable_parquet_pushdown = options.enable_parquet_pushdown;
    overrides.schema_force_view_types = options.schema_force_view_types;
    overrides.wrap_partition_values = options.wrap_partition_values;
    if let Some(ipc) = options.schema_ipc.as_ref() {
        overrides.schema = Some(schema_from_ipc(ipc)?);
    }
    let base = DeltaScanConfig::new();
    let scan = apply_overrides(base, overrides);
    apply_file_column_builder(scan, snapshot)
}
```

### Target Files
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_ext/src/delta_control_plane.rs` (source-of-truth pattern)
- `rust/datafusion_python/src/codeanatomy_ext.rs` (schema IPC serialization helper reuse)

### Deprecate/Delete After Completion
- Consider deprecating any plugin options that duplicate the control-plane path once parity is achieved.

### Implementation Checklist
- [x] Add schema IPC to plugin options.
- [x] Reuse builder path with snapshot when file column name is set.
- [x] Add a test: plugin provider scan config matches control-plane scan config for the same table.

---

## Scope 4: ABI and Runtime Compatibility Gate

### Problem
Plugin load can succeed even when ABI or major versions are incompatible, risking undefined behavior.

### Architecture and Code Pattern
Enforce manifest version checks before registration; fail fast with detailed diagnostics.

### Representative Snippet
```python
# src/datafusion_engine/delta/control_plane.py or a new plugin loader module

def _check_plugin_manifest(manifest: Mapping[str, object], runtime: Mapping[str, object]) -> None:
    required = ["plugin_abi_major", "df_ffi_major", "datafusion_major", "arrow_major"]
    for key in required:
        if key not in manifest:
            raise DataFusionEngineError(f"Missing {key} in plugin manifest", kind=ErrorKind.PLUGIN)
    if manifest["plugin_abi_major"] != runtime["plugin_abi_major"]:
        raise DataFusionEngineError("Plugin ABI mismatch", kind=ErrorKind.PLUGIN)
    if manifest["datafusion_major"] != runtime["datafusion_major"]:
        raise DataFusionEngineError("DataFusion major mismatch", kind=ErrorKind.PLUGIN)
```

### Target Files
- `rust/datafusion_python/src/codeanatomy_ext.rs` (manifest payload provider)
- `src/datafusion_engine/delta/control_plane.py` or a dedicated plugin loader module
- `src/datafusion_ext.pyi` (if public contract changes)

### Deprecate/Delete After Completion
- None (adds guardrails).

### Implementation Checklist
- [x] Collect runtime version metadata (DataFusion/Arrow/FFI/ABI).
- [x] Enforce checks on plugin load/registration paths.
- [x] Add diagnostics output: manifest + runtime versions.

---

## Scope 5: UDF Metadata Parity and Deployment Audits

### Problem
We need a stable, verifiable source of truth for UDF metadata across native, plugin, and fallback paths.

### Architecture and Code Pattern
Use `registry_snapshot` + `udf_docs` as canonical and add parity checks in tests and runtime diagnostics.

### Representative Snippet
```rust
// rust/datafusion_ext/tests/udf_conformance.rs
#[test]
fn plugin_and_native_snapshots_match() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let native = registry_snapshot::registry_snapshot(&ctx.state());
    let plugin = load_plugin_snapshot()?; // new test helper
    assert_eq!(native.scalar, plugin.scalar);
    assert_eq!(native.return_types, plugin.return_types);
    Ok(())
}
```

### Target Files
- `rust/datafusion_ext/tests/udf_conformance.rs`
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `tests/unit/test_fallback_udf_snapshot.py`
- `src/datafusion_engine/lineage/diagnostics.py` (audit payload)

### Deprecate/Delete After Completion
- None.

### Implementation Checklist
- [x] Add parity tests for native vs plugin vs fallback snapshots.
- [x] Add a diagnostics payload field showing UDF manifest and snapshot hashes.
- [x] Ensure information_schema parity remains green.
- [x] Include compat-layer and modularized UDF modules in snapshot parity checks.

---

## Scope 6: UDF Correctness Cleanup (StableHash64)

### Problem
`StableHash64Udf::coerce_types` is inconsistent with the signature and error message, indicating a stale or incorrect implementation.

### Architecture and Code Pattern
Remove or correct `coerce_types` for UDFs where it is unnecessary.

### Representative Snippet
```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for StableHash64Udf {
    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![DataType::Utf8])
    }
}
```

### Target Files
- `rust/datafusion_ext/src/udf_custom.rs`

### Deprecate/Delete After Completion
- None.

### Implementation Checklist
- [x] Fix/remove incorrect coerce logic and error messages.
- [x] Add a unit test for arity and type coercion.

---

## Scope 7: Modularize Rust UDF Implementations

### Problem
`udf_custom.rs` is monolithic, causing review friction, merge conflicts, and poor domain separation.

### Architecture and Code Pattern
Split UDFs into domain modules and re-export constructors in a new `udf/mod.rs`.

### Representative Snippet
```rust
// rust/datafusion_ext/src/udf/mod.rs
pub mod hash;
pub mod span;
pub mod cdf;
pub mod string;
pub mod collection;
pub mod metadata;
pub mod struct_ops;

pub use hash::*;
pub use span::*;
```

### Target Files
- `rust/datafusion_ext/src/udf_custom.rs` (source to split)
- `rust/datafusion_ext/src/udf/mod.rs` (new)
- `rust/datafusion_ext/src/udf/*.rs` (new modules)
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/lib.rs`

### Deprecate/Delete After Completion
- `rust/datafusion_ext/src/udf_custom.rs`

### Implementation Checklist
- [x] Create domain modules and move UDFs by category.
- [x] Re-export constructor functions in `udf/mod.rs`.
- [x] Update registry and snapshot wiring to new modules.
- [x] Add module-local tests where possible.

---

## Scope 8: DataFusion Compatibility Shim

### Problem
UDFs import DataFusion APIs directly across many modules, making upgrades noisy.

### Architecture and Code Pattern
Centralize DataFusion API imports and version-specific helpers in a compat layer.

### Representative Snippet
```rust
// rust/datafusion_ext/src/compat.rs
pub use datafusion_expr::{ScalarUDF, ScalarUDFImpl, Signature};

pub fn signature_with_names(sig: Signature, names: Vec<String>) -> Signature {
    sig.with_parameter_names(names).unwrap_or(sig)
}
```

### Target Files
- `rust/datafusion_ext/src/compat.rs` (new)
- `rust/datafusion_ext/src/udf/*.rs`
- `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_registry.rs`

### Deprecate/Delete After Completion
- Direct `datafusion_expr::*` imports in UDF modules.

### Implementation Checklist
- [x] Create compat module with re-exports and helpers.
- [x] Replace direct imports in UDF modules.
- [x] Document DataFusion version notes in compat module.

---

## Scope 9: Expand simplify() Coverage + Short-Circuit Audit

### Problem
Only a subset of UDFs implement `simplify()` and `short_circuits()`, leaving performance on the table.

### Architecture and Code Pattern
Add constant-folding for literal arguments and declare short-circuit behavior where applicable.

### Representative Snippet
```rust
// rust/datafusion_ext/src/udf/span.rs
fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
    if args.iter().all(|arg| matches!(arg, Expr::Literal(_, _))) {
        let folded = fold_span_len(&args)?;
        return Ok(ExprSimplifyResult::Simplified(lit(folded)));
    }
    Ok(ExprSimplifyResult::Original(args))
}
```

### Target Files
- `rust/datafusion_ext/src/udf/span.rs`
- `rust/datafusion_ext/src/udf/collection.rs`
- `rust/datafusion_ext/src/udf/metadata.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deprecate/Delete After Completion
- None.

### Implementation Checklist
- [x] Identify candidates for constant-folding (span/list/map UDFs).
- [x] Implement `simplify()` with correct type/nullability preservation.
- [x] Audit UDFs for `short_circuits()` and implement where applicable.
- [x] Add tests verifying simplification results.

---

## Scope 10: Pattern/Regex Caching (DashMap)

### Problem
Pattern-based UDFs can recompile regex/patterns per invocation.

### Architecture and Code Pattern
Introduce a thread-safe cache for compiled patterns.

### Representative Snippet
```rust
use dashmap::DashMap;
use std::sync::LazyLock;

static PATTERN_CACHE: LazyLock<DashMap<String, Regex>> = LazyLock::new(DashMap::new);
```

### Target Files
- `rust/datafusion_ext/src/udf/common.rs`
- `rust/datafusion_ext/Cargo.toml`

### Deprecate/Delete After Completion
- None.

### Implementation Checklist
- [x] Add `dashmap` dependency.
- [x] Cache compiled patterns for qname/pattern UDFs.
- [x] Add a small perf regression test or microbenchmark if feasible.

---

## Scope 11: Deprecation Strategy Documentation

### Problem
UDF deprecations are not documented, making upgrades risky.

### Architecture and Code Pattern
Create a canonical deprecation strategy document with rules, timelines, and snapshot behavior.

### Representative Snippet
```markdown
# docs/architecture/datafusion_udf_deprecation_strategy.md
- Soft vs hard deprecation definitions
- Snapshot/alias behavior during deprecation
- Removal policy and compatibility window
```

### Target Files
- `docs/architecture/datafusion_udf_deprecation_strategy.md` (new)
- `docs/plans/datafusion_rust_udf_best_in_class_deployment_plan_v1.md` (link)

### Deprecate/Delete After Completion
- None.

### Implementation Checklist
- [x] Define deprecation levels and timelines.
- [x] Document snapshot and alias behavior during deprecation.
- [x] Link strategy doc from plan and relevant architecture docs.

See: `docs/architecture/datafusion_udf_deprecation_strategy.md`

---

## Cross-Cutting Checklist
- [x] All plan changes wired through ConfigOptions, with identical behavior for native and plugin paths.
- [x] UDF snapshot validation and fallback registration are consistent.
- [x] Plugin ABI/version guardrails added and tested.
- [x] Delta provider scan config parity validated (control plane vs plugin).
- [x] Update diagnostics to emit UDF snapshot hash and manifest versions.
- [x] UDF modularization completed with compat layer adoption.
- [x] simplify()/short_circuit coverage expanded and tested.
- [x] Pattern caching enabled without changing query semantics.
- [x] Deprecation strategy document published.

---

## Proposed Deprecations (Global)
- If plugin delta provider fully mirrors control-plane behavior, deprecate any ad-hoc or duplicated delta provider entrypoints that bypass scan config builder logic.
- If fallback is intentionally minimal, deprecate any callers that assume full Rust UDF availability in fallback mode.
- Remove `rust/datafusion_ext/src/udf_custom.rs` after module split.

---

## Verification Strategy
- Unit tests for fallback snapshot alignment.
- Conformance tests verifying native vs plugin snapshot parity.
- Integration test: plugin Delta provider scan config parity with control plane.
- CI diagnostics emission for snapshot hash + manifest versions.

---

## Deliverables
- Implementation across scopes 1-11 as above.
- Updated tests and diagnostics.
- Deployment notes for plugin compatibility checks.
- UDF deprecation strategy document.
