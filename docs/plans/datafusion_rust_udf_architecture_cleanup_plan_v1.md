# DataFusion Rust UDF Architecture Cleanup Plan v1

Status: Implemented (design-phase breaking changes, validated 2026-02-03)
Owner: Codex

## Goals
- Eliminate duplicated UDF surfaces so registry snapshots are the **single source of truth**.
- Align SQL table-function behavior with DataFusion’s documented best-in-class patterns.
- Tighten Delta plugin parity by collapsing scan config to one canonical payload.
- Reduce long-term drift across Rust, Python bindings, and plugin paths.

## Non-Goals
- No changes to upstream DataFusion/delta-rs APIs.
- No new user-facing UDF features beyond architectural alignment.

---

## Scope 1: Remove Per-UDF Python Wrappers; Add a Generic UDF Expression Helper

### Problem
Python exposes per-UDF helpers that duplicate registry metadata and allow drift from
the canonical `registry_snapshot`/`udf_docs` truth.

### Architecture and Code Pattern
Expose a **single** generic helper that resolves a UDF by name from the session registry
(when a context is provided) and constructs the expression using the registered
`ScalarUDF`. All per-UDF wrapper functions are removed.

### Representative Snippet
```rust
// rust/datafusion_python/src/codeanatomy_ext.rs (generic helper)
#[pyfunction]
#[pyo3(signature = (name, *args, ctx=None))]
fn udf_expr(
    py: Python<'_>,
    name: String,
    args: &Bound<'_, PyTuple>,
    ctx: Option<PyRef<PySessionContext>>,
) -> PyResult<PyExpr> { ... }
```

### Target Files
- `rust/datafusion_python/src/codeanatomy_ext.rs` (add `udf_expr` export)
- `rust/datafusion_python/src/lib.rs` (module exports)
- `rust/datafusion_ext.pyi` (public API surface)

### Deprecate/Delete After Completion
- `rust/datafusion_python/src/udf_custom_py.rs`
- `rust/datafusion_python/src/udf_builtin.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- Per-UDF exports in `rust/datafusion_python/src/codeanatomy_ext.rs`

### Implementation Checklist
- [x] Add `udf_expr(name, *args, ctx=None)` in Rust bindings.
- [x] Update Python docs/stubs to document the generic helper.
- [x] Replace call sites that import per-UDF helpers.
- [x] Remove per-UDF modules and exports.
- [x] Update tests to use `udf_expr` or SQL (no remaining per-UDF wrapper usage found).

---

## Scope 2: Remove `range_table` Alias; Prefer Built-In Table Functions

### Problem
We register a custom `range_table` alias even though DataFusion already provides
`range` / `generate_series` table functions. This duplicates UDTF surfaces and
complicates registry snapshots.

### Architecture and Code Pattern
Rely on DataFusion’s built-in table function set (`all_default_table_functions()`).
Remove the custom `range_table` entry from the registry/docs/tests.

### Representative Snippet
```rust
// rust/datafusion_ext/src/udf_registry.rs
pub fn table_udf_specs() -> Vec<TableUdfSpec> {
    table_udfs![
        // range_table removed; use built-ins instead
    ]
}
```

### Target Files
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`
- `docs/architecture/*` references to `range_table`

### Deprecate/Delete After Completion
- Any `range_table` documentation, tests, or snapshot expectations.

### Implementation Checklist
- [x] Remove `range_table` from registry registration.
- [x] Remove `range_table` from docs/snapshots.
- [x] Update SQL examples/tests to use `range` or `generate_series`.

---

## Scope 3: Replace Minimal `read_parquet` / `read_csv` UDTFs With Full-Fidelity Providers

### Problem
The current `udtf_external.rs` implementation is intentionally minimal and does not
surface the full option/pushdown surface described in DataFusion’s UDTF docs. This
creates a “cheap but divergent” path compared to `register_*` / `ListingTable` usage.

### Architecture and Code Pattern
Replace the minimal UDTFs with **full-fidelity** provider factories that:
- Parse and constant-fold argument expressions.
- Build `ListingTable` with full read options (schema hints, partitioning, compression,
  file_sort_order, and object store options).
- Preserve pushdown behavior by returning a proper provider.

### Representative Snippet
```rust
// rust/datafusion_ext/src/udtf_sources.rs (new)
fn read_parquet_udtf(ctx: &SessionContext, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
    let args = simplify_args(ctx, args)?;
    let path = parse_required_path(&args)?;
    let options = build_parquet_listing_options(&args)?;
    let table_path = ListingTableUrl::parse(path.as_str())?;
    let config = ListingTableConfig::new(table_path).with_listing_options(options);
    Ok(Arc::new(ListingTable::try_new(config)?))
}
```

### Target Files
- `rust/datafusion_ext/src/udtf_external.rs` → replace with `udtf_sources.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deprecate/Delete After Completion
- `rust/datafusion_ext/src/udtf_external.rs` (minimal UDTFs)
- Old docs for `read_parquet/read_csv` that list only minimal args

### Implementation Checklist
- [x] Create `udtf_sources.rs` with full-fidelity `read_parquet/read_csv`.
- [x] Support constant-folding of expressions and strict literal validation.
- [x] Wire full option parsing into `ListingTable` configuration.
- [x] Update registry/docs/snapshot to reflect new signatures.
- [x] Replace tests to assert pushdown-capable providers.

---

## Scope 4: Collapse Plugin Delta Scan Overrides Into a Canonical `scan_config` Payload

### Problem
Plugin options allow per-field overrides (`enable_parquet_pushdown`, etc.), duplicating
control-plane logic and risking divergence from session-derived behavior.

### Architecture and Code Pattern
Replace individual override fields with a single `scan_config` payload built on the host
from `delta_scan_config_from_session`, and parsed in the plugin to construct
`DeltaScanConfig`.

### Representative Snippet
```rust
// rust/df_plugin_codeanatomy/src/lib.rs
#[derive(Deserialize)]
struct DeltaProviderOptions {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    scan_config: DeltaScanConfigPayload, // canonical
    // ... protocol gates, version/timestamp, etc.
}
```

### Target Files
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_python/src/codeanatomy_ext.rs`
- `src/datafusion_engine/delta/plugin_options.py`
- `src/datafusion_ext.pyi`

### Deprecate/Delete After Completion
- Per-field scan overrides in plugin options:
  `file_column_name`, `enable_parquet_pushdown`, `schema_force_view_types`,
  `wrap_partition_values`, `schema_ipc`

### Implementation Checklist
- [x] Define a canonical `scan_config` payload schema.
- [x] Host: build `scan_config` via `delta_scan_config_from_session`.
- [x] Plugin: parse payload into `DeltaScanConfig`.
- [x] Remove legacy override fields and update tests/docs.

---

## Cross-Cutting Checklist
- [x] Update docs and stubs for new API surfaces.
- [x] Update registry snapshot expectations and parity tests.
- [x] Add migration notes for breaking changes.
- [ ] Rebuild Rust artifacts after changes (`scripts/rebuild_rust_artifacts.sh`).

---

## Deliverables
- New plan-aligned Rust/Python UDF surface (generic helper only).
- Replaced `read_*` UDTFs with full-fidelity provider factories.
- Canonicalized Delta plugin scan config payload.
- Removal of deprecated modules and documentation alignment.
