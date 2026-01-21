# DataFusion Built-in Schema Migration Plan (Design-Phase)

## Goals
- Replace custom schema logic with DataFusion built-ins wherever overlap exists.
- Move schema discovery and evolution to scan-boundary mechanisms.
- Prefer DataFusion catalog, DDL, and `information_schema` surfaces for introspection.
- Keep schema contracts as source-of-truth while leveraging DF-native behavior.

## Non-goals
- Changing dataset semantics or naming conventions.
- Removing schema contracts entirely (they remain the canonical spec).
- Avoiding Rust: this plan assumes minimal Rust where Python APIs do not expose DF hooks.

## DataFusion built-ins we will pivot to
- Catalog/Schema/Table providers (hierarchy and async table resolution).
- `CREATE EXTERNAL TABLE`, `CREATE VIEW`, `DESCRIBE <query>`.
- `information_schema.tables`, `information_schema.columns`, `information_schema.df_settings`.
- Listing table scan config + PhysicalExprAdapterFactory (schema evolution).
- Parquet scan settings (`schema_force_view_types`, `skip_metadata`, `binary_as_string`, etc.).
- DeltaTableProvider (Rust) for schema-aware Delta scans.

## Baseline updates (Jan 2026)
- Runtime registers dataset specs + view specs via `dataset_spec_catalog()` and
  `nested_view_specs()` in `datafusion_engine/runtime.py`.
- Schema registry validation now includes dataset spec names in the expected list.
- Schema handshake now uses DataFusion table schema after registration.
- Rust adapter factory now uses DataFusion's `DefaultPhysicalExprAdapterFactory`, with
  a PyO3 installer for contexts that lack the Python registration hook.
- Global schema registry has been removed; callers must request explicit registries.

---

## Scope 1: Replace fragment schema specs with DataFusion `DESCRIBE` / schema()
Status: Completed

### Target file list
- `src/datafusion_engine/query_fragments.py` (remove `_FRAGMENT_SCHEMA_SPECS`)
- `src/schema_spec/view_specs.py` (add ViewSpec factory from ctx)
- `src/datafusion_engine/runtime.py` (view registration flow)

### Code pattern
```python
from dataclasses import dataclass

from datafusion import SessionContext

from schema_spec.view_specs import ViewSpec


def view_spec_from_sql(ctx: SessionContext, *, name: str, sql: str) -> ViewSpec:
    schema = ctx.sql(sql).schema()
    return ViewSpec(name=name, sql=sql, schema=schema)
```

### Implementation checklist
- [x] Remove `_FRAGMENT_SCHEMA_SPECS` and `SqlFragment.schema` auto-derivation.
- [x] Add `view_spec_from_sql(...)` helper using `ctx.sql(...).schema()`.
- [x] Register views using computed schemas rather than manual maps.
- [ ] Update normalization/cpg pipelines to rely on `ViewSpec` from DF schema.

Notes
- Fragment view registration now computes schemas from DataFusion; pipeline SQL
  fragments continue to flow through their existing `SqlFragment` paths.

---

## Scope 2: DataFusion-native schema handshake (no PyArrow inference)
Status: Completed

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `src/schema_spec/schema_inference.py`

### Code pattern
```python
from datafusion import SessionContext

from schema_spec.system import ddl_fingerprint_from_schema


def ddl_fingerprint_from_table(ctx: SessionContext, *, name: str) -> str:
    schema = ctx.table(name).schema()
    return ddl_fingerprint_from_schema(name, schema)
```

### Implementation checklist
- [x] Replace `_infer_dataset_schema` with DF table schema (`ctx.table(name).schema()`).
- [x] Compute DDL fingerprints from DF schema only.
- [x] Defer schema validation until after table registration (DF-native schema).

Notes
- PyArrow inference is no longer used for the handshake path.

---

## Scope 3: Scan-boundary schema evolution via PhysicalExprAdapterFactory
Status: Completed

### Target file list
- `rust/datafusion_ext/src/lib.rs` (ListingTable provider builder + adapter wiring)
- `src/datafusion_engine/registry_bridge.py` (register DF provider, no manual cast)
- `src/datafusion_engine/runtime.py` (enable adapter for scans)

### Code pattern
```rust
use std::sync::Arc;

use datafusion_catalog_listing::ListingTableConfig;
use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;

let config = ListingTableConfig::new(path)
    .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));
```

### Implementation checklist
- [x] Add Rust helper to register the adapter factory (PyO3 installer).
- [x] Add Rust helper to build a ListingTable with adapter factory.
- [x] Expose the provider via PyCapsule for Python registration.
- [x] Remove Python-side evolution compatibility checks (adapter is authoritative).
- [x] Ensure Delta and Parquet providers use the same adapter path.

Notes
- DataFusion 51 Python lacks `register_physical_expr_adapter_factory`; the Rust
  installer is now the default integration path.

---

## Scope 4: Catalog-driven registration (DF catalog/SchemaProvider)
Status: Partially complete

### Target file list
- `src/datafusion_engine/catalog_provider.py`
- `src/schema_spec/catalog_registry.py`
- `src/datafusion_engine/runtime.py`
- `rust/datafusion_ext/src/lib.rs`

### Code pattern
```python
from datafusion import SessionContext

from datafusion_engine.catalog_provider import register_registry_catalog

register_registry_catalog(
    ctx,
    registry=dataset_registry,
    catalog_name="registry",
    schema_name="public",
)
```

### Implementation checklist
- [x] Replace schema-only registration with `TableProvider` registration.
- [x] Use DataFusion `CatalogProvider` to expose dataset registry entries.
- [ ] Add async lookup for remote metadata when needed (SchemaProvider.table).

Notes
- Runtime now registers dataset specs via empty `TableProvider` registrations; registry
  catalog provider is used for Ibis-backed registrations.

---

## Scope 5: DeltaTableProvider for schema-aware Delta scans
Status: Completed

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```rust
use deltalake::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};

let scan_config = DeltaScanConfigBuilder::new().build(&snapshot)?;
let provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config)?;
ctx.register_table("my_delta", Arc::new(provider))?;
```

### Implementation checklist
- [x] Expose a Rust helper to create DeltaTableProvider for a registry entry.
- [x] Register Delta tables via provider when available.
- [x] Use provider schema as the source of truth for fingerprints.

Notes
- Delta registration now attempts the Rust helper first and falls back to
  `DeltaTable.__datafusion_table_provider__()` when needed.

---

## Scope 6: View type policy and output coercion via DF config
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
config = config.set(
    "datafusion.execution.parquet.schema_force_view_types",
    "true",
)
config = config.set("datafusion.optimizer.expand_views_at_output", "true")
```

### Implementation checklist
- [x] Standardize view-type settings via DF config, remove local coercions.
- [x] Use `expand_views_at_output` where output coercion is needed.

---

## Scope 7: Introspection pivot to `information_schema`
Status: Completed

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/manifest.py` (diagnostics consumption)

### Code pattern
```python
rows = ctx.sql(
    "SELECT table_name, column_name, data_type FROM information_schema.columns"
).to_arrow_table()
```

### Implementation checklist
- [x] Replace custom schema snapshots with `information_schema` views.
- [x] Prefer `DESCRIBE <query>` for computed schemas.

Notes
- `SchemaIntrospector` already uses `information_schema` and `DESCRIBE`.

---

## Cross-cutting: Target file list

### New files
- None added; Rust installer in `datafusion_ext` replaces the Python bridge helper.

### Modified files
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/query_fragments.py`
- `src/schema_spec/view_specs.py`
- `src/schema_spec/system.py`
- `rust/datafusion_ext/src/lib.rs`

---

## Master implementation checklist
- [x] Replace fragment schema maps with DF schema inference.
- [x] Compute schema fingerprints from DF table schema.
- [x] Use ListingTable + PhysicalExprAdapterFactory for evolution.
- [~] Register registry tables via CatalogProvider/SchemaProvider.
- [x] Use DeltaTableProvider for Delta datasets.
- [x] Centralize view-type policy using DF configs.
- [x] Pivot introspection to `information_schema` and `DESCRIBE <query>`.
- [x] Update docs/examples to reference DF built-in flows.
