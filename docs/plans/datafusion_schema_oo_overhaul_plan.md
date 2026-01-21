# DataFusion Schema-Driven OO Overhaul Plan (Design-Phase Spec)

## Goals
- Make schema contracts first-class, versioned, and programmatic across DataFusion + Ibis.
- Support dynamic discovery, late binding, and object-oriented dataset handles.
- Harden schema stability (nested types, view types, partition columns, metadata).
- Provide a compiler-like rule pipeline grounded in schema introspection.
- Preserve portability: Python-first prototypes with a clean Rust escalation path.

## Design tenets
- **Schema is the source of truth.** Registration, views, and policies derive from schema contracts.
- **Nested storage, exploded query surfaces.** Store nested bundles, expose relational views.
- **Explicit evolution rules.** No silent drift; all adaptation is versioned and audited.
- **OO datasets.** Datasets are objects with lifecycle methods and introspection surfaces.
- **Diagnostics-first.** Every registration and view creation emits stable artifacts.

## Architecture overview (proposed)
- **SchemaProfiles:** Session-level settings for schema hardening and view-type policy.
- **ScanPolicy:** Table-level scan knobs (Parquet, listing, partitions, stats, view types).
- **SchemaIntrospector:** DESCRIBE/SHOW/plan-schema discovery and verification.
- **DatasetHandle:** OO dataset object with schema/DDL/registration/view lifecycle.
- **ViewSpec/Lens:** Schema-validated views derived from nested bundles.
- **DynamicCatalog:** Catalog/Schema providers to resolve datasets on demand.
- **EvolutionAdapter:** Version-aware adaptation using `SchemaEvolutionSpec` + Rust adapters.
- **InferenceHarness:** Inferred schema → fingerprint → canonical schema handshake.

## Status (Jan 2026)
- Scope 1: Completed (schema hardening profile + presets wired into runtime).
- Scope 2: Completed (expanded scan options + registry application + snapshots).
- Scope 3: Completed (SchemaIntrospector + diagnostics snapshot wiring).
- Scope 4: Completed (DatasetHandle + DatasetSpec wiring).
- Scope 5: Completed (ViewSpec infra, contract wiring, nested fragment schemas).
- Scope 6: Completed (registry-backed catalog provider + dataset/repo handle support).
- Scope 7: Partially complete (adapter factory + runtime hook; evolution rules + binding still pending).
- Scope 8: Completed (inference harness + DDL fingerprint + handshake enforcement).

---

## Scope 1: Schema-hardening profiles (session-level)

### Target file list
- `src/datafusion_engine/runtime.py` (extend profile presets)
- `src/datafusion_engine/compile_options.py` (policy hooks, if needed)
- `src/schema_spec/system.py` (integration points for profile usage)

### Code pattern
```python
from dataclasses import dataclass

from datafusion import SessionConfig


@dataclass(frozen=True)
class SchemaHardeningProfile:
    """Define schema-stability settings for DataFusion sessions."""

    enable_view_types: bool = False
    timezone: str = "UTC"
    show_schema_in_explain: bool = True
    show_types_in_format: bool = True
    strict_aggregate_schema_check: bool = True

    def apply(self, config: SessionConfig) -> SessionConfig:
        config = config.set("datafusion.explain.show_schema", str(self.show_schema_in_explain).lower())
        config = config.set("datafusion.format.types_info", str(self.show_types_in_format).lower())
        config = config.set("datafusion.execution.time_zone", self.timezone)
        config = config.set(
            "datafusion.execution.skip_physical_aggregate_schema_check",
            str(not self.strict_aggregate_schema_check).lower(),
        )
        config = config.set(
            "datafusion.sql_parser.map_string_types_to_utf8view",
            str(self.enable_view_types).lower(),
        )
        config = config.set(
            "datafusion.execution.parquet.schema_force_view_types",
            str(self.enable_view_types).lower(),
        )
        return config
```

### Implementation checklist
- [x] Define `SchemaHardeningProfile` with stable defaults.
- [x] Wire profile into `DataFusionRuntimeProfile.session_config()`.
- [x] Add named presets for schema hardening.

---

## Scope 2: Schema-aware scan policy (table-level)

### Target file list
- `src/schema_spec/system.py` (expand `DataFusionScanOptions`)
- `src/datafusion_engine/registry_bridge.py` (apply new settings and options)
- `src/ibis_engine/registry.py` (snapshot new fields)
- `src/schema_spec/specs.py` (external table options pass-through)

### Code pattern
```python
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class DataFusionScanOptions:
    """Per-table scan policy aligned with schema stability."""

    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = False
    skip_arrow_metadata: bool | None = None
    binary_as_string: bool | None = None
    schema_force_view_types: bool | None = None
    listing_table_factory_infer_partitions: bool | None = None
    listing_table_ignore_subdirectory: bool | None = None
    list_files_cache_ttl: str | None = None
    list_files_cache_limit: str | None = None
    meta_fetch_concurrency: int | None = None
    collect_statistics: bool | None = None
    file_extension: str | None = None
    cache: bool = False
    listing_mutable: bool = False
    unbounded: bool = False
```

### Implementation checklist
- [x] Extend `DataFusionScanOptions` with view-type, metadata, and listing toggles.
- [x] Apply new settings via `SET` in `_apply_scan_settings`.
- [x] Thread `skip_arrow_metadata`, `binary_as_string`, and `schema_force_view_types`
      into `register_parquet`/`register_listing_table` or DDL OPTIONS.

---

## Scope 3: Schema introspection and verification service

### Target file list
- `src/datafusion_engine/schema_introspection.py` (new)
- `src/datafusion_engine/runtime.py` (wire diagnostics hooks)
- `src/datafusion_engine/schema_registry.py` (integration with validators)

### Code pattern
```python
from dataclasses import dataclass

from datafusion import SessionContext


@dataclass(frozen=True)
class SchemaIntrospector:
    """Expose schema reflection across tables and queries."""

    ctx: SessionContext

    def describe_query(self, sql: str) -> list[dict[str, object]]:
        rows = self.ctx.sql(f"DESCRIBE {sql}").to_arrow_table().to_pylist()
        return [dict(row) for row in rows]

    def table_columns(self, table_name: str) -> list[dict[str, object]]:
        query = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_name = '{table_name}'"
        )
        return [dict(row) for row in self.ctx.sql(query).to_arrow_table().to_pylist()]

    def settings_snapshot(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.ctx.sql("SELECT * FROM information_schema.df_settings").to_pylist()]
```

### Implementation checklist
- [x] Add `SchemaIntrospector` and expose it from the runtime profile.
- [x] Use `DESCRIBE <query>` to validate view/output schemas during registration.
- [x] Emit schema snapshots into diagnostics sinks for auditing.

---

## Scope 4: OO DatasetHandle + registry integration

### Target file list
- `src/schema_spec/dataset_handle.py` (new)
- `src/schema_spec/system.py` (wire `DatasetSpec` to handles)
- `src/ibis_engine/registry.py` (registry creation helpers)
- `src/datafusion_engine/registry_bridge.py` (registration entrypoint)

### Code pattern
```python
from dataclasses import dataclass

from datafusion import SessionContext

from schema_spec.system import DatasetSpec
from datafusion_engine.registry_bridge import register_dataset_df


@dataclass(frozen=True)
class DatasetHandle:
    """Object-oriented dataset handle with schema + lifecycle."""

    spec: DatasetSpec

    def schema(self):
        return self.spec.schema()

    def ddl(self, *, location: str) -> str:
        config = self.spec.table_spec.external_table_config(location=location)
        return self.spec.external_table_sql(config)

    def register(self, ctx: SessionContext, *, location) -> None:
        register_dataset_df(ctx, name=self.spec.name, location=location)

    def register_views(self, ctx: SessionContext) -> None:
        for view in self.spec.contract().views:
            ctx.sql(view.sql).collect()
```

### Implementation checklist
- [x] Add `DatasetHandle` wrapper with schema/DDL/registration methods.
- [x] Provide `DatasetSpec.to_handle()` helper for fluent usage.
- [x] Enforce registry naming and schema version in handle creation.

Notes
- Dataset handles now resolve both dataset and contract view specs.

---

## Scope 5: ViewSpec / Lens (exploded views as schema objects)

### Target file list
- `src/schema_spec/view_specs.py` (new)
- `src/datafusion_engine/schema_registry.py` (nested schema utilities)
- `src/datafusion_engine/query_fragments.py` (view generation integration)
- `src/datafusion_engine/runtime.py` (view registry recording)

### Code pattern
```python
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class ViewSpec:
    """Schema-validated view definition."""

    name: str
    sql: str
    schema: pa.Schema

    def validate(self, ctx) -> None:
        rows = ctx.sql(f"DESCRIBE {self.name}").to_arrow_table().to_pylist()
        actual = {row["column_name"]: row["data_type"] for row in rows if row.get("column_name")}
        expected = {field.name: str(field.type) for field in self.schema}
        if actual != expected:
            raise ValueError(f"View schema mismatch for {self.name}.")
```

### Implementation checklist
- [x] Add `ViewSpec` and bind to DatasetSpec contracts.
- [x] Validate view schemas via `DESCRIBE`.
- [x] Record view SQL in `DataFusionViewRegistry`.
- [x] Populate `SqlFragment.schema` for nested view generation and bind `ViewSpec` to
      `query_fragments.py`/`schema_registry.py`.

Notes
- View registration is available via `datafusion_engine.runtime.register_view_specs(...)`.

---

## Scope 6: Dynamic catalog, schema providers, and factory resolution

### Target file list
- `src/datafusion_engine/catalog_provider.py` (new)
- `src/datafusion_engine/registry_bridge.py` (resolver integration)
- `rust/datafusion_ext/src/lib.rs` (optional Rust provider + PyCapsule)

### Code pattern
```python
from datafusion.catalog import CatalogProvider, SchemaProvider


class RegistrySchemaProvider(SchemaProvider):
    """Resolve tables from a registry snapshot (cached)."""

    def __init__(self, registry):
        self._registry = registry
        self._tables = {}

    def table(self, name: str):
        if name not in self._tables:
            location = self._registry.catalog.get(name)
            self._tables[name] = location
        return self._tables.get(name)

    def table_names(self):
        return set(self._registry.catalog.names())


class RegistryCatalogProvider(CatalogProvider):
    """Registry-backed catalog provider."""

    def __init__(self, registry):
        self._schema = RegistrySchemaProvider(registry)

    def schema(self, name: str):
        return self._schema if name == "public" else None

    def schema_names(self):
        return {"public"}
```

### Implementation checklist
- [x] Implement Python `CatalogProvider`/`SchemaProvider` with cached snapshot semantics.
- [x] Add factory resolving `dataset://` and `repo://` handles to providers.
- [x] Define Rust PyCapsule exports for provider factories (memory-backed).

---

## Scope 7: Schema evolution and PhysicalExprAdapter (Rust escalation path)

### Target file list
- `src/schema_spec/system.py` (evolution spec usage)
- `rust/datafusion_ext/src/lib.rs` (new adapter + provider)
- `src/datafusion_engine/registry_bridge.py` (adapter registration hook)

### Code pattern
```rust
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::datasource::physical_plan::FileScanConfig;

pub struct SchemaEvolutionAdapterFactory;

impl PhysicalExprAdapterFactory for SchemaEvolutionAdapterFactory {
    fn create_adapter(
        &self,
        scan_config: &FileScanConfig,
    ) -> datafusion_common::Result<Box<dyn PhysicalExprAdapter>> {
        // Build adapter to map logical schema to physical file schema.
        Ok(Box::new(MyAdapter::new(scan_config.clone())))
    }
}
```

### Implementation checklist
- [ ] Define canonical evolution rules per dataset in `SchemaEvolutionSpec`.
- [x] Implement a Rust adapter factory (no-op rewrite scaffold).
- [x] Expose adapter factory via PyCapsule.
- [ ] Wire adapter factory into DataFusion Python binding (no public API yet).

---

## Scope 8: Schema inference harness + contract handshake

### Target file list
- `src/schema_spec/schema_inference.py` (new)
- `src/schema_spec/system.py` (fingerprint utilities)
- `src/ibis_engine/registry.py` (registry snapshot extended with fingerprints)

### Code pattern
```python
from dataclasses import dataclass

import pyarrow as pa

from schema_spec.system import ddl_fingerprint_from_schema, dataset_spec_from_schema


@dataclass(frozen=True)
class SchemaInferenceHarness:
    """Resolve inferred schema to a canonical contract."""

    def infer_and_register(self, name: str, table: pa.Table):
        inferred = table.schema
        ddl_hash = ddl_fingerprint_from_schema(name, inferred)
        spec = dataset_spec_from_schema(name, inferred)
        return spec, ddl_hash
```

### Implementation checklist
- [x] Add inference harness to capture inferred schema + DDL fingerprint.
- [x] Persist the fingerprint with registry metadata for reproducibility.
- [x] Require re-registration against canonical schema in subsequent runs.

---

## Cross-cutting: Target file list (consolidated)

### New files
- `src/datafusion_engine/schema_introspection.py`
- `src/schema_spec/dataset_handle.py`
- `src/schema_spec/view_specs.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/schema_spec/schema_inference.py`
- `docs/plans/datafusion_schema_oo_overhaul_examples.md`

### Modified files
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `rust/datafusion_ext/src/lib.rs`

---

## Implementation checklist (master)
- [x] Add schema-hardening session profile presets and policies.
- [x] Expand `DataFusionScanOptions` and apply new scan settings + DDL options.
- [x] Implement `SchemaIntrospector` and wire diagnostics outputs.
- [x] Introduce `DatasetHandle` and integrate with registry usage.
- [x] Add `ViewSpec` binding in nested query fragments (schemas still missing).
- [x] Implement registry-backed Catalog/Schema providers (Python).
- [x] Add optional Rust provider + adapter (PyCapsule) for production-grade evolution.
- [x] Build schema inference harness with DDL fingerprint + registry persistence.
- [x] Update registry snapshot payloads with new schema policy fields.
- [x] Add documentation and example usage for new API surface.
