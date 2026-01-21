# Extract Registry DataFusion Pivot Plan (Design-Phase)

## Goals
- Make all extract registry modules depend directly on the central dataset/schema registry.
- Replace extract registry introspection with DataFusion catalog + DDL + information_schema.
- Convert extract query/pipeline definitions into DataFusion-native views or UDTFs.
- Keep Ibis as a supported integration layer over the shared DataFusion SessionContext.
- Preserve dataset contracts while shifting schema reality to DataFusion.
- Fully deprecate and delete legacy extract registry modules (no compatibility adapters).

## Non-goals
- Changing dataset semantics or output names.
- Removing dataset contracts or schema specs (they remain canonical).
- Rewriting extract kernels unrelated to registry/metadata wiring.
- Deprecating Ibis; Ibis remains a supported front-end backed by DataFusion.

## DataFusion built-ins we will pivot to
- `CatalogProvider → SchemaProvider → TableProvider` for registry resolution.
- `CREATE EXTERNAL TABLE`, `CREATE VIEW`, `DESCRIBE`, `SHOW TABLES/COLUMNS`.
- `information_schema.tables`, `information_schema.columns`, `information_schema.df_settings`.
- UDTF / TableProvider (PyCapsule) for dynamic or non-SQL extract sources.

## Ibis integration stance
- Ibis remains an optional integration layer that wraps a shared `datafusion.SessionContext`.
- All registry-driven schemas, tables, and views are registered in DataFusion first; Ibis
  consumes them via the shared context (no parallel registry).
- SQL is the handoff: Ibis compiles to SQL, DataFusion executes the SQL in the shared context.

### Ibis integration contract (shared SessionContext + SQL compilation)
```python
import ibis
from datafusion_engine.runtime import DataFusionRuntimeProfile

profile = DataFusionRuntimeProfile()
ctx = profile.session_context()

con = ibis.datafusion.connect(ctx)
expr = con.table("codeintel.public.ast_files_v1").select("path", "file_id")
sql = con.compile(expr)
df = ctx.sql(sql)
```

Notes
- Ibis does not register tables or views; DataFusion does.
- Ibis must target the same catalog/schema that DataFusion registers.

## Baseline updates (Jan 2026)
- Central dataset catalog lives in `schema_spec.catalog_registry.dataset_spec_catalog()`.
- Runtime uses DataFusion-native schema inference and information_schema for introspection.
- Rust PyCapsule helpers are available for ListingTable and Delta providers.

---

## Scope 0: Delete legacy extract registry modules (no adapters)
Status: Planned

### Deleted file list
- `src/extract/registry_builders.py`
- `src/extract/registry_bundles.py`
- `src/extract/registry_definitions.py`
- `src/extract/registry_extractors.py`
- `src/extract/registry_fields.py`
- `src/extract/registry_ids.py`
- `src/extract/registry_pipelines.py`
- `src/extract/registry_rows.py`
- `src/extract/registry_specs.py`
- `src/extract/registry_templates.py`
- `src/extract/registry_validation.py`

### Implementation checklist
- [ ] Replace all call sites to use the central registry and DataFusion APIs.
- [ ] Remove all `src/extract/registry_*.py` modules listed above.
- [ ] Confirm no compatibility adapters remain in `extract` registry namespace.

Notes
- This is a hard deprecation: no transitional wrappers or legacy shims.

---

## Scope 1: Centralize extract registry metadata in the central registry
Status: Planned

### Target file list
- `src/schema_spec/system.py` (new extract metadata model)
- `src/schema_spec/catalog_registry.py` (extract metadata accessors)
- `src/relspec/extract/registry_template_specs.py` (source of template specs)

### Code pattern
```python
from dataclasses import dataclass

from schema_spec.catalog_registry import dataset_spec_catalog


@dataclass(frozen=True)
class ExtractMetadata:
    template: str | None
    bundles: tuple[str, ...]
    derived_ids: tuple[str, ...]
    join_keys: tuple[str, ...]


def extract_metadata(name: str) -> ExtractMetadata:
    return dataset_spec_catalog().extract_metadata(name)
```

### Implementation checklist
- [ ] Define an `ExtractMetadata` model on the central registry side.
- [ ] Store extract metadata alongside `DatasetSpec` or `ContractSpec`.
- [ ] Add `dataset_spec_catalog().extract_metadata(name)` accessor.
- [ ] Remove row/table registry sources entirely (no `DatasetRow` / `DATASET_ROWS`).

Notes
- Central metadata must carry fields currently kept in extract registry rows
  (template, bundles, derived ids, join keys, postprocess, evidence metadata).

---

## Scope 2: Directly wire extract registry modules to central registry
Status: Planned

### Target file list
- All callers of `src/extract/registry_*.py`

### Code pattern
```python
from schema_spec.catalog_registry import dataset_spec_catalog


def dataset_spec(name: str) -> DatasetSpec:
    return dataset_spec_catalog().dataset_spec(name)


def dataset_schema(name: str) -> SchemaLike:
    return dataset_spec_catalog().dataset_schema(name)
```

### Implementation checklist
- [ ] Replace registry calls with central registry accessors in all call sites.
- [ ] Replace registry-derived schemas with DataFusion `DESCRIBE` or `schema()` data.
- [ ] Replace registry-derived queries with DataFusion views or UDTFs.
- [ ] Remove all registry module imports from `extract/*` and downstream packages.

Notes
- Extract modules should not import `relspec.rules.*` for registry construction once
  central metadata is authoritative.

---

## Scope 3: Replace query/pipeline registry with DataFusion-native views or UDTFs
Status: Planned

### Target file list
- `src/extract/schema_ops.py`
- `src/datafusion_engine/runtime.py` (view registration)
- `src/datafusion_engine/catalog_provider.py`
- `rust/datafusion_ext/src/lib.rs` (optional UDTF/Provider)

### Code pattern
```python
from datafusion import SessionContext

def register_extract_view(ctx: SessionContext, name: str, sql: str) -> None:
    ctx.sql(f"CREATE VIEW {name} AS {sql}").collect()
```

### Implementation checklist
- [ ] Emit SQL view definitions from central metadata + policy.
- [ ] Register views via DataFusion DDL and DataFusion view registry.
- [ ] Keep Ibis integration by compiling to SQL against the shared SessionContext.
- [ ] Ensure Ibis can resolve DataFusion-registered views with fully qualified names.
- [ ] Replace postprocess kernels with SQL or UDFs where possible.
- [ ] For non-SQL kernels, expose a Rust-backed TableProvider or UDTF.

Notes
- UDTFs must return a TableProvider and accept only literal args per DF docs.
- Ibis remains supported by targeting the same tables/views registered in DataFusion.

---

## Scope 4: Replace registry validation with DataFusion introspection
Status: Planned

### Target file list
- `src/datafusion_engine/schema_introspection.py`

### Code pattern
```python
rows = ctx.sql(
    "SELECT table_name, column_name, data_type FROM information_schema.columns"
).to_arrow_table()
```

### Implementation checklist
- [ ] Replace join-key validation with `information_schema.columns` checks.
- [ ] Use `DESCRIBE table` as the canonical schema display.
- [ ] Keep domain-specific validations only when DF cannot express them.

---

## Scope 5: Deprecate local field/bundle/id registries where DF can provide it
Status: Planned

### Target file list
- Central registry metadata and DataFusion DDL/introspection

### Implementation checklist
- [ ] Replace field catalog use with `DatasetSpec.schema()` or `DESCRIBE`.
- [ ] Map bundles to `information_schema.tables` and view outputs.
- [ ] Replace hash-id utilities with DF SQL functions or UDFs where possible.
- [ ] Move extractor defaults into central metadata.

Notes
- Keep local utilities only for ingestion-time hashing not expressible in SQL.

---

## Scope 6: Remove legacy registry row tables and templates
Status: Planned

### Target file list
- (covered by Scope 0 deletion list)

### Implementation checklist
- [ ] Remove row-table generation (`ExtractDatasetRowSpec` tables).
- [ ] Remove template expansion tables from extract registry.
- [ ] Confirm no legacy registry tables remain in storage/diagnostics.

---

## Scope 7: Docs and tests
Status: Planned

### Target file list
- `docs/plans/extract_registry_datafusion_pivot_plan.md`
- `docs/python_library_reference/datafusion_schema.md`
- `docs/python_library_reference/Datafusion_ibis_integration.md`
- `tests/` (new or updated registry tests)

### Implementation checklist
- [ ] Document the DataFusion-first extract registry contract with Ibis integration.
- [ ] Add tests for information_schema-based validation.
- [ ] Add tests for view registration from central metadata.
- [ ] Add tests ensuring Ibis can query DataFusion-registered views via shared context.
- [ ] Add tests validating Ibis SQL compilation uses DataFusion dialect/namespace.

---

## Cross-cutting: Target file list

### New files
- `src/schema_spec/extract_metadata.py` (if we separate extract metadata types)

### Modified files
- `src/schema_spec/catalog_registry.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/ibis_engine/backend.py`
- `src/ibis_engine/registry.py`
- `rust/datafusion_ext/src/lib.rs`

### Deleted files
- `src/extract/registry_builders.py`
- `src/extract/registry_bundles.py`
- `src/extract/registry_definitions.py`
- `src/extract/registry_extractors.py`
- `src/extract/registry_fields.py`
- `src/extract/registry_ids.py`
- `src/extract/registry_pipelines.py`
- `src/extract/registry_rows.py`
- `src/extract/registry_specs.py`
- `src/extract/registry_templates.py`
- `src/extract/registry_validation.py`

---

## Master implementation checklist
- [ ] Delete all legacy extract registry modules (no adapters).
- [ ] Centralize extract metadata in the central registry.
- [ ] Rewrite extract registry modules to use central registry accessors directly.
- [ ] Replace query/pipeline specs with DataFusion views or UDTFs.
- [ ] Pivot validation to DataFusion introspection surfaces.
- [ ] Deprecate field/bundle/id registries where DF provides equivalents.
- [ ] Remove legacy registry row tables and templates.
- [ ] Update docs/tests for the new DataFusion-first contract with Ibis support.
