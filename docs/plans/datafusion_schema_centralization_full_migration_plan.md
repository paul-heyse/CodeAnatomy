 # DataFusion Schema Centralization Full Migration Plan (Design-Phase)
 
 ## Goals
 - Make the DataFusion `SessionContext` the single schema authority for all datasets.
 - Eliminate Python-side schema inference/registry tables that bypass DataFusion.
 - Use `information_schema` and DataFusion catalog APIs for schema discovery and validation.
 - Keep Ibis as a front-end over the shared `SessionContext` (SQL handoff).
 - Maintain dataset names and semantics while consolidating schema ownership.
 
 ## Non-goals
 - Changing dataset semantics, names, or row payloads.
 - Removing Ibis or rewriting extraction logic unrelated to schema authority.
 - Introducing new test-only code paths or monkeypatching.
 
 ## Architecture target
 DataFusion owns schema reality; other systems query DataFusion.
 
 ```mermaid
 flowchart TD
   extractorRows[ExtractorRowDicts] --> ibisMemtable[Ibis_memtable]
   dfContext[DataFusion_SessionContext] --> dfSchema[DF_schema_introspection]
   dfSchema --> ibisAlign[Ibis_align_to_DF_schema]
   ibisMemtable --> ibisAlign
   ibisAlign --> ibisPlan[Ibis_query_and_projection]
   ibisPlan --> dfExec[DataFusion_execution]
 ```
 
## Inventory of remaining non-DF schema authorities
- **Resolved (removed/migrated)**: `schema_spec/schema_inference.py`, `extract/spec_tables.py`,
  `normalize/spec_tables.py`, `cpg/spec_tables.py`, `storage/deltalake/registry_models.py`,
  `obs/diagnostics_schemas.py`; `incremental/snapshot.py` now uses DF schemas.
- **Resolved (constrained)**: `arrowdsl/schema/schema.py`, `arrowdsl/schema/build.py`,
  `arrowdsl/schema/serialization.py` now avoid schema inference/creation and only align/encode
  DataFusion‑sourced schemas.
- **Retained (non-authoritative)**: `schema_spec/system.py` remains for contract metadata and
  table alignment, but no longer drives schema discovery.
 
 ---
 
## Scope 0: Decommission legacy schema authorities (delete list)
Status: Completed

### Objective
Remove all Python-side schema authority modules so DataFusion is the only
source of schema truth.

### Legacy modules to delete or collapse
- Delete: `src/schema_spec/schema_inference.py`
- Delete: `src/extract/spec_tables.py`
- Delete: `src/normalize/spec_tables.py`
- Delete: `src/cpg/spec_tables.py`
- Delete: `src/storage/deltalake/registry_models.py`
- Delete: `src/obs/diagnostics_schemas.py`
- Replace schema construction in: `src/incremental/snapshot.py`
- Constrain to alignment only: `src/arrowdsl/schema/schema.py`
- **Moved to Scope 6**: `src/arrowdsl/schema/build.py`, `src/arrowdsl/schema/serialization.py`

### Code pattern
```python
# Remove module-level schema construction.
# Replace with DataFusion-introspected schemas and views.
from datafusion_engine.runtime import DataFusionRuntimeProfile

ctx = DataFusionRuntimeProfile().session_context()
schema = ctx.table("libcst_files_v1").schema()
```

---

## Scope 1: SessionContext schema authority baseline
 Status: Completed
 
 ### Objective
 Ensure every dataset schema is registered in DataFusion and discoverable via
 `information_schema.tables` and `information_schema.columns`.
 
 ### Target files
 - `src/datafusion_engine/schema_registry.py`
 - `src/datafusion_engine/runtime.py`
 - `src/datafusion_engine/schema_introspection.py`
 
 ### Implementation checklist
- [x] Ensure all dataset schemas are registered via `SessionContext` at runtime.
- [x] Extend schema introspection helpers as needed (column inventory, nested paths).
- [x] Add a single, documented schema authority entry point in `datafusion_engine`.
 
### Code pattern
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_introspection import SchemaIntrospector

ctx = DataFusionRuntimeProfile().session_context()
tables = SchemaIntrospector(ctx).tables_snapshot()
columns = SchemaIntrospector(ctx).table_columns("libcst_files_v1")
schema = ctx.table("libcst_files_v1").schema()
```

 ---
 
## Scope 2: Replace schema_spec inference with DataFusion introspection
 Status: Completed
 
 ### Target files
 - `src/schema_spec/system.py`
 - `src/schema_spec/schema_inference.py`
 - `src/schema_spec/catalog_registry.py`
 
 ### Implementation checklist
- [x] Remove dataset spec inference from `schema_spec` for extract datasets.
- [x] Replace `SchemaInferenceHarness` to rely on `SessionContext` schemas.
- [x] Use `information_schema` fingerprints (or `ctx.table(name).schema()`) for validation.
- [x] Remove remaining extract dataset spec exports from the catalog.
 
### Code pattern
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile

ctx = DataFusionRuntimeProfile().session_context()
schema = ctx.table(dataset_name).schema()
ddl_rows = ctx.sql(f"DESCRIBE {dataset_name}").to_arrow_table().to_pylist()
```

 ---
 
## Scope 3: Replace spec tables with DataFusion registry views
 Status: Completed
 
 ### Target files
 - `src/extract/spec_tables.py`
 - `src/normalize/spec_tables.py`
 - `src/cpg/spec_tables.py`
 - `src/storage/deltalake/registry_models.py`
 
 ### Implementation checklist
- [x] Remove Arrow spec table definitions for extract schemas.
- [x] Remove normalize/CPG spec tables and downstream dependencies.
- [x] Use DataFusion catalog or dedicated registry views instead of Arrow tables.
- [x] Keep registry diagnostics as DataFusion tables or views sourced from `information_schema`.
 
### Code pattern
```python
# Replace Arrow registry tables with DataFusion-native views.
ctx.sql(
    \"\"\"
    CREATE VIEW registry.normalize_rule_families AS
    SELECT * FROM information_schema.tables
    \"\"\"
)
```

 ---
 
## Scope 4: Param tables, diagnostics, and observability schemas
 Status: Completed
 
 ### Target files
 - `src/ibis_engine/param_tables.py`
 - `src/obs/diagnostics_schemas.py`
 - `src/datafusion_engine/schema_registry.py`
 
 ### Implementation checklist
- [x] Register param table schemas via `SessionContext` and infer via DF schema.
- [x] Move diagnostics schemas into the DataFusion registry, exposing them via `SessionContext`.
- [x] Replace Python-defined diagnostic schema checks with DataFusion introspection.
 
### Code pattern
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
import ibis

ctx = DataFusionRuntimeProfile().session_context()
con = ibis.datafusion.connect(ctx)
expr = con.table("params.p_file_ids")
schema = ctx.table("params.p_file_ids").schema()
```

 ---
 
## Scope 5: Incremental snapshot schemas
 Status: Completed
 
 ### Target files
 - `src/incremental/snapshot.py`
 - `src/incremental/diff.py`
 - `src/incremental/invalidations.py`
 
 ### Implementation checklist
- [x] Use DataFusion-registered schemas for snapshot tables instead of `pa.schema`.
- [x] Validate snapshot schema via `information_schema` before write/merge operations.
 
### Code pattern
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_introspection import SchemaIntrospector

ctx = DataFusionRuntimeProfile().session_context()
schema = ctx.table("repo_files_v1").schema()
columns = SchemaIntrospector(ctx).table_columns("repo_files_v1")
```

 ---
 
## Scope 6: ArrowDSL schema utilities alignment
 Status: Completed
 
 ### Target files
 - `src/arrowdsl/schema/schema.py`
 - `src/arrowdsl/schema/build.py`
 - `src/arrowdsl/schema/serialization.py`
 
 ### Implementation checklist
- [x] Constrain helpers to accept schemas sourced from DataFusion (`schema.py` cleaned).
- [x] Remove direct schema creation in ArrowDSL where DataFusion already owns the schema.
- [x] Keep only alignment/encoding utilities, not schema construction.
 
### Code pattern
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
from arrowdsl.schema.schema import align_table

ctx = DataFusionRuntimeProfile().session_context()
schema = ctx.table("libcst_files_v1").schema()
aligned = align_table(table, schema=schema, safe_cast=True)
```

 ---
 
## Scope 7: Validation, tests, and documentation
 Status: Completed
 
 ### Target files
 - `src/datafusion_engine/extract_registry.py`
 - `tests/unit/test_registry_generators.py`
 - `docs/python_library_reference/datafusion_schema.md`
 
 ### Implementation checklist
- [x] Validate schema presence and columns via `information_schema` in registry checks.
- [x] Remove tests that assume Arrow registry tables exist for extract datasets.
- [x] Update docs to state SessionContext is the single schema authority.
 
### Code pattern
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_introspection import SchemaIntrospector

ctx = DataFusionRuntimeProfile().session_context()
columns = SchemaIntrospector(ctx).columns_snapshot()
```

 ---
 
 ## Migration notes
 - Ibis remains a front-end; DataFusion registers schemas and executes SQL.
 - Any existing Arrow schema tables should become DataFusion views or be dropped if obsolete.
 - Prefer DataFusion `information_schema` introspection over bespoke Python catalogs.
