# DataFusion Schema Cross-Repo Integration Plan (Design-Phase)

## Goals
- Use DataFusion-native schema capabilities for cross-repo coordination.
- Replace bespoke schema checks with DataFusion schema sources and invariants.
- Improve drift handling via scan-time adapters and TableSchema contracts.
- Standardize diagnostics with DataFusion introspection and DFSchema views.
- Maintain high performance with zero-copy interop and pushdown-friendly plans.

## Non-goals
- Changing dataset semantics, names, or row payloads.
- Reintroducing Python-only schema authority.
- Adding test-only logic or monkeypatching in production code.

---

## Scope 0: Cross-repo CatalogProvider chain and dynamic lookup
Status: Completed

### Objective
Mount multiple repo catalogs under one SessionContext using CatalogProvider and
SchemaProvider chains with explicit defaults, so cross-repo queries are native.

### Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/relspec/registry/datasets.py`
- `src/datafusion_engine/schema_introspection.py`

### Implementation checklist
- [x] Add a multi-catalog registry provider and mount it under a single SessionContext.
- [x] Add default catalog/schema selectors for cross-repo routing.
- [x] Record catalog snapshots in diagnostics for debugging.

### Code pattern
```python
provider = build_catalog_provider(repos=repo_configs)
ctx.register_catalog_provider("codeintel", provider)
ctx.set_default_catalog_and_schema("codeintel", "public")
```

---

## Scope 1: TableSchema contract for partitioned datasets
Status: Completed

### Objective
Adopt TableSchema as the authoritative contract for file schema + partition
columns, and enforce the contract at registration time.

### Target files
- `src/datafusion_engine/registry_bridge.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/schema_spec/system.py`

### Implementation checklist
- [x] Introduce a TableSchema contract object with file schema + partitions.
- [x] Enforce partition ordering/type invariants at registration.
- [x] Surface TableSchema payloads in diagnostics.

### Code pattern
```python
table_schema = TableSchema(file_schema, partition_cols)
provider = ListingTableProvider(table_schema=table_schema, options=options)
ctx.register_table(name, provider)
```

---

## Scope 2: Schema adapters and evolution hooks at scan boundary
Status: Completed

### Objective
Use schema adapter factories (PhysicalExprAdapterFactory) to align drifted
schemas at scan time instead of per-query casts.

### Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `rust/datafusion_ext/src/lib.rs`

### Implementation checklist
- [x] Register schema adapter factories per dataset family.
- [x] Route dataset-specific evolution policy to scan registration.
- [x] Emit adapter decisions into diagnostics.

### Code pattern
```python
factory = schema_evolution_adapter_factory(table_schema)
ctx.register_physical_expr_adapter_factory(factory)
```

---

## Scope 3: DFSchema invariants and ambiguity checks
Status: Completed

### Objective
Enforce DataFusion invariants on `(relation, name)` uniqueness and surface
DFSchema tree output for ambiguous joins across repos.

### Target files
- `src/relspec/rules/validation.py`
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/runtime.py`

### Implementation checklist
- [x] Validate DFSchema uniqueness before execution.
- [x] Add DFSchema tree_string payloads to diagnostics.
- [x] Fail fast on ambiguous schemas.

### Code pattern
```python
dfschema = df.schema()
validate_df_schema_invariants(dfschema)
diagnostics["dfschema_tree"] = dfschema.tree_string()
```

---

## Scope 4: TableProvider metadata for defaults and logical plans
Status: Completed

### Objective
Use TableProvider metadata (`get_column_default`, `get_logical_plan`,
`constraints`) for contracts, provenance, and schema enforcement.

### Target files
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/relspec/contracts.py`

### Implementation checklist
- [x] Record provider defaults and logical plan metadata.
- [x] Surface defaults and constraints in contract diagnostics.
- [x] Replace Python-only defaults with provider metadata.

### Code pattern
```python
provider = introspector.table_provider(name)
defaults = provider.get_column_default("col") if provider else None
logical = provider.get_logical_plan() if provider else None
```

---

## Scope 5: Typed parameters via prepared statements
Status: Completed

### Objective
Use prepared statements and `information_schema.parameters` for schema-aware
param validation across repos.

### Target files
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/param_tables.py`
- `src/relspec/rules/validation.py`

### Implementation checklist
- [x] Register prepared statements for rule SQL.
- [x] Validate parameter types from information_schema parameters.
- [x] Align param table schemas with DataFusion typed parameters.

### Code pattern
```python
stmt = ctx.prepare("SELECT * FROM params.p_file_ids WHERE file_id = ?")
params = introspector.parameters_snapshot()
```

---

## Scope 6: Schema-affecting configuration policy
Status: Completed

### Objective
Centralize schema-affecting config knobs (view types, parser typing, timezones)
into a single policy applied per runtime profile.

### Target files
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`

### Implementation checklist
- [x] Define policy profiles for view types and parser typing.
- [x] Enforce policy in SessionConfig and scan settings.
- [x] Emit policy snapshots for diagnostics.

### Code pattern
```python
config = config.set("datafusion.sql_parser.map_string_types_to_utf8view", "true")
config = config.set("datafusion.optimizer.expand_views_at_output", "true")
```

---

## Scope 7: Arrow metadata and extension types as UDTs
Status: Completed

### Objective
Use Arrow field metadata and extension types to encode semantic types (spans,
goids, symbol refs) and preserve them across plans.

### Target files
- `src/datafusion_engine/schema_registry.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/arrowdsl/schema/serialization.py`

### Implementation checklist
- [x] Attach semantic metadata to nested fields for schema provenance.
- [x] Preserve metadata in custom UDFs and table providers.
- [x] Add validation that metadata survives registration and query planning.

### Code pattern
```python
field = pa.field("span", span_type, metadata={"semantic_type": "Span"})
```

---

## Scope 8: Arrow boundary projection (requested_schema)
Status: Completed

### Objective
Use `__arrow_c_array__(requested_schema=...)` to enforce schema projection at
interop boundaries for zero-copy alignment.

### Target files
- `src/arrowdsl/core/interop.py`
- `src/datafusion_engine/registry_bridge.py`

### Implementation checklist
- [x] Add schema-aware projection for RecordBatch interop.
- [x] Use requested_schema for external ingestion paths.
- [x] Validate that projection preserves ordering and metadata.

### Code pattern
```python
batch.__arrow_c_array__(requested_schema=expected_schema)
```

---

## Scope 9: Schema-aware optimizer and rewrite hooks
Status: Partially complete

### Objective
Replace bespoke SQL fragments with optimizer rules and schema-aware rewrite
hooks for nested attribute access and span normalization.

### Target files
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/query_fragments.py`

### Implementation checklist
- [x] Add rewrite hooks for attrs map access and span ABI normalization.
- [x] Ensure optimizer rules retain schema metadata.
- [x] Validate rewrites via EXPLAIN schema output.
- [ ] Migrate remaining `query_fragments` call sites to the new rewrites.

### Code pattern
```python
rewrite = normalize_span_fields(expr, span_schema=SPAN_ABI)
```

---

## Execution order
1. Scope 0: Cross-repo CatalogProvider chain and dynamic lookup.
2. Scope 1: TableSchema contract for partitioned datasets.
3. Scope 2: Schema adapters and evolution hooks at scan boundary.
4. Scope 3: DFSchema invariants and ambiguity checks.
5. Scope 4: TableProvider metadata for defaults and logical plans.
6. Scope 5: Typed parameters via prepared statements.
7. Scope 6: Schema-affecting configuration policy.
8. Scope 7: Arrow metadata and extension types as UDTs.
9. Scope 8: Arrow boundary projection (requested_schema).
10. Scope 9: Schema-aware optimizer and rewrite hooks.

---

## Decommission targets (post‑migration cleanup)
Remove these modules and bespoke paths once DataFusion-native replacements are
fully wired and all call sites are migrated.

### Scope 0: CatalogProvider chain
- ✅ `schema_spec/catalog_registry.py` (dataset discovery + registry façade).
- ✅ `schema_spec/system.py` → `SchemaRegistry` class and registry mutation helpers.
- ✅ `relspec/registry/datasets.py` (dataset/contract catalog assembly from spec tables).

### Scope 1–2: TableSchema + schema adapters
- ⏳ Runtime DDL synthesis in `schema_spec/system.py` (`table_spec_from_schema`)
  for registration paths.
- ✅ Registry‑side schema handshake helpers that depend on Python‑derived DDL.

### Scope 3: DFSchema invariants
- ⏳ SQLGlot-only missing-column checks in `relspec/rules/validation.py` when
  DFSchema/EXPLAIN coverage is complete.

### Scope 4: TableProvider metadata
- ✅ Python defaults/constraints fallbacks in `relspec/contracts.py` and
  `schema_spec/system.py` once provider metadata is authoritative.

### Scope 5: Typed parameters
- ✅ Python param schema enforcement in `datafusion_engine/param_tables.py` once
  DataFusion typed parameters and prepared statements are the only validation path.

### Scope 7–9: Metadata, projection, rewrites
- ⏳ Bespoke attribute/span extraction SQL fragments in
  `datafusion_engine/query_fragments.py` that are replaced by optimizer rewrites.
- ⏳ Ad hoc interop projection code in `arrowdsl/core/interop.py` once
  `requested_schema` is the single boundary mechanism.
