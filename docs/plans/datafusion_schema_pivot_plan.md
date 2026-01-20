# DataFusion Schema Pivot Plan (Schema-First, Nested Tables, On-Demand Queries)

## Goals
- Make DataFusion the canonical schema registry for nested/struct/map shapes.
- Emit nested tables directly (no flattening) and access nested fields at query time.
- Avoid proliferating registered views. Only create `cpg_nodes` and `cpg_edges` when needed.
- Replace schema usage across the codebase with DataFusion-native schemas.

## Constraints and decisions
- No NestedSchemaSpec or wrapper model. Schemas are explicit `pyarrow.Schema` constants.
- No permanent view objects for nested domains. All projections are on-demand SQL/DataFrame queries.
- Only `cpg_nodes` and `cpg_edges` are allowed as registered views when required by consumers.
- Tables derive from schemas, not the other way around.

## Canonical nested tables (targets)
| Domain | Nested table (new) | Replaces (flattened sources) |
| --- | --- | --- |
| SCIP | `scip_index_v1` | scip_metadata_v1, scip_documents_v1, scip_occurrences_v1, scip_symbol_info_v1, scip_symbol_relationships_v1, scip_external_symbol_info_v1, scip_diagnostics_v1 |
| LibCST | `libcst_files_v1` | py_cst_parse_manifest_v1, py_cst_parse_errors_v1, py_cst_name_refs_v1, py_cst_imports_v1, py_cst_callsites_v1, py_cst_defs_v1, py_cst_type_exprs_v1 |
| AST | `ast_files_v1` | py_ast_nodes_v1, py_ast_edges_v1, py_ast_errors_v1, ast_defs |
| Tree-sitter | `tree_sitter_files_v1` | ts_nodes_v1, ts_errors_v1, ts_missing_v1 |
| Symtable | `symtable_files_v1` | py_sym_scopes_v1, py_sym_symbols_v1, py_sym_scope_edges_v1, py_sym_namespace_edges_v1, py_sym_function_partitions_v1 |
| Bytecode/Bytemap | `bytecode_files_v1` | py_bc_code_units_v1, py_bc_instructions_v1, py_bc_exception_table_v1, py_bc_blocks_v1, py_bc_cfg_edges_v1, py_bc_errors_v1 |
| CPG graph | `cpg_graph_v1` (optional nested graph table) | cpg_nodes_v1, cpg_edges_v1, cpg_props_v1 |

---

## Scope 1: DataFusion schema registry core

### Target file list
- `src/datafusion_engine/schema_registry.py` (new)
- `src/datafusion_engine/__init__.py` (optional re-export)

### Code pattern
```python
import pyarrow as pa
from datafusion import SessionContext

SCIP_SCHEMA = pa.schema(
    [
        pa.field(
            "documents",
            pa.list_(
                pa.struct(
                    [
                        pa.field("path", pa.string()),
                        pa.field("language", pa.string()),
                        pa.field(
                            "occurrences",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("occ_id", pa.string()),
                                        pa.field("symbol", pa.string()),
                                        pa.field("roles", pa.int32()),
                                        pa.field("range", pa.list_(pa.int32())),
                                        pa.field("attrs", pa.map_(pa.string(), pa.string())),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        )
    ]
)

SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "scip_index_v1": SCIP_SCHEMA,
}


def register_schema(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    arrays = [pa.array([], type=field.type) for field in schema]
    batch = pa.record_batch(arrays, schema=schema)
    ctx.register_record_batches(name, [batch])


def register_all_schemas(ctx: SessionContext) -> None:
    for name, schema in SCHEMA_REGISTRY.items():
        register_schema(ctx, name, schema)
```

### Implementation checklist
- [ ] Add `schema_registry.py` with explicit `pa.schema(...)` constants.
- [ ] Provide `register_schema()` and `register_all_schemas()`.
- [ ] Keep field ordering stable for deterministic SQL.

---

## Scope 2: Define nested table contracts in registries

### Target file list
- `src/extract/registry_rows.py`
- `src/extract/registry_fields.py` (reduce to metadata, remove canonical schema role)
- `src/extract/registry_templates.py`
- `src/normalize/registry_rows.py`
- `src/cpg/registry_rows.py`
- `src/cpg/registry_tables.py`

### Code pattern
```python
DATASET_ROWS = (
    DatasetRow(
        name="scip_index_v1",
        template="scip",
        version=1,
        schema_name="scip_index_v1",
        fields=None,
        row_fields=None,
    ),
)
```

### Implementation checklist
- [ ] Add new dataset rows for each nested table target.
- [ ] Deprecate flattened dataset rows (remove from defaults).
- [ ] Preserve dataset metadata (stage, determinism, evidence) while removing schema coupling.

---

## Scope 3: Emit nested tables directly in extract pipelines

### Target file list
- `src/extract/scip_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/ast_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/helpers.py` (shared struct builders)

### Code pattern
```python
def _build_scip_index_table(
    documents: list[dict[str, object]],
) -> pa.Table:
    rows = [{"documents": documents}]
    return pa.Table.from_pylist(rows, schema=SCIP_SCHEMA)
```

### Implementation checklist
- [ ] Refactor each extractor to produce one nested row per file (or per repo) in a single table.
- [ ] Use struct/list/map columns for nodes, edges, spans, attributes.
- [ ] Keep schema enforcement via `pa.Table.from_pylist(..., schema=SCHEMA)`.

---

## Scope 4: Delta persistence for nested tables

### Target file list
- `src/storage/deltalake/*` (registry writers)
- `src/datafusion_engine/registry_loader.py`

### Code pattern
```python
write_deltalake(path, table, mode="overwrite", schema_mode="merge")
```

### Implementation checklist
- [ ] Persist nested tables to Delta unchanged (no view-based reshaping).
- [ ] Register nested tables in DataFusion by name only (no derived views).
- [ ] Ensure nested schemas round-trip through Delta metadata.

---

## Scope 5: On-demand query helpers (no registered views)

### Target file list
- `src/datafusion_engine/query_fragments.py` (new)
- `src/cpg/relationship_plans.py` (use fragments)
- `src/extract/evidence_plan.py` (use fragments)

### Code pattern
```python
def scip_occurrences_sql(table: str = "scip_index_v1") -> str:
    return f"""
    SELECT
      doc['path'] AS path,
      occ['occ_id'] AS occ_id,
      occ['symbol'] AS symbol,
      occ['roles'] AS roles,
      occ['range'] AS span,
      occ['attrs'] AS attrs
    FROM {table}
    CROSS JOIN unnest({table}.documents) AS doc
    CROSS JOIN unnest(doc['occurrences']) AS occ
    """
```

### Implementation checklist
- [ ] Add SQL fragment builders for each domain (SCIP, CST, AST, tree-sitter, symtable, bytecode).
- [ ] Use nested field access, `unnest`, `named_struct`, `map`, `array_agg` per `datafusion_schema.md`.
- [ ] Do not register views; return SQL for inline use.

---

## Scope 6: CPG build with minimal views (nodes/edges only)

### Target file list
- `src/cpg/relationship_plans.py`
- `src/datafusion_engine/registry_loader.py` (optional helper)

### Code pattern
```python
def register_cpg_views(ctx: SessionContext) -> None:
    ctx.sql("CREATE OR REPLACE VIEW cpg_nodes AS " + cpg_nodes_sql())
    ctx.sql("CREATE OR REPLACE VIEW cpg_edges AS " + cpg_edges_sql())
```

### Implementation checklist
- [ ] Build `cpg_nodes_sql()` and `cpg_edges_sql()` from nested sources.
- [ ] Register only these two views when a consumer needs table names.
- [ ] Prefer direct `ctx.sql(...)` for single-shot queries to avoid view proliferation.

---

## Scope 7: Replace schema sources across extract/normalize/cpg

### Target file list
- `src/extract/*.py`
- `src/normalize/*.py`
- `src/cpg/*.py`
- `src/schema_spec/*` (deprecate as canonical schema source)

### Code pattern
```python
from datafusion_engine.schema_registry import schema_for

SCIP_SCHEMA = schema_for("scip_index_v1")
```

### Implementation checklist
- [ ] Replace schema lookups with DataFusion schema registry.
- [ ] Remove dependency on `schema_spec` for structural schema.
- [ ] Keep only metadata helpers where required by policy/evidence.

---

## Scope 8: Validation and regression checks

### Target file list
- `tests/unit/test_datafusion_schema_registry.py` (new)
- `tests/unit/test_nested_table_roundtrip.py` (new)
- `tests/unit/test_cpg_nodes_edges_sql.py` (new)

### Code pattern
```python
def test_scip_index_schema_roundtrip() -> None:
    ctx = SessionContext()
    register_registry_exports(ctx, "build", targets=("extract",))
    assert ctx.table("scip_index_v1").schema() == SCIP_SCHEMA
```

### Implementation checklist
- [ ] Validate nested tables match canonical schemas.
- [ ] Verify query fragments run without view registration.
- [ ] Ensure only cpg_nodes/cpg_edges are registered views when needed.

---

## Execution sequencing
1) Add schema registry and nested dataset rows.
2) Refactor extract pipelines to emit nested tables.
3) Update Delta writers and DataFusion loader for nested tables.
4) Implement on-demand SQL fragments and CPG nodes/edges views.
5) Replace schema usage and remove flattened table references.
6) Add tests to lock schema and query behavior.
