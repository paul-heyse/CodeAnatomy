# DataFusion Nested Schema Integration Plan

## Objective
Make all 1:1 nested datasets intrinsic to their root complex schemas so schema and SQL wiring no longer depends on scattered, manual mappings. Derived or normalized outputs remain separate and are layered on top of intrinsic nested projections.

## Decisions
- Keep one root complex schema per extractor (libcst, scip, ast, tree_sitter, symtable, bytecode).
- Auto-derive intrinsic nested child schemas and base SQL from the root schema.
- Keep derived columns (hash ids, span conversions, joins) in query fragments, but build them on top of the auto-derived base.
- Do not introduce a new nested schema model class; use plain dicts and helper functions.

## Scope Item 1: Inventory and classify intrinsic vs derived outputs
Create a single index that captures dataset name, root schema, nested path, and whether the dataset
is intrinsic (direct projection) or derived (adds computed columns).

Representative code pattern:
```python
NESTED_DATASET_INDEX: dict[str, dict[str, str]] = {
    "cst_parse_manifest": {"root": "libcst_files_v1", "path": "parse_manifest", "role": "intrinsic"},
    "cst_parse_errors": {"root": "libcst_files_v1", "path": "parse_errors", "role": "intrinsic"},
    "cst_name_refs": {"root": "libcst_files_v1", "path": "name_refs", "role": "intrinsic"},
    "cst_imports": {"root": "libcst_files_v1", "path": "imports", "role": "intrinsic"},
    "cst_callsites": {"root": "libcst_files_v1", "path": "callsites", "role": "intrinsic"},
    "cst_defs": {"root": "libcst_files_v1", "path": "defs", "role": "intrinsic"},
    "cst_type_exprs": {"root": "libcst_files_v1", "path": "type_exprs", "role": "intrinsic"},
    "ast_nodes": {"root": "ast_files_v1", "path": "nodes", "role": "intrinsic"},
    "ast_edges": {"root": "ast_files_v1", "path": "edges", "role": "intrinsic"},
    "ast_errors": {"root": "ast_files_v1", "path": "errors", "role": "intrinsic"},
    "ts_nodes": {"root": "tree_sitter_files_v1", "path": "nodes", "role": "intrinsic"},
    "ts_errors": {"root": "tree_sitter_files_v1", "path": "errors", "role": "intrinsic"},
    "ts_missing": {"root": "tree_sitter_files_v1", "path": "missing", "role": "intrinsic"},
    "symtable_scopes": {"root": "symtable_files_v1", "path": "blocks", "role": "derived"},
    "symtable_symbols": {"root": "symtable_files_v1", "path": "blocks.symbols", "role": "derived"},
    "symtable_scope_edges": {"root": "symtable_files_v1", "path": "blocks", "role": "derived"},
    "scip_metadata": {"root": "scip_index_v1", "path": "metadata", "role": "intrinsic"},
    "scip_documents": {"root": "scip_index_v1", "path": "documents", "role": "intrinsic"},
    "scip_occurrences": {"root": "scip_index_v1", "path": "documents.occurrences", "role": "derived"},
    "scip_symbol_information": {"root": "scip_index_v1", "path": "symbols", "role": "intrinsic"},
    "scip_external_symbol_information": {
        "root": "scip_index_v1",
        "path": "external_symbols",
        "role": "intrinsic",
    },
    "scip_symbol_relationships": {
        "root": "scip_index_v1",
        "path": "symbols.relationships",
        "role": "derived",
    },
    "scip_diagnostics": {
        "root": "scip_index_v1",
        "path": "documents.occurrences.diagnostics",
        "role": "derived",
    },
    "py_bc_code_units": {"root": "bytecode_files_v1", "path": "code_objects", "role": "intrinsic"},
    "py_bc_instructions": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions",
        "role": "derived",
    },
    "py_bc_blocks": {"root": "bytecode_files_v1", "path": "code_objects.blocks", "role": "derived"},
    "py_bc_cfg_edges": {
        "root": "bytecode_files_v1",
        "path": "code_objects.cfg_edges",
        "role": "derived",
    },
    "bytecode_exception_table": {
        "root": "bytecode_files_v1",
        "path": "code_objects.exception_table",
        "role": "derived",
    },
    "bytecode_errors": {"root": "bytecode_files_v1", "path": "errors", "role": "intrinsic"},
}
```

Target file list:
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `docs/plans/datafusion_nested_schema_integration_plan.md`

Implementation checklist:
- [ ] Enumerate all nested list/struct fields per root schema.
- [ ] Tag each dataset as intrinsic or derived overlay.
- [ ] Capture multi-level paths (SCIP occurrences/diagnostics, symtable symbols, bytecode code objects).

## Scope Item 2: Add a nested registry with naming rules
Introduce a minimal registry that preserves existing dataset names and allows multiple outputs per
path (e.g., `symtable_scopes` and `symtable_scope_edges` both derive from `blocks`).

Representative code pattern:
```python
def nested_spec_for(name: str) -> dict[str, str]:
    return NESTED_DATASET_INDEX[name]


def nested_dataset_names() -> tuple[str, ...]:
    return tuple(sorted(NESTED_DATASET_INDEX))


def datasets_for_path(root: str, path: str) -> tuple[str, ...]:
    return tuple(
        name
        for name, spec in NESTED_DATASET_INDEX.items()
        if spec["root"] == root and spec["path"] == path
    )
```

Target file list:
- `src/datafusion_engine/schema_registry.py` or `src/datafusion_engine/nested_registry.py`

Implementation checklist:
- [ ] Preserve existing dataset names from `query_fragments.py` and pipeline call sites.
- [ ] Provide lookup helpers for dataset name -> (root, path).
- [ ] Allow multiple dataset names per nested path.

## Scope Item 3: Derive child schemas from root structs
Generate child schemas by projecting the nested struct fields and inheriting identity columns.

Representative code pattern:
```python
def nested_schema_for(name: str) -> pa.Schema:
    root, path = nested_path_for(name)
    root_schema = schema_for(root)
    row_struct = struct_for_path(root_schema, path)
    identity = identity_fields_for(name, root_schema, row_struct)
    base_fields = [pa.field(field, root_schema.field(field).type) for field in identity]
    row_fields = [pa.field(field.name, field.type, field.nullable) for field in row_struct]
    return pa.schema([*base_fields, *row_fields])
```

Target file list:
- `src/datafusion_engine/schema_registry.py` or `src/datafusion_engine/nested_registry.py`
- `src/normalize/registry_specs.py` (schema resolution)

Implementation checklist:
- [ ] Implement `struct_for_path` to traverse list<struct> paths.
- [ ] Define identity columns per dataset (e.g., file_id/path for file-backed roots, index_id/path for SCIP).
- [ ] Make `schema_for` resolve nested names via `nested_schema_for`.

## Scope Item 4: Build base SQL for intrinsic nested datasets
Create a single `nested_base_sql` builder to unnest the root path and project base columns.

Representative code pattern:
```python
def nested_base_sql(name: str, *, table: str | None = None) -> str:
    root, path = nested_path_for(name)
    root_alias = "root"
    sql = f"FROM {table or root} AS {root_alias}"
    current = root_alias
    for idx, step in enumerate(path.split(".")):
        if is_list_path(root, step, parent=current):
            alias = f"n{idx}"
            sql += f"\nCROSS JOIN unnest({current}.{step}) AS {alias}"
            current = alias
        else:
            current = f"{current}.{step}"
    identity = identity_fields_for(name, schema_for(root), struct_for_path(schema_for(root), path))
    identity_select = ", ".join(f\"{root_alias}.{col} AS {col}\" for col in identity)
    row_select = \", \".join(f\"{current}['{field}'] AS {field}\" for field in struct_field_names(root, path))
    return f\"SELECT {identity_select}, {row_select}\\n{sql}\"
```

Target file list:
- `src/datafusion_engine/schema_registry.py` or `src/datafusion_engine/nested_registry.py`
- `src/datafusion_engine/query_fragments.py`

Implementation checklist:
- [ ] Generate base SQL for list paths and struct-only paths (e.g., `scip_metadata`).
- [ ] Support multi-level list paths (documents.occurrences, blocks.symbols, code_objects.*).
- [ ] Prefer identity columns derived from the registry when row structs lack them.

## Scope Item 5: Refactor query fragments to layer on base SQL
Keep derived columns (hash ids, span conversions) but express them in terms of the base nested SQL.

Representative code pattern:
```python
def libcst_imports_sql(table: str = "libcst_files_v1") -> str:
    base = nested_base_sql("cst_imports", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id,
      base.path,
      base.file_sha256,
      base.kind,
      base.module,
      base.relative_level,
      base.name,
      base.asname,
      base.is_star,
      base.stmt_bstart,
      base.stmt_bend,
      base.alias_bstart,
      base.alias_bend,
      {_hash_expr("cst_import", "base.file_id", "base.kind", "base.alias_bstart", "base.alias_bend")} AS import_id
    FROM base
    """
```

Target file list:
- `src/datafusion_engine/query_fragments.py`

Implementation checklist:
- [ ] Replace direct unnest SQL with base SQL CTEs.
- [ ] Keep existing derived columns and hashes intact.
- [ ] Confirm no dataset name drift (names remain identical at call sites).

## Scope Item 6: Integrate nested schema lookup across the code base
Ensure all schema lookups and checks can resolve nested datasets without a static mapping list.

Representative code pattern:
```python
def schema_for(name: str) -> pa.Schema:
    if name in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[name]
    if is_nested_dataset(name):
        return nested_schema_for(name)
    raise KeyError(f"Unknown DataFusion schema: {name!r}.")
```

Target file list:
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/registry_specs.py`
- `src/extract/*_extract.py` (schema resolution paths)

Implementation checklist:
- [ ] Route nested dataset names through the nested registry.
- [ ] Keep root registration behavior unchanged.
- [ ] Update any schema_name listings to include derived nested names.
- [ ] Preserve `register_nested_table` usage for root table registration prior to fragment execution.

## Scope Item 7: Tests and verification
Add unit tests to validate schema derivation and base SQL generation for representative datasets.

Representative code pattern:
```python
def test_nested_schema_for_tree_sitter_nodes() -> None:
    schema = nested_schema_for("ts_nodes")
    assert "ts_node_id" in schema.names
    assert "file_id" in schema.names


def test_nested_sql_for_scip_documents() -> None:
    sql = nested_base_sql("scip_documents")
    assert "CROSS JOIN unnest" in sql
    assert "documents" in sql


def test_nested_sql_for_scip_metadata() -> None:
    sql = nested_base_sql("scip_metadata")
    assert "metadata" in sql
```

Target file list:
- `tests/unit/test_datafusion_nested_registry.py`

Implementation checklist:
- [ ] Validate nested schema field presence and ordering.
- [ ] Validate SQL contains expected unnest paths.
- [ ] Add one multi-level path test (scip occurrences or symtable symbols).
- [ ] Add one struct-only path test (scip metadata).

## Scope Item 8: Catalog + schema provider integration
Extend the existing runtime schema registry install path so nested datasets are registered alongside
root schemas when `enable_schema_registry` is enabled.

Representative code pattern:
```python
def register_nested_schemas(ctx: SessionContext) -> None:
    for name in nested_dataset_names():
        register_schema(ctx, name, nested_schema_for(name))


def register_all_schemas(ctx: SessionContext) -> None:
    _register_root_schemas(ctx)
    register_nested_schemas(ctx)
```

Target file list:
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

Implementation checklist:
- [ ] Extend `register_all_schemas` to register nested dataset schemas.
- [ ] Wire nested registration into `DataFusionRuntimeProfile._install_schema_registry`.
- [ ] Rely on existing `enable_information_schema` for introspection.

## Scope Item 9: Schema metadata + type introspection guardrails
Use Arrow metadata and DataFusion `arrow_typeof`/`arrow_cast` to enforce schema consistency and detect
drift in nested projections.

Representative code pattern:
```python
def validate_nested_types(ctx: SessionContext, name: str) -> None:
    sql = f\"SELECT arrow_typeof(*) AS row_type FROM {name} LIMIT 1\"
    ctx.sql(sql).collect()


def validate_schema_metadata(schema: pa.Schema) -> None:
    meta = schema.metadata or {}
    _ = meta.get(SCHEMA_META_NAME)
    _ = meta.get(SCHEMA_META_VERSION)
```

Target file list:
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/query_fragments.py`
- `src/arrowdsl/core/schema_constants.py`

Implementation checklist:
- [ ] Encode schema name/version metadata on root schemas using `SCHEMA_META_NAME`/`SCHEMA_META_VERSION`.
- [ ] Add a type-check hook for nested datasets in debug/diagnostic runs.
- [ ] Use `arrow_cast` in fragments when types can drift (e.g., map attr values).

## Scope Item 10: External table DDL for root schemas
Register root nested schemas using explicit `CREATE EXTERNAL TABLE` statements to codify partitions,
ordering, and unbounded semantics where applicable.

Representative code pattern:
```python
def register_root_external_table(
    ctx: SessionContext,
    spec: DatasetSpec,
    *,
    location: DatasetLocation,
) -> None:
    ddl = spec.table_spec.to_create_external_table_sql(
        ExternalTableConfig(
            location=str(location.path),
            file_format="PARQUET",
            partitioned_by=spec.table_spec.key_fields,
            dialect="datafusion",
        ),
    )
    ctx.sql(ddl).collect()
```

Target file list:
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`

Implementation checklist:
- [ ] Translate ordering/partitioning metadata into DDL.
- [ ] Add optional unbounded registration for streaming-safe sources.
- [ ] Use dataset locations from the registry/manifest, not table names.
- [ ] Preserve current registration paths for in-memory tables.

## Scope Item 11: Structured construction utilities for nested outputs
Use `named_struct`, `struct`, and map helpers in SQL fragments to standardize nested output
construction and reduce ad-hoc JSON/string encodings.

Representative code pattern:
```python
def build_span_struct(prefix: str) -> str:
    return (
        \"named_struct('start', named_struct('line0', {p}.start_line, 'col', {p}.start_col), \"
        \"'end', named_struct('line0', {p}.end_line, 'col', {p}.end_col))\".format(p=prefix)
    )
```

Target file list:
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/nested_registry.py` (if introduced)

Implementation checklist:
- [ ] Add helper functions to build struct/map expressions in SQL fragments.
- [ ] Refactor fragments that currently inline nested struct access.
- [ ] Keep derived ids and hashes unchanged.

## Scope Item 12: Introspection-based validation for nested datasets
Use `information_schema` to validate that derived nested datasets resolve correctly.

Representative code pattern:
```python
def assert_nested_dataset_registered(ctx: SessionContext, name: str) -> None:
    rows = ctx.sql(\"SELECT table_name FROM information_schema.tables\").collect()
    if name not in {row[0].as_py() for row in rows}:
        raise ValueError(f\"Missing nested dataset: {name}\")
```

Target file list:
- `src/datafusion_engine/runtime.py`
- `src/hamilton_pipeline/modules/outputs.py`

Implementation checklist:
- [ ] Add a diagnostic step that enumerates nested datasets.
- [ ] Emit a single report table with missing or mismatched schemas.
- [ ] Wire diagnostics into existing output reporting.

## Deliverables
- Central nested dataset index aligned with `query_fragments.py`.
- Nested schema derivation helpers and identity column resolution.
- Base SQL builder for intrinsic nested datasets.
- Query fragments refactored to use base SQL.
- Schema resolution integrated for nested dataset names and runtime registration.
- Unit tests covering schema derivation and SQL generation.
