# DataFusion Nested Schema Integration Plan

## Objective
Make all 1:1 nested datasets intrinsic to their root complex schemas so schema and SQL wiring no longer depends on scattered, manual mappings. Derived or normalized outputs remain separate and are layered on top of intrinsic nested projections.

## Decisions
- Keep one root complex schema per extractor (libcst, scip, ast, tree_sitter, symtable, bytecode).
- Auto-derive intrinsic nested child schemas and base SQL from the root schema.
- Keep derived columns (hash ids, span conversions, joins) in query fragments, but build them on top of the auto-derived base.
- Do not introduce a new nested schema model class; use plain dicts and helper functions.

## Scope Item 1: Inventory and classify intrinsic vs derived outputs
Status: Completed
Create a single index that captures dataset name, root schema, nested path, and whether the dataset
is intrinsic (direct projection) or derived (adds computed columns).

Representative code pattern:
```python
NESTED_DATASET_INDEX: dict[str, dict[str, object]] = {
    "cst_nodes": {
        "root": "libcst_files_v1",
        "path": "nodes",
        "role": "intrinsic",
        "context": {},
    },
    "cst_edges": {
        "root": "libcst_files_v1",
        "path": "edges",
        "role": "intrinsic",
        "context": {},
    },
    "cst_parse_manifest": {
        "root": "libcst_files_v1",
        "path": "parse_manifest",
        "role": "intrinsic",
        "context": {},
    },
    "cst_parse_errors": {
        "root": "libcst_files_v1",
        "path": "parse_errors",
        "role": "derived",
        "context": {},
    },
    "cst_name_refs": {
        "root": "libcst_files_v1",
        "path": "name_refs",
        "role": "derived",
        "context": {},
    },
    "cst_imports": {
        "root": "libcst_files_v1",
        "path": "imports",
        "role": "derived",
        "context": {},
    },
    "cst_callsites": {
        "root": "libcst_files_v1",
        "path": "callsites",
        "role": "derived",
        "context": {},
    },
    "cst_defs": {
        "root": "libcst_files_v1",
        "path": "defs",
        "role": "derived",
        "context": {},
    },
    "cst_type_exprs": {
        "root": "libcst_files_v1",
        "path": "type_exprs",
        "role": "derived",
        "context": {},
    },
    "ast_nodes": {"root": "ast_files_v1", "path": "nodes", "role": "derived", "context": {}},
    "ast_edges": {"root": "ast_files_v1", "path": "edges", "role": "derived", "context": {}},
    "ast_errors": {"root": "ast_files_v1", "path": "errors", "role": "derived", "context": {}},
    "ts_nodes": {"root": "tree_sitter_files_v1", "path": "nodes", "role": "derived", "context": {}},
    "ts_errors": {
        "root": "tree_sitter_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
    "ts_missing": {
        "root": "tree_sitter_files_v1",
        "path": "missing",
        "role": "derived",
        "context": {},
    },
    "symtable_scopes": {
        "root": "symtable_files_v1",
        "path": "blocks",
        "role": "derived",
        "context": {},
    },
    "symtable_symbols": {
        "root": "symtable_files_v1",
        "path": "blocks.symbols",
        "role": "derived",
        "context": {"block_id": "blocks.block_id"},
    },
    "symtable_scope_edges": {
        "root": "symtable_files_v1",
        "path": "blocks",
        "role": "derived",
        "context": {},
    },
    "scip_metadata": {"root": "scip_index_v1", "path": "metadata", "role": "derived", "context": {}},
    "scip_documents": {
        "root": "scip_index_v1",
        "path": "documents",
        "role": "derived",
        "context": {},
    },
    "scip_occurrences": {
        "root": "scip_index_v1",
        "path": "documents.occurrences",
        "role": "derived",
        "context": {"relative_path": "documents.relative_path"},
    },
    "scip_symbol_information": {
        "root": "scip_index_v1",
        "path": "symbols",
        "role": "derived",
        "context": {},
    },
    "scip_external_symbol_information": {
        "root": "scip_index_v1",
        "path": "external_symbols",
        "role": "derived",
        "context": {},
    },
    "scip_symbol_relationships": {
        "root": "scip_index_v1",
        "path": "symbols.relationships",
        "role": "derived",
        "context": {"parent_symbol": "symbols.symbol"},
    },
    "scip_diagnostics": {
        "root": "scip_index_v1",
        "path": "documents.occurrences.diagnostics",
        "role": "derived",
        "context": {
            "relative_path": "documents.relative_path",
            "occ_range": "documents.occurrences.range",
        },
    },
    "py_bc_code_units": {
        "root": "bytecode_files_v1",
        "path": "code_objects",
        "role": "derived",
        "context": {},
    },
    "py_bc_instructions": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "py_bc_blocks": {
        "root": "bytecode_files_v1",
        "path": "code_objects.blocks",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "py_bc_cfg_edges": {
        "root": "bytecode_files_v1",
        "path": "code_objects.cfg_edges",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "bytecode_exception_table": {
        "root": "bytecode_files_v1",
        "path": "code_objects.exception_table",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "bytecode_errors": {
        "root": "bytecode_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
}
```

Target file list:
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `docs/plans/datafusion_nested_schema_integration_plan.md`

Implementation checklist:
- [x] Enumerate all nested list/struct fields per root schema.
- [x] Tag each dataset as intrinsic or derived overlay.
- [x] Capture multi-level paths (SCIP occurrences/diagnostics, symtable symbols, bytecode code objects).

## Scope Item 2: Add a nested registry with naming rules
Status: Completed
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
        sorted(
            name
            for name, spec in NESTED_DATASET_INDEX.items()
            if spec["root"] == root and spec["path"] == path
        )
    )
```

Target file list:
- `src/datafusion_engine/schema_registry.py` or `src/datafusion_engine/nested_registry.py`

Implementation checklist:
- [x] Preserve existing dataset names from `query_fragments.py` and pipeline call sites.
- [x] Provide lookup helpers for dataset name -> (root, path).
- [x] Allow multiple dataset names per nested path.
- [x] Add a reverse lookup helper for root/path -> dataset names.

## Scope Item 3: Derive child schemas from root structs
Status: Completed
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
- [x] Implement `struct_for_path` to traverse list<struct> paths.
- [x] Define identity columns per dataset (e.g., file_id/path for file-backed roots, index_id/path for SCIP).
- [x] Make `schema_for` resolve nested names via `nested_schema_for`.
- [x] Expand intrinsic dataset coverage for nested schema registration beyond `cst_parse_manifest`.

## Scope Item 4: Build base SQL for intrinsic nested datasets
Status: Completed
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
- [x] Generate base SQL for list paths and struct-only paths (e.g., `scip_metadata`).
- [x] Support multi-level list paths (documents.occurrences, blocks.symbols, code_objects.*).
- [x] Prefer identity columns derived from the registry when row structs lack them.

## Scope Item 5: Refactor query fragments to layer on base SQL
Status: Completed
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
- [x] Replace direct unnest SQL with base SQL CTEs.
- [x] Keep existing derived columns and hashes intact.
- [x] Confirm no dataset name drift (names remain identical at call sites).

## Scope Item 6: Integrate nested schema lookup across the code base
Status: Completed
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
- [x] Route nested dataset names through the nested registry.
- [x] Keep root registration behavior unchanged.
- [x] Update schema name listings to include intrinsic nested names.
- [x] Preserve `register_nested_table` usage for root table registration prior to fragment execution.

## Scope Item 7: Tests and verification
Status: Completed
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
- [x] Validate nested schema field presence and ordering.
- [x] Validate SQL contains expected unnest paths.
- [x] Add one multi-level path test (scip occurrences or symtable symbols).
- [x] Add one struct-only path test (scip metadata).

## Scope Item 8: Catalog + schema provider integration
Status: Completed
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
- [x] Extend `register_all_schemas` to register intrinsic nested dataset schemas.
- [x] Wire nested registration into `DataFusionRuntimeProfile._install_schema_registry`.
- [x] Rely on existing `enable_information_schema` for introspection.

## Scope Item 9: Schema metadata + type introspection guardrails
Status: Completed (metadata + arrow_cast guardrails added; may expand per new drift cases)
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
- [x] Encode schema name/version metadata on root schemas using `SCHEMA_META_NAME`/`SCHEMA_META_VERSION`.
- [x] Add a type-check hook for nested datasets in debug/diagnostic runs.
- [x] Use `arrow_cast` in fragments where types can drift (map attr values in bytecode).

## Scope Item 10: External table DDL for root schemas
Status: Completed
Register root nested schemas using explicit `CREATE EXTERNAL TABLE` statements to codify partitions,
ordering, and unbounded semantics where applicable.

Representative code pattern:
```python
def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> DataFrame:
    ddl = datafusion_external_table_sql(
        name=name,
        location=location,
        runtime_profile=runtime_profile,
    )
    if ddl is not None and name in SCHEMA_REGISTRY:
        ctx.sql(ddl).collect()
        return ctx.table(name)
    return _fallback_registration(ctx, name=name, location=location)
```

Target file list:
- `src/datafusion_engine/registry_bridge.py`

Implementation checklist:
- [x] Translate ordering/partitioning metadata into DDL.
- [x] Add optional unbounded registration for streaming-safe sources.
- [x] Use dataset locations from the registry/manifest, not table names.
- [x] Preserve current registration paths for in-memory tables.

## Scope Item 11: Structured construction utilities for nested outputs
Status: Completed
Use `named_struct`, `struct`, and map helpers in SQL fragments to standardize nested output
construction and reduce ad-hoc JSON/string encodings.

Representative code pattern:
```python
def _named_struct(fields: Sequence[tuple[str, str]]) -> str:
    parts = \", \".join(f\"'{name}', {expr}\" for name, expr in fields)
    return f\"named_struct({parts})\"


def bytecode_instructions_sql(table: str = \"bytecode_files_v1\") -> str:
    base = nested_base_sql(\"py_bc_instructions\", table=table)
    attrs_struct = _named_struct(
        (
            (\"instr_index\", _map_cast(\"base.attrs\", \"instr_index\", \"INT\")),
            (\"argval_str\", _map_value(\"base.attrs\", \"argval_str\")),
            (\"starts_line\", _map_cast(\"base.attrs\", \"starts_line\", \"INT\")),
        )
    )
    return f\"\"\"
    WITH base AS ({base}),
    decorated AS (
      SELECT
        base.*,
        {attrs_struct} AS attrs_struct
      FROM base
    )
    SELECT
      decorated.attrs_struct['instr_index'] AS instr_index,
      decorated.attrs_struct['argval_str'] AS argval_str
    FROM decorated
    \"\"\"
```

Target file list:
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/nested_registry.py` (if introduced)

Implementation checklist:
- [x] Add helper functions to build struct/map expressions in SQL fragments.
- [x] Refactor fragments that currently inline nested struct access.
- [x] Keep derived ids and hashes unchanged.

## Scope Item 12: Introspection-based validation for nested datasets
Status: Completed
Use `information_schema` to validate that derived nested datasets resolve correctly.

Representative code pattern:
```python
def datafusion_schema_registry_validation_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    rows = []
    for record in records:
        for name in record.get(\"missing\", []):
            rows.append(
                {
                    \"schema_name\": name,
                    \"issue_type\": \"missing\",
                }
            )
    return table_from_rows(DATAFUSION_SCHEMA_REGISTRY_VALIDATION_V1, rows)
```

Target file list:
- `src/datafusion_engine/runtime.py`
- `src/hamilton_pipeline/modules/outputs.py`

Implementation checklist:
- [x] Add a diagnostic step that enumerates nested datasets.
- [x] Emit a single report table with missing or mismatched schemas.
- [x] Wire diagnostics into existing output reporting.

## Deliverables
- Central nested dataset index aligned with `query_fragments.py` (LibCST nodes/edges included).
- Nested schema derivation helpers and identity column resolution with intrinsic registrations expanded.
- Base SQL builder for intrinsic nested datasets.
- Query fragments refactored to use base SQL.
- Schema resolution integrated for nested dataset names and runtime registration.
- Unit tests covering schema derivation and SQL generation.
