# Symtable Best-in-Class Extraction Plan (Design-Phase)

## Goals
- Capture full Python 3.13 symtable semantics (scopes, symbols, namespaces, typing scopes).
- Emit stable identifiers and byte-anchored spans for durable joins and graph nodes.
- Keep all symtable <-> AST/CST binding logic inside `src/cpg/`.
- Provide DataFusion-native nested schemas plus normalized views for query ergonomics.
- Add validation, diagnostics, and performance hardening.

## Non-goals
- Replacing existing AST/CST extraction logic.
- Using `python -m symtable` output as an interchange format.
- Refactoring the CPG model or edge taxonomy.

## Constraints
- Binding between symtable and AST/CST occurs in `src/cpg/` (no binding logic added to
  extractors or normalize modules).

---

## Scope 1: Symtable schema extension (v1) + core extraction
Status: Planned

### Target file list
- `src/extract/symtable_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/extract_templates.py`
- `src/datafusion_engine/extract_builders.py`

### Code pattern
```python
scope_type = tbl.get_type()
scope_type_name = scope_type.name
scope_type_value = int(scope_type.value)
qualpath = build_qualpath(parent_qualpath, tbl.get_name(), ordinal)
scope_id = stable_scope_id(
    file_id=file_ctx.file_id,
    qualpath=qualpath,
    lineno=tbl.get_lineno() or 0,
    scope_type=scope_type_name,
    ordinal=ordinal,
)

block_row = {
    "block_id": int(tbl.get_id()),  # keep local id for within-run linking
    "parent_block_id": parent_local_id,
    "block_type": scope_type_name,
    "name": tbl.get_name(),
    "lineno1": int(tbl.get_lineno() or 0),
    "span_hint": span_hint,
    "scope_id": scope_id,
    "scope_local_id": int(tbl.get_id()),
    "scope_type_value": scope_type_value,
    "qualpath": qualpath,
    "attrs": attrs_map(
        {
            "compile_type": options.compile_type,
            "scope_role": scope_role,
        }
    ),
}
```

### Implementation checklist
- [ ] Extend `symtable_files_v1` nested schema (`SYM_BLOCK_T`/`SYM_SYMBOL_T`) with
  stable ids, `qualpath`, and `scope_type_value` (no new dataset name).
- [ ] Keep `block_id` as the symtable local id; add `scope_id` as a stable field.
- [ ] Persist `compile_type` in file/blocks `attrs` (string) for auditability.
- [ ] Ensure new fields are nullable to keep backward compatibility.
- [ ] Update extraction to fill new fields while keeping v1 output shape.

---

## Scope 2: Namespace edges + function/class partitions
Status: Planned

### Target file list
- `src/extract/symtable_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/cpg/kinds_ultimate.py`
- `src/cpg/registry_fields.py`
- `src/cpg/registry_tables.py`

### Code pattern
```python
if sym.is_namespace():
    namespaces = list(sym.get_namespaces())
    symbol_row["namespace_count"] = len(namespaces)
    symbol_row["namespace_block_ids"] = [int(child.get_id()) for child in namespaces]

if scope_type_name == "FUNCTION":
    block_row["function_partitions"] = {
        "parameters": list(tbl.get_parameters()),
        "locals": list(tbl.get_locals()),
        "globals": list(tbl.get_globals()),
        "nonlocals": list(tbl.get_nonlocals()),
        "frees": list(tbl.get_frees()),
    }

if scope_type_name == "CLASS":
    block_row["class_methods"] = list(tbl.get_methods())
```

### Implementation checklist
- [ ] Extend `SYM_SYMBOL_T` with `namespace_count` + `namespace_block_ids` (list).
- [ ] Extend `SYM_BLOCK_T` with `function_partitions` struct and `class_methods` list.
- [ ] Use `get_namespaces()` only when `is_namespace` is true.
- [ ] Add DataFusion fragment views that explode these new fields from `symtable_files_v1`.
- [ ] Add CPG field specs for `namespace_count`, `methods`, `parameters`, etc.
- [ ] Ensure meta scopes (TYPE_PARAMETERS/TYPE_VARIABLE) flow into partitions when present.

---

## Scope 3: Symtable <-> AST/CST anchoring (CPG layer only)
Status: Planned

### Target file list
- `src/cpg/kinds_ultimate.py`
- `src/cpg/registry_templates.py`
- `src/cpg/registry_tables.py`
- `src/cpg/spec_tables.py`
- `src/cpg/registry_fields.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

### Code pattern
```sql
-- CPG anchor join (kept in src/cpg)
WITH sym_scopes AS (
  SELECT file_id, path, scope_id, scope_name, scope_type, lineno
  FROM symtable_scopes
),
cst_defs AS (
  SELECT file_id, path, def_name, bstart, bend
  FROM cst_defs
)
SELECT
  s.scope_id,
  d.bstart AS span_start_byte,
  d.bend AS span_end_byte,
  CASE
    WHEN s.scope_type IN ('FUNCTION', 'CLASS') AND s.lineno = d.bstart_line1 THEN 0.95
    ELSE 0.7
  END AS anchor_confidence
FROM sym_scopes s
LEFT JOIN cst_defs d
  ON s.path = d.path
 AND s.scope_name = d.def_name
 AND s.lineno = d.bstart_line1;
```

### Implementation checklist
- [ ] Implement anchoring joins in `src/cpg/` (no anchoring in extractors).
- [ ] Prefer AST/CST byte spans; fall back to line-based anchors with lower confidence.
- [ ] Add `anchor_confidence` and `anchor_reason` fields to CPG tables.
- [ ] Wire anchor outputs into CPG build module (`cpg_build.py`).
- [ ] Keep ambiguous anchors; track with `ambiguity_group_id`.

---

## Scope 4: Binding inventory + resolution edges (CPG layer)
Status: Planned

### Target file list
- `src/cpg/kinds_ultimate.py`
- `src/cpg/kinds_registry_props.py`
- `src/cpg/registry_fields.py`
- `src/cpg/registry_templates.py`
- `src/cpg/registry_tables.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

### Code pattern
```python
def classify_binding(symbol_row: Mapping[str, object]) -> dict[str, object]:
    is_nonlocal = bool(symbol_row["is_nonlocal"])
    is_global = bool(symbol_row["is_global"] or symbol_row["is_declared_global"])
    is_free = bool(symbol_row["is_free"])
    is_param = bool(symbol_row["is_parameter"])

    if is_nonlocal:
        scoping_class = "NONLOCAL_REF"
    elif is_global:
        scoping_class = "GLOBAL_REF"
    elif is_free:
        scoping_class = "FREE_REF"
    elif is_param:
        scoping_class = "PARAM_LOCAL"
    else:
        scoping_class = "LOCAL"

    return {
        "binding_id": f"{symbol_row['scope_id']}:BIND:{symbol_row['name']}",
        "binding_kind": scoping_class,
        "declared_here": bool(
            symbol_row["is_parameter"]
            or symbol_row["is_assigned"]
            or symbol_row["is_imported"]
            or symbol_row["is_namespace"]
            or symbol_row["is_annotated"]
        ),
    }
```

### Implementation checklist
- [ ] Add CPG binding nodes derived from symtable symbol flags.
- [ ] Emit `BINDING_RESOLVES_TO` edges for GLOBAL/NONLOCAL/FREE with confidence scores.
- [ ] Preserve ambiguity when multiple outer bindings match; use group ids.
- [ ] Update CPG props enums for new `binding_kind` and resolution kinds.
- [ ] Keep binding derivations in `src/cpg/` and wire via `cpg_build.py`.

---

## Scope 5: Cross-validation diagnostics (symtable vs bytecode)
Status: Planned

### Target file list
- `src/cpg/kinds_ultimate.py`
- `src/cpg/registry_tables.py`
- `src/obs/diagnostics_schemas.py`
- `src/obs/diagnostics_tables.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

### Code pattern
```python
errs: list[str] = []
sym_free = set(scope_row["frees"])
code_free = set(code_obj_row["co_freevars"])
if sym_free != code_free:
    errs.append(
        f"freevars mismatch scope_id={scope_row['scope_id']} sym={sorted(sym_free)} "
        f"code={sorted(code_free)}"
    )
```

### Implementation checklist
- [ ] Add a diagnostics dataset for symtable vs bytecode mismatches.
- [ ] Validate `get_frees()` vs `co_freevars` and parameters vs arg counts.
- [ ] Record diagnostics with severity + evidence ids for traceability.
- [ ] Emit diagnostics as CPG nodes/edges for graph visibility.

---

## Scope 6: DataFusion schema + view integration (nested views + attrs expansion)
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/catalog_registry.py`

### Code pattern
```python
def symtable_namespace_edges_sql(table: str = "symtable_files_v1") -> str:
    base = nested_base_sql("symtable_symbols", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      unnest(base.namespace_block_ids) AS child_block_id,
      base.namespace_count AS namespace_count
    FROM base
    """

def symtable_symbol_attrs_sql(table: str = "symtable_files_v1") -> str:
    base = nested_base_sql("symtable_symbols", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      kv['key'] AS attr_key,
      kv['value'] AS attr_value
    FROM base
    CROSS JOIN unnest(map_entries(base.attrs)) AS kv
    """
```

### Implementation checklist
- [ ] Extend `symtable_files_v1` nested schema in `SCHEMA_REGISTRY` (no new dataset name).
- [ ] Add fragment views for the new fields (namespace edges, partitions, methods).
- [ ] Keep existing `symtable_scopes/symbols/scope_edges` views stable.
- [ ] Validate nested types with `arrow_typeof` and record schema snapshots.
- [ ] Document versioned view names and dataset specs in catalog registry.
- [ ] Add attrs expansion views using `map_entries` and `map_extract` for diagnostics.

---

## Scope 6A: Schema hardening + view-type controls (DataFusion settings)
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/runtime_profile.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
SchemaHardeningProfile(
    enable_view_types=False,
    expand_views_at_output=False,
    show_schema_in_explain=True,
    show_types_in_format=True,
    strict_aggregate_schema_check=True,
    timezone="UTC",
)
```

### Implementation checklist
- [ ] Ensure `schema_hardening_name="schema_hardening"` is the default for symtable runs.
- [ ] Decide on `enable_view_types` policy (pin to False for stable Utf8).
- [ ] Add settings overrides for symtable-heavy sessions:
  - `datafusion.execution.collect_statistics`
  - `datafusion.execution.meta_fetch_concurrency`
  - `datafusion.runtime.list_files_cache_ttl`
- [ ] Record schema hardening settings in diagnostics for traceability.

---

## Scope 6B: Schema introspection + validation gates
Status: Planned

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
introspector = SchemaIntrospector(ctx)
describe_rows = introspector.describe_query("SELECT * FROM symtable_scopes")
columns = introspector.table_columns("symtable_scopes")

ctx.sql(
    "SELECT arrow_typeof(blocks) AS blocks_type "
    "FROM symtable_files_v1 LIMIT 1"
).collect()
```

### Implementation checklist
- [ ] Add a schema validation gate that runs `DESCRIBE` on new symtable views.
- [ ] Use `arrow_typeof()` to assert nested field types match the canonical schema.
- [ ] Capture `information_schema.columns` snapshots for symtable tables/views.
- [ ] Fail fast when nested schemas drift (before CPG join steps).

---

## Scope 6C: Listing-table registration with canonical schema + cache tuning
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/listing_table_provider.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
ctx.register_listing_table(
    "symtable_files_v1",
    path=str(symtable_path),
    schema=SYM_TABLE_SCHEMA,
    file_extension=".parquet",
    file_sort_order=[("path", "ascending")],
)
```

### Implementation checklist
- [ ] Register symtable bundles with explicit `schema=SYM_TABLE_SCHEMA`.
- [ ] Set listing-table cache TTL/limit based on workload (append-only vs immutable).
- [ ] Pin `skip_metadata` behavior for parquet metadata conflicts.
- [ ] Use `file_sort_order` hints to improve planning on path-scoped queries.

---

## Scope 7: Performance and incrementalization
Status: Planned

### Target file list
- `src/extract/symtable_extract.py`
- `src/extract/helpers.py`
- `src/hamilton_pipeline/modules/extraction.py`

### Code pattern
```python
cache_key = (file_ctx.file_id, file_ctx.file_sha256, options.compile_type)
cached = symtable_cache.get(cache_key)
if cached is not None:
    return cached

rows = _extract_symtable_for_context(file_ctx, compile_type=options.compile_type)
symtable_cache[cache_key] = rows
return rows
```

### Implementation checklist
- [ ] Add in-memory cache keyed by `(file_id, file_sha256, compile_type)`.
- [ ] Avoid repeated text decoding and symtable re-parsing for unchanged files.
- [ ] Enable bounded parallelism for per-file extraction (respect memory limits).
- [ ] Keep extractor streaming-safe and avoid large intermediate lists when possible.

---

## Scope 8: Tests + contract harness
Status: Planned

### Target file list
- `tests/unit/test_symtable_extract.py`
- `tests/unit/test_symtable_partitions.py`
- `tests/integration/test_symtable_views.py`
- `tests/_helpers/` (fixtures + test data)

### Code pattern
```python
@pytest.mark.parametrize(
    "code, expected_flags",
    [
        ("x = 1", {"x": {"is_assigned": True, "is_local": True}}),
        ("global x\nx = 1", {"x": {"is_global": True}}),
        ("def f():\n    return x", {"x": {"is_free": True}}),
    ],
)
def test_symbol_flags(code: str, expected_flags: dict[str, dict[str, bool]]) -> None:
    rows = run_symtable_extract(code)
    assert_flags(rows, expected_flags)
```

### Implementation checklist
- [ ] Add fixture programs for TYPE_PARAMETERS/TYPE_ALIAS/TYPE_VARIABLE scopes.
- [ ] Add tests for namespace edges and function partitions.
- [ ] Add view-level tests for DataFusion `symtable_*` fragments.
- [ ] Add CPG binding/anchor tests using real CST spans (no monkeypatching).

---

## Open questions
- Should function partitions/class methods live as structured fields in `SYM_BLOCK_T`, or be
  serialized into `attrs` as JSON for maximum backward compatibility?
- Do we want a dedicated diagnostics dataset for symtable cross-validation or embed in
  existing `DIAG` nodes?
- What is the preferred policy for ambiguous anchors (multiple matching defs)?
