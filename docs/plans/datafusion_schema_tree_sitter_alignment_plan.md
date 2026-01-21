# DataFusion Schema Alignment Enhancements Plan

## Goals
- Replace bespoke schema validation and coordination with DataFusion-native introspection.
- Increase robustness by enforcing schema contracts at registration and scan boundaries.
- Improve diagnostics and observability for tree-sitter integration and cross-checks.
- Preserve high performance via schema-aware caches, ordering hints, and pushdowns.

## Non-goals
- Redesigning the tree-sitter extractor output schema.
- Changing coordinate standards away from byte-based spans.
- Replacing LibCST/AST as the primary Python source of truth.

---

## Scope 1: Function inventory and signature validation via information_schema
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
def validate_df_function_signatures(
    ctx: SessionContext,
    *,
    required: dict[str, int],
) -> None:
    rows = (
        ctx.sql(
            "SELECT routine_name, ordinal_position "
            "FROM information_schema.parameters "
            "WHERE routine_name IN ('arrow_typeof', 'arrow_metadata', 'map_entries', 'unnest')"
        )
        .to_arrow_table()
        .to_pylist()
    )
    counts: dict[str, int] = {}
    for row in rows:
        name = str(row.get("routine_name") or "")
        pos = row.get("ordinal_position")
        if name:
            counts[name] = max(counts.get(name, 0), int(pos or 0))
    _validate_required_function_counts(required, counts)
```

### Implementation checklist
- [x] Add a tree-sitter specific required-function inventory keyed to view SQL usage.
- [x] Reuse existing signature-validation helpers instead of bespoke checks.
- [x] Emit diagnostics artifacts when function signatures drift.
- [x] Remove redundant bespoke function checks once information_schema validation passes.

---

## Scope 2: View schema and semantic metadata validation (Span contracts)
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
def _validate_span_semantic_type(ctx: SessionContext, *, table: str, column: str) -> None:
    rows = (
        ctx.sql(
            "SELECT arrow_metadata("
            f"{column}, 'semantic_type') AS semantic_type "
            f"FROM {table} LIMIT 1"
        )
        .to_arrow_table()
        .to_pylist()
    )
    semantic_type = rows[0].get("semantic_type") if rows else None
    if semantic_type != "Span":
        raise ValueError(f"Expected Span semantic type on {table}.{column}.")
```

### Implementation checklist
- [x] Enforce semantic metadata checks for `span` columns in `ts_*` views.
- [x] Validate `DESCRIBE` output for `ts_*` views against registered schema names.
- [x] Add view-level checks for `col_unit`, `line_base`, `end_exclusive` consistency.
- [x] Remove ad-hoc span validation once semantic checks are in place.

---

## Scope 3: Schema-aware diagnostics and plan schema capture
Status: In progress

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`

### Code pattern
```python
payload = {
    "event_time_unix_ms": int(time.time() * 1000),
    "view": "ts_defs",
    "logical_plan_schema": _table_logical_plan(ctx, name="ts_defs", schema=True),
    "df_settings": _df_settings_snapshot(ctx),
}
diagnostics_sink.record_artifact("datafusion_tree_sitter_plan_schema_v1", payload)
```

### Implementation checklist
- [x] Enable `datafusion.explain.show_schema` in runtime profile defaults.
- [x] Capture `display_indent_schema()` for `ts_*` views in diagnostics.
- [x] Record `information_schema.df_settings` to track schema-affecting config.
- [ ] Use collected SQL parser spans in error artifacts when available.

---

## Scope 4: Canonical schema override at registration (reduce drift)
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/extract_registry.py`

### Code pattern
```python
schema = schema_for("tree_sitter_files_v1")
df = ctx.register_listing_table(
    name="tree_sitter_files_v1",
    path=str(location.path),
    schema=schema,
    skip_metadata=True,
    file_sort_order=[("path", "ASC")],
)
```

### Implementation checklist
- [ ] Pass canonical schema overrides for registered extract datasets.
- [ ] Default `skip_metadata=True` for Parquet sources that embed conflicting metadata.
- [ ] Apply `file_sort_order` and partition columns from schema metadata.
- [ ] Remove bespoke schema alignment fixes in view SQL once overrides are stable.

---

## Scope 5: Schema evolution adapters at scan boundary (Rust-backed)
Status: Planned

### Target file list
- `rust/` (new adapter module)
- `src/datafusion_engine/registry_bridge.py`
- `docs/python_library_reference/datafusion_schema.md`

### Code pattern
```rust
let adapter = factory.create(logical_schema.clone(), physical_schema.clone());
let rewritten = adapter.rewrite(physical_expr)?;
```

### Implementation checklist
- [ ] Implement a `PhysicalExprAdapter` policy for missing columns in old files.
- [ ] Fill new fields with defaults (or NULL) at scan time, not in SQL views.
- [ ] Wire adapter factory into file scans for Parquet/listing tables.
- [ ] Document schema evolution rules per dataset (strict, additive, cast).

---

## Scope 6: Type shaping in views (Arrow-first schema control)
Status: Completed

### Target file list
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```sql
SELECT
  arrow_cast(attrs, 'Map(Utf8, Utf8)') AS attrs,
  arrow_typeof(span) AS span_type,
  struct(start_line, start_col, end_line, end_col) AS span_hint
FROM ts_nodes
```

### Implementation checklist
- [x] Add explicit `arrow_cast` where nested type stability is required.
- [x] Use explicit `AS` aliases to stabilize output column names.
- [x] Prefer `struct`/`map_entries` for nested shaping over bespoke JSON encodings.
- [x] Remove redundant post-projection cleanup if Arrow types are stable.

---

## Scope 7: Metadata caches, ordering hints, and adaptive execution
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/compile_options.py`

### Code pattern
```python
config = (
    SessionConfig()
    .set("datafusion.execution.metadata_cache", "true")
    .set("datafusion.execution.statistics_cache", "true")
    .set("datafusion.execution.list_files_cache", "true")
    .set("datafusion.execution.enable_dynamic_filters", "true")
)
```

### Implementation checklist
- [ ] Enable metadata/statistics/list-files caches in the runtime profile.
- [ ] Record cache settings and cache metrics in diagnostics.
- [ ] Use `WITH ORDER` or `file_sort_order` for stable ordering contracts.
- [ ] Enable dynamic filters and TopK pushdown for tree-sitter cross-check queries.

---

## Scope 8: Delta-backed schema contracts for extracts (optional)
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `docs/plans/deltalake_advanced_integration_plan.md`

### Code pattern
```python
dt = DeltaTable(location)
ctx.register_table("tree_sitter_files_v1", dt)
schema = dt.schema().to_pyarrow()
```

### Implementation checklist
- [ ] Register DeltaTable providers when extracts are stored in Delta.
- [ ] Compare Delta log schema vs DataFusion schema fingerprints.
- [ ] Enforce schema-on-write for extract materializations.
- [ ] Use version/time-travel selectors for reproducible schema diagnostics.

---

## Scope 9: Cleanup and bespoke deprecations
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
if legacy_check_enabled:
    warnings.warn("Legacy schema check replaced by information_schema validation.")
```

### Implementation checklist
- [ ] Remove bespoke schema validation paths now covered by information_schema.
- [ ] Consolidate diagnostics into DataFusion artifacts (no duplicate bespoke logs).
- [ ] Ensure all view SQL uses canonical schema types before removing shims.
- [ ] Add regression tests that verify schema alignment after cleanup.
