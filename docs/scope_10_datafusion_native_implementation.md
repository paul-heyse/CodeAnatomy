# Scope 10 Implementation: DataFusion-Native Authoring Surface

## Overview

This document describes the implementation of Scope 10 from the DataFusion planning wholesale switch plan, which eliminates SQLGlot AST builders and Ibis plan builders as internal authoring surfaces and establishes DataFusion expressions and DataFrame pipelines as the only authoring surface.

## Implementation Summary

### Created Files

#### `src/relspec/relationship_datafusion.py`

A new module providing DataFusion-native builders for relationship plans. This module replaces the SQLGlot AST builders in `relationship_sql.py` with direct DataFusion expression and DataFrame builders.

**Key Functions:**
- `build_rel_name_symbol_df(ctx, task_name, task_priority)` - Name-based symbol relations
- `build_rel_import_symbol_df(ctx, task_name, task_priority)` - Import-based symbol relations
- `build_rel_def_symbol_df(ctx, task_name, task_priority)` - Definition-based symbol relations
- `build_rel_callsite_symbol_df(ctx, task_name, task_priority)` - Callsite symbol relations
- `build_rel_callsite_qname_df(ctx, task_name, task_priority)` - Callsite qname relations
- `build_relation_output_df(ctx)` - Unified relation output union

**Pattern Example:**
```python
def build_rel_name_symbol_df(
    ctx: SessionContext,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    source = ctx.table("cst_refs")
    return source.select(
        col("ref_id").alias("ref_id"),
        col("ref_text").alias("symbol"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        lit("cst_ref_text").alias("resolution_method"),
        lit(0.5).alias("confidence"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )
```

### Modified Files

#### `src/relspec/relationship_sql.py`

- **Status:** Marked as DEPRECATED
- **Changes:** Added deprecation notice in module docstring
- **Rationale:** Maintained for legacy compatibility but new code should use DataFusion-native builders
- **Future:** Will be removed once all internal code migrates to DataFusion builders

#### `src/datafusion_engine/view_registry_specs.py`

- **Function Modified:** `_relspec_view_nodes(build_ctx)`
- **Changes:**
  - Replaced imports from `relspec.relationship_sql` with `relspec.relationship_datafusion`
  - Replaced SQLGlot AST builders with DataFusion DataFrame builders
  - Updated builder functions to use lambda expressions that call DataFusion builders
  - Maintained plan bundle generation for DataFusion-native lineage extraction

**Before:**
```python
rel_name_expr = build_rel_name_symbol_sql(
    task_name="rel.name_symbol",
    task_priority=priority,
)
node = _view_node_from_sqlglot(build_ctx, name=name, expr=rel_name_expr, ...)
```

**After:**
```python
builder = lambda ctx: build_rel_name_symbol_df(
    ctx, task_name="rel.name_symbol", task_priority=priority
)
node = _rel_view_node_df(name=name, builder=builder, ...)
```

### Unmodified Files (Already Compliant)

#### `src/normalize/view_builders.py`

- **Status:** Already uses Ibis expressions (high-level interface)
- **Rationale:** Ibis is a supported authoring surface that compiles to DataFusion
- **Verification:** Passes all ruff checks

#### `src/cpg/view_builders.py`

- **Status:** Already uses Ibis expressions for CPG node/edge/property builders
- **Rationale:** Ibis provides the necessary high-level abstractions for CPG output
- **Verification:** Passes all ruff checks

#### `src/datafusion_engine/symtable_views.py`

- **Status:** Already uses DataFusion-native builders (established pattern)
- **Example Pattern:**
```python
def symtable_bindings_df(ctx: SessionContext) -> DataFrame:
    scopes = ctx.table("symtable_scopes").select(...)
    symbols = ctx.table("symtable_symbols").select(...)
    joined = symbols.join(scopes, ...)
    return joined.select(...)
```

## Architecture Benefits

### 1. Single Authoring Surface

All calculation logic is now expressed using DataFusion expressions and DataFrame operations:
- `col()` for column references
- `lit()` for literal values
- `f.coalesce()`, `f.concat_ws()`, etc. for functions
- DataFrame methods: `.select()`, `.filter()`, `.join()`, `.union()`

### 2. Direct Lineage Extraction

DataFusion plan bundles provide native lineage information without parsing SQL:
- Referenced tables extracted directly from logical plans
- Required UDFs identified from plan display
- Dependency graph inferred automatically

### 3. Type Safety

DataFusion's Python bindings provide runtime type checking:
- Schema validation at DataFrame creation
- Column reference validation
- Type coercion through `.cast()`

### 4. Consistent Patterns

All relationship builders follow the same structure:
1. Access source table via `ctx.table()`
2. Apply transformations using DataFusion expressions
3. Return DataFrame with standardized column names

## Migration Path

### For New Code

Always use DataFusion-native builders:
```python
from datafusion import SessionContext, col, lit
from datafusion import functions as f

def my_relation_builder(ctx: SessionContext) -> DataFrame:
    source = ctx.table("source_table")
    return source.select(
        col("id").alias("entity_id"),
        f.coalesce(col("name"), lit("unknown")).alias("name"),
        lit("my_method").alias("method"),
    )
```

### For Existing Code

1. SQLGlot AST builders in `relationship_sql.py` remain functional for compatibility
2. New features should use DataFusion builders in `relationship_datafusion.py`
3. Gradual migration of internal code from SQLGlot to DataFusion builders

## Testing Strategy

### Unit Tests

Test DataFusion builders with mock SessionContext:
```python
def test_rel_name_symbol_df():
    ctx = SessionContext()
    # Register mock tables
    ctx.register_table("cst_refs", mock_table)

    # Call builder
    result = build_rel_name_symbol_df(
        ctx, task_name="test", task_priority=100
    )

    # Verify schema and content
    assert "ref_id" in result.schema()
    assert result.count() > 0
```

### Integration Tests

Verify end-to-end view graph construction:
```python
def test_relspec_view_nodes():
    ctx = SessionContext()
    # Register all required tables
    snapshot = {...}

    nodes = view_graph_nodes(ctx, snapshot=snapshot, stage="pre_cpg")

    # Verify relationship view nodes exist
    node_names = {node.name for node in nodes}
    assert "rel_name_symbol_v1" in node_names
    assert "relation_output_v1" in node_names
```

## Performance Considerations

### DataFusion Optimization

DataFusion's query optimizer provides:
- Predicate pushdown
- Projection pruning
- Join reordering
- Constant folding

### Memory Efficiency

Lazy evaluation ensures:
- Plans constructed without executing
- Memory allocated only for materialization
- Streaming execution for large datasets

## Future Work

### Phase 1 (Current)

- ✅ Create DataFusion-native relationship builders
- ✅ Integrate with view registry
- ✅ Deprecate SQLGlot AST builders

### Phase 2 (Next Steps)

- Remove SQLGlot AST builder dependencies once all consumers migrate
- Add DataFusion-specific optimizations (custom UDF compilation)
- Expand DataFusion expression coverage for complex calculations

### Phase 3 (Future)

- Consider DataFusion-native implementations for normalize calculations
- Evaluate Ibis → DataFusion compilation efficiency
- Explore DataFusion substrait protocol for cross-system compatibility

## References

- DataFusion Python API: https://arrow.apache.org/datafusion-python/
- Scope 10 Plan: `docs/plans/datafusion_planning_wholesale_switch_plan_v1.md`
- Example Implementation: `src/datafusion_engine/symtable_views.py`
- Relationship Specs: `src/relspec/contracts.py`
