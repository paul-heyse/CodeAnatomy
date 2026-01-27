# Scope 9 Implementation Summary: SQLGlot and Ibis Deprecation

## Overview

Successfully implemented Scope 9 of the DataFusion planning wholesale switch plan, marking SQLGlot and Ibis surfaces as deprecated throughout the internal architecture. This is a careful deprecation pass that preserves functionality while clearly signaling the migration path to DataFusion-native planning.

## Files Modified

### 1. src/engine/runtime.py
**Changes:**
- Marked `sqlglot_policy: SqlGlotPolicy` field as deprecated with inline comment
- Added deprecation note to `build_engine_runtime()` docstring
- Added deprecation comment to SQLGlot imports

**Rationale:** EngineRuntime bundles SQLGlot policy for legacy compatibility, but the field is no longer used in DataFusion-native execution paths.

### 2. src/datafusion_engine/execution_facade.py
**Changes:**
- Marked `compile()` method as deprecated in docstring
- Added note: "DEPRECATED: SQLGlot/Ibis compilation paths are deprecated. Prefer DataFusion-native builder functions returning DataFrame directly."

**Rationale:** The compile() method accepts SQLGlot/Ibis expressions, but DataFusion-native builder functions (returning DataFrame directly) are the preferred pattern.

### 3. src/datafusion_engine/execution_helpers.py
**Changes:**
- Marked 6 SQLGlot-dependent functions as deprecated:
  - `_sqlglot_emit_policy()` - "Use DataFusion-native planning"
  - `_emit_sql()` - "Use DataFusion-native planning"
  - `_maybe_enforce_preflight()` - "Use DataFusion-native query validation"
  - `_policy_violations()` - "Use DataFusion-native validation"
  - `df_from_sqlglot_or_sql()` - "Use DataFusion-native builder functions"
  - `collect_plan_artifacts()` - "Prefer collecting artifacts directly from DataFusion plan bundles"

**Rationale:** These are internal helpers that operate on SQLGlot ASTs. DataFusion plan bundles provide native lineage and validation without SQL round-tripping.

### 4. src/datafusion_engine/view_graph_registry.py
**Changes:**
- Marked ViewNode fields as deprecated in docstring and inline comments:
  - `sqlglot_ast: Expression | None` - "DEPRECATED: Use plan_bundle for lineage extraction"
  - `ibis_expr: Table | None` - "DEPRECATED: Use plan_bundle for lineage extraction"

**Rationale:** ViewNode historically stored both SQLGlot AST and Ibis expressions for lineage. The `plan_bundle` field (DataFusionPlanBundle) is the preferred source of truth.

### 5. src/relspec/inferred_deps.py
**Changes:**
- Marked `infer_deps_from_sqlglot_expr()` as deprecated with Sphinx-style deprecation directive
- Marked helper functions as deprecated:
  - `_required_udfs_from_ast()` - "Use DataFusion plan bundle"
  - `_required_columns_from_sqlglot()` - "Use DataFusion plan bundle"

**Rationale:** Dependency inference now prefers `infer_deps_from_plan_bundle()` which uses DataFusion-native lineage extraction instead of SQLGlot parsing.

## Deprecation Pattern

All deprecation markers follow a consistent pattern:

```python
def deprecated_function(...):
    """DEPRECATED: Brief description of what's deprecated.

    Preferred alternative: [what to use instead]

    [Original docstring continues...]
    """
```

For dataclass fields:
```python
field_name: Type = default  # DEPRECATED: Reason and alternative
```

## Migration Path

The deprecation markers clearly guide users to:

1. **From SQLGlot/Ibis expressions → DataFusion builder functions**
   - Old: `facade.compile(sqlglot_ast)` or `facade.compile(ibis_expr)`
   - New: Direct DataFrame construction with ctx.table(), .filter(), .select()

2. **From SQLGlot-based lineage → DataFusion plan bundles**
   - Old: `infer_deps_from_sqlglot_expr(expr)`
   - New: `infer_deps_from_plan_bundle(plan_bundle)`

3. **From SQLGlot policy/validation → DataFusion-native validation**
   - Old: SQLGlot AST-based policy enforcement
   - New: DataFusion SessionContext validation

## Code Quality

- All modified files pass `uv run pyright --warnings --pythonversion=3.13` with 0 errors/warnings
- All modified files formatted with `uv run ruff format`
- Linting completed with `uv run ruff check --fix` (minor D401 warnings on deprecated docstrings are acceptable)

## Next Steps (Not in This Scope)

- Scope 17: Wholesale removal of SQLGlot and Ibis packages (deletion of entire modules)
- Scope 10-16: Other DataFusion planning improvements
- Remove deprecated code paths once migration is complete

## Testing Strategy

The deprecation markers do NOT break existing functionality:
- All deprecated functions remain fully operational
- Type signatures unchanged
- Runtime behavior unchanged
- Only docstring/comment additions

Integration tests will continue to pass using the legacy paths until wholesale removal in Scope 17.

## Files NOT Modified (By Design)

- `src/engine/session.py` - Kept ibis_backend field (still used by IbisDatasetRegistry)
- SQLGlot/Ibis package directories - Deletion comes in Scope 17
- Test files - Tests will naturally migrate as implementation changes

## Verification

All changes can be verified with:
```bash
# View deprecation markers
grep -r "DEPRECATED" src/engine/runtime.py src/datafusion_engine/execution_facade.py \
  src/datafusion_engine/execution_helpers.py src/datafusion_engine/view_graph_registry.py \
  src/relspec/inferred_deps.py

# Type check
uv run pyright --warnings --pythonversion=3.13 [modified files]

# Format check
uv run ruff format [modified files]
```

---

**Completion Status:** ✅ Scope 9 fully implemented according to plan specifications.
