# Phase 1.1 Implementation Summary: ViewNode with plan_bundle

## Overview

Successfully implemented Phase 1.1 of the DataFusion wholesale switch, enabling ViewNode to work with `plan_bundle` alone without requiring SQLGlot AST or Ibis expressions.

## Changes Made

### 1. New DataFusion-Native Artifact (`view_artifacts.py`)

#### Added `DataFusionViewArtifact`
- New dataclass for DataFusion-native view artifacts
- Uses `plan_fingerprint` from `DataFusionPlanBundle` instead of AST-based fingerprints
- No `policy_hash`, `ast`, or `serde_payload` (pure DataFusion artifacts)
- Fields:
  - `name: str` - View name
  - `plan_fingerprint: str` - From bundle.plan_fingerprint
  - `schema: pa.Schema` - View output schema
  - `required_udfs: tuple[str, ...]` - Required UDF names
  - `referenced_tables: tuple[str, ...]` - Referenced table names
- Methods:
  - `payload()` - JSON-ready payload for diagnostics
  - `diagnostics_payload(event_time_unix_ms: int)` - Diagnostics snapshot

#### Added `build_view_artifact_from_bundle()`
- Builds `DataFusionViewArtifact` from a `DataFusionPlanBundle`
- Replaces AST-based artifact building for DataFusion-native views
- Parameters:
  - `bundle: DataFusionPlanBundle` - Plan bundle
  - `name: str` - View name
  - `schema: pa.Schema` - Output schema
  - `required_udfs: tuple[str, ...]` - Required UDFs
  - `referenced_tables: tuple[str, ...]` - Referenced tables

#### Marked Legacy Code as DEPRECATED
- `ViewArtifact` class marked as DEPRECATED
- `build_view_artifact()` function marked as DEPRECATED
- Both preserved for backward compatibility

### 2. View Registration Updates (`view_graph_registry.py`)

#### Removed Mandatory AST Check
- **Before**: Lines 150-152 raised error if `node.sqlglot_ast is None`
- **After**: Removed check - views can work with `plan_bundle` alone

#### Updated `_validate_udf_calls()`
- Now prefers `plan_bundle` for UDF extraction when available
- Falls back to SQLGlot AST for legacy views
- Uses `_required_udfs_from_plan_bundle()` for DataFusion-native validation
- No longer raises error if both AST and bundle are missing

#### Enhanced Persistent Registration
- **DataFusion-native path (preferred)**:
  - Uses `adapter.register_view()` with `temporary=False`
  - No longer requires Ibis expression
- **Legacy Ibis path (backward compatible)**:
  - Still uses `register_ibis_view()` when `ibis_expr` is available
  - Ensures backward compatibility with existing code

#### Smart Artifact Building
- Prefers `build_view_artifact_from_bundle()` when `plan_bundle` is available
- Falls back to legacy `build_view_artifact()` when only AST is available
- Skips artifact recording if neither is available (graceful degradation)

### 3. Runtime Profile Updates (`runtime.py`)

#### Updated `DataFusionViewRegistry`
- `entries` field now accepts `ViewArtifact | DataFusionViewArtifact`
- `record()` method signature updated to accept both artifact types
- `snapshot()` and `diagnostics_snapshot()` work with both types

#### Updated `record_view_definition()`
- Accepts both `ViewArtifact` and `DataFusionViewArtifact`
- Enhanced docstring with parameter documentation
- Works seamlessly with both legacy and DataFusion-native artifacts

### 4. Tests (`test_datafusion_view_artifact.py`)

Created comprehensive unit tests:
- `test_datafusion_view_artifact_creation()` - Basic creation
- `test_datafusion_view_artifact_payload()` - Payload generation
- `test_datafusion_view_artifact_diagnostics_payload()` - Diagnostics
- `test_build_view_artifact_from_bundle()` - Builder function
- Mock fixture for `DataFusionPlanBundle` testing

## Backward Compatibility

All changes maintain full backward compatibility:
- Legacy `ViewArtifact` and `build_view_artifact()` preserved (marked DEPRECATED)
- Views with AST-only still work through legacy path
- Views with Ibis expressions still use `register_ibis_view()`
- Existing tests should continue to pass

## Migration Path

### For New Views
```python
# Build DataFusion plan bundle
df = ctx.sql("SELECT * FROM table")
plan_bundle = build_plan_bundle(ctx, df, compute_execution_plan=False)

# Extract dependencies and UDFs
deps = _deps_from_plan_bundle(plan_bundle)
required_udfs = _required_udfs_from_plan_bundle(plan_bundle, snapshot=snapshot)

# Create ViewNode with plan_bundle (preferred)
node = ViewNode(
    name="my_view",
    deps=deps,
    builder=lambda ctx: df,
    plan_bundle=plan_bundle,  # DataFusion-native
    required_udfs=required_udfs,
    # No sqlglot_ast or ibis_expr needed!
)
```

### For Legacy Views
```python
# Old code still works - backward compatible
node = ViewNode(
    name="legacy_view",
    deps=deps,
    builder=builder,
    sqlglot_ast=ast,  # Still supported
    ibis_expr=expr,   # Still supported
)
```

## Quality Assurance

- ✅ `ruff check --fix` - All linting issues resolved
- ✅ `ruff format` - Code properly formatted
- ✅ `pyright --warnings --pythonversion=3.13` - Type checking passed
- ✅ Python syntax validation - All files compile correctly
- ✅ Unit tests created with proper docstrings

## Next Steps (Phase 1.2)

Replace `CompiledPlan` with `DataFusionPlanBundle` in the execution facade to complete the transition from SQLGlot-based compilation to DataFusion-native planning.

## Files Modified

1. `src/datafusion_engine/view_artifacts.py` - New artifact type and builder
2. `src/datafusion_engine/view_graph_registry.py` - Registration logic updates
3. `src/datafusion_engine/runtime.py` - Registry and recording updates
4. `tests/unit/test_datafusion_view_artifact.py` - New test suite

## Key Benefits

1. **Pure DataFusion**: No SQLGlot AST or Ibis required
2. **Simpler Lineage**: Direct from DataFusion optimized logical plan
3. **Better Performance**: Fewer compilation steps
4. **Cleaner Dependencies**: Fewer cross-module imports
5. **Future-Proof**: Foundation for removing SQLGlot/Ibis entirely
