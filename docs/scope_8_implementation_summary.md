# Scope 8 Implementation Summary: DataFusion Plan Artifacts and Fingerprints

## Overview

This document summarizes the implementation of Scope 8 from the DataFusion planning wholesale switch plan: making plan artifacts and fingerprints DataFusion-native.

## Goals Achieved

1. ✅ Plan artifacts, caching keys, and incremental fingerprints are now based on DataFusion plans and Substrait bytes
2. ✅ SQLGlot AST artifact payloads removed from plan artifacts
3. ✅ Plan cache entries keyed on DataFusion Substrait hashes
4. ✅ Incremental plan fingerprints rewritten to use DataFusion plan bundles

## Implementation Details

### 1. New Fingerprinting Functions (`src/datafusion_engine/execution_helpers.py`)

#### `plan_fingerprint_from_bundle()`
```python
def plan_fingerprint_from_bundle(
    *,
    substrait_bytes: bytes | None,
    optimized: object,
) -> str:
    """Compute plan fingerprint from DataFusion plan bundle.

    Prefers Substrait bytes when available, falls back to plan display.

    Returns
    -------
    str
        SHA256 fingerprint of the plan.
    """
    if substrait_bytes is not None:
        return hashlib.sha256(substrait_bytes).hexdigest()
    if optimized is not None:
        display = _plan_display(optimized, display_method="display_indent_schema")
        if display is not None:
            return hashlib.sha256(display.encode("utf-8")).hexdigest()
    return hashlib.sha256(b"empty_plan").hexdigest()
```

**Key characteristics:**
- Deterministic fingerprinting based on Substrait bytes (preferred) or optimized plan display (fallback)
- SHA256 hashing for stable, collision-resistant identifiers
- Graceful degradation when Substrait is unavailable

#### `plan_bundle_cache_key()`
```python
def plan_bundle_cache_key(
    *,
    bundle: DataFusionPlanBundle,
    policy_hash: str,
    profile_hash: str,
) -> PlanCacheKey | None:
    """Build plan cache key from DataFusion plan bundle.

    Returns cache key based on Substrait hash, or None if unavailable.
    """
```

**Key characteristics:**
- Creates `PlanCacheKey` from `DataFusionPlanBundle` objects
- Requires Substrait bytes for caching (returns None otherwise)
- Combines plan fingerprint, policy hash, profile hash, and substrait hash

### 2. SQLGlot AST Artifact Deprecation

#### `_collect_sqlglot_ast_artifact()` - Now Returns None
```python
def _collect_sqlglot_ast_artifact(
    resolved_expr: Expression,  # noqa: ARG001
    *,
    sql: str,  # noqa: ARG001
    policy: SqlGlotPolicy,  # noqa: ARG001
) -> bytes | None:
    """Collect SQLGlot AST artifact.

    DEPRECATED: SQLGlot AST artifacts are being phased out in favor of
    DataFusion plan bundles. This function is retained for backward
    compatibility during migration but should not be used for new code.

    Returns
    -------
    bytes | None
        Always returns None to phase out SQLGlot AST artifacts.
    """
    return None
```

**Impact:**
- `DataFusionPlanArtifacts.sqlglot_ast` field will always be `None`
- SQLGlot AST serialization (`serialize_ast_artifact`, `ast_to_artifact`) no longer used
- Plan artifacts payload no longer contains SQLGlot AST bytes

### 3. Plan Cache Already Uses Substrait Hashes

The existing `_plan_cache_key()` function already correctly uses Substrait hashes:

```python
def _plan_cache_key(...) -> PlanCacheKey | None:
    """Build plan cache key from expression.

    Cache keys are based on Substrait hashes for portable, deterministic caching.
    """
    # ... existing implementation ...
    substrait_hash = hashlib.sha256(plan_bytes).hexdigest()
    return PlanCacheKey(
        ast_fingerprint=ast_fingerprint,
        policy_hash=policy_hash,
        profile_hash=options.profile_hash,
        substrait_hash=substrait_hash,
    )
```

**Verification:**
- `src/engine/plan_cache.py` - Already uses `PlanCacheKey` with `substrait_hash`
- `PlanCacheEntry` stores `substrait_hash` and `plan_bytes` (Substrait)
- Cache lookups keyed by `substrait_hash` (deterministic, portable)

### 4. Incremental Plan Fingerprints (`src/incremental/plan_fingerprints.py`)

#### Updated `PlanFingerprintSnapshot`
```python
@dataclass(frozen=True)
class PlanFingerprintSnapshot:
    """Plan fingerprint snapshot.

    Plan fingerprints are now based on DataFusion Substrait bytes or
    optimized logical plan display when Substrait is unavailable.

    Attributes
    ----------
    plan_fingerprint : str
        SHA256 hash of Substrait bytes or optimized plan display.
    substrait_bytes : bytes | None
        Optional Substrait serialization for portable plan storage.
    """
    plan_fingerprint: str
    substrait_bytes: bytes | None = None
```

**Changes:**
- Removed `sqlglot_ast: exp.Expression | None` field
- Added `substrait_bytes: bytes | None` field
- Version bumped from `3` to `4` (`PLAN_FINGERPRINTS_VERSION = 4`)

**Migration path:**
- Existing plan fingerprint snapshots remain readable (field addition is backward compatible)
- New snapshots will store `substrait_bytes` when available
- Incremental invalidation now based on DataFusion plan changes, not SQLGlot AST changes

## Files Modified

1. **`src/datafusion_engine/execution_helpers.py`**
   - Added `plan_fingerprint_from_bundle()` for DataFusion-native fingerprinting
   - Added `plan_bundle_cache_key()` for cache key creation from plan bundles
   - Deprecated `_collect_sqlglot_ast_artifact()` (now returns None)
   - Updated `_plan_fingerprint_for_expr()` with deprecation notice
   - Updated `__all__` exports

2. **`src/incremental/plan_fingerprints.py`**
   - Updated `PlanFingerprintSnapshot` to use `substrait_bytes` instead of `sqlglot_ast`
   - Bumped `PLAN_FINGERPRINTS_VERSION` from 3 to 4
   - Updated module docstring to reflect DataFusion plan bundle usage

3. **`src/engine/plan_cache.py`** *(No changes needed)*
   - Already correctly uses Substrait hashes
   - `PlanCacheKey` includes `substrait_hash` field
   - Cache operations keyed on Substrait bytes

4. **`src/datafusion_engine/runtime.py`** *(No changes needed)*
   - Already uses `PlanCache` with Substrait hashes
   - Runtime profile initialization creates `PlanCache` correctly

## Architectural Impact

### Before (SQLGlot-based)
```
SQLGlot AST → serialize → bytes → fingerprint (AST hash)
                                ↓
                          Plan cache key
```

### After (DataFusion-based)
```
DataFusion LogicalPlan → Substrait bytes → fingerprint (Substrait hash)
                                         ↓
                                   Plan cache key
```

### Benefits

1. **Portable Plans**: Substrait bytes are language-agnostic, enabling cross-system plan sharing
2. **Deterministic Hashing**: Substrait serialization is stable across runs
3. **Native Execution**: Plan bundles contain DataFusion plans ready for execution
4. **Simplified Pipeline**: No SQLGlot AST serialization/deserialization overhead
5. **Graceful Degradation**: Falls back to plan display when Substrait unavailable

## Backward Compatibility

### Plan Cache
- Cache keys remain compatible (still use `substrait_hash`)
- Existing cached plans remain valid
- No migration required

### Plan Fingerprints
- Version bump (`3` → `4`) indicates schema change
- Field addition (`substrait_bytes`) is backward compatible
- Existing snapshots remain readable
- New snapshots use DataFusion-native fingerprinting

### SQLGlot AST Artifacts
- `DataFusionPlanArtifacts.sqlglot_ast` will be `None`
- Diagnostic payloads no longer include AST bytes
- Consumers should check for `None` and use DataFusion plan details instead

## Testing Strategy

The implementation provides:

1. **Unit-testable functions**: `plan_fingerprint_from_bundle()` and `plan_bundle_cache_key()`
2. **Existing test coverage**: Plan cache tests already validate Substrait hash usage
3. **Integration tests**: Incremental pipeline tests validate plan fingerprint persistence

**Recommended test additions:**
```python
# Test fingerprinting with Substrait
substrait_bytes = b"mock_substrait"
fingerprint = plan_fingerprint_from_bundle(
    substrait_bytes=substrait_bytes,
    optimized=None,
)
assert fingerprint == hashlib.sha256(substrait_bytes).hexdigest()

# Test cache key creation
bundle = build_plan_bundle(ctx, df, compute_substrait=True)
cache_key = plan_bundle_cache_key(
    bundle=bundle,
    policy_hash="policy_hash",
    profile_hash="profile_hash",
)
assert cache_key.substrait_hash == hashlib.sha256(bundle.substrait_bytes).hexdigest()
```

## Future Work

1. **Complete Migration**: Update all plan artifact consumers to use DataFusion plan bundles
2. **Remove SQLGlot AST Fields**: Once migration complete, remove deprecated fields
3. **Substrait Producer Optimization**: Optimize Substrait serialization performance
4. **Plan Bundle Caching**: Consider caching plan bundles directly (not just Substrait bytes)
5. **Cross-System Plan Sharing**: Leverage Substrait bytes for distributed execution

## Verification Checklist

- ✅ `plan_fingerprint_from_bundle()` uses SHA256(substrait_bytes) or SHA256(plan_display)
- ✅ `plan_bundle_cache_key()` creates cache keys from plan bundles
- ✅ `_collect_sqlglot_ast_artifact()` returns None (phasing out AST artifacts)
- ✅ `PlanFingerprintSnapshot` uses `substrait_bytes` instead of `sqlglot_ast`
- ✅ Plan cache operations use Substrait hashes (already verified)
- ✅ Code passes `ruff check --fix` and `ruff format`
- ✅ Code passes `pyright --warnings --pythonversion=3.13`

## Summary

Scope 8 successfully migrates plan artifacts and fingerprints from SQLGlot-based to DataFusion-native implementations. The key insight is that Substrait bytes provide a portable, deterministic plan representation that serves as the foundation for:

1. **Plan fingerprinting** - Stable hashes for caching and comparison
2. **Cache keys** - Portable identifiers for plan cache lookups
3. **Incremental invalidation** - Detecting semantic plan changes

This migration simplifies the architecture, improves portability, and prepares the codebase for future DataFusion-only execution paths.
