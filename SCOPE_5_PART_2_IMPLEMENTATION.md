# Scope 5 Part 2 Implementation Summary

## Objective
Complete SQLGlot compiler gate with AST serde and rewrite tests.

## Implementation Overview

### 1. AstArtifact Dataclass (`src/sqlglot_tools/optimizer.py`)

Added a new `AstArtifact` dataclass to capture serialized SQLGlot AST with metadata:

```python
@dataclass(frozen=True)
class AstArtifact:
    """SQLGlot AST artifact with serialization metadata."""

    sql: str
    ast_json: str
    policy_hash: str

    def to_dict(self) -> dict[str, str]:
        """Return dictionary representation for serialization."""
```

**Fields:**
- `sql`: Original SQL text
- `ast_json`: JSON-serialized SQLGlot AST (via `sqlglot.serde.dump`)
- `policy_hash`: Hash of the SQLGlot policy used for compilation

### 2. Serialization Functions (`src/sqlglot_tools/optimizer.py`)

Added four new functions for AST artifact management:

#### `serialize_ast_artifact(artifact: AstArtifact) -> str`
- Serializes an `AstArtifact` to JSON string
- Uses `json.dumps` with `sort_keys=True` for deterministic output

#### `deserialize_ast_artifact(serialized: str) -> AstArtifact`
- Deserializes an `AstArtifact` from JSON string
- Validates required fields and raises `ValueError` on invalid input

#### `ast_to_artifact(expr: Expression, *, sql: str | None = None, policy: SqlGlotPolicy | None = None) -> AstArtifact`
- Converts a SQLGlot expression to an `AstArtifact`
- Uses `sqlglot.serde.dump` for AST serialization
- Captures policy hash for version tracking

#### `artifact_to_ast(artifact: AstArtifact) -> Expression`
- Converts an `AstArtifact` back to a SQLGlot expression
- Uses `sqlglot.serde.load` for deserialization
- Raises `ValueError` on deserialization failures

### 3. Preflight Enforcement (`src/datafusion_engine/compile_options.py`)

Added `enforce_preflight` flag to `DataFusionCompileOptions`:

```python
@dataclass(frozen=True)
class DataFusionCompileOptions:
    # ... existing fields ...
    enforce_preflight: bool = False
```

**Purpose:** When enabled, forces SQL through preflight qualification pipeline before compilation.

### 4. Preflight Hook (`src/datafusion_engine/bridge.py`)

Added preflight enforcement integration:

#### `_maybe_enforce_preflight(expr: Expression, *, options: DataFusionCompileOptions) -> None`
- Validates expressions through `preflight_sql` when `enforce_preflight=True`
- Raises `ValueError` if preflight validation fails
- Integrated into `compile_sqlglot_expr` compilation pipeline

**Integration Point:**
```python
def compile_sqlglot_expr(...) -> Expression:
    # ... normalization ...
    _maybe_enforce_preflight(sg_expr, options=resolved)
    return sg_expr
```

### 5. Regression Test Harness (`tests/sqlglot/test_rewrite_regressions.py`)

Created comprehensive test suite using `sqlglot.executor.execute` for semantic validation:

#### Test Categories:

1. **Basic Semantics** (`test_rewrite_semantics_basic`)
   - Validates basic GROUP BY aggregation

2. **Normalization Preservation** (`test_rewrite_semantics_with_normalization`)
   - Ensures normalization preserves query semantics
   - Compares original vs normalized results

3. **AST Artifact Roundtrip** (`test_ast_artifact_roundtrip`)
   - Tests serialization/deserialization roundtrip
   - Validates semantic equivalence after restoration

4. **Semantic Preservation** (`test_ast_artifact_preserves_semantics`)
   - Tests complex queries (GROUP BY with HAVING)
   - Ensures artifacts preserve exact semantics

5. **Metadata Validation** (`test_ast_artifact_metadata`)
   - Validates artifact structure and metadata fields

6. **Serialization Format** (`test_serialization_format`)
   - Ensures JSON format correctness

7. **Join Rewrites** (`test_join_rewrite_semantics`)
   - Tests join normalization semantics

8. **Subquery Rewrites** (`test_subquery_rewrite_semantics`)
   - Tests subquery normalization with IN clauses

## Key Design Decisions

### 1. JSON String Storage for AST
- `ast_json` is stored as JSON string rather than raw dict
- Enables easy persistence and transmission
- Uses `json.dumps` for consistent serialization

### 2. Policy Hash Tracking
- Each artifact captures the policy hash at creation time
- Enables version tracking and compatibility checks
- Useful for cache invalidation and debugging

### 3. Executor-Based Testing
- Uses `sqlglot.executor.execute` for semantic validation
- Compares result rows rather than AST structure
- Catches semantic regressions that structural tests miss

### 4. Optional Preflight Enforcement
- `enforce_preflight` defaults to `False` for backward compatibility
- When enabled, acts as compiler gate before DataFusion execution
- Useful for catching SQL errors early in development/testing

## Files Modified

1. `/home/paul/CodeAnatomy/src/sqlglot_tools/optimizer.py`
   - Added: `AstArtifact` dataclass
   - Added: `serialize_ast_artifact`, `deserialize_ast_artifact`
   - Added: `ast_to_artifact`, `artifact_to_ast`
   - Updated: `__all__` exports

2. `/home/paul/CodeAnatomy/src/datafusion_engine/compile_options.py`
   - Added: `enforce_preflight` field to `DataFusionCompileOptions`

3. `/home/paul/CodeAnatomy/src/datafusion_engine/bridge.py`
   - Added: `_maybe_enforce_preflight` function
   - Updated: `compile_sqlglot_expr` to call preflight check
   - Added: `preflight_sql` import

## Files Created

1. `/home/paul/CodeAnatomy/tests/sqlglot/test_rewrite_regressions.py`
   - 8 regression tests using SQLGlot executor
   - Tests cover basic queries, joins, subqueries, and aggregations
   - Validates artifact serialization roundtrip

2. `/home/paul/CodeAnatomy/verify_implementation.py`
   - Verification script for manual testing
   - Tests all new functions and imports

## Usage Examples

### Creating and Serializing AST Artifacts

```python
from sqlglot_tools.optimizer import (
    ast_to_artifact,
    serialize_ast_artifact,
    deserialize_ast_artifact,
    artifact_to_ast,
    parse_sql_strict,
    default_sqlglot_policy,
)

# Parse SQL
policy = default_sqlglot_policy()
sql = "SELECT a, SUM(b) FROM t GROUP BY a"
expr = parse_sql_strict(sql, dialect=policy.read_dialect)

# Create artifact
artifact = ast_to_artifact(expr, sql=sql, policy=policy)

# Serialize for storage
serialized = serialize_ast_artifact(artifact)

# Later: deserialize and restore
restored_artifact = deserialize_ast_artifact(serialized)
restored_expr = artifact_to_ast(restored_artifact)
```

### Enabling Preflight Enforcement

```python
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.bridge import compile_sqlglot_expr

# Enable preflight validation
options = DataFusionCompileOptions(
    enforce_preflight=True,
    schema_map={"t": {"a": "String", "b": "Int64"}},
)

# Compilation will fail if SQL doesn't pass preflight checks
sqlglot_expr = compile_sqlglot_expr(
    ibis_expr,
    backend=backend,
    options=options,
)
```

### Running Regression Tests

```bash
# Run all regression tests
pytest tests/sqlglot/test_rewrite_regressions.py -v

# Run specific test
pytest tests/sqlglot/test_rewrite_regressions.py::test_ast_artifact_roundtrip -v
```

## Testing Strategy

### Unit Tests
- AST artifact creation and metadata validation
- Serialization format correctness
- Roundtrip preservation

### Integration Tests
- Normalization semantic preservation
- Join and subquery rewrite validation
- End-to-end artifact restoration with execution

### Regression Prevention
- Executor-based semantic comparison
- Sorted result comparison for determinism
- Multiple query patterns (aggregation, joins, subqueries)

## Future Enhancements

1. **Artifact Storage Backend**
   - Integration with plan cache for artifact storage
   - Disk-based artifact persistence for debugging

2. **Policy Compatibility Checks**
   - Validate policy_hash on artifact restoration
   - Automatic migration for policy changes

3. **Extended Test Coverage**
   - Window functions
   - CTEs (Common Table Expressions)
   - Set operations (UNION, INTERSECT, EXCEPT)

4. **Performance Benchmarks**
   - Serialization/deserialization overhead measurement
   - Preflight enforcement impact on compile time

## Verification

Run the verification script to test the implementation:

```bash
python verify_implementation.py
```

Expected output:
```
Test 1: AstArtifact creation...
  Created artifact with policy_hash: <hash>...

Test 2: Serialization roundtrip...
  Serialized length: <N> bytes
  Deserialized SQL: SELECT a, b FROM t WHERE a = 'x'
  ✓ Roundtrip successful

Test 3: AST restoration...
  Restored SQL: <restored SQL>
  ✓ AST restored

Test 4: DataFusionCompileOptions.enforce_preflight...
  ✓ enforce_preflight flag available

Test 5: Preflight enforcement function...
  ✓ _maybe_enforce_preflight imported

✅ All verification tests passed!
```

## Conclusion

This implementation provides:
1. ✅ Complete AST serialization infrastructure
2. ✅ Preflight enforcement gate for compile-time validation
3. ✅ Comprehensive executor-based regression test suite
4. ✅ Backward-compatible integration with existing compilation pipeline

All requirements from Scope 5 Part 2 of the Combined Library Utilization Plan have been successfully implemented.
