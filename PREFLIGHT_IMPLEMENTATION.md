# SQLGlot Preflight Infrastructure Implementation

## Overview
Implementation of Scope 5 Part 1 from the Combined Library Utilization Plan: SQLGlot Preflight Infrastructure.

## Objective
Build mandatory preflight qualification infrastructure for SQL ingress with structured error recording and multi-stage qualification tracking.

## Implementation Details

### 1. PreflightResult Dataclass
**Location:** `/home/paul/CodeAnatomy/src/sqlglot_tools/optimizer.py` (lines 339-350)

```python
@dataclass(frozen=True)
class PreflightResult:
    """Result of SQL preflight qualification."""

    original_sql: str
    qualified_expr: Expression | None
    annotated_expr: Expression | None
    canonicalized_expr: Expression | None
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    policy_hash: str | None
    schema_map_hash: str | None
```

**Purpose:** Captures the complete state of SQL preflight qualification through all stages.

**Fields:**
- `original_sql`: The raw input SQL text
- `qualified_expr`: Qualified SQLGlot expression (after qualification stage)
- `annotated_expr`: Type-annotated expression (after annotation stage)
- `canonicalized_expr`: Canonicalized expression (after canonicalization stage)
- `errors`: Tuple of error messages encountered during processing
- `warnings`: Tuple of warning messages (used in lenient mode)
- `policy_hash`: SHA-256 hash of the SQLGlot policy configuration
- `schema_map_hash`: SHA-256 hash of the schema mapping

### 2. preflight_sql() Function
**Location:** `/home/paul/CodeAnatomy/src/sqlglot_tools/optimizer.py` (lines 1181-1372)

```python
def preflight_sql(
    sql: str,
    *,
    schema: SchemaMapping | None = None,
    dialect: str = "datafusion",
    strict: bool = True,
    policy: SqlGlotPolicy | None = None,
) -> PreflightResult:
    """Preflight SQL through qualify + annotate + canonicalize pipeline."""
```

**Purpose:** Orchestrates multi-stage SQL preflight processing with error handling.

**Parameters:**
- `sql`: SQL text to preflight
- `schema`: Optional schema mapping for qualification
- `dialect`: SQLGlot dialect (default: "datafusion")
- `strict`: Whether to fail fast on errors (True) or continue with warnings (False)
- `policy`: Optional SQLGlot policy override

**Processing Stages:**
1. **Parse Stage**: Parse SQL text into SQLGlot expression
2. **Qualify Stage**: Qualify columns with schema, expand stars, validate
3. **Annotate Stage**: Add type annotations using schema
4. **Canonicalize Stage**: Apply canonicalization rules

**Error Handling:**
- **Strict mode (strict=True)**: Returns immediately on first error
- **Lenient mode (strict=False)**: Continues processing, converting errors to warnings

**Hash Computation:**
- Computes `policy_hash` using `sqlglot_policy_snapshot_for()`
- Computes `schema_map_hash` by JSON-serializing and hashing the schema mapping

### 3. emit_preflight_diagnostics() Function
**Location:** `/home/paul/CodeAnatomy/src/sqlglot_tools/optimizer.py` (lines 1375-1433)

```python
def emit_preflight_diagnostics(result: PreflightResult) -> dict[str, object]:
    """Emit structured diagnostics from a preflight result."""
```

**Purpose:** Extracts structured diagnostics from a PreflightResult for logging or storage.

**Returns:** Dictionary containing:
- `original_sql`: Input SQL text
- `policy_hash`: Policy configuration hash
- `schema_map_hash`: Schema mapping hash
- `errors`: List of error messages
- `warnings`: List of warning messages
- `has_errors`: Boolean flag for error presence
- `has_warnings`: Boolean flag for warning presence
- `stages_completed`: Dictionary tracking which stages completed successfully
  - `qualified`: Whether qualification completed
  - `annotated`: Whether annotation completed
  - `canonicalized`: Whether canonicalization completed
- `qualified_sql`: SQL text of qualified expression (if available)
- `annotated_sql`: SQL text of annotated expression (if available)
- `canonicalized_sql`: SQL text of canonicalized expression (if available)

## Integration with Existing Infrastructure

### Reuses Existing Functions
- `parse_sql_strict()`: For SQL parsing with template sanitization
- `qualify_strict()`: For strict qualification with validation
- `_prepare_for_qualification()`: For canonicalization and transforms
- `_annotate_expression()`: For type annotation
- `canonicalize()`: For final canonicalization
- `sqlglot_policy_snapshot_for()`: For policy hash computation
- `payload_hash()`: For consistent hashing

### Follows Existing Patterns
- Frozen dataclasses for immutability
- Tuple types for error/warning collections
- Schema hashing using PyArrow schemas
- JSON serialization for nested structures
- Dialect handling consistent with other functions

## Exports
Added to `__all__`:
- `PreflightResult`
- `preflight_sql`
- `emit_preflight_diagnostics`

## Test Coverage
Created test file: `/home/paul/CodeAnatomy/test_preflight.py`

**Test scenarios:**
1. Successful preflight with valid SQL and schema
2. Parse failure with invalid SQL
3. Qualification failure with missing columns
4. Diagnostic emission from successful result
5. Lenient mode behavior with errors

## Usage Example

```python
from sqlglot_tools.optimizer import (
    default_sqlglot_policy,
    emit_preflight_diagnostics,
    preflight_sql,
)

# Successful preflight
sql = "SELECT a, b FROM t WHERE a > 1"
schema = {"t": {"a": "int", "b": "int"}}
policy = default_sqlglot_policy()

result = preflight_sql(
    sql,
    schema=schema,
    dialect=policy.read_dialect,
    strict=True,
    policy=policy,
)

if result.errors:
    print(f"Preflight failed: {result.errors}")
else:
    print(f"Preflight succeeded")
    print(f"Policy hash: {result.policy_hash}")
    print(f"Schema hash: {result.schema_map_hash}")

    # Emit diagnostics
    diagnostics = emit_preflight_diagnostics(result)
    print(f"Stages completed: {diagnostics['stages_completed']}")
```

## Key Design Decisions

1. **Immutable Result Type**: Using frozen dataclass ensures thread safety and prevents accidental mutations

2. **Optional Expression Fields**: Each stage can fail independently, so expressions are Optional

3. **Strict vs Lenient Modes**: Allows callers to choose between fail-fast and best-effort processing

4. **Hash Stability**: Both policy and schema hashes use stable, versioned serialization for reproducibility

5. **Stage Tracking**: Explicitly tracks which stages completed to aid debugging and observability

6. **Error Accumulation**: In lenient mode, accumulates all errors as warnings for comprehensive reporting

## Future Enhancements
- Add timing metrics for each stage
- Include SQLGlot parse tree debug information
- Add validation rules enforcement
- Support custom diagnostic formatters
- Cache preflight results by (sql, schema_hash, policy_hash) tuple
