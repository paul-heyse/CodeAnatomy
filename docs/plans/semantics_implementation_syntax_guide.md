# Semantics Implementation Syntax Guide

**Companion to:** `semantics_integration_wiring_plan.md`, `semantics_programmatic_extensions_plan.md`
**Purpose:** Explicit DataFusion, Rust UDF, and DeltaLake syntax for implementation, plus ast-grep/cq validation workflows

---

## Part 1: DataFusion Python API Patterns

### 1.1 Core DataFrame Operations

**Join with filter (the semantic compiler pattern):**

```python
from datafusion import SessionContext, col, lit
from datafusion import functions as f

def build_span_overlap_join(
    ctx: SessionContext,
    left_table: str,
    right_table: str,
    *,
    file_key: str = "file_id",
) -> DataFrame:
    """Build span overlap join with file partitioning."""
    left = ctx.table(left_table)
    right = ctx.table(right_table)

    # Equi-join on file identity first (highly selective)
    joined = left.join(
        right,
        join_keys=([file_key], [file_key]),
        how="inner",
    )

    # Then filter for span overlap: left.start < right.end AND right.start < left.end
    return joined.filter(
        (col(f"{left_table}.bstart") < col(f"{right_table}.bend")) &
        (col(f"{right_table}.bstart") < col(f"{left_table}.bend"))
    )
```

**SQL expression execution (preferred for complex filters):**

```python
def build_relationship_with_sql_filter(
    ctx: SessionContext,
    left_table: str,
    right_table: str,
    filter_sql: str,
) -> DataFrame:
    """Build relationship using SQL for complex filter logic."""
    # Register views first
    left_df = ctx.table(left_table)
    right_df = ctx.table(right_table)

    ctx.register_table("_left", left_df)
    ctx.register_table("_right", right_df)

    # Use SQL for complex join logic
    return ctx.sql(f"""
        SELECT
            l.entity_id,
            r.symbol,
            l.path,
            l.bstart,
            l.bend,
            '{left_table}_to_{right_table}' AS origin
        FROM _left l
        JOIN _right r ON l.file_id = r.file_id
        WHERE {filter_sql}
    """)
```

### 1.2 Window Functions for Ranking

**Deterministic winner selection (the quality plan pattern):**

```python
def select_best_match(
    ctx: SessionContext,
    candidates_table: str,
    *,
    partition_key: str,
    order_by: list[str],
) -> DataFrame:
    """Select best match per partition using window function."""
    candidates = ctx.table(candidates_table)

    # Build ORDER BY clause
    order_clause = ", ".join(order_by)

    # Use SQL for window function (cleaner than DataFrame API)
    return ctx.sql(f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_key}
                    ORDER BY {order_clause}
                ) AS _rank
            FROM {candidates_table}
        )
        SELECT * EXCLUDE (_rank)
        FROM ranked
        WHERE _rank = 1
    """)
```

**Ambiguity group assignment:**

```python
def assign_ambiguity_groups(
    ctx: SessionContext,
    table: str,
    *,
    group_key_expr: str,
) -> DataFrame:
    """Assign stable ambiguity group IDs."""
    return ctx.sql(f"""
        SELECT *,
            stable_hash64({group_key_expr}) AS ambiguity_group_id,
            COUNT(*) OVER (PARTITION BY {group_key_expr}) AS candidates_in_group
        FROM {table}
    """)
```

### 1.3 Schema Introspection

**Validate table exists before use:**

```python
from datafusion_engine.schema.introspection import table_names_snapshot

def require_tables(ctx: SessionContext, *table_names: str) -> None:
    """Validate required tables exist."""
    available = set(table_names_snapshot(ctx))
    missing = set(table_names) - available
    if missing:
        raise ValueError(f"Missing required tables: {sorted(missing)}")
```

**Get schema for validation:**

```python
def get_table_schema(ctx: SessionContext, table_name: str) -> pa.Schema:
    """Get Arrow schema for a registered table."""
    df = ctx.table(table_name)
    return df.schema()


def validate_required_columns(
    ctx: SessionContext,
    table_name: str,
    required_columns: set[str],
) -> None:
    """Validate table has required columns."""
    schema = get_table_schema(ctx, table_name)
    actual = {f.name for f in schema}
    missing = required_columns - actual
    if missing:
        raise ValueError(f"Table {table_name} missing columns: {sorted(missing)}")
```

### 1.4 View Registration

**Register DataFrame as view:**

```python
def register_semantic_view(
    ctx: SessionContext,
    name: str,
    df: DataFrame,
) -> None:
    """Register a DataFrame as a named view."""
    # DataFusion's register_table accepts DataFrame
    ctx.register_table(name, df)
```

**Create view with SQL:**

```python
def create_view_sql(
    ctx: SessionContext,
    view_name: str,
    sql: str,
) -> None:
    """Create a view using SQL."""
    ctx.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
```

---

## Part 2: Rust UDF Integration Patterns

### 2.1 Required UDFs for Semantic Pipeline

These UDFs must be registered before semantic compilation:

| UDF Name | Signature | Purpose |
|----------|-----------|---------|
| `stable_hash64` | `(string...) -> int64` | Deterministic entity ID generation |
| `col_to_byte` | `(line_no, col, line_start_byte, col_unit) -> int64` | SCIP coordinate conversion |
| `span_make` | `(start, end) -> struct` | Span struct construction |
| `prefixed_hash64` | `(prefix, string...) -> string` | Prefixed ID generation |

### 2.2 UDF Validation Pattern

**Validate UDFs before pipeline execution:**

```python
from datafusion_engine.udf.runtime import rust_udf_snapshot, validate_required_udfs

REQUIRED_SEMANTIC_UDFS: frozenset[str] = frozenset({
    "stable_hash64",
    "col_to_byte",
    "span_make",
    "prefixed_hash64",
})


def validate_semantic_udfs(ctx: SessionContext) -> None:
    """Validate all required UDFs are registered."""
    available = rust_udf_snapshot(ctx)
    validate_required_udfs(
        available,
        required=REQUIRED_SEMANTIC_UDFS,
        context="semantic pipeline",
    )
```

### 2.3 UDF Usage in SQL

**Entity ID generation:**

```sql
SELECT
    stable_hash64(file_id, CAST(bstart AS VARCHAR), CAST(bend AS VARCHAR)) AS entity_id,
    *
FROM cst_refs
```

**SCIP byte offset conversion:**

```sql
SELECT
    o.*,
    col_to_byte(o.start_line, o.start_char, li.line_start_byte, o.col_unit) AS bstart,
    col_to_byte(o.end_line, o.end_char, li.line_start_byte, o.col_unit) AS bend
FROM scip_occurrences o
JOIN file_line_index_v1 li
    ON o.file_id = li.file_id
    AND o.start_line = li.line_no
```

**Prefixed hash for namespaced IDs:**

```sql
SELECT
    prefixed_hash64('def', file_id, name, CAST(bstart AS VARCHAR)) AS def_entity_id,
    *
FROM cst_defs
```

### 2.4 Rust UDF Implementation Reference

**Scalar UDF skeleton (for new UDFs):**

```rust
use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct StableHash64Udf {
    signature: Signature,
}

impl StableHash64Udf {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StableHash64Udf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "stable_hash64" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        // Implementation: concatenate string args, hash with xxhash64
        // Return ColumnarValue::Array or ColumnarValue::Scalar
        todo!()
    }
}
```

---

## Part 3: DeltaLake Integration Patterns

### 3.1 Reading Delta Tables

**Register Delta table as TableProvider:**

```python
from datafusion import SessionContext
from deltalake import DeltaTable

def register_delta_input(
    ctx: SessionContext,
    table_name: str,
    delta_path: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Register a Delta table as a DataFusion table provider."""
    dt = DeltaTable(delta_path, storage_options=storage_options)

    # Best path: use TableProvider FFI (DataFusion 43+)
    ctx.register_table(table_name, dt)
```

**Read specific version (time travel):**

```python
def register_delta_at_version(
    ctx: SessionContext,
    table_name: str,
    delta_path: str,
    version: int,
) -> None:
    """Register Delta table at specific version."""
    dt = DeltaTable(delta_path, version=version)
    ctx.register_table(table_name, dt)
```

### 3.2 Writing to Delta

**Write DataFrame to Delta (compute in DataFusion, commit via delta-rs):**

```python
from deltalake import write_deltalake

def write_semantic_output(
    ctx: SessionContext,
    view_name: str,
    delta_path: str,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Write semantic view to Delta table."""
    df = ctx.table(view_name)

    # Collect to Arrow (execute the plan)
    batches = df.collect()

    # Convert to PyArrow Table
    import pyarrow as pa
    table = pa.Table.from_batches(batches)

    # Write via delta-rs
    write_deltalake(
        delta_path,
        table,
        mode=mode,
        partition_by=partition_by,
        schema_mode="merge",  # Allow schema evolution
    )
```

### 3.3 CDF (Change Data Feed) Integration

**Read CDF for incremental processing:**

```python
from deltalake import DeltaTable

def get_cdf_changes(
    delta_path: str,
    *,
    starting_version: int,
    ending_version: int | None = None,
) -> pa.Table:
    """Get change data feed between versions."""
    dt = DeltaTable(delta_path)

    return dt.load_cdf(
        starting_version=starting_version,
        ending_version=ending_version,
    ).read_all()
```

**Register CDF as DataFusion table (Rust-level, for reference):**

```rust
// In Rust, use DeltaCdfTableProvider for native CDF querying
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::operations::cdf::CdfLoadBuilder;

let cdf_builder = CdfLoadBuilder::new(log_store, snapshot)
    .with_starting_version(start_version);

let cdf_provider = DeltaCdfTableProvider::try_new(cdf_builder)?;
ctx.register_table("cdf_changes", Arc::new(cdf_provider))?;
```

### 3.4 Schema Enforcement

**Validate schema before write:**

```python
def validate_delta_schema_compatibility(
    table: pa.Table,
    delta_path: str,
) -> None:
    """Validate Arrow schema is compatible with existing Delta schema."""
    try:
        dt = DeltaTable(delta_path)
        existing_schema = dt.schema().to_pyarrow()

        # Check for incompatible changes
        for field in table.schema:
            if field.name in existing_schema.names:
                existing_field = existing_schema.field(field.name)
                if not _types_compatible(field.type, existing_field.type):
                    raise ValueError(
                        f"Incompatible type for {field.name}: "
                        f"{field.type} vs {existing_field.type}"
                    )
    except Exception:
        # Table doesn't exist yet, any schema is valid
        pass


def _types_compatible(new_type: pa.DataType, existing_type: pa.DataType) -> bool:
    """Check if types are compatible for schema evolution."""
    # Same type is always compatible
    if new_type == existing_type:
        return True

    # Allow widening: int32 -> int64, float32 -> float64
    widening_allowed = {
        (pa.int32(), pa.int64()),
        (pa.float32(), pa.float64()),
    }
    return (new_type, existing_type) in widening_allowed
```

---

## Part 4: ast-grep Validation Patterns

### 4.1 Pre-Implementation Validation

**Find all existing join patterns before refactoring:**

```bash
# Find all DataFrame.join() calls in semantics module
ast-grep run -p '$DF.join($$$ARGS)' -l python src/semantics/

# Find all ctx.sql() calls with JOIN
ast-grep run -p 'ctx.sql($SQL)' -l python src/semantics/ | grep -i join

# Find all filter() calls (potential span overlap patterns)
ast-grep run -p '$DF.filter($$$)' -l python src/semantics/
```

**Validate no hardcoded table names remain:**

```bash
# Find string literals that look like table names
ast-grep run -p '"cst_refs"' -l python src/semantics/
ast-grep run -p '"cst_defs"' -l python src/semantics/
ast-grep run -p '"scip_occurrences"' -l python src/semantics/
```

### 4.2 Post-Implementation Validation

**Verify all DataFrames use builder pattern:**

```bash
# Find direct ctx.table() calls (should go through registry)
ast-grep scan --inline-rules "$(cat <<'YAML'
id: direct-table-access
language: Python
rule:
  pattern: ctx.table($TABLE_NAME)
  not:
    inside:
      kind: function_definition
      has:
        pattern: def _
severity: warning
message: Direct ctx.table() call - should use semantic catalog
YAML
)" src/semantics/
```

**Verify join strategies use inference:**

```bash
# Find manual join key specification (should be inferred)
ast-grep run -p 'join_keys=($$$)' -l python src/semantics/
```

### 4.3 Refactoring Patterns

**Rename function across codebase:**

```bash
# Preview rename
ast-grep run -p 'build_rel_name_symbol_df($$$ARGS)' \
    -r 'compile_name_symbol_relationship($$$ARGS)' \
    -l python src/

# Apply rename
ast-grep run -p 'build_rel_name_symbol_df($$$ARGS)' \
    -r 'compile_name_symbol_relationship($$$ARGS)' \
    -l python -U src/
```

**Add import statement:**

```bash
# Find files using SemanticCompiler without import
ast-grep scan --inline-rules "$(cat <<'YAML'
id: missing-import
language: Python
rule:
  pattern: SemanticCompiler($$$)
  not:
    inside:
      kind: module
      has:
        pattern: from semantics.compiler import SemanticCompiler
YAML
)" src/
```

---

## Part 5: cq Skill Validation Workflows

### 5.1 Pre-Change Impact Analysis

**Before modifying SemanticCompiler.relate():**

```bash
# Analyze all call sites
/cq calls SemanticCompiler.relate

# Check parameter flow for join_type
/cq impact SemanticCompiler.relate --param join_type

# Verify signature change viability
/cq sig-impact SemanticCompiler.relate \
    --to "relate(self, left: str, right: str, *, strategy: JoinStrategyType, origin: str)"
```

**Before modifying build_cpg():**

```bash
# Find all callers
/cq calls build_cpg

# Check what flows through runtime_profile
/cq impact build_cpg --param runtime_profile --depth 3
```

### 5.2 Import Cycle Detection

**Validate no cycles introduced:**

```bash
# Check for import cycles in semantics module
/cq imports --cycles --include "src/semantics/"

# Check specific module
/cq imports --module semantics.compiler
```

### 5.3 Exception Handling Audit

**Verify proper error handling:**

```bash
# Find exception patterns in semantics
/cq exceptions --include "src/semantics/"

# Check for bare except clauses
/cq exceptions --include "src/semantics/" | grep "bare except"
```

### 5.4 Scope Analysis for Refactoring

**Before extracting nested functions:**

```bash
# Analyze closure captures
/cq scopes src/semantics/compiler.py

# Check for free variables that would break extraction
/cq scopes src/semantics/pipeline.py
```

### 5.5 Combined Validation Workflow

**Full pre-implementation check:**

```bash
#!/bin/bash
# pre_implementation_check.sh

echo "=== Import Cycle Check ==="
./scripts/cq imports --cycles --include "src/semantics/"

echo "=== Exception Handling Audit ==="
./scripts/cq exceptions --include "src/semantics/" --limit 20

echo "=== Call Site Census for SemanticCompiler ==="
./scripts/cq calls SemanticCompiler --include "src/"

echo "=== Side Effects Check ==="
./scripts/cq side-effects --include "src/semantics/"
```

**Full post-implementation validation:**

```bash
#!/bin/bash
# post_implementation_validate.sh

echo "=== Verify No Direct Table Access ==="
ast-grep run -p 'ctx.table("$TABLE")' -l python src/semantics/

echo "=== Verify All Joins Use Strategy Inference ==="
ast-grep run -p 'join_keys=($$$)' -l python src/semantics/ | wc -l

echo "=== Verify No Hardcoded Table Names ==="
for table in cst_refs cst_defs cst_imports scip_occurrences; do
    count=$(ast-grep run -p "\"$table\"" -l python src/semantics/ 2>/dev/null | wc -l)
    echo "  $table: $count occurrences"
done

echo "=== Run Type Checker ==="
uv run pyright src/semantics/ --warnings

echo "=== Run Tests ==="
uv run pytest tests/unit/test_semantic* -v
```

---

## Part 6: Implementation Checklists

### 6.1 Per-Task Validation Checklist

For each task in the integration plan:

```markdown
## Task X.Y: [Task Name]

### Pre-Implementation
- [ ] Run `/cq calls` for affected functions
- [ ] Run `/cq impact` for modified parameters
- [ ] Check `/cq imports --cycles` baseline
- [ ] Run `ast-grep` to find existing patterns

### Implementation
- [ ] Follow DataFusion API patterns from this guide
- [ ] Use Rust UDF patterns from Section 2
- [ ] Use DeltaLake patterns from Section 3
- [ ] Add type annotations to all new functions

### Post-Implementation
- [ ] Run `ast-grep` validation patterns
- [ ] Run `/cq imports --cycles` (no new cycles)
- [ ] Run `uv run pyright` (no type errors)
- [ ] Run `uv run pytest tests/unit/test_semantic*`
- [ ] Run `uv run ruff check src/semantics/`
```

### 6.2 Phase Completion Gates

**Phase 1 (Foundation) Gate:**

```bash
# All must pass before Phase 2
uv run pytest tests/unit/test_semantic* -v
./scripts/cq imports --cycles --include "src/semantics/" | grep -c "cycle" | grep -q "^0$"
ast-grep run -p 'SCOPE_SEMANTICS' -l python src/obs/ | grep -q "scopes.py"
```

**Phase 2 (Integration) Gate:**

```bash
# All must pass before Phase 3
uv run pytest tests/integration/test_semantic_pipeline.py -v
./scripts/cq calls build_rel_name_symbol_df | grep -c "call site" | grep -q "^0$"
ast-grep run -p 'from relspec.relationship_datafusion import' -l python src/ | wc -l | grep -q "^0$"
```

**Phase 3 (Observability) Gate:**

```bash
# All must pass before Phase 4
ast-grep run -p 'stage_span($$$)' -l python src/semantics/compiler.py | grep -q "normalize"
ast-grep run -p 'stage_span($$$)' -l python src/semantics/compiler.py | grep -q "relate"
ast-grep run -p 'SCOPE_SEMANTICS' -l python src/semantics/ | wc -l | grep -v "^0$"
```

---

## Part 7: Error Handling Patterns

### 7.1 Graceful Degradation

**Handle missing optional tables:**

```python
def build_view_with_optional_input(
    ctx: SessionContext,
    required_table: str,
    optional_table: str,
) -> DataFrame:
    """Build view with graceful degradation for optional inputs."""
    from datafusion_engine.schema.introspection import table_names_snapshot

    available = set(table_names_snapshot(ctx))

    if required_table not in available:
        raise ValueError(f"Required table missing: {required_table}")

    base = ctx.table(required_table)

    if optional_table in available:
        optional = ctx.table(optional_table)
        return base.join(optional, join_keys=(["file_id"], ["file_id"]), how="left")
    else:
        # Return base with null columns for optional table's contribution
        return base.select(
            col("*"),
            lit(None).cast(pa.string()).alias("optional_field"),
        )
```

### 7.2 Schema Mismatch Handling

**Validate and adapt schema:**

```python
def adapt_schema_if_needed(
    df: DataFrame,
    expected_schema: pa.Schema,
) -> DataFrame:
    """Adapt DataFrame schema to match expected, if possible."""
    actual_schema = df.schema()

    projections = []
    for expected_field in expected_schema:
        if expected_field.name in actual_schema.names:
            actual_field = actual_schema.field(expected_field.name)
            if actual_field.type != expected_field.type:
                # Try to cast
                projections.append(
                    col(expected_field.name).cast(expected_field.type)
                )
            else:
                projections.append(col(expected_field.name))
        else:
            # Add null column for missing field
            projections.append(
                lit(None).cast(expected_field.type).alias(expected_field.name)
            )

    return df.select(*projections)
```

---

## Appendix A: Quick Reference Card

### DataFusion Python

| Operation | Syntax |
|-----------|--------|
| Get table | `ctx.table("name")` |
| Register view | `ctx.register_table("name", df)` |
| SQL query | `ctx.sql("SELECT ...")` |
| Join | `df.join(other, join_keys=([...], [...]), how="inner")` |
| Filter | `df.filter(col("x") > 0)` |
| Select | `df.select(col("a"), col("b").alias("c"))` |
| Window | `f.row_number().over(Window.partition_by(...).order_by(...))` |
| Aggregate | `df.aggregate([], [f.count(col("x"))])` |

### Rust UDFs

| UDF | Usage |
|-----|-------|
| `stable_hash64` | `stable_hash64(col1, col2, ...)` |
| `col_to_byte` | `col_to_byte(line_no, col, line_start, unit)` |
| `span_make` | `span_make(start, end)` |
| `prefixed_hash64` | `prefixed_hash64('prefix', col1, col2)` |

### DeltaLake

| Operation | Syntax |
|-----------|--------|
| Read table | `DeltaTable(path)` |
| Register | `ctx.register_table("name", delta_table)` |
| Write | `write_deltalake(path, table, mode="overwrite")` |
| Time travel | `DeltaTable(path, version=N)` |
| CDF | `dt.load_cdf(starting_version=N)` |

### ast-grep

| Pattern | Matches |
|---------|---------|
| `$VAR` | Single AST node |
| `$$$VAR` | Zero or more nodes |
| `$_VAR` | Non-capturing |
| `-U` flag | Apply changes |

### cq

| Command | Purpose |
|---------|---------|
| `calls` | Find call sites |
| `impact` | Trace parameter flow |
| `sig-impact` | Signature change analysis |
| `imports` | Import cycle detection |
| `exceptions` | Exception handling audit |
