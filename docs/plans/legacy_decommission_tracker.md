# Legacy Decommission Tracker

This document tracks legacy code that can be safely removed after the Combined Library Utilization Implementation is complete and validated. The code listed here has been superseded by modern implementations using native DataFusion features, Delta CDF, and streaming query patterns.

## Overview

As we migrate to:
- Native DataFusion information_schema queries (vs. imperative override tracking)
- Delta Change Data Feed for incremental processing (vs. snapshot diffing)
- Streaming DataFrame/RecordBatch APIs (vs. materialized Arrow tables)

...several legacy components become obsolete. This tracker documents what can be removed and provides verification steps.

---

## Scope 6 Legacy Targets: Schema Registry Consolidation

**Source File:** `/home/paul/CodeAnatomy/src/datafusion_engine/schema_introspection.py`

### 1. `_TABLE_DEFINITION_OVERRIDES` (Module-level dict)

**Location:** Line 20

**Description:**
```python
_TABLE_DEFINITION_OVERRIDES: dict[int, dict[str, str]] = {}
```

Module-level dictionary tracking CREATE TABLE statements keyed by session context ID.

**Replacement:** TableProvider metadata + `information_schema.tables` queries + `SHOW CREATE TABLE` statements

**Status:** Deprecated with warning comment

**Notes:**
- Uses `id(ctx)` as key, which is fragile
- No cleanup when session contexts are destroyed
- Replaced by native DataFusion schema introspection


### 2. `record_table_definition_override()` function

**Location:** Lines 43-71

**Description:**
Records a CREATE TABLE DDL override for a session context.

**Signature:**
```python
def record_table_definition_override(
    ctx: SessionContext,
    *,
    name: str,
    ddl: str,
) -> None
```

**Replacement:** TableProvider metadata stored directly with table providers

**Status:** Already has deprecation warning emitted at runtime

**Call Sites:**
- `/home/paul/CodeAnatomy/src/datafusion_engine/registry_bridge.py`
- `/home/paul/CodeAnatomy/src/datafusion_engine/schema_registry.py`

**Notes:**
- Function emits `DeprecationWarning` when called
- All callers should migrate to TableProviderMetadata pattern


### 3. `table_definition_override()` function

**Location:** Lines 74-99

**Description:**
Retrieves recorded CREATE TABLE DDL override for a table.

**Signature:**
```python
def table_definition_override(ctx: SessionContext, *, name: str) -> str | None
```

**Replacement:** Query `information_schema` or use `SHOW CREATE TABLE`

**Status:** Already has deprecation warning emitted at runtime

**Call Sites:**
- Called within `SchemaIntrospector.table_definition()` as fallback (lines 788, 790)
- `/home/paul/CodeAnatomy/src/datafusion_engine/registry_bridge.py`
- `/home/paul/CodeAnatomy/src/datafusion_engine/schema_registry.py`

**Notes:**
- Function emits `DeprecationWarning` when called
- Used as fallback when `SHOW CREATE TABLE` query fails
- Should be removed once all table providers support native DDL reflection

---

## Scope 7 Legacy Targets: Delta CDF Incremental

**Source File:** `/home/paul/CodeAnatomy/src/incremental/diff.py`

### 4. `diff_snapshots()` function

**Location:** Lines 96-116

**Description:**
Diffs two repo snapshots using SQL FULL OUTER JOIN to compute file-level changes.

**Signature:**
```python
def diff_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table
```

**Replacement:** `diff_snapshots_with_delta_cdf()` using Delta CDF for incremental reads

**Status:** Keep as fallback for non-CDF scenarios, mark for eventual removal

**Call Sites:**
- `/home/paul/CodeAnatomy/src/hamilton_pipeline/modules/incremental.py` (line 558)
  - Used in non-CDF branch when CDF is unavailable

**Notes:**
- Materializes entire previous and current snapshots
- Performs full table join even when only a few files changed
- Should remain as fallback until all Delta tables have CDF enabled
- Mark for removal once CDF is universally available


### 5. `_added_sql()` helper

**Location:** Lines 52-67

**Description:**
Generates SQL for "added" files when no previous snapshot exists.

**Signature:**
```python
def _added_sql(cur_table: str) -> str
```

**Replacement:** CDF insert operations capture this natively

**Status:** Remove when `diff_snapshots()` is removed

**Notes:**
- Only used by `diff_snapshots()` and `diff_snapshots_with_cdf()`


### 6. `_diff_sql()` helper

**Location:** Lines 70-93

**Description:**
Generates SQL FULL OUTER JOIN query to diff current and previous snapshots.

**Signature:**
```python
def _diff_sql(cur_table: str, prev_table: str) -> str
```

**Replacement:** CDF operations provide change types directly

**Status:** Remove when `diff_snapshots()` is removed

**Notes:**
- Derives change_kind from join results: 'added', 'deleted', 'modified', 'renamed', 'unchanged'
- CDF provides `_change_type` column with similar semantics


### 7. `_session_context()` helper

**Location:** Lines 33-34

**Description:**
Creates a session context from a runtime profile.

**Signature:**
```python
def _session_context(profile: DataFusionRuntimeProfile) -> SessionContext
```

**Replacement:** Direct profile.session_context() calls

**Status:** Remove when `diff_snapshots()` is removed

**Notes:**
- Thin wrapper, no longer needed


### 8. `_register_table()` helper

**Location:** Lines 37-40

**Description:**
Registers a pyarrow.Table with a temporary name in a session context.

**Signature:**
```python
def _register_table(ctx: SessionContext, table: pa.Table, *, prefix: str) -> str
```

**Replacement:** N/A (streaming queries don't require registration)

**Status:** Remove when `diff_snapshots()` is removed

**Notes:**
- Generates UUID-based temporary table names
- Used for materializing Arrow tables in session context


### 9. `_deregister_table()` helper

**Location:** Lines 43-49

**Description:**
Safely deregisters a temporary table from session context.

**Signature:**
```python
def _deregister_table(ctx: SessionContext, name: str | None) -> None
```

**Replacement:** N/A (streaming queries don't require cleanup)

**Status:** Remove when `diff_snapshots()` is removed

**Notes:**
- Handles missing `deregister_table` method gracefully
- Suppresses various exception types during cleanup

---

## Scope 3 + 7 Legacy Targets: Streaming + CDF

### 10. `read_delta_table_from_path()` function

**Source File:** `/home/paul/CodeAnatomy/src/datafusion_engine/runtime.py`

**Location:** Line 4331

**Description:**
Reads a Delta table snapshot and materializes it as a pyarrow.Table.

**Signature:**
```python
def read_delta_table_from_path(
    path: str,
    *,
    storage_options: Mapping[str, str] | None = None,
    delta_scan: DeltaScanOptions | None = None,
) -> pa.Table
```

**Replacement:** Register Delta table as external table and query via streaming DataFrame API

**Status:** Mark for deprecation once all call sites migrate to streaming

**Call Sites Materializing Arrow Tables:**

1. **`src/incremental/scip_snapshot.py`**
   - Reads SCIP index snapshots
   - Should use streaming queries instead

2. **`src/incremental/snapshot.py`**
   - Reads repo file snapshots
   - Should use streaming queries instead

3. **`src/incremental/invalidations.py`**
   - Reads invalidation records
   - Should use streaming queries instead

4. **`src/incremental/fingerprint_changes.py`**
   - Reads fingerprint change records
   - Should use streaming queries instead

5. **`src/hamilton_pipeline/modules/incremental.py`**
   - Reads previous snapshot for diffing
   - Should migrate to CDF + streaming

6. **`src/hamilton_pipeline/modules/params.py`**
   - Loads parameter tables
   - Should use streaming queries instead

7. **`src/hamilton_pipeline/modules/outputs.py`**
   - Loads output tables
   - Should use streaming queries instead

8. **`src/hamilton_pipeline/arrow_adapters.py`**
   - Used in `ArrowDeltaLoader.load_data()` (line 63)
   - Should be removed with adapter classes

**Notes:**
- Materializes entire table into memory
- Bypasses streaming query benefits
- Each call site should migrate to:
  1. Register Delta table using `register_delta_table_external()`
  2. Query using `ctx.sql()` to get streaming DataFrame
  3. Process using DataFusion execution plans

---

## Scope 3 + 10 Legacy Targets: Streaming + DataFusion-native

### 11. `ArrowDeltaLoader` class

**Source File:** `/home/paul/CodeAnatomy/src/hamilton_pipeline/arrow_adapters.py`

**Location:** Lines 21-71

**Description:**
Hamilton data loader adapter that loads Delta tables as materialized pyarrow.Table.

**Replacement:** Hamilton pipelines should consume DataFusion DataFrame or Ibis expressions directly

**Status:** Remove once Hamilton pipelines are refactored for streaming

**Dependencies:**
- Uses `read_delta_table_from_path()` internally
- Registered with Hamilton via `register_adapter()`

**Notes:**
- Forces materialization of potentially large tables
- Prevents lazy query optimization
- Replacement pattern:
  - Accept `SessionContext` + table name instead of materialized table
  - Use DataFusion/Ibis expressions throughout Hamilton DAG
  - Materialize only at final output stage


### 12. `ArrowDeltaSaver` class

**Source File:** `/home/paul/CodeAnatomy/src/hamilton_pipeline/arrow_adapters.py`

**Location:** Lines 74-123

**Description:**
Hamilton data saver adapter that writes pyarrow.Table to Delta tables.

**Replacement:** Hamilton pipelines should emit DataFusion DataFrame or Ibis expressions for lazy evaluation

**Status:** Remove once Hamilton pipelines are refactored for streaming

**Dependencies:**
- Uses `write_table_delta()` from storage module
- Registered with Hamilton via `register_adapter()`

**Notes:**
- Requires materialized Arrow table
- Replacement pattern:
  - Accept DataFusion DataFrame or Ibis expression
  - Use native DataFusion `COPY TO` or similar for direct streaming writes
  - Avoid intermediate materialization

---

## Migration Checklist Template

For each legacy target, follow this checklist:

### Planning Phase
- [ ] Identify all call sites using grep/code search
- [ ] Document replacement pattern with code example
- [ ] Assess risk and impact (low/medium/high)
- [ ] Create backup branch for rollback if needed

### Implementation Phase
- [ ] Implement replacement functionality
- [ ] Update all direct call sites
- [ ] Update all indirect call sites (functions that wrap the legacy function)
- [ ] Add deprecation warning to legacy function (if not already present)
- [ ] Update documentation to reference new pattern

### Validation Phase
- [ ] Run full test suite
- [ ] Verify replacement works correctly in production workload
- [ ] Check performance metrics (memory, speed) meet or exceed legacy
- [ ] Confirm no remaining usages via grep/ripgrep

### Removal Phase
- [ ] Remove legacy function/class
- [ ] Remove associated tests that only test legacy behavior
- [ ] Remove deprecation warnings from any related code
- [ ] Update CHANGELOG with removal notice
- [ ] Tag commit with migration milestone

---

## Verification Queries

Use these commands to verify removal progress and find remaining usages:

### Check for Schema Override Usage
```bash
# Find record_table_definition_override calls
grep -r "record_table_definition_override" src/

# Find table_definition_override calls
grep -r "table_definition_override" src/

# Find _TABLE_DEFINITION_OVERRIDES references
grep -r "_TABLE_DEFINITION_OVERRIDES" src/
```

### Check for Snapshot Diff Usage
```bash
# Find diff_snapshots calls (non-CDF)
grep -r "diff_snapshots(" src/

# Find associated helper usage
grep -r "_added_sql\|_diff_sql\|_register_table\|_deregister_table" src/
```

### Check for Materialized Delta Table Usage
```bash
# Find read_delta_table_from_path calls
grep -r "read_delta_table_from_path" src/

# Find materialization sites in incremental modules
grep -r "\.to_arrow_table()" src/incremental/
grep -r "\.to_arrow_table()" src/hamilton_pipeline/
```

### Check for Hamilton Arrow Adapters Usage
```bash
# Find ArrowDeltaLoader usage
grep -r "ArrowDeltaLoader" src/

# Find ArrowDeltaSaver usage
grep -r "ArrowDeltaSaver" src/

# Find adapter registration
grep -r "register_arrow_delta_adapters" src/
```

### Comprehensive Check
```bash
# Check all deprecated patterns at once
rg "record_table_definition_override|table_definition_override|diff_snapshots\(|read_delta_table_from_path|ArrowDeltaLoader|ArrowDeltaSaver" src/ --files-with-matches
```

---

## Removal Priority

### Phase 1: High Priority (Remove First)
1. **Schema override functions** - Already deprecated, simple to remove
   - `record_table_definition_override()`
   - `table_definition_override()`
   - `_TABLE_DEFINITION_OVERRIDES`

### Phase 2: Medium Priority (Remove After CDF Stabilization)
2. **Non-CDF snapshot diffing** - Keep as fallback initially
   - `diff_snapshots()` and its helpers
   - Remove once Delta CDF is proven stable in production

### Phase 3: Low Priority (Remove After Full Streaming Migration)
3. **Materialized Delta readers** - Requires Hamilton refactoring
   - `read_delta_table_from_path()`
   - Call sites in incremental modules
   - Call sites in Hamilton modules

4. **Hamilton Arrow adapters** - Requires pipeline architecture change
   - `ArrowDeltaLoader`
   - `ArrowDeltaSaver`
   - Remove once Hamilton pipelines work with streaming DataFrames

---

## Success Criteria

Legacy code is successfully decommissioned when:

1. All grep queries return zero results
2. All tests pass without the legacy code
3. Production workloads run successfully for at least one full cycle
4. Memory usage decreases (no full table materializations)
5. Query performance meets or exceeds baseline
6. Documentation updated to reflect new patterns
7. No deprecation warnings in logs

---

## Rollback Plan

If issues arise after removal:

1. **Immediate Rollback:**
   - Revert commit removing legacy code
   - Restore from backup branch
   - File incident report with details

2. **Investigation:**
   - Identify which replacement pattern failed
   - Determine root cause
   - Document findings in this tracker

3. **Re-attempt:**
   - Fix replacement implementation
   - Add specific test coverage for failure case
   - Follow migration checklist again

---

## Notes

- This tracker should be updated as legacy code is removed
- Mark sections as **[REMOVED - YYYY-MM-DD]** when complete
- Keep this document until all phases complete for historical reference
- Consider automated checks in CI to prevent reintroduction of legacy patterns
