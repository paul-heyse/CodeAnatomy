# Idempotent Delta Writes Implementation - Scope 10

## Overview

Implemented idempotent write support for Delta Lake using `CommitProperties` with `app_id` and `version` for retry safety. This enables safe "at-least-once" semantics where duplicate commits are automatically detected and skipped.

## Implementation Summary

### 1. Files Modified

#### **src/storage/deltalake/delta.py**

Added the following components:

1. **`IdempotentWriteOptions` dataclass**
   - Contains `app_id` (unique application/pipeline identifier) and `version` (commit sequence number)
   - Frozen dataclass for immutability

2. **`write_deltalake_idempotent()` function**
   - Main function for idempotent Delta writes
   - Creates `Transaction` object from idempotent options
   - Passes transaction via `CommitProperties.app_transactions`
   - Includes retry logic with duplicate commit detection
   - Supports all standard Delta write options (mode, partition_by, predicate, etc.)

3. **`write_table_delta_idempotent()` convenience function**
   - Wrapper that takes `run_id` and `commit_sequence` directly
   - Creates `IdempotentWriteOptions` internally
   - Simplifies common use case

4. **`_is_duplicate_commit_error()` helper**
   - Detects when a commit was rejected due to duplicate (app_id, version)
   - Allows retry logic to skip duplicate commits gracefully

5. **`_write_deltalake_idempotent_once()` helper**
   - Executes a single Delta write with proper parameter handling
   - Handles conditional parameters (writer_properties, predicate, etc.)

#### **src/obs/datafusion_runs.py**

Extended the existing `DataFusionRun` class:

1. **Added `_commit_sequence` field**
   - Tracks current commit sequence for idempotent writes
   - Starts at 0 for new runs

2. **Added `commit_sequence` property**
   - Read-only access to current commit sequence

3. **Added `next_commit_version()` method**
   - Returns `(IdempotentWriteOptions, DataFusionRun)` tuple
   - Idempotent options use current sequence
   - Returns new run instance with incremented sequence
   - Immutable pattern for thread safety

4. **Added `with_sequence()` method**
   - Sets commit sequence to specific value
   - Useful for resuming from checkpoints

5. **Added `create_run_context()` helper**
   - Creates new run or resumes existing run by ID
   - Convenience function for idempotent write workflows

6. **Updated `payload()` method**
   - Includes `commit_sequence` in diagnostics payload

### 2. Key Design Decisions

#### Delta Lake Transaction Mechanism

Delta Lake's idempotent commit mechanism uses `Transaction` objects passed via `CommitProperties`:

```python
from deltalake._internal import Transaction
from deltalake import CommitProperties

# Create transaction with app_id and version
transaction = Transaction(app_id="my_run_123", version=5)

# Pass in commit properties
commit_props = CommitProperties(app_transactions=[transaction])

# Delta Lake will reject duplicates with same (app_id, version)
write_deltalake(path, data, commit_properties=commit_props)
```

#### Immutable Run Pattern

`DataFusionRun.next_commit_version()` returns a new run instance rather than mutating:

```python
run = create_run_context(label="pipeline")
options1, run = run.next_commit_version()  # version=0, run updated
options2, run = run.next_commit_version()  # version=1, run updated
```

This pattern:
- Prevents accidental state mutation
- Enables safe concurrent usage
- Makes commit sequence explicit in control flow

#### Retry Safety

When a commit with duplicate (app_id, version) is attempted:

1. Delta Lake rejects the commit with `CommitFailedError`
2. `_is_duplicate_commit_error()` detects the specific error
3. The write function treats it as success (not an error)
4. This enables safe retry logic without data duplication

#### Integration with Existing Code

The implementation:
- Preserves all existing `DataFusionRun` functionality
- Adds idempotent writes as optional feature
- Maintains backward compatibility
- Follows existing code patterns (e.g., similar to `_write_deltalake_once`)

### 3. Usage Examples

#### Basic Idempotent Write

```python
from storage.deltalake.delta import (
    IdempotentWriteOptions,
    DeltaWriteOptions,
    write_deltalake_idempotent,
)

# Explicit idempotent options
idempotent = IdempotentWriteOptions(
    app_id="pipeline_run_123",
    version=0,
)

# Write with idempotency
result = write_deltalake_idempotent(
    path="/data/delta_table",
    data=arrow_table,
    options=DeltaWriteOptions(mode="append"),
    idempotent=idempotent,
)
```

#### Using Run Context

```python
from obs.datafusion_runs import create_run_context

# Create run context
run = create_run_context(label="incremental_build")

# First commit
opts1, run = run.next_commit_version()
write_deltalake_idempotent(path1, data1, options=..., idempotent=opts1)

# Second commit (automatic sequence increment)
opts2, run = run.next_commit_version()
write_deltalake_idempotent(path2, data2, options=..., idempotent=opts2)
```

#### Convenience Wrapper

```python
from storage.deltalake.delta import write_table_delta_idempotent

# Simple API for common case
result = write_table_delta_idempotent(
    table=arrow_table,
    path="/data/delta_table",
    options=DeltaWriteOptions(mode="append"),
    run_id="pipeline_run_123",
    commit_sequence=5,
)
```

#### Resume from Checkpoint

```python
# Resume existing run
run = create_run_context(
    label="resumed_pipeline",
    run_id="saved_run_id_123",
)

# Continue from last successful commit
run = run.with_sequence(last_successful_sequence + 1)

# Next writes use resumed sequence
opts, run = run.next_commit_version()
```

### 4. Error Handling

#### Duplicate Commits

When a duplicate (app_id, version) is detected:
- The write is skipped (no exception raised)
- The function returns normally with the current table version
- This enables safe retry patterns

#### Other Errors

Non-duplicate errors (concurrent conflicts, schema issues, etc.):
- Follow existing retry logic from `DeltaWriteRetryPolicy`
- Use exponential backoff
- Re-raise after max attempts

### 5. Testing Considerations

To test idempotent writes:

1. **Duplicate Detection**
   - Write with (app_id, version)
   - Retry with same (app_id, version)
   - Verify no error and data not duplicated

2. **Sequence Tracking**
   - Create run context
   - Get multiple commit versions
   - Verify sequence increments correctly

3. **Resume from Checkpoint**
   - Create run with specific sequence
   - Verify next commit uses correct version

4. **Concurrent Writes**
   - Multiple processes with different app_ids
   - Verify isolation and correctness

### 6. Integration Points

The idempotent write functionality integrates with:

1. **Existing Delta Write Functions**
   - `write_table_delta()` - baseline non-idempotent writes
   - `write_dataset_delta()` - dataset writes
   - `upsert_dataset_partitions_delta()` - partition upserts

2. **DataFusion Pipeline**
   - `DataFusionRun` tracks both lifecycle and commit sequence
   - Diagnostics sinks record commit_sequence in payloads

3. **Retry Infrastructure**
   - `DeltaWriteRetryPolicy` applies to idempotent writes
   - Duplicate detection happens before retry logic

### 7. Benefits

1. **Retry Safety**: Safe to retry failed writes without data duplication
2. **At-Least-Once Semantics**: Guarantees data is written at least once
3. **Pipeline Resumption**: Can resume from checkpoints with commit sequence
4. **Observability**: Commit sequence tracked in diagnostics
5. **Backward Compatible**: Existing code unaffected, opt-in feature

### 8. Limitations

1. **Delta Lake Requirement**: Requires Delta Lake, not Parquet
2. **Version Tracking**: Application must track (app_id, version) pairs
3. **Sequence Gaps**: Gaps in version sequence are allowed but may complicate auditing
4. **No Distributed Sequence**: Each run maintains its own sequence counter

### 9. Future Enhancements

Potential future improvements:

1. **Persistent Run State**: Save run state (ID, sequence) to Delta for recovery
2. **Sequence Validation**: Detect and warn about sequence gaps or duplicates
3. **Transaction Log**: Record commit attempts with outcomes
4. **DataFusion Integration**: Use idempotent writes in `write_datafusion_delta()`
5. **Batch Writes**: Support multiple table writes with single commit sequence

## API Reference

### IdempotentWriteOptions

```python
@dataclass(frozen=True)
class IdempotentWriteOptions:
    """Options for idempotent Delta writes."""
    app_id: str      # Unique application/pipeline identifier
    version: int     # Commit sequence number for this app_id
```

### write_deltalake_idempotent

```python
def write_deltalake_idempotent(
    path: str,
    data: DeltaWriteInput,
    *,
    options: DeltaWriteOptions,
    idempotent: IdempotentWriteOptions | None = None,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    """Write to Delta with idempotent commit properties."""
```

### write_table_delta_idempotent

```python
def write_table_delta_idempotent(
    table: DeltaWriteInput,
    path: str,
    *,
    options: DeltaWriteOptions,
    run_id: str,
    commit_sequence: int,
    storage_options: StorageOptions | None = None,
) -> DeltaWriteResult:
    """Write Delta table with idempotent commit properties."""
```

### DataFusionRun Extensions

```python
@dataclass
class DataFusionRun:
    # ... existing fields ...
    _commit_sequence: int = 0

    @property
    def commit_sequence(self) -> int:
        """Current commit sequence number."""

    def next_commit_version(self) -> tuple[IdempotentWriteOptions, DataFusionRun]:
        """Return idempotent options and updated run for next commit."""

    def with_sequence(self, sequence: int) -> DataFusionRun:
        """Return a run with a specific commit sequence."""
```

### create_run_context

```python
def create_run_context(
    *,
    label: str = "idempotent_run",
    run_id: str | None = None,
    sink: DiagnosticsSink | None = None,
    metadata: Mapping[str, object] | None = None,
) -> DataFusionRun:
    """Create a run context for idempotent writes."""
```

## Files Added

- `/home/paul/CodeAnatomy/docs/examples/idempotent_delta_writes_example.py` - Comprehensive usage examples
- `/home/paul/CodeAnatomy/docs/idempotent_delta_writes_implementation.md` - This document

## Files Modified

- `/home/paul/CodeAnatomy/src/storage/deltalake/delta.py` - Added idempotent write functionality (+279 lines)
- `/home/paul/CodeAnatomy/src/obs/datafusion_runs.py` - Extended DataFusionRun for commit tracking (+123 lines)

## Summary

Successfully implemented Scope 10 - Idempotent Delta Writes via CommitProperties. The implementation:

✅ Adds `IdempotentWriteOptions` dataclass with app_id and version
✅ Implements `write_deltalake_idempotent()` with retry safety
✅ Adds `write_table_delta_idempotent()` convenience wrapper
✅ Extends `DataFusionRun` with commit sequence tracking
✅ Provides `create_run_context()` helper for run management
✅ Handles duplicate commits gracefully
✅ Maintains backward compatibility
✅ Includes comprehensive examples and documentation
