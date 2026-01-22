"""Example: Idempotent Delta Lake Writes with CommitProperties.

This example demonstrates how to use the idempotent write functionality
to enable safe retries of Delta Lake writes.
"""

from storage.deltalake.delta import (
    DeltaWriteOptions,
    IdempotentWriteOptions,
    write_deltalake_idempotent,
    write_table_delta_idempotent,
)
from obs.datafusion_runs import create_run_context
import pyarrow as pa


# Example 1: Basic idempotent write with explicit options
def example_explicit_idempotent_write():
    """Write Delta table with explicit idempotent options."""
    # Create a sample PyArrow table
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300],
    })

    # Define idempotent write options
    idempotent_opts = IdempotentWriteOptions(
        app_id="my_pipeline_run_12345",
        version=0,  # First commit in this run
    )

    # Define standard Delta write options
    write_opts = DeltaWriteOptions(
        mode="append",
        schema_mode="merge",
    )

    # Write with idempotency
    result = write_deltalake_idempotent(
        path="/tmp/delta_table",
        data=table,
        options=write_opts,
        idempotent=idempotent_opts,
    )

    print(f"Wrote to Delta table at {result.path}, version {result.version}")

    # If this write is retried with the same app_id + version,
    # Delta Lake will recognize it as a duplicate and skip it


# Example 2: Using DataFusionRun for automatic commit sequence tracking
def example_run_context_idempotent_write():
    """Write multiple Delta tables with automatic commit sequence tracking."""
    # Create a run context
    run = create_run_context(label="incremental_pipeline")

    print(f"Started run: {run.run_id}")

    # First write: get idempotent options and updated run
    table1 = pa.table({
        "id": [1, 2, 3],
        "category": ["A", "B", "A"],
    })

    idempotent_opts1, run = run.next_commit_version()
    print(f"First commit: app_id={idempotent_opts1.app_id}, version={idempotent_opts1.version}")

    result1 = write_deltalake_idempotent(
        path="/tmp/delta_table_1",
        data=table1,
        options=DeltaWriteOptions(mode="append"),
        idempotent=idempotent_opts1,
    )

    # Second write: automatic sequence increment
    table2 = pa.table({
        "id": [4, 5, 6],
        "category": ["B", "C", "C"],
    })

    idempotent_opts2, run = run.next_commit_version()
    print(f"Second commit: app_id={idempotent_opts2.app_id}, version={idempotent_opts2.version}")

    result2 = write_deltalake_idempotent(
        path="/tmp/delta_table_2",
        data=table2,
        options=DeltaWriteOptions(mode="append"),
        idempotent=idempotent_opts2,
    )

    print(f"Completed {run.commit_sequence} commits")


# Example 3: Using convenience wrapper
def example_convenience_wrapper():
    """Write Delta table using the convenience wrapper."""
    table = pa.table({
        "timestamp": [1000, 2000, 3000],
        "event": ["start", "process", "end"],
    })

    # Use convenience wrapper that takes run_id and commit_sequence directly
    result = write_table_delta_idempotent(
        table=table,
        path="/tmp/events_table",
        options=DeltaWriteOptions(mode="append"),
        run_id="pipeline_run_67890",
        commit_sequence=5,  # This is the 6th commit in the run (0-indexed)
    )

    print(f"Wrote to {result.path} with idempotency (run_id=pipeline_run_67890, version=5)")


# Example 4: Retry safety demonstration
def example_retry_safety():
    """Demonstrate retry safety of idempotent writes."""
    table = pa.table({
        "id": [1, 2, 3],
        "value": [10, 20, 30],
    })

    run = create_run_context(label="retry_demo")
    idempotent_opts, run = run.next_commit_version()

    write_opts = DeltaWriteOptions(
        mode="append",
        retry_policy=None,  # Disable automatic retries for this demo
    )

    # First attempt - succeeds
    try:
        result1 = write_deltalake_idempotent(
            path="/tmp/retry_safe_table",
            data=table,
            options=write_opts,
            idempotent=idempotent_opts,
        )
        print(f"First write succeeded: version {result1.version}")
    except Exception as e:
        print(f"First write failed: {e}")

    # Retry with same idempotent options - Delta Lake will recognize
    # this as a duplicate and skip it (no error raised)
    try:
        result2 = write_deltalake_idempotent(
            path="/tmp/retry_safe_table",
            data=table,
            options=write_opts,
            idempotent=idempotent_opts,  # Same app_id and version
        )
        print(f"Retry succeeded (duplicate skipped): version {result2.version}")
    except Exception as e:
        print(f"Retry failed: {e}")


# Example 5: Resuming from checkpoint
def example_resume_from_checkpoint():
    """Resume a pipeline from a specific commit sequence."""
    # Imagine we saved the run_id and last successful commit sequence
    saved_run_id = "pipeline_run_abc123"
    last_successful_sequence = 42

    # Resume the run
    run = create_run_context(
        label="resumed_pipeline",
        run_id=saved_run_id,
    )

    # Set the commit sequence to continue from where we left off
    run = run.with_sequence(last_successful_sequence + 1)

    print(f"Resumed run {run.run_id} at commit sequence {run.commit_sequence}")

    # Next write will use sequence 43
    table = pa.table({"id": [1, 2, 3]})
    idempotent_opts, run = run.next_commit_version()

    print(f"Next commit will be: app_id={idempotent_opts.app_id}, version={idempotent_opts.version}")


# Example 6: Idempotent overwrite with predicate
def example_idempotent_overwrite():
    """Perform idempotent partition overwrite."""
    table = pa.table({
        "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
        "metric": ["cpu", "memory", "cpu"],
        "value": [75.5, 80.2, 65.3],
    })

    run = create_run_context(label="partition_update")
    idempotent_opts, run = run.next_commit_version()

    # Overwrite specific partition with idempotency
    result = write_deltalake_idempotent(
        path="/tmp/metrics_table",
        data=table,
        options=DeltaWriteOptions(
            mode="overwrite",
            predicate="date = '2024-01-01'",  # Only overwrite this partition
            partition_by=["date"],
        ),
        idempotent=idempotent_opts,
    )

    print(f"Idempotently overwrote partition: version {result.version}")


if __name__ == "__main__":
    print("=== Example 1: Explicit Idempotent Write ===")
    example_explicit_idempotent_write()

    print("\n=== Example 2: Run Context with Automatic Sequencing ===")
    example_run_context_idempotent_write()

    print("\n=== Example 3: Convenience Wrapper ===")
    example_convenience_wrapper()

    print("\n=== Example 4: Retry Safety ===")
    example_retry_safety()

    print("\n=== Example 5: Resume from Checkpoint ===")
    example_resume_from_checkpoint()

    print("\n=== Example 6: Idempotent Overwrite ===")
    example_idempotent_overwrite()
