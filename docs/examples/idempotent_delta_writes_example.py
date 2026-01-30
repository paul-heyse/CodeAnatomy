"""Example: Idempotent Delta Lake writes with WritePipeline.

This example demonstrates how to pass app_id/version through WritePipeline
format options so Delta commits are idempotent.
"""

from __future__ import annotations

import logging

import pyarrow as pa

from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from obs.datafusion_runs import create_run_context
from storage.deltalake.delta import IdempotentWriteOptions

logger = logging.getLogger(__name__)


def _write_delta_idempotent(
    path: str,
    table: pa.Table,
    *,
    idempotent: IdempotentWriteOptions,
    mode: WriteMode = WriteMode.APPEND,
    schema_mode: str | None = None,
    partition_by: tuple[str, ...] = (),
) -> None:
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    df = datafusion_from_arrow(ctx, name="delta_write", value=table)
    format_options: dict[str, object] = {
        "app_id": idempotent.app_id,
        "version": idempotent.version,
    }
    if schema_mode is not None:
        format_options["schema_mode"] = schema_mode
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=path,
            format=WriteFormat.DELTA,
            mode=mode,
            partition_by=partition_by,
            format_options=format_options,
        )
    )


def example_explicit_idempotent_write() -> None:
    """Write Delta table with explicit idempotent options."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }
    )
    idempotent_opts = IdempotentWriteOptions(app_id="my_pipeline_run_12345", version=0)
    _write_delta_idempotent(
        "/tmp/delta_table",
        table,
        idempotent=idempotent_opts,
        schema_mode="merge",
    )
    logger.info("Wrote idempotent Delta commit with app_id=%s", idempotent_opts.app_id)


def example_run_context_idempotent_write() -> None:
    """Write Delta tables with automatic commit sequencing."""
    run = create_run_context(label="incremental_pipeline")
    table1 = pa.table({"id": [1, 2, 3], "category": ["A", "B", "A"]})
    opts1, run = run.next_commit_version()
    _write_delta_idempotent("/tmp/delta_table_1", table1, idempotent=opts1)
    table2 = pa.table({"id": [4, 5, 6], "category": ["B", "C", "C"]})
    opts2, run = run.next_commit_version()
    _write_delta_idempotent("/tmp/delta_table_2", table2, idempotent=opts2)
    logger.info("Completed %s commits for run_id=%s", run.commit_sequence, run.run_id)


def example_retry_safety() -> None:
    """Demonstrate retry safety of idempotent writes."""
    table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
    run = create_run_context(label="retry_demo")
    idempotent_opts, _ = run.next_commit_version()
    _write_delta_idempotent("/tmp/retry_safe_table", table, idempotent=idempotent_opts)
    _write_delta_idempotent("/tmp/retry_safe_table", table, idempotent=idempotent_opts)
    logger.info("Retried idempotent write for app_id=%s", idempotent_opts.app_id)


if __name__ == "__main__":
    example_explicit_idempotent_write()
    example_run_context_idempotent_write()
    example_retry_safety()
