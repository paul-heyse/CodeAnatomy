"""Helpers for seeding Delta tables in tests."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.arrow_seed import register_arrow_table

if TYPE_CHECKING:
    from datafusion import SessionContext


def write_delta_table(
    tmp_path: Path,
    *,
    table: pa.Table,
    profile: DataFusionRuntimeProfile | None = None,
    table_name: str = "delta_table",
    partition_by: tuple[str, ...] | None = None,
    schema_mode: str | None = None,
    mode: WriteMode = WriteMode.OVERWRITE,
) -> tuple[DataFusionRuntimeProfile, SessionContext, Path]:
    """Write a Delta table to a temporary path and return context metadata.

    Parameters
    ----------
    tmp_path
        Temporary path fixture.
    table
        PyArrow table to write.
    profile
        Optional runtime profile to reuse.
    table_name
        Subdirectory name for the Delta table.
    partition_by
        Optional partition columns.
    schema_mode
        Optional Delta schema mode.
    mode
        Write mode for the Delta table.

    Returns
    -------
    tuple[DataFusionRuntimeProfile, SessionContext, Path]
        Runtime profile, session context, and Delta table path.
    """
    runtime_profile = profile or DataFusionRuntimeProfile()
    ctx = runtime_profile.session_context()
    seed = register_arrow_table(ctx, name="delta_seed", value=table)
    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    format_options: dict[str, object] = {}
    if schema_mode is not None:
        format_options["schema_mode"] = schema_mode
    destination = tmp_path / table_name
    pipeline.write(
        WriteRequest(
            source=seed,
            destination=str(destination),
            format=WriteFormat.DELTA,
            mode=mode,
            partition_by=partition_by,
            format_options=format_options or None,
        )
    )
    return runtime_profile, ctx, destination
