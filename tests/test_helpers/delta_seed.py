"""Helpers for seeding Delta tables in tests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.arrow_seed import register_arrow_table

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class DeltaSeedOptions:
    """Options for writing Delta tables in tests."""

    profile: DataFusionRuntimeProfile | None = None
    table_name: str = "delta_table"
    partition_by: tuple[str, ...] | None = None
    schema_mode: str | None = None
    mode: WriteMode = WriteMode.OVERWRITE


def write_delta_table(
    tmp_path: Path,
    *,
    table: pa.Table,
    options: DeltaSeedOptions | None = None,
) -> tuple[DataFusionRuntimeProfile, SessionContext, Path]:
    """Write a Delta table to a temporary path and return context metadata.

    Parameters
    ----------
    tmp_path
        Temporary path fixture.
    table
        PyArrow table to write.
    options
        Optional seed configuration overrides.

    Returns
    -------
    tuple[DataFusionRuntimeProfile, SessionContext, Path]
        Runtime profile, session context, and Delta table path.
    """
    resolved_options = options or DeltaSeedOptions()
    runtime_profile = (
        resolved_options.profile
        if resolved_options.profile is not None
        else DataFusionRuntimeProfile()
    )
    ctx = runtime_profile.session_context()
    seed = register_arrow_table(ctx, name="delta_seed", value=table)
    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    format_options: dict[str, object] = {}
    if resolved_options.schema_mode is not None:
        format_options["schema_mode"] = resolved_options.schema_mode
    destination = tmp_path / resolved_options.table_name
    pipeline.write(
        WriteRequest(
            source=seed,
            destination=str(destination),
            format=WriteFormat.DELTA,
            mode=resolved_options.mode,
            partition_by=resolved_options.partition_by or (),
            format_options=format_options or None,
        )
    )
    return runtime_profile, ctx, destination
