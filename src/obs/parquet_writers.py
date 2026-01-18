"""Delta dataset writers for observability artifacts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from arrowdsl.io.delta import DeltaWriteOptions, write_dataset_delta


def write_obs_dataset(
    base_dir: str | Path,
    *,
    name: str,
    table: pa.Table,
    overwrite: bool = True,
    options: DeltaWriteOptions | None = None,
) -> str:
    """Write an observability dataset directory.

    Returns
    -------
    str
        Dataset directory path.
    """
    base = Path(base_dir)
    dataset_dir = base / name
    mode = "overwrite" if overwrite else "error"
    resolved = options or DeltaWriteOptions(
        mode=mode, schema_mode="overwrite" if overwrite else None
    )
    result = write_dataset_delta(table, str(dataset_dir), options=resolved)
    return result.path


__all__ = ["write_obs_dataset"]
