"""Extract plan builder helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from extract.coordination.materialization import (
    DataFusionPlanArtifact,
    datafusion_plan_from_reader,
    record_batch_reader_from_row_batches,
)
from extract.session import ExtractSession


def build_plan_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
    *,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    """Build a DataFusion plan artifact from row-batch iterables.

    Returns:
        DataFusionPlanArtifact: Planned extract artifact.
    """
    reader = record_batch_reader_from_row_batches(name, row_batches)
    return datafusion_plan_from_reader(name, reader, session=session)


__all__ = ["build_plan_from_row_batches"]
