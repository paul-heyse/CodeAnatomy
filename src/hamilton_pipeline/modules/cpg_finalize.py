"""Parameterized subDAG helpers for CPG finalization."""

from __future__ import annotations

from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.cpg_finalize_utils import finalize_cpg_table
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from relspec.runtime_artifacts import TableLike


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="stage",
        artifact="cpg_finalize_table",
    )
)
def final_table(
    table: TableLike,
    *,
    table_name: str,
    runtime_profile_spec: RuntimeProfileSpec,
) -> TableLike:
    """Finalize a CPG table within a parameterized subDAG.

    Returns
    -------
    TableLike
        Finalized CPG table.
    """
    return finalize_cpg_table(
        table,
        name=table_name,
        runtime_profile_spec=runtime_profile_spec,
    )


__all__ = ["final_table"]
