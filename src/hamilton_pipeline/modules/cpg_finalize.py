"""Parameterized subDAG helpers for CPG finalization."""

from __future__ import annotations

from hamilton.function_modifiers import tag

from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.cpg_finalize_utils import finalize_cpg_table
from relspec.runtime_artifacts import TableLike


@tag(layer="execution", artifact="cpg_finalize_table", kind="stage")
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
