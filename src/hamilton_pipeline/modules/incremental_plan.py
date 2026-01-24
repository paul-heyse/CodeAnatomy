"""Hamilton nodes for incremental plan fingerprinting."""

from __future__ import annotations

from typing import TYPE_CHECKING

from hamilton.function_modifiers import tag

from incremental.delta_context import DeltaAccessContext
from incremental.plan_fingerprints import read_plan_fingerprints, write_plan_fingerprints
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from incremental.types import IncrementalConfig
from relspec.incremental import diff_plan_fingerprints, plan_fingerprint_map
from relspec.plan_catalog import PlanCatalog

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.execution import IbisExecutionContext
    from relspec.incremental import IncrementalDiff


@tag(layer="incremental", artifact="incremental_plan_diff", kind="mapping")
def incremental_plan_diff(
    plan_catalog: PlanCatalog,
    incremental_config: IncrementalConfig,
    ctx: ExecutionContext,
    ibis_execution: IbisExecutionContext,
) -> IncrementalDiff | None:
    """Compute incremental plan diff and persist current fingerprints.

    Returns
    -------
    IncrementalDiff | None
        Diff summary when incremental is enabled, otherwise None.
    """
    if not incremental_config.enabled or incremental_config.state_dir is None:
        return None
    try:
        runtime = IncrementalRuntime.build(ctx=ctx)
    except ValueError:
        return None
    state_store = StateStore(root=incremental_config.state_dir)
    context = DeltaAccessContext(runtime=runtime)
    previous = read_plan_fingerprints(state_store, context=context)
    current = plan_fingerprint_map(plan_catalog)
    diff = diff_plan_fingerprints(previous, current)
    write_plan_fingerprints(state_store, current, execution=ibis_execution)
    return diff


__all__ = ["incremental_plan_diff"]
