"""Ibis bridge helpers for extract plan outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import SourceToIbisOptions, source_to_ibis

type ExtractPlanSource = Plan | IbisPlan | IbisTable | TableLike | RecordBatchReaderLike


def bridge_plan(
    source: ExtractPlanSource,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str | None = None,
    overwrite: bool = True,
) -> IbisPlan:
    """Bridge a single extract plan source into an Ibis plan.

    Returns
    -------
    IbisPlan
        Ibis plan registered on the backend.
    """
    return source_to_ibis(
        source,
        options=SourceToIbisOptions(
            ctx=ctx,
            backend=backend,
            name=name,
            overwrite=overwrite,
        ),
    )


def bridge_plan_bundle(
    plans: Mapping[str, ExtractPlanSource],
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name_prefix: str | None = None,
    overwrite: bool = True,
) -> dict[str, IbisPlan]:
    """Bridge a bundle of plan sources into Ibis plans.

    Returns
    -------
    dict[str, IbisPlan]
        Mapping of plan names to Ibis plans registered on the backend.
    """
    out: dict[str, IbisPlan] = {}
    base_options = SourceToIbisOptions(
        ctx=ctx,
        backend=backend,
        name=None,
        overwrite=overwrite,
    )
    for name, source in plans.items():
        view_name = f"{name_prefix}_{name}" if name_prefix else name
        out[name] = source_to_ibis(
            source,
            options=replace(base_options, name=view_name),
        )
    return out


__all__ = [
    "ExtractPlanSource",
    "bridge_plan",
    "bridge_plan_bundle",
]
