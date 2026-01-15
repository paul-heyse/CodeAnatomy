"""Typed accessors for plan.runner to avoid import cycles."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from arrowdsl.core.context import ExecutionContext
    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
    from arrowdsl.plan.plan import Plan
    from arrowdsl.schema.schema import SchemaMetadataSpec
    from ibis_engine.plan import IbisPlan


class AdapterRunOptionsProto(Protocol):
    """AdapterRunOptions signature surface used by plan adapters."""

    def __init__(self, **kwargs: object) -> None: ...


class PlanRunResultProto(Protocol):
    """PlanRunResult signature surface used by adapter wrappers."""

    def __init__(
        self,
        *,
        value: TableLike | RecordBatchReaderLike,
        kind: str,
    ) -> None: ...

    value: TableLike | RecordBatchReaderLike
    kind: str


class PlanRunnerModule(Protocol):
    """Typed module surface for arrowdsl.plan.runner."""

    AdapterRunOptions: type[AdapterRunOptionsProto]
    PlanRunResult: type[PlanRunResultProto]

    def run_plan(
        self,
        plan: Plan,
        *,
        ctx: ExecutionContext,
        prefer_reader: bool = False,
        metadata_spec: SchemaMetadataSpec | None = None,
        attach_ordering_metadata: bool = True,
    ) -> PlanRunResultProto:
        """Execute a plan and return a materialization-aware result."""
        ...

    def run_plan_adapter(
        self,
        plan: Plan | IbisPlan,
        *,
        ctx: ExecutionContext,
        options: AdapterRunOptionsProto | None = None,
    ) -> PlanRunResultProto:
        """Execute a plan or Ibis plan via the adapter runner."""
        ...

    def run_plan_bundle_adapter(
        self,
        plans: Mapping[str, Plan | IbisPlan],
        *,
        ctx: ExecutionContext,
        options: AdapterRunOptionsProto | None = None,
        metadata_specs: Mapping[str, SchemaMetadataSpec] | None = None,
    ) -> dict[str, TableLike | RecordBatchReaderLike]:
        """Execute a bundle of plans or Ibis plans via the adapter runner."""
        ...


def plan_runner_module() -> PlanRunnerModule:
    """Return the plan runner module with a typed surface.

    Returns
    -------
    PlanRunnerModule
        Typed view of the plan runner module.
    """
    module = importlib.import_module("arrowdsl.plan.runner")
    return cast("PlanRunnerModule", module)


__all__ = [
    "AdapterRunOptionsProto",
    "PlanRunResultProto",
    "PlanRunnerModule",
    "plan_runner_module",
]
