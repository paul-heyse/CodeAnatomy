"""Canonical plan IR for ArrowDSL fallback compilation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from arrowdsl.ops.catalog import OpDef


@dataclass(frozen=True)
class OpNode:
    """Operation node with arguments and optional nested inputs."""

    name: str
    args: dict[str, object]
    inputs: tuple[PlanIR, ...] = ()


@dataclass(frozen=True)
class PlanIR:
    """Plan IR as a sequence of operations."""

    nodes: tuple[OpNode, ...]

    def validate(self, *, catalog: dict[str, OpDef]) -> None:
        """Validate that all operation names exist in the catalog.

        Raises
        ------
        ValueError
            Raised when an operation name is not in the catalog.
        """
        for node in self.nodes:
            if node.name not in catalog:
                msg = f"Unknown op: {node.name!r}."
                raise ValueError(msg)
            for input_plan in node.inputs:
                input_plan.validate(catalog=catalog)


__all__ = ["OpNode", "PlanIR"]
