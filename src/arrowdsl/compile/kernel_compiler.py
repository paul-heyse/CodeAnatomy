"""Kernel compiler for ArrowDSL fallback execution."""

from __future__ import annotations

from arrowdsl.compute.kernels import resolve_kernel
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.ir.plan import PlanIR
from arrowdsl.ops.catalog import OpDef


class KernelCompiler:
    """Apply PlanIR nodes using kernel implementations."""

    def __init__(self, *, catalog: dict[str, OpDef]) -> None:
        self._catalog = catalog

    def apply(self, plan: PlanIR, *, table: TableLike, ctx: ExecutionContext) -> TableLike:
        """Apply kernel operations to a materialized table.

        Returns
        -------
        TableLike
            Table after applying kernel operations.

        Raises
        ------
        ValueError
            Raised when a kernel implementation is missing for an op.
        """
        plan.validate(catalog=self._catalog)
        out = table
        for node in plan.nodes:
            op_def = self._catalog[node.name]
            if op_def.kernel_name is None:
                msg = f"Op {node.name!r} has no kernel implementation."
                raise ValueError(msg)
            kernel = resolve_kernel(op_def.kernel_name, ctx=ctx)
            out = kernel(out, **node.args)
        return out


__all__ = ["KernelCompiler"]
