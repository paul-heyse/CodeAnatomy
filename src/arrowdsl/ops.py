"""Plan-lane and kernel-lane operation protocols."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from arrowdsl.pyarrow_protocols import DeclarationLike, TableLike
from arrowdsl.runtime import ExecutionContext, Ordering, OrderingEffect


class PlanOp(Protocol):
    """Protocol for Acero plan operations."""

    @property
    def ordering_effect(self) -> OrderingEffect:
        """Return the ordering effect for this operation."""
        ...

    @property
    def is_pipeline_breaker(self) -> bool:
        """Return whether the operation is a pipeline breaker."""
        ...

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build an Acero declaration for this operation."""
        ...

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Return the updated ordering metadata for this operation."""
        ...


@runtime_checkable
class KernelOp(Protocol):
    """Protocol for kernel-lane operations on tables."""

    @property
    def ordering_effect(self) -> OrderingEffect:
        """Return the ordering effect for this operation."""
        ...

    def apply(self, table: TableLike, ctx: ExecutionContext) -> TableLike:
        """Apply the operation to a table."""
        ...

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Return the updated ordering metadata for this operation."""
        ...
