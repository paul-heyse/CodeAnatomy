"""Dynamic dispatch fixture module."""

from __future__ import annotations

from app.models import BuildContext
from app.services import ServiceRegistry


class DynamicRouter:
    """Router using getattr-based dispatch."""

    def __init__(self, registry: ServiceRegistry) -> None:
        """Initialize router with service registry."""
        self.registry = registry

    def dispatch(self, operation: str, ctx: BuildContext) -> str:
        """Dispatch operation to the corresponding router method.

        Returns:
            str: Handler output for the operation.
        """
        handler = getattr(self, f"op_{operation}")
        return handler(ctx)

    def op_direct(self, ctx: BuildContext) -> str:
        """Resolve and execute direct-operation handler.

        Returns:
            str: Resolved direct-operation output.

        Raises:
            RuntimeError: If the selected factory unexpectedly returns an awaitable.
        """
        candidate = self.registry.resolve_factory("direct")
        value = candidate(ctx)
        if isinstance(value, str):
            return value
        msg = "unexpected awaitable in op_direct"
        raise RuntimeError(msg)


def forwarding_adapter(
    router: DynamicRouter,
    operation: str,
    ctx: BuildContext,
) -> str:
    """Forward a typed context to the dispatch target for query coverage.

    Returns:
        str: Router dispatch output.
    """
    return router.dispatch(operation, ctx)
