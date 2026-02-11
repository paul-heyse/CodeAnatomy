# ruff: noqa: D102, D107, DOC201, TID252
"""Dynamic dispatch fixture module."""

from __future__ import annotations

from .models import BuildContext
from .services import ServiceRegistry


class DynamicRouter:
    """Router using getattr-based dispatch."""

    def __init__(self, registry: ServiceRegistry) -> None:
        self.registry = registry

    def dispatch(self, operation: str, ctx: BuildContext) -> str:
        handler = getattr(self, f"op_{operation}")
        return handler(ctx)

    def op_direct(self, ctx: BuildContext) -> str:
        candidate = self.registry.resolve_factory("direct")
        value = candidate(ctx)
        if isinstance(value, str):
            return value
        msg = "unexpected awaitable in op_direct"
        raise RuntimeError(msg)


def forwarding_adapter(
    router: DynamicRouter, operation: str, *args: object, **kwargs: object
) -> str:
    """Forward *args/**kwargs to dispatch target for query coverage."""
    return router.dispatch(operation, *args, **kwargs)
