"""Public API surface for hermetic Python fixture."""

from __future__ import annotations

from app.dispatch import DynamicRouter
from app.models import BuildContext
from app.services import AsyncService, ServiceRegistry
from app.services import resolve as service_resolve


# Intentional name collision with app.services.resolve.
def resolve(ctx: BuildContext) -> str:
    """Resolve API symbol identity for target ambiguity tests.

    Returns:
        str: Resolver output keyed by module/symbol/line.
    """
    return f"api:{ctx.module_name}:{ctx.symbol_name}:{ctx.line}"


def build_pipeline() -> tuple[ServiceRegistry, DynamicRouter, AsyncService]:
    """Build fixture pipeline components.

    Returns:
        tuple[ServiceRegistry, DynamicRouter, AsyncService]: Fixture pipeline parts.
    """
    events: list[str] = []

    def recorder(message: str) -> None:
        events.append(message)

    registry = ServiceRegistry()
    registry.register("direct", service_resolve)

    router = DynamicRouter(registry)
    service = AsyncService(name="alpha", value="resolved", on_emit=recorder)
    service.handle("payload")

    ctx = BuildContext(module_name="app.api", symbol_name="resolve", line=27)
    _ = resolve(ctx)
    _ = service_resolve(ctx)
    return registry, router, service
