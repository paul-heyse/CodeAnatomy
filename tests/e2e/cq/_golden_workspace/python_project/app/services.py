"""Service implementations with decorators and async flows."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable

from app.models import BuildContext, Handler, Service


def traced(func: Callable[..., str]) -> Callable[..., str]:
    """Wrap a callable for decorator and callsite coverage.

    Returns:
        Callable[..., str]: Wrapped callable.
    """

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> str:
        return func(*args, **kwargs)

    return wrapper


class AsyncService(Service[str], Handler):
    """Concrete implementation used by search/neighborhood tests."""

    def __init__(self, name: str, value: str, on_emit: Callable[[str], None]) -> None:
        """Initialize async fixture service with callback emitter."""
        super().__init__(name=name, value=value)
        self._on_emit = on_emit

    @traced
    def handle(self, payload: str) -> str:
        """Resolve service value and append payload text.

        Returns:
            str: Rendered handler output.
        """
        resolved = self.resolve()
        return f"{resolved}:{payload}"

    async def emit_async(self, payload: str) -> str:
        """Emit asynchronously after handling payload.

        Returns:
            str: Emitted payload text.
        """
        rendered = self.handle(payload)
        self._on_emit(rendered)
        return rendered


def resolve(ctx: BuildContext) -> str:
    """Resolve service symbol identity with app.api.resolve name collision.

    Returns:
        str: Resolver output keyed by module/symbol/line.
    """
    return f"service:{ctx.module_name}:{ctx.symbol_name}:{ctx.line}"


class ServiceRegistry:
    """Registry for dynamic dispatch and reference tests."""

    def __init__(self) -> None:
        """Initialize empty registry of named service factories."""
        self._factories: dict[str, Callable[[BuildContext], Awaitable[str] | str]] = {}

    def register(self, name: str, factory: Callable[[BuildContext], Awaitable[str] | str]) -> None:
        """Register named factory for later dynamic resolution."""
        self._factories[name] = factory

    def resolve_factory(self, name: str) -> Callable[[BuildContext], Awaitable[str] | str]:
        """Resolve a factory by name.

        Returns:
            Callable[[BuildContext], Awaitable[str] | str]: Registered service factory.
        """
        return self._factories[name]
