# ruff: noqa: D102, D107, D401, DOC201, TID252
"""Service implementations with decorators and async flows."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable

from .models import BuildContext, Handler, Service


def traced(func: Callable[..., str]) -> Callable[..., str]:
    """Decorator used for decorator and callsite coverage."""

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> str:
        return func(*args, **kwargs)

    return wrapper


class AsyncService(Service[str], Handler):
    """Concrete implementation used by search/neighborhood tests."""

    def __init__(self, name: str, value: str, on_emit: Callable[[str], None]) -> None:
        super().__init__(name=name, value=value)
        self._on_emit = on_emit

    @traced
    def handle(self, payload: str) -> str:
        resolved = self.resolve()
        return f"{resolved}:{payload}"

    async def emit_async(self, payload: str) -> str:
        rendered = self.handle(payload)
        self._on_emit(rendered)
        return rendered


def resolve(ctx: BuildContext) -> str:
    """Resolver intentionally name-colliding with app.api.resolve."""
    return f"service:{ctx.module_name}:{ctx.symbol_name}:{ctx.line}"


class ServiceRegistry:
    """Registry for dynamic dispatch and reference tests."""

    def __init__(self) -> None:
        self._factories: dict[str, Callable[[BuildContext], Awaitable[str] | str]] = {}

    def register(self, name: str, factory: Callable[[BuildContext], Awaitable[str] | str]) -> None:
        self._factories[name] = factory

    def resolve_factory(self, name: str) -> Callable[[BuildContext], Awaitable[str] | str]:
        return self._factories[name]
