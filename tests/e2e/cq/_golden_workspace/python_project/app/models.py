# ruff: noqa: D107, DOC201, UP046
"""Domain models for hermetic CQ fixture."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class BuildContext:
    """Context object used by resolver functions."""

    module_name: str
    symbol_name: str
    line: int


class Handler(Protocol):
    """Protocol used for interface/class style queries."""

    def handle(self, payload: str) -> str:
        """Transform payload and return new value."""


class Service(Generic[T]):
    """Generic base service used by implementations."""

    def __init__(self, name: str, value: T) -> None:
        self.name = name
        self.value = value

    def resolve(self) -> T:
        """Resolve the service value."""
        return self.value
