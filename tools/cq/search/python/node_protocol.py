"""Protocol contracts for Python ast-grep node access."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol


class PythonNodePosition(Protocol):
    """Byte/line/column location for one node boundary."""

    @property
    def line(self) -> int:
        """Return 1-based source line for the node boundary."""
        ...

    @property
    def column(self) -> int:
        """Return 0-based source column for the node boundary."""
        ...

    @property
    def byte(self) -> int:
        """Return 0-based UTF-8 byte offset for the node boundary."""
        ...


class PythonNodeRange(Protocol):
    """Source range contract returned by ``PythonNodeAccess.range()``."""

    @property
    def start(self) -> PythonNodePosition:
        """Return start boundary position."""
        ...

    @property
    def end(self) -> PythonNodePosition:
        """Return end boundary position."""
        ...


class PythonNodeAccess(Protocol):
    """Structural access protocol for Python lane node operations."""

    def kind(self) -> str:
        """Return tree-sitter node kind."""
        ...

    def text(self) -> str:
        """Return node source text."""
        ...

    def field(self, name: str) -> PythonNodeAccess | None:
        """Return named child field when present."""
        ...

    def parent(self) -> PythonNodeAccess | None:
        """Return parent node when present."""
        ...

    def children(self) -> Iterable[PythonNodeAccess]:
        """Iterate over child nodes."""
        ...

    def is_named(self) -> bool:
        """Return whether node is a named syntax node."""
        ...

    def range(self) -> PythonNodeRange:
        """Return source range span for the node."""
        ...


__all__ = ["PythonNodeAccess", "PythonNodePosition", "PythonNodeRange"]
