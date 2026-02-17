"""Protocols for CLI runtime dependencies."""

from __future__ import annotations

from typing import Protocol


class ConsoleFilePort(Protocol):
    """Minimal writable stream surface used by CLI render paths."""

    def write(self, text: str) -> object:
        """Write text to the output stream."""
        ...

    def flush(self) -> object:
        """Flush buffered stream output."""
        ...


class ConsolePort(Protocol):
    """Minimal console protocol used by CQ CLI output paths."""

    @property
    def file(self) -> ConsoleFilePort:
        """Expose the console text stream used by rich rendering."""
        ...

    def print(self, *args: object, **kwargs: object) -> None:
        """Write one rendered console line."""
        ...


__all__ = ["ConsoleFilePort", "ConsolePort"]
