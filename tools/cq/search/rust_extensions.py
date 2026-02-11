"""Rust-analyzer deep extension plane for CQ enrichment.

This module provides typed structures and stub functions for rust-analyzer
specific extensions (macro expansion, runnables). All functions are capability-gated
and fail-open.
"""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class RustMacroExpansionV1(CqStruct, frozen=True):
    """Result of rust-analyzer/expandMacro request.

    Parameters
    ----------
    name
        Macro name.
    expansion
        Expanded source text.
    expansion_byte_len
        Byte length of the expanded text.
    """

    name: str
    expansion: str
    expansion_byte_len: int = 0


class RustRunnableV1(CqStruct, frozen=True):
    """Normalized runnable from rust-analyzer/runnables request.

    Parameters
    ----------
    label
        Runnable label (e.g., "test my_test").
    kind
        Runnable kind: "cargo", "test", or "bench".

    Args:
        Cargo arguments for the runnable.
    location_uri
        File URI where runnable is defined.
    location_line
        Line number where runnable is defined (0-indexed).
    """

    label: str
    kind: str
    args: tuple[str, ...] = ()
    location_uri: str | None = None
    location_line: int = 0


def expand_macro(
    session: object,
    uri: str,
    line: int,
    col: int,
) -> RustMacroExpansionV1 | None:
    """Expand macro at position. Fail-open. Rust-analyzer specific.

    Parameters
    ----------
    session
        Rust LSP session (_RustLspSession).
    uri
        Document URI.
    line
        Line position (0-indexed).
    col
        Column position.

    Returns:
    -------
    RustMacroExpansionV1 | None
        Expanded macro, or None if unavailable or not a macro position.
    """


def get_runnables(
    session: object,
    uri: str,
) -> tuple[RustRunnableV1, ...]:
    """Get runnables for a file. Fail-open. Rust-analyzer specific.

    Parameters
    ----------
    session
        Rust LSP session (_RustLspSession).
    uri
        Document URI.

    Returns:
    -------
    tuple[RustRunnableV1, ...]
        Runnables for the file, or empty tuple if unavailable.
    """
    _ = session
    _ = uri
    return ()


__all__ = [
    "RustMacroExpansionV1",
    "RustRunnableV1",
    "expand_macro",
    "get_runnables",
]
