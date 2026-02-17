"""Parse-session lifecycle helpers for tree-sitter runtime."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.core.parse import ParseSession

if TYPE_CHECKING:
    from tree_sitter import Parser, Tree


__all__ = [
    "clear_parse_session",
    "create_parse_session",
    "parse_with_session",
    "session_stats",
]


def create_parse_session(parser_factory: Callable[[], Parser]) -> ParseSession:
    """Create a parse session with parser factory.

    Returns:
        ParseSession: New parse-session instance.
    """
    return ParseSession(parser_factory)


def parse_with_session(
    session: ParseSession,
    *,
    file_key: str | None,
    source_bytes: bytes,
) -> tuple[Tree | None, tuple[object, ...], bool]:
    """Parse one source payload using an existing session.

    Returns:
        tuple[Tree | None, tuple[object, ...], bool]: Parse output tuple from session.
    """
    return session.parse(file_key=file_key, source_bytes=source_bytes)


def clear_parse_session(session: ParseSession) -> None:
    """Reset cached session entries and counters."""
    session.clear()


def session_stats(session: ParseSession) -> dict[str, int]:
    """Return plain-dict parse-session stats.

    Returns:
        dict[str, int]: Session cache and parse counters.
    """
    stats = session.stats()
    return {
        "entries": int(stats.entries),
        "cache_hits": int(stats.cache_hits),
        "cache_misses": int(stats.cache_misses),
        "parse_count": int(stats.parse_count),
        "reparse_count": int(stats.reparse_count),
        "edit_failures": int(stats.edit_failures),
    }
