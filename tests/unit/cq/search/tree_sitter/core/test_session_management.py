"""Unit tests for tree-sitter session-management wrappers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from tools.cq.search.tree_sitter.core.parse import ParseSession
from tools.cq.search.tree_sitter.core.session_management import (
    clear_parse_session,
    create_parse_session,
    session_stats,
)

if TYPE_CHECKING:
    from tree_sitter import Parser


class _Parser:
    pass


def _parser_factory() -> _Parser:
    return _Parser()


def test_create_parse_session_returns_parse_session() -> None:
    """Session factory should return a ParseSession instance."""
    session = create_parse_session(cast("Callable[[], Parser]", _parser_factory))
    assert isinstance(session, ParseSession)


def test_clear_parse_session_resets_entries() -> None:
    """Clearing a session should reset tracked cache entry count."""
    session = create_parse_session(cast("Callable[[], Parser]", _parser_factory))
    clear_parse_session(session)
    stats = session_stats(session)
    assert stats["entries"] == 0
