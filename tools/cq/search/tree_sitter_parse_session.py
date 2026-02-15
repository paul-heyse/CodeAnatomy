"""Shared tree-sitter parse session for incremental request-scoped parsing."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_parse_contracts import (
    ParseSessionStatsV1,
    PointV1,
    TreeSitterInputEditV1,
)
from tools.cq.search.tree_sitter_parser_controls import (
    apply_parser_controls,
    parser_controls_from_env,
)
from tools.cq.search.tree_sitter_stream_source import parse_streaming_source

if TYPE_CHECKING:
    from tree_sitter import Parser, Tree


_LANGUAGE_SESSION_LOCK = threading.Lock()
_LF: int = 0x0A


@dataclass(slots=True)
class _ParseEntry:
    tree: Tree
    source_bytes: bytes


def _byte_offset_to_point(source_bytes: bytes, offset: int) -> PointV1:
    safe_offset = max(0, min(int(offset), len(source_bytes)))
    row = 0
    line_start = 0
    for idx, value in enumerate(source_bytes[:safe_offset]):
        if value == _LF:
            row += 1
            line_start = idx + 1
    return row, safe_offset - line_start


def _compute_input_edit(old_bytes: bytes, new_bytes: bytes) -> TreeSitterInputEditV1 | None:
    if old_bytes == new_bytes:
        return None
    old_len = len(old_bytes)
    new_len = len(new_bytes)
    prefix = 0
    max_prefix = min(old_len, new_len)
    while prefix < max_prefix and old_bytes[prefix] == new_bytes[prefix]:
        prefix += 1

    old_suffix = old_len
    new_suffix = new_len
    while old_suffix > prefix and new_suffix > prefix:
        if old_bytes[old_suffix - 1] != new_bytes[new_suffix - 1]:
            break
        old_suffix -= 1
        new_suffix -= 1

    start_byte = prefix
    old_end_byte = old_suffix
    new_end_byte = new_suffix
    return TreeSitterInputEditV1(
        start_byte=start_byte,
        old_end_byte=old_end_byte,
        new_end_byte=new_end_byte,
        start_point=_byte_offset_to_point(old_bytes, start_byte),
        old_end_point=_byte_offset_to_point(old_bytes, old_end_byte),
        new_end_point=_byte_offset_to_point(new_bytes, new_end_byte),
    )


def _changed_ranges(*, old_tree: Tree, new_tree: Tree) -> tuple[object, ...]:
    try:
        return tuple(old_tree.changed_ranges(new_tree))
    except (RuntimeError, TypeError, ValueError):
        return ()


class ParseSession:
    """Incremental parse session keyed by file cache key."""

    def __init__(self, parser_factory: Callable[[], Parser]) -> None:
        """Initialize incremental parse session state."""
        self._parser_factory = parser_factory
        self._entries: dict[str, _ParseEntry] = {}
        self._cache_hits = 0
        self._cache_misses = 0
        self._parse_count = 0
        self._reparse_count = 0
        self._edit_failures = 0
        self._lock = threading.Lock()

    def clear(self) -> None:
        """Clear all cached parse entries and counters."""
        with self._lock:
            self._entries.clear()
            self._cache_hits = 0
            self._cache_misses = 0
            self._parse_count = 0
            self._reparse_count = 0
            self._edit_failures = 0

    def stats(self) -> ParseSessionStatsV1:
        """Return current parse session counters.

        Returns:
            ParseSessionStatsV1: Snapshot of session counters.
        """
        with self._lock:
            return ParseSessionStatsV1(
                entries=len(self._entries),
                cache_hits=self._cache_hits,
                cache_misses=self._cache_misses,
                parse_count=self._parse_count,
                reparse_count=self._reparse_count,
                edit_failures=self._edit_failures,
            )

    def parse(
        self,
        *,
        file_key: str | None,
        source_bytes: bytes,
    ) -> tuple[Tree | None, tuple[object, ...], bool]:
        """Parse bytes and return incremental tree-sitter parse results.

        Returns:
            tuple[Tree | None, tuple[object, ...], bool]: ``(tree,
                changed_ranges, reused)`` tuple.
        """
        parser = self._parser_factory()
        apply_parser_controls(parser, parser_controls_from_env())
        if not file_key:
            return self._parse_uncached(parser=parser, source_bytes=source_bytes)

        with self._lock:
            entry = self._entries.get(file_key)
        if entry is None:
            return self._parse_initial(
                parser=parser,
                file_key=file_key,
                source_bytes=source_bytes,
            )

        if entry.source_bytes == source_bytes:
            return self._parse_unchanged(entry)
        return self._parse_with_edit(
            parser=parser,
            file_key=file_key,
            entry=entry,
            source_bytes=source_bytes,
        )

    def _parse_uncached(
        self,
        *,
        parser: Parser,
        source_bytes: bytes,
    ) -> tuple[Tree | None, tuple[object, ...], bool]:
        self._cache_misses += 1
        self._parse_count += 1
        tree = parse_streaming_source(parser, source_bytes)
        return tree, (), False

    def _parse_initial(
        self,
        *,
        parser: Parser,
        file_key: str,
        source_bytes: bytes,
    ) -> tuple[Tree | None, tuple[object, ...], bool]:
        tree = parse_streaming_source(parser, source_bytes)
        with self._lock:
            self._cache_misses += 1
            self._parse_count += 1
            if tree is not None:
                self._entries[file_key] = _ParseEntry(tree=tree, source_bytes=source_bytes)
        return tree, (), False

    def _parse_unchanged(
        self,
        entry: _ParseEntry,
    ) -> tuple[Tree | None, tuple[object, ...], bool]:
        with self._lock:
            self._cache_hits += 1
        return entry.tree, (), True

    def _parse_with_edit(
        self,
        *,
        parser: Parser,
        file_key: str,
        entry: _ParseEntry,
        source_bytes: bytes,
    ) -> tuple[Tree | None, tuple[object, ...], bool]:
        edit = _compute_input_edit(entry.source_bytes, source_bytes)
        if edit is None:
            return self._parse_replacement(
                parser=parser,
                file_key=file_key,
                source_bytes=source_bytes,
            )
        old_tree = entry.tree
        if not self._apply_edit(old_tree=old_tree, edit=edit):
            return self._parse_replacement(
                parser=parser,
                file_key=file_key,
                source_bytes=source_bytes,
            )

        new_tree = parse_streaming_source(parser, source_bytes, old_tree=old_tree)
        if new_tree is None:
            with self._lock:
                self._cache_misses += 1
                self._reparse_count += 1
            return None, (), True
        changed = _changed_ranges(old_tree=old_tree, new_tree=new_tree)
        with self._lock:
            self._cache_misses += 1
            self._reparse_count += 1
            self._entries[file_key] = _ParseEntry(tree=new_tree, source_bytes=source_bytes)
        return new_tree, changed, True

    def _parse_replacement(
        self,
        *,
        parser: Parser,
        file_key: str,
        source_bytes: bytes,
    ) -> tuple[Tree | None, tuple[object, ...], bool]:
        tree = parse_streaming_source(parser, source_bytes)
        with self._lock:
            self._cache_misses += 1
            self._reparse_count += 1
            if tree is not None:
                self._entries[file_key] = _ParseEntry(tree=tree, source_bytes=source_bytes)
        return tree, (), True

    def _apply_edit(self, *, old_tree: Tree, edit: TreeSitterInputEditV1) -> bool:
        try:
            old_tree.edit(
                edit.start_byte,
                edit.old_end_byte,
                edit.new_end_byte,
                edit.start_point,
                edit.old_end_point,
                edit.new_end_point,
            )
        except (RuntimeError, TypeError, ValueError):
            with self._lock:
                self._edit_failures += 1
            return False
        return True


_SESSIONS: dict[str, ParseSession] = {}


def get_parse_session(*, language: str, parser_factory: Callable[[], Parser]) -> ParseSession:
    """Return process-local parse session for one language lane.

    Returns:
        ParseSession: Session instance for the requested language.
    """
    with _LANGUAGE_SESSION_LOCK:
        existing = _SESSIONS.get(language)
        if existing is not None:
            return existing
        session = ParseSession(parser_factory)
        _SESSIONS[language] = session
        return session


def clear_parse_session(*, language: str | None = None) -> None:
    """Clear one language parse session or all sessions."""
    with _LANGUAGE_SESSION_LOCK:
        if language is None:
            sessions = list(_SESSIONS.values())
            _SESSIONS.clear()
        else:
            one = _SESSIONS.pop(language, None)
            sessions: list[ParseSession] = [one] if one is not None else []
    for session in sessions:
        session.clear()


__all__ = [
    "ParseSession",
    "clear_parse_session",
    "get_parse_session",
]
