from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from tools.cq.search.tree_sitter_parse_session import ParseSession

if TYPE_CHECKING:
    from tree_sitter import Parser


def _fake_parser_factory() -> Parser:
    return cast("Parser", _FakeParser())


class _FakeTree:
    def __init__(self, source_bytes: bytes) -> None:
        self.source_bytes = source_bytes
        self.edits: list[tuple[int, int, int]] = []

    def edit(
        self,
        start_byte: int,
        old_end_byte: int,
        new_end_byte: int,
        _start_point: tuple[int, int],
        _old_end_point: tuple[int, int],
        _new_end_point: tuple[int, int],
    ) -> None:
        self.edits.append((start_byte, old_end_byte, new_end_byte))

    def changed_ranges(self, _new_tree: object) -> tuple[tuple[int, int], ...]:
        return ((0, 1),)


class _FakeParser:
    def parse(self, source_bytes: bytes, old_tree: object | None = None) -> _FakeTree | None:
        _ = old_tree
        return _FakeTree(source_bytes)


def test_parse_session_reuses_and_reparses_incrementally() -> None:
    session = ParseSession(cast("Callable[[], Parser]", _fake_parser_factory))

    tree1, changed1, reused1 = session.parse(file_key="a.py", source_bytes=b"x = 1\n")
    assert tree1 is not None
    assert changed1 == ()
    assert reused1 is False

    tree2, changed2, reused2 = session.parse(file_key="a.py", source_bytes=b"x = 1\n")
    assert tree2 is tree1
    assert changed2 == ()
    assert reused2 is True

    tree3, changed3, reused3 = session.parse(file_key="a.py", source_bytes=b"x = 2\n")
    assert tree3 is not None
    assert tree3 is not tree1
    assert changed3 == ((0, 1),)
    assert reused3 is True

    stats = session.stats()
    assert stats.entries == 1
    assert stats.cache_hits == 1
    assert stats.reparse_count >= 1
