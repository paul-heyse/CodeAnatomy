"""Incremental parsing helpers for tree-sitter extraction."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

from tree_sitter import Parser, Point, Range, Tree

from extract.coordination.line_offsets import LineOffsets


@dataclass(frozen=True)
class InputEdit:
    """Tree-sitter input edit derived from byte diffs."""

    start_byte: int
    old_end_byte: int
    new_end_byte: int
    start_point: Point
    old_end_point: Point
    new_end_point: Point


@dataclass(frozen=True)
class TreeSitterParseResult:
    """Incremental parse result container."""

    tree: Tree | None
    changed_ranges: tuple[Range, ...]
    used_incremental: bool


@dataclass(frozen=True)
class _CacheEntry:
    tree: Tree
    source: bytes


class TreeSitterCache:
    """Small LRU cache of parsed trees for incremental parsing."""

    def __init__(self, *, max_entries: int) -> None:
        if max_entries < 1:
            msg = "TreeSitterCache max_entries must be >= 1."
            raise ValueError(msg)
        self._max_entries = max_entries
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()

    def parse(
        self,
        *,
        parser: Parser,
        key: str,
        source: bytes,
    ) -> TreeSitterParseResult:
        """Parse source bytes with incremental cache support.

        Returns
        -------
        TreeSitterParseResult
            Parsed tree, changed ranges, and incremental usage flag.
        """
        entry = self._entries.get(key)
        if entry is None:
            tree = parser.parse(source)
            result = _parse_result(tree=tree, used_incremental=False)
            if tree is not None:
                self._store(key, tree=tree, source=source)
            return result
        if source == entry.source:
            self._entries.move_to_end(key)
            return TreeSitterParseResult(
                tree=entry.tree,
                changed_ranges=(),
                used_incremental=True,
            )
        edit = _compute_edit(entry.source, source)
        if edit is None:
            tree = parser.parse(source)
            result = _parse_result(tree=tree, used_incremental=True)
            if tree is not None:
                self._store(key, tree=tree, source=source)
            return result
        entry.tree.edit(
            edit.start_byte,
            edit.old_end_byte,
            edit.new_end_byte,
            edit.start_point,
            edit.old_end_point,
            edit.new_end_point,
        )
        new_tree = parser.parse(source, old_tree=entry.tree)
        if new_tree is None:
            return TreeSitterParseResult(tree=None, changed_ranges=(), used_incremental=True)
        changed = tuple(entry.tree.changed_ranges(new_tree))
        self._store(key, tree=new_tree, source=source)
        return TreeSitterParseResult(
            tree=new_tree,
            changed_ranges=changed,
            used_incremental=True,
        )

    def _store(self, key: str, *, tree: Tree, source: bytes) -> None:
        self._entries[key] = _CacheEntry(tree=tree, source=source)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


def _compute_edit(old: bytes, new: bytes) -> InputEdit | None:
    if old == new:
        return None
    prefix_len = _common_prefix_len(old, new)
    old_suffix_len = _common_suffix_len(old, new, prefix_len)
    old_end = len(old) - old_suffix_len
    new_end = len(new) - old_suffix_len
    old_offsets = LineOffsets.from_bytes(old)
    new_offsets = LineOffsets.from_bytes(new)
    start_line0, start_col = old_offsets.point_from_byte(prefix_len)
    old_end_line0, old_end_col = old_offsets.point_from_byte(old_end)
    new_end_line0, new_end_col = new_offsets.point_from_byte(new_end)
    start_point = Point(start_line0, start_col)
    old_end_point = Point(old_end_line0, old_end_col)
    new_end_point = Point(new_end_line0, new_end_col)
    return InputEdit(
        start_byte=prefix_len,
        old_end_byte=old_end,
        new_end_byte=new_end,
        start_point=start_point,
        old_end_point=old_end_point,
        new_end_point=new_end_point,
    )


def _common_prefix_len(left: bytes, right: bytes) -> int:
    limit = min(len(left), len(right))
    idx = 0
    while idx < limit and left[idx] == right[idx]:
        idx += 1
    return idx


def _common_suffix_len(left: bytes, right: bytes, prefix_len: int) -> int:
    left_idx = len(left) - 1
    right_idx = len(right) - 1
    count = 0
    while left_idx >= prefix_len and right_idx >= prefix_len:
        if left[left_idx] != right[right_idx]:
            break
        left_idx -= 1
        right_idx -= 1
        count += 1
    return count


def _parse_result(tree: Tree | None, *, used_incremental: bool) -> TreeSitterParseResult:
    return TreeSitterParseResult(
        tree=tree,
        changed_ranges=(),
        used_incremental=used_incremental,
    )
