"""Streaming parser helpers for tree-sitter sources."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from tree_sitter import Tree


def build_stream_reader(
    source_bytes: bytes,
    *,
    chunk_size: int = 64 * 1024,
) -> Callable[[int, tuple[int, int]], bytes]:
    """Build callback reader compatible with tree-sitter parser.parse.

    Returns:
        Callable[[int, tuple[int, int]], bytes]: Streaming parse callback reader.
    """
    safe_chunk_size = max(1, int(chunk_size))
    source_len = len(source_bytes)

    def _reader(byte_offset: int, _point: tuple[int, int]) -> bytes:
        if byte_offset < 0 or byte_offset >= source_len:
            return b""
        end = min(source_len, byte_offset + safe_chunk_size)
        return source_bytes[byte_offset:end]

    return _reader


def parse_streaming_source(
    parser: object,
    source_bytes: bytes,
    *,
    old_tree: Tree | None = None,
    chunk_size: int = 64 * 1024,
) -> Tree | None:
    """Parse via streaming callback with fail-open bytes fallback.

    Returns:
        Tree | None: Parsed syntax tree when parsing succeeds.
    """
    parser_any = cast("Any", parser)
    reader = build_stream_reader(source_bytes, chunk_size=chunk_size)
    try:
        return cast("Tree | None", parser_any.parse(reader, old_tree=old_tree, encoding="utf8"))
    except TypeError:
        pass
    try:
        return cast("Tree | None", parser_any.parse(reader, old_tree=old_tree))
    except TypeError:
        pass
    if old_tree is None:
        return cast("Tree | None", parser_any.parse(source_bytes))
    return cast("Tree | None", parser_any.parse(source_bytes, old_tree=old_tree))


__all__ = ["build_stream_reader", "parse_streaming_source"]
