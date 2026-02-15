"""Tests for streaming source callback parser helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.stream_source import (
    build_stream_reader,
    parse_streaming_source,
)


class _Parser:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def parse(
        self,
        source: object,
        old_tree: object = None,
        encoding: str | None = None,
    ) -> str:
        self.calls.append((source, old_tree, encoding))
        if callable(source):
            return "tree_from_stream"
        return "tree_from_bytes"


def test_build_stream_reader_chunks_data() -> None:
    reader = build_stream_reader(b"abcdef", chunk_size=2)
    assert reader(0, (0, 0)) == b"ab"
    assert reader(2, (0, 2)) == b"cd"
    assert reader(10, (0, 0)) == b""


def test_parse_streaming_source_prefers_stream_callback() -> None:
    parser = _Parser()
    tree = parse_streaming_source(parser, b"hello world")
    assert tree == "tree_from_stream"
    assert parser.calls
