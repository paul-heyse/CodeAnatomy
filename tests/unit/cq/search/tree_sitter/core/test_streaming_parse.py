"""Tests for tree-sitter streaming parse helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.streaming_parse import (
    build_stream_reader,
    parse_streaming_source,
)


class _Parser:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def parse(self, source: object, **kwargs: object) -> object:
        self.calls.append((source, kwargs))
        if callable(source):
            msg = "callback unsupported"
            raise TypeError(msg)
        return {"source": source, "kwargs": kwargs}


def test_build_stream_reader_respects_chunk_boundaries() -> None:
    """Stream reader should return bounded chunks and empty past end."""
    reader = build_stream_reader(b"abcdef", chunk_size=2)

    assert reader(0, (0, 0)) == b"ab"
    assert reader(2, (0, 0)) == b"cd"
    assert reader(9, (0, 0)) == b""


def test_parse_streaming_source_falls_back_to_bytes_parse() -> None:
    """Streaming parser should fail-open to bytes-based parse when callback forms fail."""
    parser = _Parser()
    result = parse_streaming_source(parser, b"payload", old_tree=None)

    assert isinstance(result, dict)
    assert result["source"] == b"payload"
    assert result["kwargs"] == {}
