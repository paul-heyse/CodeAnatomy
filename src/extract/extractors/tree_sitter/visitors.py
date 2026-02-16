"""Tree-sitter visitor/query context contracts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from tree_sitter import Node, Parser, Range

from extract.coordination.context import FileContext
from extract.extractors.tree_sitter.cache import TreeSitterCache
from extract.extractors.tree_sitter.queries import TreeSitterQueryPack
from extract.extractors.tree_sitter.setup import TreeSitterExtractOptions


@dataclass
class _QueryStats:
    match_count: int = 0
    capture_count: int = 0
    match_limit_exceeded: bool = False


@dataclass
class _QueryRows:
    captures: list[dict[str, object]]
    defs: list[dict[str, object]]
    calls: list[dict[str, object]]
    imports: list[dict[str, object]]
    docstrings: list[dict[str, object]]
    stats: _QueryStats


@dataclass
class _ParseStats:
    parse_ms: int
    parse_timed_out: bool
    used_incremental: bool


@dataclass
class _NodeStats:
    node_count: int = 0
    named_count: int = 0
    error_count: int = 0
    missing_count: int = 0


@dataclass(frozen=True)
class _CaptureInfo:
    query_name: str
    capture_name: str
    pattern_index: int


@dataclass(frozen=True)
class _ImportInfo:
    kind: str
    module: str | None
    name: str | None
    asname: str | None
    alias_index: int | None
    level: int | None


@dataclass(frozen=True)
class _ParseContext:
    parser: Parser
    data: bytes | bytearray | memoryview
    cache: TreeSitterCache | None
    cache_key: str
    use_callback: bool


@dataclass(frozen=True)
class _QueryContext:
    root: Node
    data: bytes | bytearray | memoryview
    file_ctx: FileContext
    options: TreeSitterExtractOptions
    query_pack: TreeSitterQueryPack
    ranges: Sequence[Range]


__all__ = [
    "_CaptureInfo",
    "_ImportInfo",
    "_NodeStats",
    "_ParseContext",
    "_ParseStats",
    "_QueryContext",
    "_QueryRows",
    "_QueryStats",
]
