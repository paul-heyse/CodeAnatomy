"""Tree-sitter extractor option setup."""

from __future__ import annotations

from dataclasses import dataclass

from extract.infrastructure.options import ParallelOptions, RepoOptions, WorklistQueueOptions


@dataclass(frozen=True)
class TreeSitterExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions):
    """Configure tree-sitter extraction options."""

    include_nodes: bool = True
    include_errors: bool = True
    include_missing: bool = True
    include_edges: bool = True
    include_captures: bool = True
    include_defs: bool = True
    include_calls: bool = True
    include_imports: bool = True
    include_docstrings: bool = True
    include_stats: bool = True
    extensions: tuple[str, ...] | None = None
    parser_timeout_micros: int | None = None
    query_match_limit: int = 10_000
    query_timeout_micros: int | None = None
    max_text_bytes: int = 256
    max_docstring_bytes: int = 2048
    incremental: bool = False
    incremental_cache_size: int = 256
    included_ranges: tuple[tuple[int, int], ...] | None = None
    parse_callback_threshold_bytes: int | None = 5_000_000
    parse_callback_chunk_size: int = 65_536


__all__ = ["TreeSitterExtractOptions"]
