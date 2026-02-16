"""Tree-sitter extraction subsystem."""

from __future__ import annotations

from extract.extractors.tree_sitter.builders import (
    extract_ts,
    extract_ts_plans,
    extract_ts_tables,
)
from extract.extractors.tree_sitter.cache import (
    InputEdit,
    TreeSitterCache,
    TreeSitterParseResult,
)
from extract.extractors.tree_sitter.queries import (
    QuerySpec,
    TreeSitterQueryPack,
    compile_query_pack,
)
from extract.extractors.tree_sitter.setup import TreeSitterExtractOptions

__all__ = [
    "InputEdit",
    "QuerySpec",
    "TreeSitterCache",
    "TreeSitterExtractOptions",
    "TreeSitterParseResult",
    "TreeSitterQueryPack",
    "compile_query_pack",
    "extract_ts",
    "extract_ts_plans",
    "extract_ts_tables",
]
