"""Tree-sitter extraction subsystem.

This subpackage provides:
- Main extraction logic (extract)
- Incremental parsing cache (cache)
- Query pack compilation (queries)
"""

from __future__ import annotations

from extract.extractors.tree_sitter.cache import (
    InputEdit,
    TreeSitterCache,
    TreeSitterParseResult,
)
from extract.extractors.tree_sitter.extract import (
    TreeSitterExtractOptions,
    extract_ts,
    extract_ts_plans,
    extract_ts_tables,
)
from extract.extractors.tree_sitter.queries import (
    QuerySpec,
    TreeSitterQueryPack,
    compile_query_pack,
)

__all__ = [
    # cache
    "InputEdit",
    # queries
    "QuerySpec",
    "TreeSitterCache",
    "TreeSitterExtractOptions",
    "TreeSitterParseResult",
    "TreeSitterQueryPack",
    "compile_query_pack",
    # extract
    "extract_ts",
    "extract_ts_plans",
    "extract_ts_tables",
]
