"""Tree-sitter query-pack lint helpers for CQ search."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter_node_schema import build_schema_index, load_grammar_schema
from tools.cq.search.tree_sitter_query_contracts import lint_query_pack_source
from tools.cq.search.tree_sitter_query_registry import load_query_pack_sources

if TYPE_CHECKING:
    from tree_sitter import Language, Query

try:
    import tree_sitter_python as _tree_sitter_python
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None

try:
    import tree_sitter_rust as _tree_sitter_rust
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_rust = None

try:
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterLanguage = None
    _TreeSitterQuery = None


class QueryPackLintResultV1(CqStruct, frozen=True):
    """Lint result payload for search query packs."""

    status: str
    errors: tuple[str, ...] = ()


@lru_cache(maxsize=1)
def _python_language() -> Language | None:
    if _tree_sitter_python is None or _TreeSitterLanguage is None:
        return None
    return _TreeSitterLanguage(_tree_sitter_python.language())


@lru_cache(maxsize=1)
def _rust_language() -> Language | None:
    if _tree_sitter_rust is None or _TreeSitterLanguage is None:
        return None
    return _TreeSitterLanguage(_tree_sitter_rust.language())


def _compile_query(language: Language, source: str) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterQuery(language, source)


def _lint_language(language: str) -> tuple[str, ...]:
    schema = load_grammar_schema(language)
    if schema is None:
        return ()

    schema_index = build_schema_index(schema)
    sources = load_query_pack_sources(language, include_distribution=False)
    if not sources:
        return ()

    language_obj = _python_language() if language == "python" else _rust_language()
    if language_obj is None:
        return ()
    language_runtime: Language = language_obj

    def compile_for_language(query_source: str) -> Query:
        return _compile_query(language_runtime, query_source)

    errors: list[str] = []
    for source in sources:
        if not source.pack_name.endswith(".scm"):
            continue
        issues = lint_query_pack_source(
            language=language,
            pack_name=source.pack_name,
            source=source.source,
            schema_index=schema_index,
            compile_query=compile_for_language,
        )
        errors.extend(
            f"{issue.language}:{issue.pack_name}:{issue.code}:{issue.message}" for issue in issues
        )
    return tuple(errors)


def lint_search_query_packs() -> QueryPackLintResultV1:
    """Run query-pack lint checks used by search enrichment.

    Returns:
        QueryPackLintResultV1: Lint status and tuple of errors.
    """
    errors = (*_lint_language("python"), *_lint_language("rust"))
    if errors:
        return QueryPackLintResultV1(status="failed", errors=tuple(errors))
    return QueryPackLintResultV1(status="ok", errors=())


__all__ = ["QueryPackLintResultV1", "lint_search_query_packs"]
