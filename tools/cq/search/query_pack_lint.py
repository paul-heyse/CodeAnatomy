"""Tree-sitter query-pack lint helpers for CQ search."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter_grammar_drift import build_grammar_drift_report
from tools.cq.search.tree_sitter_language_registry import (
    load_language_registry,
    load_tree_sitter_language,
)
from tools.cq.search.tree_sitter_node_schema import (
    GrammarSchemaIndex,
    build_schema_index,
    load_grammar_schema,
)
from tools.cq.search.tree_sitter_query_contracts import lint_query_pack_source
from tools.cq.search.tree_sitter_query_registry import QueryPackSourceV1, load_query_pack_sources

if TYPE_CHECKING:
    from tree_sitter import Language, Query

try:
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterQuery = None


class QueryPackLintResultV1(CqStruct, frozen=True):
    """Lint result payload for search query packs."""

    status: str
    errors: tuple[str, ...] = ()


def _compile_query(language: Language, source: str) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterQuery(language, source)


def _predicate_pack_missing_pushdown(*, pack_name: str, source_text: str) -> bool:
    if not pack_name.endswith("70_predicate_filters.scm"):
        return False
    return not any(token in source_text for token in ("#match?", "#any-of?", "#eq?", "#cq-"))


def _lint_pack_sources(
    *,
    language: str,
    sources: tuple[QueryPackSourceV1, ...],
    schema_index: GrammarSchemaIndex,
    compile_query: Callable[[str], Query],
) -> list[str]:
    errors: list[str] = []
    for source in sources:
        pack_name = getattr(source, "pack_name", "")
        source_text = getattr(source, "source", "")
        if not isinstance(pack_name, str) or not pack_name.endswith(".scm"):
            continue
        if isinstance(source_text, str) and _predicate_pack_missing_pushdown(
            pack_name=pack_name,
            source_text=source_text,
        ):
            errors.append(
                f"{language}:{pack_name}:predicate_pack_missing_pushdown:missing predicate filters"
            )
        issues = lint_query_pack_source(
            language=language,
            pack_name=pack_name,
            source=source_text if isinstance(source_text, str) else "",
            schema_index=schema_index,
            compile_query=compile_query,
        )
        errors.extend(
            f"{issue.language}:{issue.pack_name}:{issue.code}:{issue.message}" for issue in issues
        )
    return errors


def _lint_language(language: str) -> tuple[str, ...]:
    registry = load_language_registry(language)
    if registry is None:
        return ()
    if not registry.node_kinds:
        return (f"{language}:registry:empty_node_kinds:language registry has no node kinds",)

    schema = load_grammar_schema(language)
    if schema is None:
        return ()

    schema_index = build_schema_index(schema)
    sources = load_query_pack_sources(language, include_distribution=False)
    if not sources:
        return ()
    drift_report = build_grammar_drift_report(language=language, query_sources=sources)

    language_obj = load_tree_sitter_language(language)
    if language_obj is None:
        return ()
    language_runtime = cast("Language", language_obj)

    def compile_for_language(query_source: str) -> Query:
        return _compile_query(language_runtime, query_source)

    errors = _lint_pack_sources(
        language=language,
        sources=sources,
        schema_index=schema_index,
        compile_query=compile_for_language,
    )
    if not drift_report.compatible:
        errors.extend(f"{language}:grammar_drift:{error}" for error in drift_report.errors)
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
