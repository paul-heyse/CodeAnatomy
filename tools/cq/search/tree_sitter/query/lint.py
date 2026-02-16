"""Tree-sitter query-pack lint helpers for CQ search."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.query_models import (
    lint_query_pack_source,
    load_pack_rules,
)
from tools.cq.search.tree_sitter.core.language_registry import (
    load_language_registry,
    load_tree_sitter_language,
)
from tools.cq.search.tree_sitter.query.drift import build_grammar_drift_report
from tools.cq.search.tree_sitter.query.registry import (
    QueryPackSourceV1,
    load_query_pack_sources,
)
from tools.cq.search.tree_sitter.schema.node_schema import (
    GrammarSchemaIndex,
    build_schema_index,
    load_grammar_schema,
)

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


def _missing_required_metadata(
    settings: dict[str, object],
    required: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(key for key in required if key not in settings)


def _contract_errors(
    *,
    language: str,
    pack_name: str,
    source_text: str,
    compile_query: Callable[[str], Query],
) -> list[str]:
    rules = load_pack_rules(language)
    if not rules.required_metadata_keys:
        return []
    try:
        query = compile_query(source_text)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return []
    settings_for_pattern = getattr(query, "pattern_settings", None)
    if not callable(settings_for_pattern):
        return []
    pattern_count = int(getattr(query, "pattern_count", 0))
    errors: list[str] = []
    for pattern_idx in range(pattern_count):
        raw_settings = settings_for_pattern(pattern_idx)
        settings = raw_settings if isinstance(raw_settings, dict) else {}
        missing = _missing_required_metadata(settings, rules.required_metadata_keys)
        errors.extend(
            f"{language}:{pack_name}:missing_required_metadata:pattern={pattern_idx}:{key}"
            for key in missing
        )
    return errors


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
        if isinstance(source_text, str):
            errors.extend(
                _contract_errors(
                    language=language,
                    pack_name=pack_name,
                    source_text=source_text,
                    compile_query=compile_query,
                )
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
    try:
        sources = load_query_pack_sources(language, include_distribution=False)
    except (RuntimeError, TypeError, ValueError):
        return (f"{language}:query_registry_unavailable:failed to load query pack sources",)

    language_obj = load_tree_sitter_language(language)
    if not sources or language_obj is None:
        return ()
    drift_report = build_grammar_drift_report(language=language, query_sources=sources)
    language_runtime = cast("Language", language_obj)

    def compile_for_language(query_source: str) -> Query:
        return _compile_query(language_runtime, query_source)

    lint_errors = _lint_pack_sources(
        language=language,
        sources=sources,
        schema_index=schema_index,
        compile_query=compile_for_language,
    )
    if not drift_report.compatible:
        lint_errors.extend(f"{language}:grammar_drift:{error}" for error in drift_report.errors)
    return tuple(lint_errors)


def lint_search_query_packs() -> QueryPackLintResultV1:
    """Run query-pack lint checks used by search enrichment.

    Returns:
        QueryPackLintResultV1: Lint status and tuple of errors.
    """
    errors = (*_lint_language("python"), *_lint_language("rust"))
    ordered_errors = tuple(sorted(errors))
    if ordered_errors:
        return QueryPackLintResultV1(status="failed", errors=ordered_errors)
    return QueryPackLintResultV1(status="ok", errors=())


__all__ = ["QueryPackLintResultV1", "lint_search_query_packs"]
