"""Shared tree-sitter query compilation helper with rooted/non-local validation."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.contracts.query_models import load_pack_rules
from tools.cq.search.tree_sitter.core.infrastructure import load_language
from tools.cq.search.tree_sitter.query.specialization import specialize_query

if TYPE_CHECKING:
    from tree_sitter import Query

try:
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterQuery = None


@lru_cache(maxsize=256)
def compile_query(
    *,
    language: str,
    pack_name: str,
    source: str,
    request_surface: str = "artifact",
    validate_rules: bool = True,
) -> Query:
    """Compile and specialize a query with shared rooted/non-local validation.

    Args:
        language: Target language (for example, ``python`` or ``rust``).
        pack_name: Query pack name (used for error messages).
        source: Query source text.
        request_surface: Specialization surface (artifact, diagnostic, terminal).
        validate_rules: Whether to enforce rooted/non-local validation rules.

    Returns:
        Query: Compiled and specialized query.

    Raises:
        RuntimeError: If tree-sitter bindings are unavailable.
        ValueError: If validation rules are violated.
    """
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)

    lang = load_language(language)
    query = _TreeSitterQuery(lang, source)

    if validate_rules:
        rules = load_pack_rules(language)
        pattern_count = int(getattr(query, "pattern_count", 0))
        for pattern_idx in range(pattern_count):
            if rules.require_rooted and not bool(query.is_pattern_rooted(pattern_idx)):
                msg = f"{language} query pattern not rooted: {pack_name} pattern={pattern_idx}"
                raise ValueError(msg)
            if rules.forbid_non_local and bool(query.is_pattern_non_local(pattern_idx)):
                msg = f"{language} query pattern non-local: {pack_name} pattern={pattern_idx}"
                raise ValueError(msg)

    return specialize_query(query, request_surface=request_surface)


__all__ = ["compile_query"]
