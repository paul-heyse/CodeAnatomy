"""Cross-language diagnostic helpers for unified CQ execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.core.schema import DetailPayload, Finding
from tools.cq.query.language import QueryLanguageScope

if TYPE_CHECKING:
    from tools.cq.query.ir import Query


_PYTHON_QUERY_HINTS: tuple[str, ...] = (
    "def ",
    "async def ",
    "class ",
    "decorator",
    "symtable",
    "bytecode",
)


def is_python_oriented_query_text(query: str) -> bool:
    """Return True when free-text query intent appears Python-specific.

    Returns
    -------
    bool
        True when the query contains Python-specific signals.
    """
    lowered = query.lower()
    return any(hint in lowered for hint in _PYTHON_QUERY_HINTS)


def is_python_oriented_query_ir(query: Query) -> bool:
    """Return True when a parsed query requests Python-specific semantics.

    Returns
    -------
    bool
        True when query intent appears Python-oriented.
    """
    if query.entity == "decorator":
        return True
    if any(exp.kind == "bytecode_surface" for exp in query.expand):
        return True
    if query.pattern_spec is not None:
        pattern = query.pattern_spec.pattern.lower()
        context = (query.pattern_spec.context or "").lower()
        return is_python_oriented_query_text(f"{pattern} {context}")
    return False


def build_cross_language_diagnostics(
    *,
    lang_scope: QueryLanguageScope,
    python_matches: int,
    rust_matches: int,
    python_oriented: bool,
) -> list[Finding]:
    """Build structured cross-language diagnostics for merged output.

    Returns
    -------
    list[Finding]
        Diagnostic findings for cross-language intent/result mismatches.
    """
    if lang_scope != "auto":
        return []
    if not python_oriented:
        return []
    if python_matches > 0 or rust_matches == 0:
        return []
    return [
        Finding(
            category="cross_language_hint",
            message=("Python-oriented query produced no Python matches; Rust matches were found."),
            severity="warning",
            details=DetailPayload(
                kind="cross_language_hint",
                data={
                    "python_matches": python_matches,
                    "rust_matches": rust_matches,
                },
            ),
        )
    ]


__all__ = [
    "build_cross_language_diagnostics",
    "is_python_oriented_query_ir",
    "is_python_oriented_query_text",
]
