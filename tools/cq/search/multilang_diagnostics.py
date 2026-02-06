"""Cross-language diagnostic helpers for unified CQ execution."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from tools.cq.core.schema import DetailPayload, Finding
from tools.cq.query.language import QueryLanguage, QueryLanguageScope, expand_language_scope

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

    Returns:
    -------
    bool
        True when the query contains Python-specific signals.
    """
    lowered = query.lower()
    return any(hint in lowered for hint in _PYTHON_QUERY_HINTS)


def is_python_oriented_query_ir(query: Query) -> bool:
    """Return True when a parsed query requests Python-specific semantics.

    Returns:
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

    Returns:
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


CapabilityLevel = Literal["full", "partial", "none"]


CAPABILITY_MATRIX: dict[str, dict[QueryLanguage, CapabilityLevel]] = {
    "entity:function": {"python": "full", "rust": "full"},
    "entity:class": {"python": "full", "rust": "full"},
    "entity:import": {"python": "full", "rust": "full"},
    "entity:callsite": {"python": "full", "rust": "full"},
    "entity:method": {"python": "full", "rust": "full"},
    "entity:module": {"python": "none", "rust": "partial"},
    "entity:decorator": {"python": "full", "rust": "none"},
    "pattern_query": {"python": "full", "rust": "full"},
    "scope_filter": {"python": "full", "rust": "none"},
    "bytecode_filter": {"python": "full", "rust": "none"},
    "expand:callers": {"python": "full", "rust": "none"},
    "expand:callees": {"python": "full", "rust": "none"},
    "macro:calls": {"python": "full", "rust": "partial"},
    "macro:impact": {"python": "full", "rust": "none"},
    "macro:sig-impact": {"python": "full", "rust": "none"},
    "macro:imports": {"python": "full", "rust": "none"},
    "macro:exceptions": {"python": "full", "rust": "none"},
    "macro:side-effects": {"python": "full", "rust": "none"},
    "macro:scopes": {"python": "full", "rust": "none"},
    "macro:bytecode-surface": {"python": "full", "rust": "none"},
}


def build_capability_diagnostics(
    *,
    features: Sequence[str],
    lang_scope: QueryLanguageScope,
) -> list[Finding]:
    """Build structured diagnostics for features with limited language support.

    Args:
        features: Feature keys to check (e.g. ``"entity:function"``, ``"scope_filter"``).
        lang_scope: Requested language scope.

    Returns:
        Diagnostic findings for capability limitations.
    """
    if lang_scope not in {"auto", "python", "rust"}:
        return []
    diagnostics: list[Finding] = []
    languages: tuple[QueryLanguage, ...] = expand_language_scope(lang_scope)
    for feature in features:
        caps = CAPABILITY_MATRIX.get(feature)
        if caps is None:
            continue
        diagnostics.extend(
            Finding(
                category="capability_limitation",
                message=(
                    f"{feature} has {level} support for {lang}"
                    if level == "partial"
                    else f"{feature} is not supported for {lang}"
                ),
                severity="info",
                details=DetailPayload(
                    kind="capability_limitation",
                    data={
                        "feature": feature,
                        "language": lang,
                        "capability_level": level,
                    },
                ),
            )
            for lang in languages
            if (level := caps.get(lang, "none")) != "full"
        )
    return diagnostics


def features_from_query(query: Query) -> list[str]:
    """Extract feature keys from a parsed query for capability checking.

    Args:
        query: Parsed query IR.

    Returns:
        Feature keys referenced by the query.
    """
    features: list[str] = []
    if query.entity is not None:
        features.append(f"entity:{query.entity}")
    if query.is_pattern_query:
        features.append("pattern_query")
    if query.scope_filter is not None:
        features.append("scope_filter")
    features.extend(f"expand:{exp.kind}" for exp in query.expand)
    if any(exp.kind == "bytecode_surface" for exp in query.expand):
        features.append("bytecode_filter")
    return features


def features_from_macro(macro_name: str) -> list[str]:
    """Extract feature keys for a macro command.

    Args:
        macro_name: Macro command name (e.g. ``"calls"``, ``"impact"``).

    Returns:
        Feature keys for the macro.
    """
    return [f"macro:{macro_name}"]


__all__ = [
    "CAPABILITY_MATRIX",
    "CapabilityLevel",
    "build_capability_diagnostics",
    "build_cross_language_diagnostics",
    "features_from_macro",
    "features_from_query",
    "is_python_oriented_query_ir",
    "is_python_oriented_query_text",
]
