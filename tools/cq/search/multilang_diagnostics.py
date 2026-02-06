"""Cross-language diagnostics and capability matrix helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from tools.cq.core.schema import DetailPayload, Finding
from tools.cq.query.language import QueryLanguage, QueryLanguageScope, expand_language_scope
from tools.cq.search.contracts import (
    CapabilitySupport,
    CrossLanguageDiagnostic,
    LanguageCapabilities,
    diagnostics_to_dicts,
)

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
    "search": {"python": "full", "rust": "full"},
    "run_q_steps": {"python": "full", "rust": "full"},
}

_SUMMARY_CAPABILITY_FEATURES: dict[str, str] = {
    "entity_query": "entity:function",
    "pattern_query": "pattern_query",
    "search": "search",
    "run_q_steps": "run_q_steps",
    "calls_macro": "macro:calls",
    "impact_macro": "macro:impact",
    "sig_impact_macro": "macro:sig-impact",
    "scopes_macro": "macro:scopes",
    "bytecode_surface_macro": "macro:bytecode-surface",
}


def _code_for_feature(feature: str) -> str:
    # Deterministic short code with stable namespace.
    return f"ML_CAP_{feature.replace(':', '_').replace('-', '_').upper()}"


def _language_levels(feature: str) -> dict[QueryLanguage, CapabilityLevel]:
    return CAPABILITY_MATRIX.get(feature, {"python": "none", "rust": "none"})


def is_python_oriented_query_text(query: str) -> bool:
    """Return True when free-text query intent appears Python-specific."""
    lowered = query.lower()
    return any(hint in lowered for hint in _PYTHON_QUERY_HINTS)


def is_python_oriented_query_ir(query: Query) -> bool:
    """Return True when a parsed query requests Python-specific semantics."""
    if query.entity == "decorator":
        return True
    if any(exp.kind == "bytecode_surface" for exp in query.expand):
        return True
    if query.pattern_spec is not None:
        pattern = query.pattern_spec.pattern.lower()
        context = (query.pattern_spec.context or "").lower()
        return is_python_oriented_query_text(f"{pattern} {context}")
    return False


def diagnostic_payload_from_finding(finding: Finding) -> dict[str, object]:
    """Convert a diagnostic finding into canonical summary payload.

    Returns:
    -------
    dict[str, object]
        Canonical diagnostic payload mapping.
    """
    details = finding.details.data
    code = details.get("code")
    intent = details.get("intent")
    languages = details.get("languages")
    counts = details.get("counts")
    remediation = details.get("remediation")
    payload = CrossLanguageDiagnostic(
        code=str(code) if code is not None else "ML000",
        severity=finding.severity,
        message=finding.message,
        intent=str(intent) if intent is not None else "unspecified",
        languages=[str(item) for item in languages] if isinstance(languages, list) else [],
        counts={
            str(k): v
            for k, v in (counts.items() if isinstance(counts, dict) else ())
            if isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool)
        },
        remediation=str(remediation) if remediation is not None else "",
        feature=str(details.get("feature")) if details.get("feature") is not None else None,
        language=str(details.get("language")) if details.get("language") is not None else None,
        capability_level=(
            str(details.get("capability_level"))
            if details.get("capability_level") is not None
            else None
        ),
    )
    return diagnostics_to_dicts([payload])[0]


def diagnostics_to_summary_payload(findings: Sequence[Finding]) -> list[dict[str, object]]:
    """Convert diagnostic findings to canonical summary payload list.

    Returns:
    -------
    list[dict[str, object]]
        Ordered payload list ready for summary emission.
    """
    return [diagnostic_payload_from_finding(finding) for finding in findings]


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
        Cross-language diagnostics for user-visible reporting.
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
            message="Python-oriented query produced no Python matches; Rust matches were found.",
            severity="warning",
            details=DetailPayload(
                kind="cross_language_hint",
                data={
                    "code": "ML001",
                    "intent": "python_oriented",
                    "languages": ["python", "rust"],
                    "counts": {
                        "python_matches": python_matches,
                        "rust_matches": rust_matches,
                    },
                    "remediation": "Use --lang rust for Rust-only intent, or refine Python-specific filters.",
                },
            ),
        )
    ]


def build_capability_diagnostics(
    *,
    features: Sequence[str],
    lang_scope: QueryLanguageScope,
) -> list[Finding]:
    """Build diagnostics for features with limited language support.

    Returns:
    -------
    list[Finding]
        Capability diagnostics for the requested feature set.
    """
    if lang_scope not in {"auto", "python", "rust"}:
        return []
    diagnostics: list[Finding] = []
    languages: tuple[QueryLanguage, ...] = expand_language_scope(lang_scope)
    for feature in features:
        if feature not in CAPABILITY_MATRIX:
            continue
        levels = _language_levels(feature)
        for lang in languages:
            level = levels.get(lang, "none")
            if level == "full":
                continue
            message = (
                f"{feature} has partial support for {lang}"
                if level == "partial"
                else f"{feature} is not supported for {lang}"
            )
            diagnostics.append(
                Finding(
                    category="capability_limitation",
                    message=message,
                    severity="info",
                    details=DetailPayload(
                        kind="capability_limitation",
                        data={
                            "code": _code_for_feature(feature),
                            "intent": "python_only_feature"
                            if lang == "rust"
                            else "capability_limit",
                            "feature": feature,
                            "language": lang,
                            "languages": list(languages),
                            "counts": {},
                            "capability_level": level,
                            "remediation": "Narrow language scope or choose supported feature set.",
                        },
                    ),
                )
            )
    return diagnostics


def build_language_capabilities(
    *,
    lang_scope: QueryLanguageScope,
) -> dict[str, object]:
    """Build canonical language capability matrix for summary payloads.

    Returns:
    -------
    dict[str, object]
        Language capability summary for Python, Rust, and shared support.
    """
    _ = lang_scope
    python_caps: dict[str, CapabilitySupport] = {}
    rust_caps: dict[str, CapabilitySupport] = {}
    shared_caps: dict[str, bool] = {}
    for summary_key, feature_key in _SUMMARY_CAPABILITY_FEATURES.items():
        levels = _language_levels(feature_key)
        py_level = levels.get("python", "none")
        rs_level = levels.get("rust", "none")
        python_caps[summary_key] = CapabilitySupport(
            supported=py_level != "none",
            level=py_level,
        )
        rust_caps[summary_key] = CapabilitySupport(
            supported=rs_level != "none",
            level=rs_level,
        )
        shared_caps[summary_key] = py_level != "none" and rs_level != "none"
    from tools.cq.core.serialization import to_builtins

    capabilities = LanguageCapabilities(
        python=python_caps,
        rust=rust_caps,
        shared=shared_caps,
    )
    return dict(to_builtins(capabilities))


def features_from_query(query: Query) -> list[str]:
    """Extract feature keys from a parsed query for capability checking.

    Returns:
    -------
    list[str]
        Feature keys derived from the parsed query structure.
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

    Returns:
    -------
    list[str]
        Feature keys associated with the macro command.
    """
    return [f"macro:{macro_name}"]


__all__ = [
    "CAPABILITY_MATRIX",
    "CapabilityLevel",
    "build_capability_diagnostics",
    "build_cross_language_diagnostics",
    "build_language_capabilities",
    "diagnostic_payload_from_finding",
    "diagnostics_to_summary_payload",
    "features_from_macro",
    "features_from_query",
    "is_python_oriented_query_ir",
    "is_python_oriented_query_text",
]
