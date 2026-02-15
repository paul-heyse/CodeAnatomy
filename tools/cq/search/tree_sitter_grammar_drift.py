"""Grammar drift and query-pack compatibility checks."""

from __future__ import annotations

import hashlib

from tools.cq.search.tree_sitter_grammar_drift_contracts import GrammarDriftReportV1
from tools.cq.search.tree_sitter_node_schema import load_grammar_schema


def _digest(parts: list[str]) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def build_grammar_drift_report(
    *,
    language: str,
    query_sources: tuple[object, ...],
) -> GrammarDriftReportV1:
    """Build compatibility report between grammar schema and query-pack set.

    Returns:
        GrammarDriftReportV1: Compatibility status and digest metadata.
    """
    schema = load_grammar_schema(language)
    if schema is None:
        return GrammarDriftReportV1(
            language=language,
            grammar_digest="missing",
            query_digest=_digest([str(getattr(row, "pack_name", "")) for row in query_sources]),
            compatible=False,
            errors=("grammar_schema_unavailable",),
        )

    grammar_parts = [
        f"{row.type}:{int(row.named)}:{','.join(row.fields)}" for row in schema.node_types
    ]
    grammar_digest = _digest(grammar_parts)
    query_digest = _digest(
        [
            f"{getattr(row, 'pack_name', '')!s}:{len(str(getattr(row, 'source', '')))}"
            for row in query_sources
        ]
    )

    errors: list[str] = []
    if not query_sources:
        errors.append("query_pack_sources_empty")
    for row in query_sources:
        pack_name = str(getattr(row, "pack_name", ""))
        if not pack_name.endswith(".scm"):
            errors.append(f"invalid_pack_name:{pack_name}")
    return GrammarDriftReportV1(
        language=language,
        grammar_digest=grammar_digest,
        query_digest=query_digest,
        compatible=not errors,
        errors=tuple(errors),
    )


__all__ = ["build_grammar_drift_report"]
