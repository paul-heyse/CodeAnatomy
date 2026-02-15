"""Contracts for tree-sitter grammar/query-pack drift checks."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class GrammarDriftReportV1(CqStruct, frozen=True):
    """Compatibility report between grammar schema and query packs."""

    language: str
    grammar_digest: str
    query_digest: str
    compatible: bool = True
    errors: tuple[str, ...] = ()


__all__ = ["GrammarDriftReportV1"]
