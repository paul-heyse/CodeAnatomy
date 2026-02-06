"""Shared request structs for CQ core orchestration and summary builders."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from tools.cq.core.schema import CqResult, Finding, RunMeta
from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage, QueryLanguageScope
from tools.cq.search.contracts import CrossLanguageDiagnostic, LanguageCapabilities


class SummaryBuildRequest(CqStruct, frozen=True):
    """Input contract for canonical multilang summary assembly."""

    lang_scope: QueryLanguageScope
    languages: Mapping[QueryLanguage, Mapping[str, object]]
    common: Mapping[str, object] | None = None
    language_order: tuple[QueryLanguage, ...] | None = None
    cross_language_diagnostics: Sequence[
        CrossLanguageDiagnostic | Mapping[str, object]
    ] | None = None
    language_capabilities: LanguageCapabilities | Mapping[str, object] | None = None
    enrichment_telemetry: Mapping[str, object] | None = None


class MergeResultsRequest(CqStruct, frozen=True):
    """Input contract for multi-language CQ result merge."""

    scope: QueryLanguageScope
    results: Mapping[QueryLanguage, CqResult]
    run: RunMeta
    diagnostics: Sequence[Finding] | None = None
    diagnostic_payloads: Sequence[Mapping[str, object]] | None = None
    language_capabilities: Mapping[str, object] | None = None
    include_section_language_prefix: bool = True


__all__ = [
    "MergeResultsRequest",
    "SummaryBuildRequest",
]
