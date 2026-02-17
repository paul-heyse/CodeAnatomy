"""Tests for semantic provider protocol contracts."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.semantic.contracts import LanguageEnrichmentProvider
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
)


def _provider(
    *,
    request: LanguageSemanticEnrichmentRequest,
    context: object,
) -> LanguageSemanticEnrichmentOutcome:
    _ = context
    return LanguageSemanticEnrichmentOutcome(
        payload={"language": request.language},
        timed_out=False,
        failure_reason=None,
        provider_root=request.root,
    )


def test_language_enrichment_provider_protocol_shape() -> None:
    """Protocol should accept callable providers with request/context kwargs."""
    provider: LanguageEnrichmentProvider = _provider
    request = LanguageSemanticEnrichmentRequest(
        language="python",
        mode="search",
        root=Path(),
        file_path=Path("a.py"),
        line=1,
        col=0,
    )

    outcome = provider(request=request, context=object())

    assert outcome.payload == {"language": "python"}
    assert outcome.provider_root == Path()
