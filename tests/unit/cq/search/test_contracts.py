"""Tests for typed multilang summary and diagnostics contracts."""

from __future__ import annotations

from tools.cq.core.contracts import SummaryBuildRequest
from tools.cq.core.multilang_summary import build_multilang_summary
from tools.cq.search._shared.search_contracts import (
    CrossLanguageDiagnostic,
    EnrichmentTelemetry,
    LanguageCapabilities,
    SearchSummaryContract,
    coerce_diagnostics,
    coerce_language_capabilities,
)


def test_search_summary_contract_preserves_required_keys() -> None:
    summary = build_multilang_summary(
        SummaryBuildRequest(
            common={"query": "build_graph"},
            lang_scope="auto",
            language_order=("python", "rust"),
            languages={
                "python": {"matches": 2, "files_scanned": 1},
                "rust": {"matches": 1, "files_scanned": 1},
            },
            cross_language_diagnostics=[],
            language_capabilities={},
        )
    )
    assert "lang_scope" in summary
    assert "language_order" in summary
    assert "languages" in summary
    assert "cross_language_diagnostics" in summary
    assert summary["cross_language_diagnostics"] == []
    assert "language_capabilities" in summary


def test_cross_language_diagnostic_coercion() -> None:
    rows = coerce_diagnostics(
        [
            {
                "code": "ML001",
                "severity": "warning",
                "message": "hint",
                "intent": "python_oriented",
                "languages": ["python", "rust"],
                "counts": {"python_matches": 0, "rust_matches": 3},
                "remediation": "refine query",
            }
        ]
    )
    assert rows
    assert isinstance(rows[0], CrossLanguageDiagnostic)
    assert rows[0].code == "ML001"
    assert rows[0].severity == "warning"
    assert rows[0].counts["rust_matches"] == 3


def test_language_capabilities_coercion() -> None:
    caps = coerce_language_capabilities(
        {
            "python": {"search": {"supported": True, "level": "full"}},
            "rust": {"search": {"supported": True, "level": "full"}},
            "shared": {"search": True},
        }
    )
    assert isinstance(caps, LanguageCapabilities)
    assert caps.python["search"].supported is True
    assert caps.rust["search"].level == "full"
    assert caps.shared["search"] is True


def test_search_summary_contract_accepts_enrichment_telemetry() -> None:
    contract = SearchSummaryContract(
        lang_scope="auto",
        language_order=["python", "rust"],
        languages={},
        enrichment_telemetry=EnrichmentTelemetry.from_mapping(
            {
                "python": {"applied": 1, "degraded": 0, "skipped": 0},
                "rust": {
                    "applied": 1,
                    "degraded": 0,
                    "skipped": 0,
                    "cache_hits": 2,
                },
            }
        ),
    )
    assert contract.enrichment_telemetry is not None
    assert contract.enrichment_telemetry.rust.cache_hits == 2
