"""Tests for typed enrichment payload view contracts."""

from __future__ import annotations

from tools.cq.search.objects.payload_views import EnrichmentPayloadView


def test_payload_view_parses_nested_sections_with_defaults() -> None:
    """Payload views should parse nested sections and default missing fields."""
    raw = {
        "symbol_grounding": {
            "definition_targets": [{"name": "target", "line": 10}],
        },
        "resolution": {
            "qualified_name_candidates": [{"name": "pkg.target"}],
            "binding_candidates": [{"name": "target"}],
        },
        "agreement": {"status": "full", "sources": ["python_ast"]},
    }

    view = EnrichmentPayloadView.from_raw(raw)

    assert view.symbol_grounding.definition_targets[0]["name"] == "target"
    assert view.resolution.qualified_name_candidates[0]["name"] == "pkg.target"
    assert view.agreement.status == "full"
    assert view.structural.scope_chain == []
