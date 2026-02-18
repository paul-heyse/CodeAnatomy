"""Tests for incremental enrichment fact clusters."""

from __future__ import annotations

from tools.cq.core.fact_types import (
    resolve_fact_clusters,
    resolve_fact_context,
    resolve_primary_language_payload,
)


def test_incremental_fact_cluster_included() -> None:
    """Include incremental enrichment cluster when payload contains incremental data."""
    payload: dict[str, object] = {
        "language": "python",
        "python": {
            "incremental": {
                "mode": "ts_sym_dis",
                "sym": {
                    "scope_graph": {"tables_count": 2},
                    "binding_resolution": {"status": "resolved"},
                },
                "dis": {
                    "cfg": {"edges_n": 4, "exc_edges_n": 1},
                    "anchor_metrics": {"anchor_defs": 1, "anchor_uses": 2},
                },
            }
        },
    }
    language, lang_payload = resolve_primary_language_payload(payload)
    context = resolve_fact_context(language=language, language_payload=lang_payload)
    clusters = resolve_fact_clusters(context=context, language_payload=lang_payload)
    titles = [cluster.title for cluster in clusters]
    assert "Incremental Enrichment" in titles
