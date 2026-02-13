"""Tests for SNB-to-CqResult renderer."""

from __future__ import annotations

from tools.cq.core.schema import mk_runmeta
from tools.cq.core.snb_schema import (
    BundleMetaV1,
    NeighborhoodGraphSummaryV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)
from tools.cq.neighborhood.snb_renderer import RenderSnbRequest, render_snb_result


def test_render_snb_result_emits_bundle_enrichment_evidence() -> None:
    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="abc123",
        subject=SemanticNodeRefV1(
            node_id="n1",
            kind="function",
            name="target",
            display_label="target",
            file_path="test.py",
        ),
        subject_label="target",
        meta=BundleMetaV1(
            tool="cq",
            workspace_root="/repo",
            lsp_servers=(
                {
                    "workspace_health": "ok",
                    "quiescent": True,
                    "position_encoding": "utf-16",
                },
            ),
        ),
        graph=NeighborhoodGraphSummaryV1(node_count=3, edge_count=2),
    )
    run = mk_runmeta(
        macro="neighborhood",
        argv=["cq", "neighborhood", "target"],
        root="/repo",
        started_ms=0.0,
        toolchain={"python": "3.13"},
    )

    result = render_snb_result(
        RenderSnbRequest(
            run=run,
            bundle=bundle,
            target="target",
            language="python",
            top_k=10,
            enable_lsp=True,
            lsp_env={
                "lsp_health": "ok",
                "lsp_quiescent": True,
                "lsp_position_encoding": "utf-16",
            },
        )
    )

    assert result.summary["bundle_id"] == "abc123"
    assert result.summary["lsp_health"] == "ok"
    assert len(result.evidence) == 1
    evidence = result.evidence[0]
    enrichment = evidence.details.get("enrichment")
    assert isinstance(enrichment, dict)
    assert enrichment["lsp_health"] == "ok"
    assert "neighborhood_bundle" in enrichment
