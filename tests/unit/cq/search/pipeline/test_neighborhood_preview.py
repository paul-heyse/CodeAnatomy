"""Tests for neighborhood preview extraction helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.neighborhood_preview import (
    build_tree_sitter_neighborhood_preview,
)
from tools.cq.search.pipeline.smart_search_types import _NeighborhoodPreviewInputs


def _config(root: Path) -> SearchConfig:
    return SearchConfig(
        root=root,
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
        with_neighborhood=False,
        argv=["search", "target"],
        started_ms=0.0,
    )


def test_neighborhood_preview_returns_disabled_note_when_not_opted_in(tmp_path: Path) -> None:
    """Neighborhood preview should short-circuit with explicit disabled note."""
    neighborhood, notes, summary_payload = build_tree_sitter_neighborhood_preview(
        ctx=_config(tmp_path),
        partition_results=[],
        sections=[],
        inputs=_NeighborhoodPreviewInputs(
            primary_target_finding=None,
            definition_matches=[],
            has_target_candidates=False,
        ),
    )

    assert neighborhood is None
    assert notes == ["tree_sitter_neighborhood_disabled_by_default"]
    assert summary_payload == {"enabled": False, "mode": "opt_in"}
