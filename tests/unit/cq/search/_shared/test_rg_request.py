"""Tests for ripgrep request contract exports."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search._shared.rg_request import CandidateCollectionRequest, RgRunRequest
from tools.cq.search._shared.types import QueryMode, SearchLimits


def test_rg_run_request_to_settings_preserves_mode_and_patterns() -> None:
    """`RgRunRequest` should serialize to settings with stable fields."""
    request = RgRunRequest(
        root=Path(),
        pattern="target",
        mode=QueryMode.IDENTIFIER,
        lang_types=("py",),
        limits=SearchLimits(),
        include_globs=["src/**"],
        extra_patterns=("alt",),
    )

    settings = request.to_settings()
    assert settings.mode == "identifier"
    assert settings.extra_patterns == ("alt",)


def test_candidate_collection_request_construction() -> None:
    """Candidate collection request should retain target language."""
    request = CandidateCollectionRequest(
        root=Path(),
        pattern="target",
        mode=QueryMode.REGEX,
        limits=SearchLimits(),
        lang="python",
    )

    assert request.lang == "python"
