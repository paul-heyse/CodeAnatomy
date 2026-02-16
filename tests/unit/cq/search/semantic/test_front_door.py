"""Tests for semantic front-door helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.semantic import front_door as front_door_module
from tools.cq.search.semantic.models import LanguageSemanticEnrichmentRequest


def test_fail_open_sets_reason() -> None:
    """Test fail open sets reason."""
    outcome = front_door_module.fail_open("x")
    assert outcome.failure_reason == "x"


def test_enrich_semantics_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test enrich semantics delegates."""
    monkeypatch.setattr(
        front_door_module,
        "enrich_with_language_semantics",
        lambda _request: type(
            "_Outcome",
            (),
            {
                "payload": {"ok": True},
                "timed_out": False,
                "failure_reason": None,
            },
        )(),
    )
    request = LanguageSemanticEnrichmentRequest(
        language="python",
        mode="search",
        root=Path(),
        file_path=Path("x.py"),
        line=1,
        col=0,
    )
    outcome = front_door_module.enrich_semantics(request)
    assert outcome.payload == {"ok": True}
